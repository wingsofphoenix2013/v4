# bybit_sync.py — приватный WS-синк Bybit (read-only + обновление статусов ордеров): auth + wallet/position/order/execution + авто-reconnect
# + периодический REST-ресинк баланса и позиций (linear)
# + фильтрация шумных pong/ping логов и запись статусов в БД (trader_position_orders, агрегаты в trader_positions)
# + публикация событий для maintainer (гармонизация TP, post-TP SL, cleanup-after-flat) в Redis Stream

# 🔸 Импорты
import os
import hmac
import time
import json
import hashlib
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, Optional

import websockets
import httpx

from trader_infra import infra
from trader_config import config

# 🔸 Логгер
log = logging.getLogger("BYBIT_SYNC")

# 🔸 Конфиг (mainnet)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", "wss://stream.bybit.com/v5/private")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")          # мс
ACCOUNT_TYPE = os.getenv("BYBIT_ACCOUNT_TYPE", "UNIFIED")     # UNIFIED | CONTRACT | SPOT
CATEGORY = "linear"                                           # деривативы USDT-perp

PING_INTERVAL_SEC = 20.0
RECONNECT_DELAY_SEC = 3.0

# 🔸 Настройка шумных логов (по умолчанию пинги/понги тихо)
LOG_PONGS = os.getenv("BYBIT_LOG_PONGS", "false").lower() == "true"
LOG_PINGS = os.getenv("BYBIT_LOG_PINGS", "false").lower() == "true"

# 🔸 Stream для maintainer’а
MAINTAINER_STREAM = "trader_maintainer_events"

# 🔸 Публикация события для maintainer’а
async def _emit_maintainer_event(fields: Dict[str, str]) -> None:
    try:
        await infra.redis_client.xadd(MAINTAINER_STREAM, fields)
        log.debug("MAINT_EVT → %s: %s", MAINTAINER_STREAM, fields)
    except Exception:
        log.exception("❌ Не удалось опубликовать событие maintainer")


# 🔸 Основной цикл воркера приватного WS (держим канал + подписки)
async def run_bybit_private_ws_sync_loop():
    if not API_KEY or not API_SECRET:
        log.debug("BYBIT_SYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск запуска")
        return

    log.debug("BYBIT_SYNC: старт приватного WS-синка %s", WS_PRIVATE)

    while True:
        try:
            async with websockets.connect(WS_PRIVATE, ping_interval=None, close_timeout=5) as ws:
                # auth
                expires = int((time.time() + 5) * 1000)
                sign_payload = f"GET/realtime{expires}"
                signature = hmac.new(API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
                await ws.send(json.dumps({"op": "auth", "args": [API_KEY, expires, signature]}))
                auth_resp = json.loads(await ws.recv())
                log.debug("BYBIT_SYNC auth: %s", auth_resp)

                # подписки: wallet + position + order + execution
                await ws.send(json.dumps({"op": "subscribe", "args": ["wallet", "position", "order", "execution"]}))
                sub_resp = json.loads(await ws.recv())
                log.debug("BYBIT_SYNC subscribe ack: %s", sub_resp)

                # цикл чтения с таймаутом (для пингов)
                while True:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL_SEC)
                        await _handle_ws_message(msg_raw)
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"op": "ping"}))
                        if LOG_PINGS:
                            log.debug("BYBIT_SYNC → ping")
                        try:
                            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            await _handle_ws_message(pong_raw)
                        except asyncio.TimeoutError:
                            log.debug("BYBIT_SYNC: нет pong — переподключение")
                            raise ConnectionError("pong timeout")

        except Exception:
            log.exception("BYBIT_SYNC: сбой канала, переподключение через %.1fs", RECONNECT_DELAY_SEC)
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# 🔸 Обработка входящих WS-сообщений (логирование кратко + обновление БД)
async def _handle_ws_message(msg_raw: str):
    try:
        msg = json.loads(msg_raw)
    except Exception:
        log.debug("BYBIT_SYNC recv (raw): %s", msg_raw)
        return

    # служебные op без topic
    if "op" in msg and "topic" not in msg:
        op = str(msg.get("op") or "").lower()
        if op == "pong":
            if LOG_PONGS:
                log.debug("BYBIT_SYNC recv pong")
            return
        if op == "ping":
            if LOG_PINGS:
                log.debug("BYBIT_SYNC recv ping")
            return
        if op in ("auth", "subscribe"):
            log.debug("BYBIT_SYNC recv op: %s", msg)
            return
        log.debug("BYBIT_SYNC recv op: %s", msg)
        return

    # топиковые события
    topic = msg.get("topic")
    data = msg.get("data")
    ts = msg.get("ts")

    items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])

    if topic == "wallet":
        head = items[0] if items else {}
        log.debug("BYBIT_SYNC wallet: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "position":
        head = items[0] if items else {}
        log.debug("BYBIT_SYNC position: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "order":
        await _handle_order_topic(items, ts)
        return

    if topic == "execution":
        await _handle_execution_topic(items, ts)
        return

    log.debug("BYBIT_SYNC recv topic=%s: %s", topic, msg)


# 🔸 Обработка топика 'order' — статусы ордеров
async def _handle_order_topic(items: list, ts: Any):
    head = items[0] if items else {}
    log.debug("BYBIT_SYNC order: items=%d head_keys=%s ts=%s", len(items), list(head.keys()) if head else [], ts)

    for it in items:
        try:
            order_link_id = _as_str(it.get("orderLinkId"))
            order_id = _as_str(it.get("orderId"))
            symbol_ws = _as_str(it.get("symbol"))
            order_status_raw = _as_str(it.get("orderStatus"))
            ext_status = _map_order_status(order_status_raw)

            price_ws = _as_decimal(it.get("price"))  # лимитная цена (если есть)
            qty_ws = _as_decimal(it.get("qty"))      # размер ордера (если есть)

            # агрегаты (могут отсутствовать)
            cum_exec_qty = _as_decimal(it.get("cumExecQty"))
            avg_price = _as_decimal(it.get("avgPrice"))
            updated_ms = _as_int(it.get("updatedTime")) or _as_int(it.get("updatedTimeNs"))
            updated_at = _ts_from_ms(updated_ms) if updated_ms else None

            # найдём нашу запись ордера
            tpo = await _find_tpo(order_link_id, order_id)
            if not tpo:
                log.debug("BYBIT_SYNC order: неизвестный ордер (linkId=%s orderId=%s) — пропуск", order_link_id, order_id)
                continue

            tpo_id = tpo["id"]
            position_uid = tpo["position_uid"]
            kind = _as_str(tpo["kind"])

            # обновим tpo
            await _update_tpo_on_order_event(
                tpo_id=tpo_id,
                order_id=order_id or None,
                ext_status=ext_status,
                filled_qty=cum_exec_qty,
                avg_fill_price=avg_price,
                exec_fee=None,
                last_ts=updated_at
            )

            # entry → зеркалим агрегаты в trader_positions
            if kind == "entry":
                await _update_trader_positions_entry(
                    position_uid=position_uid,
                    exchange="BYBIT",
                    order_link_id=order_link_id or None,
                    order_id=order_id or None,
                    ext_status=ext_status,
                    filled_qty=cum_exec_qty,
                    avg_fill_price=avg_price,
                    exec_fee=None,
                    last_ts=updated_at
                )

            # гармонизация TP: kind='tp' и у нас в TPO есть целевые price/qty
            if kind == "tp":
                tpo_price = _as_decimal(tpo.get("price"))
                tpo_qty = _as_decimal(tpo.get("qty"))
                level = _as_int(tpo.get("level"))
                symbol = _as_str(tpo.get("symbol") or symbol_ws)

                if symbol and tpo_price is not None and tpo_qty is not None:
                    ticksize = _as_decimal((config.tickers.get(symbol) or {}).get("ticksize"))
                    precision_qty = (config.tickers.get(symbol) or {}).get("precision_qty")

                    need_price = _round_price(tpo_price, ticksize)
                    need_qty = _round_qty(tpo_qty, precision_qty)

                    ex_price = _round_price(price_ws, ticksize) if price_ws is not None else None
                    ex_qty = _round_qty(qty_ws, precision_qty) if qty_ws is not None else None

                    bad_status = (ext_status in ("rejected", "expired", "canceled"))
                    mismatch = (
                        (ex_price is not None and need_price is not None and ex_price != need_price) or
                        (ex_qty is not None and need_qty is not None and ex_qty != need_qty)
                    )

                    if bad_status or mismatch:
                        sid_row = await infra.pg_pool.fetchrow(
                            "SELECT strategy_id FROM public.trader_positions WHERE position_uid = $1",
                            position_uid
                        )
                        sid_val = int(sid_row["strategy_id"]) if sid_row and sid_row["strategy_id"] is not None else None
                        if sid_val is not None:
                            await _emit_maintainer_event({
                                "type": "tp_harmonize_needed",
                                "position_uid": position_uid,
                                "strategy_id": str(sid_val),
                                "order_link_id": order_link_id or (tpo.get("order_link_id") or ""),
                                "level": str(level if level is not None else ""),
                                "ex_price": str(ex_price) if ex_price is not None else "",
                                "ex_qty": str(ex_qty) if ex_qty is not None else "",
                                "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
                                "dedupe": f"{position_uid}:tp:{level}:harmonize",
                            })

        except Exception:
            log.exception("BYBIT_SYNC order: ошибка обработки элемента: %s", it)


# 🔸 Обработка топика 'execution' — сделки (fills) по ордерам
async def _handle_execution_topic(items: list, ts: Any):
    head = items[0] if items else {}
    log.debug("BYBIT_SYNC execution: items=%d head_keys=%s ts=%s", len(items), list(head.keys()) if head else [], ts)

    for it in items:
        try:
            order_link_id = _as_str(it.get("orderLinkId"))
            order_id = _as_str(it.get("orderId"))
            exec_qty = _as_decimal(it.get("execQty")) or Decimal("0")
            exec_price = _as_decimal(it.get("execPrice"))
            exec_fee = _as_decimal(it.get("execFee")) or Decimal("0")
            exec_time_ms = _as_int(it.get("execTime")) or _as_int(it.get("execTimeNs"))
            exec_at = _ts_from_ms(exec_time_ms) if exec_time_ms else None

            if exec_qty <= 0:
                continue

            # найдём наш ордер
            tpo = await _find_tpo(order_link_id, order_id)
            if not tpo:
                log.debug("BYBIT_SYNC execution: неизвестный ордер (linkId=%s orderId=%s) — пропуск", order_link_id, order_id)
                continue

            tpo_id = tpo["id"]
            position_uid = tpo["position_uid"]
            kind = _as_str(tpo["kind"])
            prev_filled = _as_decimal(tpo.get("filled_qty")) or Decimal("0")
            prev_avg = _as_decimal(tpo.get("avg_fill_price")) or None
            prev_fee = _as_decimal(tpo.get("exec_fee")) or Decimal("0")

            # пересчёт средневзвешенной цены
            new_filled = prev_filled + exec_qty
            new_avg = exec_price if not prev_avg or prev_filled == 0 else ((prev_avg * prev_filled) + (exec_price * exec_qty)) / new_filled
            new_fee = prev_fee + exec_fee

            # обновим tpo
            await _update_tpo_on_execution(
                tpo_id=tpo_id,
                filled_qty=new_filled,
                avg_fill_price=new_avg,
                exec_fee=new_fee,
                last_ts=exec_at
            )

            # entry → зеркалим агрегаты
            if kind == "entry":
                await _update_trader_positions_entry(
                    position_uid=position_uid,
                    exchange="BYBIT",
                    order_link_id=order_link_id or None,
                    order_id=order_id or None,
                    ext_status=None,
                    filled_qty=new_filled,
                    avg_fill_price=new_avg,
                    exec_fee=new_fee,
                    last_ts=exec_at
                )

            # TP filled → post-TP SL
            if kind == "tp":
                tpo_row = await infra.pg_pool.fetchrow(
                    "SELECT qty, \"level\" FROM public.trader_position_orders WHERE id = $1",
                    tpo_id
                )
                if tpo_row:
                    level = _as_int(tpo_row["level"])
                    qty_target = _as_decimal(tpo_row["qty"]) or Decimal("0")
                    if new_filled >= qty_target and qty_target > 0:
                        left_qty = await _calc_left_qty_for_uid(position_uid)
                        if left_qty and left_qty > 0:
                            sid_row = await infra.pg_pool.fetchrow(
                                "SELECT strategy_id FROM public.trader_positions WHERE position_uid = $1",
                                position_uid
                            )
                            sid_val = int(sid_row["strategy_id"]) if sid_row and sid_row["strategy_id"] is not None else None
                            if sid_val is not None:
                                await _emit_maintainer_event({
                                    "type": "post_tp_sl_apply",
                                    "position_uid": position_uid,
                                    "strategy_id": str(sid_val),
                                    "level": str(level if level is not None else ""),
                                    "left_qty": str(left_qty),
                                    "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
                                    "dedupe": f"{position_uid}:sl:after_tp:{level}",
                                })

            # После любой сделки: если остаток стал 0 → просим maintainer снять TP/SL
            left_qty_now = await _calc_left_qty_for_uid(position_uid)
            if left_qty_now is not None and left_qty_now <= 0:
                sid_row = await infra.pg_pool.fetchrow(
                    "SELECT strategy_id FROM public.trader_positions WHERE position_uid = $1",
                    position_uid
                )
                sid_val = int(sid_row["strategy_id"]) if sid_row and sid_row["strategy_id"] is not None else None
                if sid_val is not None:
                    await _emit_maintainer_event({
                        "type": "cleanup_after_flat",
                        "position_uid": position_uid,
                        "strategy_id": str(sid_val),
                        "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
                        "dedupe": f"{position_uid}:cleanup",
                    })

        except Exception:
            log.exception("BYBIT_SYNC execution: ошибка обработки элемента: %s", it)


# 🔸 Поиск ордера в БД по order_link_id либо order_id (расширенная версия)
async def _find_tpo(order_link_id: Optional[str], order_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if order_link_id:
        row = await infra.pg_pool.fetchrow(
            """
            SELECT id, position_uid, kind, order_link_id, order_id,
                   filled_qty, avg_fill_price, exec_fee,
                   price, qty, "level", symbol, side
            FROM public.trader_position_orders
            WHERE order_link_id = $1
            """,
            order_link_id
        )
        if row:
            return dict(row)
    if order_id:
        row = await infra.pg_pool.fetchrow(
            """
            SELECT id, position_uid, kind, order_link_id, order_id,
                   filled_qty, avg_fill_price, exec_fee,
                   price, qty, "level", symbol, side
            FROM public.trader_position_orders
            WHERE order_id = $1
            """,
            order_id
        )
        if row:
            return dict(row)
    return None


# 🔸 Обновление tpo по order-событию
async def _update_tpo_on_order_event(
    *,
    tpo_id: int,
    order_id: Optional[str],
    ext_status: Optional[str],
    filled_qty: Optional[Decimal],
    avg_fill_price: Optional[Decimal],
    exec_fee: Optional[Decimal],
    last_ts: Optional[datetime],
) -> None:
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET
            order_id = COALESCE(order_id, $2),
            ext_status = COALESCE($3, ext_status),
            filled_qty = COALESCE($4, filled_qty),
            avg_fill_price = COALESCE($5, avg_fill_price),
            last_ext_event_at = COALESCE($6, last_ext_event_at)
        WHERE id = $1
        """,
        tpo_id, order_id, ext_status, filled_qty, avg_fill_price, last_ts
    )


# 🔸 Обновление tpo по execution-событию (инкрементально)
async def _update_tpo_on_execution(
    *,
    tpo_id: int,
    filled_qty: Decimal,
    avg_fill_price: Optional[Decimal],
    exec_fee: Decimal,
    last_ts: Optional[datetime],
) -> None:
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET
            filled_qty = $2,
            avg_fill_price = $3,
            exec_fee = $4,
            last_ext_event_at = COALESCE($5, last_ext_event_at)
        WHERE id = $1
        """,
        tpo_id, filled_qty, avg_fill_price, exec_fee, last_ts
    )


# 🔸 Агрегированное обновление полей в trader_positions для entry-ордера
async def _update_trader_positions_entry(
    *,
    position_uid: str,
    exchange: Optional[str],
    order_link_id: Optional[str],
    order_id: Optional[str],
    ext_status: Optional[str],
    filled_qty: Optional[Decimal],
    avg_fill_price: Optional[Decimal],
    exec_fee: Optional[Decimal],
    last_ts: Optional[datetime],
) -> None:
    sets, vals = [], []
    if exchange is not None:
        sets.append("exchange = $%d" % (len(vals) + 1)); vals.append(exchange)
    if order_link_id is not None:
        sets.append("order_link_id = COALESCE(order_link_id, $%d)" % (len(vals) + 1)); vals.append(order_link_id)
    if order_id is not None:
        sets.append("order_id = COALESCE(order_id, $%d)" % (len(vals) + 1)); vals.append(order_id)
    if ext_status is not None:
        sets.append("ext_status = $%d" % (len(vals) + 1)); vals.append(ext_status)
    if filled_qty is not None:
        sets.append("filled_qty = $%d" % (len(vals) + 1)); vals.append(filled_qty)
    if avg_fill_price is not None:
        sets.append("avg_fill_price = $%d" % (len(vals) + 1)); vals.append(avg_fill_price)
    if exec_fee is not None:
        sets.append("exec_fee = $%d" % (len(vals) + 1)); vals.append(exec_fee)
    if last_ts is not None:
        sets.append("last_ext_event_at = $%d" % (len(vals) + 1)); vals.append(last_ts)

    if not sets:
        return

    query = f"""
        UPDATE public.trader_positions
        SET {', '.join(sets)}
        WHERE position_uid = ${len(vals)+1}
    """
    vals.append(position_uid)
    await infra.pg_pool.execute(query, *vals)


# 🔸 Маппинг статусов Bybit → наши ext_status
def _map_order_status(s: str) -> Optional[str]:
    s = (s or "").strip().lower()
    if not s:
        return None
    if s in ("new", "created"):
        return "accepted"
    if s in ("partiallyfilled", "partially_filled"):
        return "partially_filled"
    if s == "filled":
        return "filled"
    if s in ("cancelled", "canceled"):
        return "canceled"
    if s == "rejected":
        return "rejected"
    if s in ("expired", "deactivated"):
        return "expired"
    if s in ("untriggered", "triggered"):
        return "accepted"
    return None


# 🔸 Периодический REST-ресинк (баланс + позиции linear)
async def run_bybit_rest_resync_job():
    if not API_KEY or not API_SECRET:
        log.debug("BYBIT_RESYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск")
        return

    try:
        bal = await _get_wallet_balance(ACCOUNT_TYPE)
        _log_balance_summary(bal)
    except Exception:
        log.exception("BYBIT_RESYNC: wallet-balance FAILED")

    try:
        pos = await _get_positions_list()
        _log_positions_summary(pos)
    except Exception:
        log.exception("BYBIT_RESYNC: position list FAILED")


# 🔸 REST-помощники
def _rest_sign(timestamp_ms: int, query_or_body: str) -> str:
    payload = f"{timestamp_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

async def _get_wallet_balance(account_type: str) -> dict:
    query = f"accountType={account_type}"
    url = f"{BASE_URL}/v5/account/wallet-balance?{query}"
    ts = int(time.time() * 1000)
    sign = _rest_sign(ts, query)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

async def _get_positions_list() -> dict:
    query = f"category={CATEGORY}"
    url = f"{BASE_URL}/v5/position/list?{query}"
    ts = int(time.time() * 1000)
    sign = _rest_sign(ts, query)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()


# 🔸 Форматтеры сводок в лог
def _log_balance_summary(bal: dict):
    acc = (bal.get("result") or {}).get("list") or []
    if not acc:
        log.debug("BYBIT_RESYNC balance: <empty>")
        return
    acc0 = acc[0]
    log.debug(
        "BYBIT_RESYNC balance: totalEquity=%s totalWallet=%s perpUPL=%s",
        acc0.get("totalEquity"), acc0.get("totalWalletBalance"), acc0.get("totalPerpUPL")
    )
    coins = acc0.get("coin") or []
    head = coins[0] if coins else {}
    log.debug("BYBIT_RESYNC coins: items=%d head=%s", len(coins), head)

def _log_positions_summary(pos: dict):
    lst = (pos.get("result") or {}).get("list") or []
    head = lst[0] if lst else {}
    log.debug("BYBIT_RESYNC positions: items=%d head=%s", len(lst), head)


# 🔸 Утилиты форматирования/арифметики/точностей
def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

def _as_int(v: Any) -> Optional[int]:
    try:
        s = _as_str(v)
        return int(s) if s != "" else None
    except Exception:
        return None

def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None

def _ts_from_ms(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.utcfromtimestamp(ms / 1000.0)
    except Exception:
        return None

def _round_qty(qty: Optional[Decimal], precision_qty: Optional[int]) -> Optional[Decimal]:
    if qty is None:
        return None
    if precision_qty is None:
        return qty
    step = Decimal("1").scaleb(-int(precision_qty))
    try:
        return qty.quantize(step, rounding=ROUND_DOWN)
    except Exception:
        return qty

def _round_price(price: Optional[Decimal], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    if price is None or ticksize is None:
        return price
    try:
        quantum = _as_decimal(ticksize) or Decimal("0")
        if quantum <= 0:
            return price
        return price.quantize(quantum, rounding=ROUND_HALF_UP)
    except Exception:
        return price

def _fmt(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)


# 🔸 Утилита: расчёт остатка позиции по uid (entry − tp − sl − close)
async def _calc_left_qty_for_uid(uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        WITH e AS (
          SELECT COALESCE(MAX(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='entry'
        ),
        t AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='tp'
        ),
        s AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='sl'
        ),
        c AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='close'
        )
        SELECT e.fq - t.fq - s.fq - c.fq AS left_qty FROM e,t,s,c
        """,
        uid
    )
    try:
        return Decimal(str(row["left_qty"])) if row and row["left_qty"] is not None else None
    except Exception:
        return None