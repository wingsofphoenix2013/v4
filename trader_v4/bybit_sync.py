# bybit_sync.py — приватный WS-синк Bybit: auth + wallet/position/order/execution + авто-reconnect
# + периодический REST-ресинк; обновление БД по событиям; публикация amend SL intent

# 🔸 Импорты
import os
import hmac
import time
import json
import hashlib
import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import websockets
import httpx

from trader_infra import infra
from trader_config import config
from bybit_intents import STREAM_NAME as INTENTS_STREAM, build_amend_sl

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

PING_INTERVAL_SEC = 20.0      # отправляем ping, если нет сообщений дольше этого интервала
RECONNECT_DELAY_SEC = 3.0     # пауза между переподключениями


# 🔸 Основной цикл воркера приватного WS (держим канал + подписки)
async def run_bybit_private_ws_sync_loop():
    if not API_KEY or not API_SECRET:
        log.info("BYBIT_SYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск запуска")
        return

    log.info("BYBIT_SYNC: старт приватного WS-синка %s", WS_PRIVATE)

    while True:
        try:
            async with websockets.connect(WS_PRIVATE, ping_interval=None, close_timeout=5) as ws:
                # auth
                expires = int((time.time() + 5) * 1000)
                sign_payload = f"GET/realtime{expires}"
                signature = hmac.new(API_SECRET.encode(), sign_payload.encode(), hashlib.sha256).hexdigest()
                await ws.send(json.dumps({"op": "auth", "args": [API_KEY, expires, signature]}))
                auth_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC auth: %s", auth_resp)

                # подписки: wallet + position + order + execution
                await ws.send(json.dumps({"op": "subscribe", "args": ["wallet", "position", "order", "execution"]}))
                sub_resp = json.loads(await ws.recv())
                log.info("BYBIT_SYNC subscribe ack: %s", sub_resp)

                # цикл чтения с таймаутом (для пингов)
                while True:
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=PING_INTERVAL_SEC)
                        await _handle_ws_message(msg_raw)
                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"op": "ping"}))
                        log.info("BYBIT_SYNC → ping")
                        try:
                            pong_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            await _handle_ws_message(pong_raw)
                        except asyncio.TimeoutError:
                            log.info("BYBIT_SYNC: нет pong — переподключение")
                            raise ConnectionError("pong timeout")

        except Exception:
            log.exception("BYBIT_SYNC: сбой канала, переподключение через %.1fs", RECONNECT_DELAY_SEC)
            await asyncio.sleep(RECONNECT_DELAY_SEC)


# 🔸 Обработка входящих WS-сообщений (с обновлением БД)
async def _handle_ws_message(msg_raw: str):
    try:
        msg = json.loads(msg_raw)
    except Exception:
        log.info("BYBIT_SYNC recv (raw): %s", msg_raw)
        return

    # служебные op-сообщения
    if "op" in msg and "topic" not in msg:
        log.info("BYBIT_SYNC recv op: %s", msg)
        return

    # топиковые события
    topic = msg.get("topic")
    data = msg.get("data")
    ts = msg.get("ts")

    if topic == "wallet":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC wallet: items=%d head=%s ts=%s", len(items), head, ts)
        return

    if topic == "position":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC position: items=%d head=%s ts=%s", len(items), head, ts)
        # (по желанию — можно обновлять агрегаты по позициям в БД)
        return

    if topic == "order":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC order: items=%d head=%s ts=%s", len(items), head, ts)
        try:
            await _apply_order_updates(items)
        except Exception:
            log.exception("BYBIT_SYNC: ошибка применения order-апдейтов")
        return

    if topic == "execution":
        items = data if isinstance(data, list) else []
        head = items[0] if items else {}
        log.info("BYBIT_SYNC execution: items=%d head=%s ts=%s", len(items), head, ts)
        try:
            await _apply_execution_updates(items)
        except Exception:
            log.exception("BYBIT_SYNC: ошибка применения execution-апдейтов")
        return

    log.info("BYBIT_SYNC recv topic=%s: %s", topic, msg)


# 🔸 Периодический REST-ресинк (баланс + позиции linear) — для наблюдаемости
async def run_bybit_rest_resync_job():
    if not API_KEY or not API_SECRET:
        log.info("BYBIT_RESYNC: ключи не заданы (BYBIT_API_KEY/SECRET) — пропуск")
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


# 🔸 Применение order-апдейтов к БД
async def _apply_order_updates(items: List[Dict[str, Any]]):
    if not items:
        return
    for it in items:
        link_id = _as_str(it.get("orderLinkId"))
        order_id = _as_str(it.get("orderId"))
        status = _as_str(it.get("orderStatus") or it.get("orderStatusVo") or it.get("status"))
        # Кумулятивы по ордеру (если отданы): qty/avg/fees могут отсутствовать в order-топике
        cum_qty = _as_dec(it.get("cumExecQty") or it.get("cumExecQuantity"))
        avg_px = _as_dec(it.get("avgPrice") or it.get("avgExecPrice"))
        fee = _as_dec(it.get("cumExecFee") or it.get("execFee"))

        if not link_id:
            continue

        try:
            if ":" in link_id:
                await _db_update_child_order_by_link(
                    order_link_id=link_id,
                    order_id=order_id or None,
                    ext_status=status or None,
                    filled_qty=cum_qty,
                    avg_fill_price=avg_px,
                    exec_fee=fee,
                )
                # если TP-ордер стал filled → возможно двигаем SL по политике (минимально: на entry)
                if status and status.lower() in ("filled", "fullyfilled", "filled_partial") and ":tp" in link_id:
                    uid = link_id.split(":")[0]
                    await _maybe_publish_amend_sl_to_entry(uid)
            else:
                await _db_update_entry_by_link(
                    position_uid=link_id,
                    order_id=order_id or None,
                    ext_status=status or None,
                    filled_qty=cum_qty,
                    avg_fill_price=avg_px,
                    exec_fee=fee,
                )
        except Exception:
            log.exception("BYBIT_SYNC: DB update failed for order link=%s", link_id)


# 🔸 Применение execution-апдейтов к БД (инкрементальные филлы)
async def _apply_execution_updates(items: List[Dict[str, Any]]):
    if not items:
        return
    for it in items:
        link_id = _as_str(it.get("orderLinkId"))
        exec_qty = _as_dec(it.get("execQty") or it.get("lastExecQty"))
        exec_price = _as_dec(it.get("execPrice") or it.get("lastExecPrice"))
        exec_fee = _as_dec(it.get("execFee") or it.get("lastExecFee"))
        if not link_id or exec_qty is None or exec_price is None:
            continue

        try:
            if ":" in link_id:
                await _db_accumulate_child_execution(link_id, exec_qty, exec_price, exec_fee)
            else:
                await _db_accumulate_entry_execution(link_id, exec_qty, exec_price, exec_fee)
        except Exception:
            log.exception("BYBIT_SYNC: DB accumulate failed for execution link=%s", link_id)


# 🔸 DB: апдейты по «шапке» позиции (entry)
async def _db_update_entry_by_link(
    *,
    position_uid: str,
    order_id: Optional[str],
    ext_status: Optional[str],
    filled_qty: Optional[Decimal],
    avg_fill_price: Optional[Decimal],
    exec_fee: Optional[Decimal],
):
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET order_id = COALESCE($2, order_id),
            ext_status = COALESCE($3, ext_status),
            filled_qty = COALESCE($4, filled_qty),
            avg_fill_price = COALESCE($5, avg_fill_price),
            exec_fee = COALESCE($6, exec_fee),
            last_ext_event_at = (now() at time zone 'UTC')
        WHERE position_uid = $1
        """,
        position_uid, order_id, ext_status,
        str(filled_qty) if filled_qty is not None else None,
        str(avg_fill_price) if avg_fill_price is not None else None,
        str(exec_fee) if exec_fee is not None else None,
    )

# 🔸 DB: апдейты по дочерним ордерам (TP/SL/close)
async def _db_update_child_order_by_link(
    *,
    order_link_id: str,
    order_id: Optional[str],
    ext_status: Optional[str],
    filled_qty: Optional[Decimal],
    avg_fill_price: Optional[Decimal],
    exec_fee: Optional[Decimal],
):
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET order_id = COALESCE($2, order_id),
            ext_status = COALESCE($3, ext_status),
            filled_qty = COALESCE($4, filled_qty),
            avg_fill_price = COALESCE($5, avg_fill_price),
            exec_fee = COALESCE($6, exec_fee),
            last_ext_event_at = (now() at time zone 'UTC')
        WHERE order_link_id = $1
        """,
        order_link_id, order_id,
        ext_status,
        str(filled_qty) if filled_qty is not None else None,
        str(avg_fill_price) if avg_fill_price is not None else None,
        str(exec_fee) if exec_fee is not None else None,
    )

# 🔸 DB: инкремент по «шапке» (пересчёт cum filled/avg)
async def _db_accumulate_entry_execution(position_uid: str, qty: Decimal, price: Decimal, fee: Optional[Decimal]):
    row = await infra.pg_pool.fetchrow(
        """
        SELECT filled_qty, avg_fill_price, exec_fee
        FROM public.trader_positions
        WHERE position_uid = $1
        """,
        position_uid
    )
    cur_qty = _as_dec(row["filled_qty"]) if row else Decimal("0")
    cur_avg = _as_dec(row["avg_fill_price"]) if row else None
    cur_fee = _as_dec(row["exec_fee"]) if row else Decimal("0")

    new_qty = (cur_qty or Decimal("0")) + qty
    new_avg = price if (cur_avg is None or new_qty == 0) else ((cur_avg * (new_qty - qty) + price * qty) / new_qty)
    new_fee = (cur_fee or Decimal("0")) + (fee or Decimal("0"))

    await infra.pg_pool.execute(
        """
        UPDATE public.trader_positions
        SET filled_qty = $2,
            avg_fill_price = $3,
            exec_fee = $4,
            last_ext_event_at = (now() at time zone 'UTC')
        WHERE position_uid = $1
        """,
        position_uid, str(new_qty), str(new_avg), str(new_fee)
    )

# 🔸 DB: инкремент по дочерним (TP/SL/close)
async def _db_accumulate_child_execution(order_link_id: str, qty: Decimal, price: Decimal, fee: Optional[Decimal]):
    row = await infra.pg_pool.fetchrow(
        """
        SELECT filled_qty, avg_fill_price, exec_fee
        FROM public.trader_position_orders
        WHERE order_link_id = $1
        """,
        order_link_id
    )
    cur_qty = _as_dec(row["filled_qty"]) if row else Decimal("0")
    cur_avg = _as_dec(row["avg_fill_price"]) if row else None
    cur_fee = _as_dec(row["exec_fee"]) if row else Decimal("0")

    new_qty = (cur_qty or Decimal("0")) + qty
    new_avg = price if (cur_avg is None or new_qty == 0) else ((cur_avg * (new_qty - qty) + price * qty) / new_qty)
    new_fee = (cur_fee or Decimal("0")) + (fee or Decimal("0"))

    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET filled_qty = $2,
            avg_fill_price = $3,
            exec_fee = $4,
            last_ext_event_at = (now() at time zone 'UTC')
        WHERE order_link_id = $1
        """,
        order_link_id, str(new_qty), str(new_avg), str(new_fee)
    )

# 🔸 Лёгкий триггер: после TP fill двигаем SL в entry (минимальная политика)
async def _maybe_publish_amend_sl_to_entry(position_uid: str):
    try:
        # entry price из positions_v4
        row = await infra.pg_pool.fetchrow(
            """
            SELECT p.symbol, p.entry_price
            FROM public.positions_v4 p
            WHERE p.position_uid = $1
            """,
            position_uid
        )
        if not row or row["entry_price"] is None:
            return
        symbol = str(row["symbol"])
        entry_price = Decimal(str(row["entry_price"]))
        intent = build_amend_sl(position_uid=position_uid, symbol=symbol, new_sl_price=entry_price)
        await infra.redis_client.xadd(INTENTS_STREAM, intent.to_stream_payload())
        log.info("BYBIT_SYNC: published amend_sl → entry (uid=%s price=%s)", position_uid, entry_price)
    except Exception:
        log.exception("BYBIT_SYNC: failed to publish amend_sl for uid=%s", position_uid)


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
        log.info("BYBIT_RESYNC balance: <empty>")
        return
    acc0 = acc[0]
    log.info("BYBIT_RESYNC balance: totalEquity=%s totalWallet=%s perpUPL=%s",
             acc0.get("totalEquity"), acc0.get("totalWalletBalance"), acc0.get("totalPerpUPL"))
    coins = acc0.get("coin") or []
    head = coins[0] if coins else {}
    log.info("BYBIT_RESYNC coins: items=%d head=%s", len(coins), head)

def _log_positions_summary(pos: dict):
    lst = (pos.get("result") or {}).get("list") or []
    head = lst[0] if lst else {}
    log.info("BYBIT_RESYNC positions: items=%d head=%s", len(lst), head)


# 🔸 Утилиты

def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode()
        except Exception:
            return str(v)
    return str(v)

def _as_dec(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        return Decimal(str(v))
    except Exception:
        return None