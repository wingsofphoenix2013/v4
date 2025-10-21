# bybit_maintainer.py — сопровождение позиции на бирже «exchange-first»: TP1→SL@entry, финализация при SL/size=0/manual; идемпотентные действия

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple, List

import httpx

from trader_infra import infra

# 🔸 Логгер сопровождающего воркера
log = logging.getLogger("BYBIT_MAINTAINER_V2")

# 🔸 Параметры режима/REST
TRADER_ORDER_MODE = (os.getenv("TRADER_ORDER_MODE", "off") or "off").strip().lower()  # off|dry_run|on
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"
DEFAULT_TRIGGER_BY = os.getenv("BYBIT_TRIGGER_BY", "LastPrice")  # LastPrice | MarkPrice | IndexPrice

# 🔸 Тайминги и поведение
SCAN_INTERVAL_SEC = float(os.getenv("MAINT_SCAN_INTERVAL_SEC", "1.5"))       # период инспекции
LOCK_TTL_SEC = int(os.getenv("MAINT_LOCK_TTL_SEC", "10"))                    # TTL локов по позиции
INTENT_TTL_SEC = int(os.getenv("MAINT_INTENT_TTL_SEC", "10"))                # TTL intent-маркеров (cancel)
RECENT_WINDOW_SEC = int(os.getenv("MAINT_RECENT_WINDOW_SEC", "120"))         # «свежие» события

# 🔸 Статусные множества
FINAL_TPO = {"canceled", "filled", "expired", "rejected"}
NONFINAL_TPO = {"planned", "submitted", "accepted", "partially_filled"}
NONFINAL_TPO_OR_NULL = {None, "planned", "submitted", "accepted", "partially_filled"}

# 🔸 Сообщим о режиме
log.info("BYBIT_MAINTAINER_V2: mode=%s, trigger_by=%s, scan=%.2fs", TRADER_ORDER_MODE, DEFAULT_TRIGGER_BY, SCAN_INTERVAL_SEC)


# 🔸 Основной цикл воркера
async def run_bybit_maintainer_loop():
    log.info("🚦 BYBIT_MAINTAINER_V2 запущен")

    while True:
        try:
            await _scan_and_maintain_positions()
        except Exception:
            log.exception("❌ Ошибка в цикле BYBIT_MAINTAINER_V2")
        await asyncio.sleep(SCAN_INTERVAL_SEC)


# 🔸 Инспекция и сопровождение всех «open» позиций
async def _scan_and_maintain_positions():
    now = datetime.utcnow()
    recent_since = now - timedelta(seconds=RECENT_WINDOW_SEC)

    pos_rows = await infra.pg_pool.fetch(
        "SELECT position_uid, strategy_id, symbol FROM public.trader_positions WHERE status='open'"
    )
    if not pos_rows:
        return

    for r in pos_rows:
        uid = _as_str(r["position_uid"])
        symbol = _as_str(r["symbol"])
        if not uid or not symbol:
            continue

        got = await _with_lock(uid)
        if not got:
            continue

        try:
            # 1) если SL filled или LEFT=0 → финализировать (биржа→система)
            sl_filled = await _has_sl_filled(uid)
            left_qty = await _calc_left_qty(uid)
            if sl_filled or (left_qty is not None and left_qty <= Decimal("0")):
                reason = "sl-hit" if sl_filled else "tp-exhausted"
                await _finalize_position(uid, symbol, reason)
                continue

            # 2) TP1 partial/full → активировать/корректировать SL@entry
            tp1_filled = await _tp1_filled_qty(uid)
            if tp1_filled and tp1_filled > Decimal("0"):
                await _activate_or_adjust_sl_after_tp1(uid, symbol)

            # 3) manual признаки: отменён наш TP/SL без нашей intent-метки → финализировать
            manual = await _detect_manual_cancel(uid, since=recent_since)
            if manual:
                await _finalize_position(uid, symbol, "manual-exchange")
                continue

        except Exception:
            log.exception("⚠️ Ошибка сопровождения uid=%s", uid)
        finally:
            await _release_lock(uid)


# 🔸 Активация/корректировка SL после TP1 (перенос на entry остатка)
async def _activate_or_adjust_sl_after_tp1(position_uid: str, symbol: str) -> None:
    # активные реальные SL
    active_sls = await infra.pg_pool.fetch(
        """
        SELECT id, order_link_id, trigger_price, qty, ext_status
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='sl'
          AND "type" IS NOT NULL
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        ORDER BY id DESC
        """,
        position_uid
    )
    # шаблон SL-after-TP1
    sl_after = await infra.pg_pool.fetchrow(
        """
        SELECT id, order_link_id, trigger_price, qty, ext_status
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='sl'
          AND activation_tp_level=1
          AND (ext_status='virtual' OR ext_status IS NULL)
        ORDER BY id ASC LIMIT 1
        """,
        position_uid
    )
    if not sl_after:
        return

    left = await _calc_left_qty(position_uid)
    if left is None or left <= Decimal("0"):
        return

    tkr = await _load_ticker_precisions(symbol)
    precision_qty = tkr.get("precision_qty")
    min_qty = tkr.get("min_qty")
    ticksize = tkr.get("ticksize")

    target_qty = _round_qty(left, precision_qty)
    if min_qty is not None and target_qty < min_qty:
        return

    # отменяем текущие активные SL (если есть)
    for sl in active_sls:
        link = _as_str(sl["order_link_id"])
        if not link:
            continue
        await _intent_mark_cancel(link)
        await _cancel_order_by_link(symbol, link)

    # определяем триггер и короткий linkId для шаблона
    trig = _as_decimal(sl_after["trigger_price"])
    if trig is None:
        entry_avg = await _fetch_entry_avg_fill_price(position_uid)
        trig = entry_avg

    direction = await _get_direction(position_uid)
    trig_dir = _calc_trigger_direction(direction)

    # берём link из шаблона, при необходимости укорачиваем и обновляем TPO
    old_link = _as_str(sl_after["order_link_id"]) or f"{position_uid}-sl-after-tp-1"
    short_link = await _ensure_short_tpo_link(position_uid, old_link, short_suffix="sla1")

    # submit SL-after-TP1
    if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
        ok, oid, rc, rm = await _submit_sl(
            symbol=symbol,
            side=_to_title_side("SELL" if (direction or "").lower() == "long" else "BUY"),
            trigger_price=_round_price(trig, ticksize),
            qty=target_qty,
            link_id=short_link,
            trigger_direction=trig_dir,
        )
        await _mark_order_after_submit(order_link_id=short_link, ok=ok, order_id=oid, retcode=rc, retmsg=rm)
    else:
        await infra.pg_pool.execute(
            "UPDATE public.trader_position_orders SET ext_status='submitted', last_ext_event_at=$2 WHERE order_link_id=$1",
            short_link, datetime.utcnow()
        )

    # синхронизируем qty/trigger в TPO (на случай коррекции)
    await infra.pg_pool.execute(
        "UPDATE public.trader_position_orders SET qty=$2, trigger_price=COALESCE(trigger_price,$3) WHERE order_link_id=$1",
        short_link, target_qty, trig
    )


# 🔸 Финализация позиции: отмена остаточных TP/SL, закрытие в портфеле/БД
async def _finalize_position(position_uid: str, symbol: str, reason: str) -> None:
    now = datetime.utcnow()

    # отмена всех нефинальных TP/SL
    open_orders = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, kind, "type", ext_status
        FROM public.trader_position_orders
        WHERE position_uid=$1
          AND kind IN ('tp','sl')
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        """,
        position_uid
    )
    for o in open_orders:
        link = _as_str(o["order_link_id"])
        otype = _as_str(o["type"])
        if not link:
            continue
        if not otype:
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='expired', last_ext_event_at=$2 WHERE order_link_id=$1",
                link, now
            )
            continue
        await _intent_mark_cancel(link)
        if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
            await _cancel_order_by_link(symbol, link)
        else:
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='canceled', last_ext_event_at=$2 WHERE order_link_id=$1",
                link, now
            )

    # если остался нетто-остаток — закрываем market RO
    left = await _calc_left_qty(position_uid)
    if left is not None and left > Decimal("0"):
        direction = await _get_direction(position_uid)
        side_title = _to_title_side("SELL" if (direction or "").lower() == "long" else "BUY")
        # короткий linkId для close
        close_link = _make_short_link(position_uid, "cls")
        if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
            ok_c, oid_c, rc_c, rm_c = await _submit_close_market(symbol, side_title, _round_qty(left, await _precision_qty(symbol)), close_link)
            await _mark_order_after_submit(order_link_id=close_link, ok=ok_c, order_id=oid_c, retcode=rc_c, retmsg=rm_c)
        else:
            await infra.pg_pool.execute(
                "INSERT INTO public.trader_position_orders (position_uid, kind, level, exchange, symbol, side, \"type\", tif, reduce_only, price, trigger_price, qty, order_link_id, ext_status, created_at) VALUES ($1,'close',NULL,'BYBIT',$2,NULL,'market','GTC',true,NULL,NULL,$3,$4,'submitted',$5) ON CONFLICT (order_link_id) DO NOTHING",
                position_uid, symbol, _round_qty(left, await _precision_qty(symbol)), close_link, now
            )

    # закрываем запись в портфеле
    await _set_position_closed(position_uid, reason, now)


# 🔸 Детект manual: отмена наших TP/SL без локального намерения
async def _detect_manual_cancel(position_uid: str, since: datetime) -> bool:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id
        FROM public.trader_position_orders
        WHERE position_uid=$1
          AND kind IN ('tp','sl')
          AND "type" IS NOT NULL
          AND ext_status='canceled'
          AND last_ext_event_at >= $2
        """,
        position_uid, since
    )
    if not rows:
        return False
    for r in rows:
        link = _as_str(r["order_link_id"])
        if not await _intent_check(link):
            return True
    return False


# 🔸 Хелперы работы с ордерами и расчёты
async def _tp1_filled_qty(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT COALESCE(filled_qty,0) AS fq
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='tp' AND "level"=1
        ORDER BY id DESC LIMIT 1
        """,
        position_uid
    )
    return _as_decimal(row["fq"]) if row else None

async def _has_sl_filled(position_uid: str) -> bool:
    row = await infra.pg_pool.fetchrow(
        "SELECT 1 FROM public.trader_position_orders WHERE position_uid=$1 AND kind='sl' AND ext_status='filled' LIMIT 1",
        position_uid
    )
    return bool(row)

async def _calc_left_qty(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        WITH e AS (
          SELECT COALESCE(MAX(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='entry'
        ),
        t AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='tp'
        ),
        c AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='close'
        )
        SELECT e.fq - t.fq - c.fq AS left_qty FROM e,t,c
        """,
        position_uid
    )
    return _as_decimal(row["left_qty"]) if row else None

async def _fetch_entry_avg_fill_price(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        "SELECT avg_fill_price FROM public.trader_position_orders WHERE position_uid=$1 AND kind='entry' ORDER BY id DESC LIMIT 1",
        position_uid
    )
    return _as_decimal(row["avg_fill_price"]) if row else None

async def _get_direction(position_uid: str) -> Optional[str]:
    row = await infra.pg_pool.fetchrow(
        "SELECT direction FROM public.positions_v4 WHERE position_uid=$1",
        position_uid
    )
    return (_as_str(row["direction"]).lower() if row and row["direction"] else None)

async def _precision_qty(symbol: str) -> Optional[int]:
    row = await infra.pg_pool.fetchrow("SELECT precision_qty FROM public.tickers_bb WHERE symbol=$1", symbol)
    return int(row["precision_qty"]) if row and row["precision_qty"] is not None else None

async def _load_ticker_precisions(symbol: str) -> Dict[str, Optional[Decimal]]:
    row = await infra.pg_pool.fetchrow("SELECT precision_qty, min_qty, ticksize FROM public.tickers_bb WHERE symbol=$1", symbol)
    if not row:
        return {"precision_qty": None, "min_qty": None, "ticksize": None}
    return {
        "precision_qty": int(row["precision_qty"]) if row["precision_qty"] is not None else None,
        "min_qty": _as_decimal(row["min_qty"]),
        "ticksize": _as_decimal(row["ticksize"]),
    }


# 🔸 Сабмиты/отмена и статусы
async def _submit_sl(*, symbol: str, side: str, trigger_price: Decimal, qty: Decimal, link_id: str, trigger_direction: int) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _fmt(qty),
        "reduceOnly": True,
        "triggerPrice": _fmt(trigger_price),
        "triggerDirection": trigger_direction,
        "triggerBy": DEFAULT_TRIGGER_BY,
        "closeOnTrigger": True,
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    if TRADER_ORDER_MODE != "on" or not API_KEY or not API_SECRET:
        log.info("[DRY_RUN MAINT] submit SL: %s trigger=%s qty=%s link=%s", symbol, _fmt(trigger_price), _fmt(qty), link_id)
        return True, None, None, None
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    log.info("submit SL: %s trigger=%s qty=%s link=%s → rc=%s msg=%s oid=%s", symbol, _fmt(trigger_price), _fmt(qty), link_id, rc, rm, oid)
    return (rc == 0), oid, rc, rm

async def _submit_close_market(symbol: str, side: str, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _fmt(qty),
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }
    if TRADER_ORDER_MODE != "on" or not API_KEY or not API_SECRET:
        log.info("[DRY_RUN MAINT] close market: %s qty=%s link=%s", symbol, _fmt(qty), link_id)
        return True, None, None, None
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    log.info("submit CLOSE: %s qty=%s link=%s → rc=%s msg=%s oid=%s", symbol, _fmt(qty), link_id, rc, rm, oid)
    return (rc == 0), oid, rc, rm

async def _cancel_order_by_link(symbol: str, link_id: str) -> None:
    if TRADER_ORDER_MODE != "on" or not API_KEY or not API_SECRET:
        await infra.pg_pool.execute(
            "UPDATE public.trader_position_orders SET ext_status='canceled', last_ext_event_at=$2 WHERE order_link_id=$1",
            link_id, datetime.utcnow()
        )
        log.info("[DRY_RUN MAINT] cancel: %s", link_id)
        return
    resp = await _bybit_post("/v5/order/cancel", {"category": CATEGORY, "symbol": symbol, "orderLinkId": link_id})
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    log.info("cancel %s → rc=%s msg=%s", link_id, rc, rm)

async def _mark_order_after_submit(*, order_link_id: str, ok: bool, order_id: Optional[str], retcode: Optional[int], retmsg: Optional[str]) -> None:
    now = datetime.utcnow()
    status = "submitted" if ok else "rejected"
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET
            order_id = COALESCE($2, order_id),
            ext_status = $3,
            last_ext_event_at = $4,
            error_last = CASE WHEN $1 THEN NULL ELSE $5 END
        WHERE order_link_id = $6
        """,
        ok, order_id, status, now, (f"retCode={retcode} retMsg={retmsg}" if not ok else None), order_link_id
    )

def _rest_sign(ts_ms: int, query_or_body: str) -> str:
    import hmac, hashlib
    payload = f"{ts_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

async def _bybit_post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    ts = _now_ms()
    body_str = _json_body(body)
    sign = _rest_sign(ts, body_str)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_str.encode("utf-8"))
        try:
            r.raise_for_status()
        except Exception:
            log.warning("⚠️ Bybit POST %s %s: %s", path, r.status_code, r.text)
        try:
            return r.json()
        except Exception:
            return {"retCode": None, "retMsg": "non-json response", "raw": r.text}

def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    try:
        res = resp.get("result") or {}
        oid = res.get("orderId")
        return _as_str(oid) if oid is not None else None
    except Exception:
        return None

def _json_body(obj: Dict[str, Any]) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


# 🔸 Локи, intent-маркеры и linkId helpers
async def _with_lock(position_uid: str) -> bool:
    key = f"bybit:maint:v2:lock:{position_uid}"
    try:
        ok = await infra.redis_client.set(key, "1", ex=LOCK_TTL_SEC, nx=True)
        return bool(ok)
    except Exception:
        return False

async def _release_lock(position_uid: str) -> None:
    key = f"bybit:maint:v2:lock:{position_uid}"
    try:
        await infra.redis_client.delete(key)
    except Exception:
        pass

async def _intent_mark_cancel(order_link_id: str) -> None:
    key = f"bybit:maint:v2:intent:cancel:{order_link_id}"
    try:
        await infra.redis_client.set(key, "1", ex=INTENT_TTL_SEC)
    except Exception:
        pass

async def _intent_check(order_link_id: str) -> bool:
    key = f"bybit:maint:v2:intent:cancel:{order_link_id}"
    try:
        v = await infra.redis_client.get(key)
        return v is not None
    except Exception:
        return False

def _make_short_link(position_uid: str, suffix: str, maxlen: int = 45) -> str:
    base = f"{position_uid}-{suffix}"
    if len(base) <= maxlen:
        return base
    keep = maxlen - (len(suffix) + 1)
    return f"{position_uid[:keep]}-{suffix}"

async def _ensure_short_tpo_link(position_uid: str, current_link: str, short_suffix: str) -> str:
    if len(current_link) <= 45:
        return current_link
    new_link = _make_short_link(position_uid, short_suffix, 45)
    # переименуем order_link_id в TPO, чтобы WS-события были маппабельны
    await infra.pg_pool.execute(
        "UPDATE public.trader_position_orders SET order_link_id=$3 WHERE position_uid=$1 AND order_link_id=$2",
        position_uid, current_link, new_link
    )
    log.info("TPO link renamed (too long): %s → %s (uid=%s)", current_link, new_link, position_uid)
    return new_link


# 🔸 Обновление статуса позиции с фолбэком, если нет колонки close_reason
async def _set_position_closed(position_uid: str, reason: str, ts: datetime) -> None:
    try:
        await infra.pg_pool.execute(
            "UPDATE public.trader_positions SET status='closed', closed_at=COALESCE(closed_at,$2), close_reason=COALESCE(close_reason,$3) WHERE position_uid=$1",
            position_uid, ts, reason
        )
    except Exception as e:
        if "close_reason" in str(e):
            await infra.pg_pool.execute(
                "UPDATE public.trader_positions SET status='closed', closed_at=COALESCE(closed_at,$2) WHERE position_uid=$1",
                position_uid, ts
            )
            log.warning("close_reason column is missing; position closed without reason (uid=%s)", position_uid)
        else:
            raise


# 🔸 Утилиты приведения и форматирования
def _as_str(v: Any) -> str:
    if v is None:
        return ""
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)

def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None

def _fmt(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)

def _round_qty(qty: Decimal, precision_qty: Optional[int]) -> Decimal:
    if qty is None:
        return Decimal("0")
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

def _to_title_side(side: str) -> str:
    s = (side or "").upper()
    return "Buy" if s == "BUY" else "Sell"

def _calc_trigger_direction(position_direction: Optional[str]) -> int:
    d = (position_direction or "").lower()
    return 2 if d == "long" else 1