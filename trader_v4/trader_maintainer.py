# trader_maintainer.py — базовый мейнтейнер: гармонизация TP, post-TP SL и принудительный flatten по команде (v1, event-driven через Redis Stream)

# 🔸 Импорты
import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple, List, Set

import httpx
import os

from trader_infra import infra
from trader_config import config

# 🔸 Логгер
log = logging.getLogger("TRADER_MAINTAINER")

# 🔸 Потоки/группы
MAINTAINER_STREAM = "trader_maintainer_events"
CG_NAME = "trader_maintainer_group"
CONSUMER_NAME = "trader_maintainer_1"

# 🔸 Режимы исполнения
def _normalize_mode(v: Optional[str]) -> str:
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disabled"):
        return "off"
    if s in ("dry_run", "dry-run", "dryrun", "test"):
        return "dry_run"
    return "on"

TRADER_ORDER_MODE = _normalize_mode(os.getenv("TRADER_ORDER_MODE"))

# 🔸 Bybit REST (окружение)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"                       # USDT-perp
DEFAULT_TRIGGER_BY = os.getenv("BYBIT_TRIGGER_BY", "LastPrice")  # LastPrice | MarkPrice | IndexPrice

# 🔸 Активные статусы ордеров
_ACTIVE_EXT: Set[str] = {"submitted", "accepted", "partially_filled"}

# 🔸 Сообщим о режиме
if TRADER_ORDER_MODE == "dry_run":
    log.debug("MAINTAINER v1: DRY_RUN (cancel/recreate/SL/flatten — только логируем)")
elif TRADER_ORDER_MODE == "off":
    log.debug("MAINTAINER v1: OFF (игнорируем входящие события)")
else:
    log.debug("MAINTAINER v1: ON (гармонизация TP, post-TP SL, принудительный flatten)")


# 🔸 Главный воркер: потребитель событий из стрима maintainer’а
async def run_trader_maintainer_loop():
    redis = infra.redis_client

    try:
        await redis.xgroup_create(MAINTAINER_STREAM, CG_NAME, id="$", mkstream=True)
        log.info("📡 CG создана: %s → %s", MAINTAINER_STREAM, CG_NAME)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ CG уже существует: %s", CG_NAME)
        else:
            log.exception("❌ Ошибка создания CG для %s", MAINTAINER_STREAM)
            return

    log.info("🚦 TRADER_MAINTAINER v1 запущен (источник=%s)", MAINTAINER_STREAM)

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_NAME,
                consumername=CONSUMER_NAME,
                streams={MAINTAINER_STREAM: ">"},
                count=50,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, raw in records:
                    try:
                        evt = _parse_event(raw)
                        if not evt:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                            continue

                        # фильтр winners
                        sid = evt.get("strategy_id")
                        if sid is None or sid not in config.trader_winners:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                            continue

                        etype = evt["type"]
                        if etype == "tp_harmonize_needed":
                            await _handle_tp_harmonize(evt)
                        elif etype == "post_tp_sl_apply":
                            await _handle_post_tp_sl(evt)
                        elif etype == "final_flatten_force":
                            await _handle_final_flatten_force(evt)
                        else:
                            log.debug("ℹ️ Пропуск неизвестного type=%s evt=%s", etype, evt)

                    except Exception:
                        log.exception("❌ Ошибка обработки события maintainer")
                    finally:
                        try:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                        except Exception:
                            log.exception("❌ Не удалось ACK запись maintainer")
        except Exception:
            log.exception("❌ Ошибка в цикле TRADER_MAINTAINER")
            await asyncio.sleep(1.0)


# 🔸 Маршруты действий

async def _handle_tp_harmonize(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    order_link_id = evt.get("order_link_id")
    level = _as_int(evt.get("level"))

    if not order_link_id or level is None:
        log.debug("⚠️ tp_harmonize: некорректное событие %s", evt)
        return

    tpo = await infra.pg_pool.fetchrow(
        """
        SELECT position_uid, symbol, side, price, qty, ext_status
        FROM public.trader_position_orders
        WHERE order_link_id = $1 AND kind='tp' AND "level" = $2
        """,
        order_link_id, level
    )
    if not tpo:
        log.debug("ℹ️ tp_harmonize: TPO не найден (link=%s)", order_link_id)
        return

    symbol = str(tpo["symbol"])
    side = _to_title_side(str(tpo["side"] or "").upper())  # "Buy"/"Sell"
    price_need = _as_decimal(tpo["price"])
    qty_need = _as_decimal(tpo["qty"])

    # точности
    ticksize = _as_decimal((config.tickers.get(symbol) or {}).get("ticksize"))
    precision_qty = (config.tickers.get(symbol) or {}).get("precision_qty")
    price_need = _round_price(price_need, ticksize)
    qty_need = _round_qty(qty_need or Decimal("0"), precision_qty)

    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] tp_harmonize: cancel+recreate %s L=%s → price=%s qty=%s", symbol, level, _fmt(price_need), _fmt(qty_need))
        return

    ok_c, rc_c, rm_c = await _cancel_by_link(symbol=symbol, link_id=order_link_id)
    log.debug("tp_harmonize: cancel link=%s → ok=%s rc=%s msg=%s", order_link_id, ok_c, rc_c, rm_c)

    ok_t, oid_t, rc_t, rm_t = await _submit_tp(symbol=symbol, side=side, price=price_need, qty=qty_need, link_id=order_link_id)
    await _mark_order_after_submit(order_link_id=order_link_id, ok=ok_t, order_id=oid_t, retcode=rc_t, retmsg=rm_t)


async def _handle_post_tp_sl(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    sid = evt["strategy_id"]
    level = _as_int(evt.get("level"))
    left_qty = _as_decimal(evt.get("left_qty") or "0")

    if level is None or left_qty is None or left_qty <= 0:
        log.debug("⚠️ post_tp_sl_apply: некорректный evt=%s", evt)
        return

    entry = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, side, avg_fill_price
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='entry'
        ORDER BY id DESC LIMIT 1
        """,
        position_uid
    )
    if not entry:
        log.debug("⚠️ post_tp_sl_apply: нет entry для uid=%s", position_uid)
        return

    symbol = str(entry["symbol"])
    entry_side = str(entry["side"] or "").upper()  # BUY/SELL
    direction = "long" if entry_side == "BUY" else "short"
    avg_fill = _as_decimal(entry["avg_fill_price"])
    if not avg_fill or avg_fill <= 0:
        log.debug("⚠️ post_tp_sl_apply: avg_fill пуст (uid=%s)", position_uid)
        return

    pol = config.strategy_policy.get(sid) or {}
    post = (pol.get("tp_sl_by_level") or {}).get(level)
    if not isinstance(post, dict):
        log.debug("⚠️ post_tp_sl_apply: нет политики для level=%s sid=%s", level, sid)
        return
    sl_mode = post.get("sl_mode")
    sl_value = _as_decimal(post.get("sl_value"))

    t = config.tickers.get(symbol) or {}
    ticksize = _as_decimal(t.get("ticksize"))
    precision_qty = t.get("precision_qty")

    trigger_price = _compute_sl_after_tp(avg_fill, direction, sl_mode, sl_value, ticksize)
    left_qty = _round_qty(left_qty, precision_qty)
    if left_qty <= 0:
        log.debug("ℹ️ post_tp_sl_apply: остаток уже 0 (uid=%s)", position_uid)
        return

    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] post_tp_sl_apply: %s L=%s → trigger=%s qty=%s", symbol, level, _fmt(trigger_price), _fmt(left_qty))
        return

    await _cancel_active_orders_for_uid(position_uid=position_uid, symbol=symbol, kinds=("sl",))

    new_link = f"{position_uid}-sl-after-tp-{level}"
    ok_s, oid_s, rc_s, rm_s = await _submit_sl(
        symbol=symbol,
        side=_to_title_side(_side_word(_opposite(direction))),
        trigger_price=trigger_price,
        qty=left_qty,
        link_id=new_link,
        trigger_direction=_calc_trigger_direction(direction),
    )
    await _mark_order_after_submit(order_link_id=new_link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)


async def _handle_final_flatten_force(evt: Dict[str, Any]) -> None:
    """
    Безусловное доведение до нуля при любом закрытии в системе:
      1) отменить активные TP/SL;
      2) посчитать left_qty;
      3) если >0 — reduceOnly Market на left_qty (после отмен — ещё раз проверить left_qty перед сабмитом).
    """
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]

    entry = await infra.pg_pool.fetchrow(
        """
        SELECT symbol, side
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='entry'
        ORDER BY id DESC LIMIT 1
        """,
        position_uid
    )
    if not entry:
        log.debug("ℹ️ flatten_force: нет entry для uid=%s", position_uid)
        return

    symbol = str(entry["symbol"])
    entry_side = str(entry["side"] or "").upper()
    direction = "long" if entry_side == "BUY" else "short"

    # отменяем активные TP/SL
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] flatten_force: cancel active TP/SL uid=%s", position_uid)
    else:
        await _cancel_active_orders_for_uid(position_uid=position_uid, symbol=symbol, kinds=("tp", "sl"))

    # первый расчёт остатка
    left_before = await _calc_left_qty_for_uid(position_uid)
    # небольшой микрослип, чтобы WS успел обновить filled (не обязателен, но полезен)
    await asyncio.sleep(0.05)
    # повторный расчёт после отмен
    left_qty = await _calc_left_qty_for_uid(position_uid)

    log.debug("flatten_force: left_before=%s left_after=%s uid=%s", _fmt(left_before), _fmt(left_qty), position_uid)

    if not left_qty or left_qty <= 0:
        log.info("flatten_force: уже flat (uid=%s)", position_uid)
        return

    # сабмитим reduceOnly market на остаток
    link_id = f"{position_uid}-flatten"
    qty = left_qty

    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] flatten_force submit: %s reduceOnly market qty=%s link=%s", symbol, _fmt(qty), link_id)
        return

    # upsert «close»-ордер в нашу таблицу, чтобы bybit_sync мог его отслеживать
    await _upsert_order(
        position_uid=position_uid,
        kind="close",
        level=None,
        exchange="BYBIT",
        symbol=symbol,
        side=_side_word(_opposite(direction)),
        otype="market",
        tif="GTC",
        reduce_only=True,
        price=None,
        trigger_price=None,
        qty=qty,
        order_link_id=link_id,
        ext_status="planned",
        qty_raw=qty,
        price_raw=None,
        calc_type=None,
        calc_value=None,
        base_price=None,
        base_kind="fill",
        activation_tp_level=None,
        trigger_by=None,
        supersedes_link_id=None,
    )

    ok_c, oid_c, rc_c, rm_c = await _submit_entry(
        symbol=symbol,
        side=_to_title_side(_side_word(_opposite(direction))),
        qty=qty,
        link_id=link_id,
        reduce_only=True,
    )
    await _mark_order_after_submit(order_link_id=link_id, ok=ok_c, order_id=oid_c, retcode=rc_c, retmsg=rm_c)
    log.info("flatten_force: submit reduceOnly close %s qty=%s ok=%s", symbol, _fmt(qty), ok_c)


# 🔸 Bybit REST helpers

def _rest_sign(ts_ms: int, query_or_body: str) -> str:
    import hmac, hashlib
    payload = f"{ts_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)

async def _bybit_post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"
    ts = _now_ms()
    body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
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

async def _cancel_by_link(*, symbol: str, link_id: str) -> Tuple[bool, Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/cancel", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    ok = (rc == 0)
    return ok, rc, rm

async def _submit_tp(*, symbol: str, side: str, price: Decimal, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,  # "Buy" | "Sell"
        "orderType": "Limit",
        "price": _str_price(price),
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.debug("MAINT TP: %s price=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", symbol, _str_price(price), _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_sl(
    *,
    symbol: str,
    side: str,                     # "Buy" | "Sell"
    trigger_price: Decimal,
    qty: Decimal,
    link_id: str,
    trigger_direction: int,        # 1=rise, 2=fall
) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": _str_qty(qty),
        "reduceOnly": True,
        "triggerPrice": _str_price(trigger_price),
        "triggerDirection": trigger_direction,
        "triggerBy": DEFAULT_TRIGGER_BY,
        "closeOnTrigger": True,
        "timeInForce": "GTC",
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.debug("MAINT SL: %s trig=%s dir=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", symbol, _str_price(trigger_price), trigger_direction, _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

async def _submit_entry(
    *,
    symbol: str,
    side: str,
    qty: Decimal,
    link_id: str,
    reduce_only: bool,
) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,  # "Buy" | "Sell"
        "orderType": "Market",
        "qty": _str_qty(qty),
        "timeInForce": "GTC",
        "reduceOnly": bool(reduce_only),
        "orderLinkId": link_id,
    }
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    ok = (rc == 0)
    log.debug("MAINT CLOSE: %s qty=%s linkId=%s → rc=%s msg=%s oid=%s", symbol, _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

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


# 🔸 Утилиты БД

async def _upsert_order(
    *,
    position_uid: str,
    kind: str,
    level: Optional[int],
    exchange: str,
    symbol: str,
    side: Optional[str],
    otype: Optional[str],
    tif: str,
    reduce_only: bool,
    price: Optional[Decimal],
    trigger_price: Optional[Decimal],
    qty: Decimal,
    order_link_id: str,
    ext_status: str,
    qty_raw: Optional[Decimal],
    price_raw: Optional[Decimal],
    calc_type: Optional[str],
    calc_value: Optional[Decimal],
    base_price: Optional[Decimal],
    base_kind: Optional[str],
    activation_tp_level: Optional[int],
    trigger_by: Optional[str],
    supersedes_link_id: Optional[str],
) -> None:
    side_norm = None if side is None else side.upper()
    otype_norm = None if otype is None else otype.lower()

    await infra.pg_pool.execute(
        """
        INSERT INTO public.trader_position_orders (
            position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
            price, trigger_price, qty, order_link_id, ext_status,
            qty_raw, price_raw,
            calc_type, calc_value, base_price, base_kind, activation_tp_level, trigger_by, supersedes_link_id
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
                $10,$11,$12,$13,$14,
                $15,$16,
                $17,$18,$19,$20,$21,$22,$23)
        ON CONFLICT (order_link_id) DO UPDATE SET
            position_uid        = EXCLUDED.position_uid,
            kind                = EXCLUDED.kind,
            level               = EXCLUDED.level,
            exchange            = EXCLUDED.exchange,
            symbol              = EXCLUDED.symbol,
            side                = EXCLUDED.side,
            "type"              = EXCLUDED."type",
            tif                 = EXCLUDED.tif,
            reduce_only         = EXCLUDED.reduce_only,
            price               = EXCLUDED.price,
            trigger_price       = EXCLUDED.trigger_price,
            qty                 = EXCLUDED.qty,
            ext_status          = EXCLUDED.ext_status,
            qty_raw             = EXCLUDED.qty_raw,
            price_raw           = EXCLUDED.price_raw,
            calc_type           = EXCLUDED.calc_type,
            calc_value          = EXCLUDED.calc_value,
            base_price          = EXCLUDED.base_price,
            base_kind           = EXCLUDED.base_kind,
            activation_tp_level = EXCLUDED.activation_tp_level,
            trigger_by          = EXCLUDED.trigger_by,
            supersedes_link_id  = EXCLUDED.supersedes_link_id,
            error_last          = NULL
        """,
        position_uid, kind, level, exchange, symbol, side_norm, otype_norm, tif, reduce_only,
        price, trigger_price, qty, order_link_id, ext_status,
        qty_raw, price_raw,
        calc_type, calc_value, base_price, base_kind, activation_tp_level, trigger_by, supersedes_link_id
    )

async def _cancel_active_orders_for_uid(*, position_uid: str, symbol: str, kinds: Tuple[str, ...]) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, kind, ext_status
        FROM public.trader_position_orders
        WHERE position_uid = $1
          AND kind = ANY ($2::text[])
          AND ext_status IN ('submitted','accepted','partially_filled')
        """,
        position_uid, list(kinds)
    )
    if not rows:
        return
    for r in rows:
        link = str(r["order_link_id"])
        ok, rc, rm = await _cancel_by_link(symbol=symbol, link_id=link)
        log.debug("cancel %s: link=%s → ok=%s rc=%s msg=%s", r["kind"], link, ok, rc, rm)


# 🔸 Парсеры/форматеры
def _parse_event(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    def _g(k: str) -> Optional[str]:
        v = raw.get(k) if k in raw else raw.get(k.encode(), None)
        return v.decode() if isinstance(v, (bytes, bytearray)) else (v if isinstance(v, str) else None)

    etype = _g("type")
    uid = _g("position_uid")
    sid = _as_int(_g("strategy_id"))
    if not etype or not uid or sid is None:
        return None

    evt: Dict[str, Any] = {
        "type": etype,
        "position_uid": uid,
        "strategy_id": sid,
        "ts": _g("ts"),
        "dedupe": _g("dedupe"),
    }
    lvl = _as_int(_g("level"))
    if lvl is not None:
        evt["level"] = lvl
    evt["order_link_id"] = _g("order_link_id")
    evt["left_qty"] = _g("left_qty")
    evt["reason"] = _g("reason")
    evt["ex_price"] = _g("ex_price")
    evt["ex_qty"] = _g("ex_qty")
    return evt

def _as_int(s: Optional[str]) -> Optional[int]:
    try:
        return int(s) if s not in (None, "", "None") else None
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

def _fmt(x: Optional[Decimal], max_prec: int = 8) -> str:
    if x is None:
        return "—"
    try:
        s = f"{x:.{max_prec}f}".rstrip("0").rstrip(".")
        return s if s else "0"
    except Exception:
        return str(x)

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

def _to_title_side(side: str) -> str:
    s = (side or "").upper()
    return "Buy" if s == "BUY" else "Sell"

def _side_word(direction: str) -> str:
    return "BUY" if (direction or "").lower() == "long" else "SELL"

def _opposite(direction: Optional[str]) -> str:
    d = (direction or "").lower()
    return "short" if d == "long" else "long"

def _calc_trigger_direction(position_direction: str) -> int:
    d = (position_direction or "").lower()
    return 2 if d == "long" else 1

def _compute_sl_after_tp(
    avg_fill: Decimal,
    direction: str,
    sl_mode: Optional[str],
    sl_value: Optional[Decimal],
    ticksize: Optional[Decimal],
) -> Decimal:
    if sl_mode == "entry" or sl_mode == "atr":
        price = avg_fill
    elif sl_mode == "percent" and sl_value is not None:
        if direction == "long":
            price = avg_fill * (Decimal("1") - sl_value / Decimal("100"))
        else:
            price = avg_fill * (Decimal("1") + sl_value / Decimal("100"))
    else:
        price = avg_fill
    return _round_price(price, ticksize)

def _str_qty(q: Decimal) -> str:
    return _fmt(q)

def _str_price(p: Decimal) -> str:
    return _fmt(p)

# 🔸 Расчёт остатка позиции по uid (entry − tp − close)
async def _calc_left_qty_for_uid(uid: str) -> Optional[Decimal]:
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
        uid
    )
    try:
        return Decimal(str(row["left_qty"])) if row and row["left_qty"] is not None else None
    except Exception:
        return None