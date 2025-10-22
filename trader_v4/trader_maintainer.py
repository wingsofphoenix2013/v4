# trader_maintainer.py — базовый мейнтейнер: гармонизация TP и post-TP SL, безопасная гигиена (v1, event-driven через Redis Stream)

# 🔸 Импорты
import os
import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple, List, Set

import httpx

from trader_infra import infra
from trader_config import config

# 🔸 Логгер
log = logging.getLogger("TRADER_MAINTAINER")

# 🔸 Потоки/группы
MAINTAINER_STREAM = "trader_maintainer_events"
CG_NAME = "trader_maintainer_group"
CONSUMER_NAME = "trader_maintainer_1"

# 🔸 Режимы исполнения (используем общий режим с процессором: on/dry_run/off)
def _normalize_mode(v: Optional[str]) -> str:
    s = (v or "").strip().lower()
    if s in ("off", "false", "0", "no", "disabled"):
        return "off"
    if s in ("dry_run", "dry-run", "dryrun", "test"):
        return "dry_run"
    return "on"

TRADER_ORDER_MODE = _normalize_mode(os.getenv("TRADER_ORDER_MODE"))

# 🔸 Bybit REST (конфигурация окружения)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # USDT-perp
DEFAULT_TRIGGER_BY = os.getenv("BYBIT_TRIGGER_BY", "LastPrice")  # LastPrice | MarkPrice | IndexPrice

# 🔸 Набор «активных» статусов ордеров в нашей БД (те, что реально висят на бирже)
_ACTIVE_EXT: Set[str] = {"submitted", "accepted", "partially_filled"}

# 🔸 Сообщим о режиме
if TRADER_ORDER_MODE == "dry_run":
    log.info("MAINTAINER v1: DRY_RUN (cancel/recreate/SL — только логируем)")
elif TRADER_ORDER_MODE == "off":
    log.info("MAINTAINER v1: OFF (игнорируем входящие события)")
else:
    log.info("MAINTAINER v1: ON (гармонизация TP + post-TP SL через Bybit REST)")

# 🔸 Главный воркер: потребитель событий из стрима maintainer’а
async def run_trader_maintainer_loop():
    redis = infra.redis_client

    # создаём Consumer Group
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

                        # фильтр по winners
                        sid = evt.get("strategy_id")
                        if sid is None or sid not in config.trader_winners:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                            continue

                        etype = evt["type"]
                        if etype == "tp_harmonize_needed":
                            await _handle_tp_harmonize(evt)
                        elif etype == "post_tp_sl_apply":
                            await _handle_post_tp_sl(evt)
                        else:
                            log.info("ℹ️ Пропуск неизвестного type=%s evt=%s", etype, evt)

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
    """
    Гармонизация TP после открытия:
      — берём наш TPO (kind='tp') по order_link_id;
      — сравниваем с биржевыми (evt.ex_price/evt.ex_qty при наличии или текущим TPO-снимком);
      — делаем cancel + recreate c ожидаемыми tpo.price / tpo.qty.
    """
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    order_link_id = evt.get("order_link_id")
    level = _as_int(evt.get("level"))

    if not order_link_id or level is None:
        log.info("⚠️ tp_harmonize: некорректное событие %s", evt)
        return

    # читаем наш TPO
    tpo = await infra.pg_pool.fetchrow(
        """
        SELECT position_uid, symbol, side, price, qty, ext_status
        FROM public.trader_position_orders
        WHERE order_link_id = $1 AND kind='tp' AND "level" = $2
        """,
        order_link_id, level
    )
    if not tpo:
        log.info("ℹ️ tp_harmonize: TPO не найден (link=%s)", order_link_id)
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

    # DRY_RUN
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] tp_harmonize: cancel+recreate %s L=%s → price=%s qty=%s", symbol, level, _fmt(price_need), _fmt(qty_need))
        return

    # cancel старого ордера (по linkId)
    ok_c, rc_c, rm_c = await _cancel_by_link(symbol=symbol, link_id=order_link_id)
    log.info("tp_harmonize: cancel link=%s → ok=%s rc=%s msg=%s", order_link_id, ok_c, rc_c, rm_c)

    # recreate TP с нашими параметрами
    ok_t, oid_t, rc_t, rm_t = await _submit_tp(symbol=symbol, side=side, price=price_need, qty=qty_need, link_id=order_link_id)
    await _mark_order_after_submit(order_link_id=order_link_id, ok=ok_t, order_id=oid_t, retcode=rc_t, retmsg=rm_t)

async def _handle_post_tp_sl(evt: Dict[str, Any]) -> None:
    """
    Постановка post-TP SL:
      — читаем политику для уровня L (tp_sl_by_level);
      — считаем trigger_price от avg_fill entry;
      — отменяем текущий активный SL и создаём новый на left_qty.
    """
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    sid = evt["strategy_id"]
    level = _as_int(evt.get("level"))
    left_qty = _as_decimal(evt.get("left_qty") or "0")

    if level is None or left_qty is None or left_qty <= 0:
        log.info("⚠️ post_tp_sl_apply: некорректный evt=%s", evt)
        return

    # найдём entry (для направления и avg_fill)
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
        log.info("⚠️ post_tp_sl_apply: нет entry для uid=%s", position_uid)
        return

    symbol = str(entry["symbol"])
    entry_side = str(entry["side"] or "").upper()  # BUY/SELL
    direction = "long" if entry_side == "BUY" else "short"
    avg_fill = _as_decimal(entry["avg_fill_price"])
    if not avg_fill or avg_fill <= 0:
        log.info("⚠️ post_tp_sl_apply: avg_fill пуст (uid=%s)", position_uid)
        return

    # политика SL после TP
    pol = config.strategy_policy.get(sid) or {}
    post = (pol.get("tp_sl_by_level") or {}).get(level)
    if not isinstance(post, dict):
        log.info("⚠️ post_tp_sl_apply: нет политики для level=%s sid=%s", level, sid)
        return
    sl_mode = post.get("sl_mode")
    sl_value = _as_decimal(post.get("sl_value"))

    # точности/шаги
    t = config.tickers.get(symbol) or {}
    ticksize = _as_decimal(t.get("ticksize"))
    precision_qty = t.get("precision_qty")

    # целевой триггер
    trigger_price = _compute_sl_after_tp(avg_fill, direction, sl_mode, sl_value, ticksize)
    left_qty = _round_qty(left_qty, precision_qty)
    if left_qty <= 0:
        log.info("ℹ️ post_tp_sl_apply: остаток уже 0 (uid=%s)", position_uid)
        return

    # DRY_RUN
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] post_tp_sl_apply: %s L=%s → trigger=%s qty=%s", symbol, level, _fmt(trigger_price), _fmt(left_qty))
        return

    # отменим текущие активные SL (если есть)
    await _cancel_active_sls_for_uid(position_uid=position_uid, symbol=symbol)

    # создадим новый SL reduceOnly
    new_link = f"{position_uid}-sl-after-tp-{level}"
    ok_s, oid_s, rc_s, rm_s = await _submit_sl(
        symbol=symbol,
        side=_to_title_side(_side_word(_opposite(direction))),  # сторона стопа противоположна направлению позиции
        trigger_price=trigger_price,
        qty=left_qty,
        link_id=new_link,
        trigger_direction=_calc_trigger_direction(direction),
    )
    await _mark_order_after_submit(order_link_id=new_link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)

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
    log.info("MAINT TP: %s price=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", symbol, _str_price(price), _str_qty(qty), link_id, rc, rm, oid)
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
    log.info("MAINT SL: %s trig=%s dir=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s", symbol, _str_price(trigger_price), trigger_direction, _str_qty(qty), link_id, rc, rm, oid)
    return ok, oid, rc, rm

def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    try:
        res = resp.get("result") or {}
        oid = res.get("orderId")
        return str(oid) if oid is not None else None
    except Exception:
        return None

# 🔸 Утилиты БД для SL-гигиены

async def _cancel_active_sls_for_uid(*, position_uid: str, symbol: str) -> None:
    rows = await infra.pg_pool.fetch(
        """
        SELECT order_link_id, ext_status
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='sl'
          AND (ext_status IN ('submitted','accepted','partially_filled'))
        """,
        position_uid
    )
    if not rows:
        return
    for r in rows:
        link = str(r["order_link_id"])
        ok, rc, rm = await _cancel_by_link(symbol=symbol, link_id=link)
        log.info("SL cancel: link=%s → ok=%s rc=%s msg=%s", link, ok, rc, rm)

# 🔸 Вспомогательные парсеры/арифметика

def _parse_event(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # event fields приходят как строки/bytes; приводим и проверяем обязательные
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

    # опциональные/типовые поля
    lvl = _as_int(_g("level"))
    if lvl is not None:
        evt["level"] = lvl
    evt["order_link_id"] = _g("order_link_id")
    evt["left_qty"] = _g("left_qty")
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