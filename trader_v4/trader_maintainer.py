# trader_maintainer.py — мейнтейнер: гармонизация TP, post-TP SL, принудительный flatten + опциональный аудит «гигиены»

# 🔸 Импорты
import asyncio
import logging
import json
from datetime import datetime, timedelta
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

# 🔸 Аудит (ENV)
MAINT_AUDIT = os.getenv("MAINT_AUDIT", "off").lower() == "on"                 # on|off
MAINT_AUDIT_INTERVAL_SEC = int(os.getenv("MAINT_AUDIT_INTERVAL_SEC", "300"))  # 5 минут по умолчанию
MAINT_AUDIT_START_DELAY_SEC = int(os.getenv("MAINT_AUDIT_START_DELAY_SEC", "120"))
MAINT_AUDIT_STALE_SEC = int(os.getenv("MAINT_AUDIT_STALE_SEC", "90"))         # порог «submitted без order_id»
MAINT_AUDIT_MAX_ORDERS_PER_TICK = int(os.getenv("MAINT_AUDIT_MAX_ORDERS_PER_TICK", "200"))

# 🔸 Активные статусы ордеров
_ACTIVE_EXT: Set[str] = {"submitted", "accepted", "partially_filled"}

# условия достаточности: лог о режимах
if TRADER_ORDER_MODE == "dry_run":
    log.debug("MAINTAINER v1: DRY_RUN (cancel/recreate/SL/flatten — логируем)")
elif TRADER_ORDER_MODE == "off":
    log.debug("MAINTAINER v1: OFF (игнорируем входящие события)")
else:
    log.debug("MAINTAINER v1: ON (гармонизация TP, post-TP SL, принудительный flatten)")

# условия достаточности: лог об аудите
if MAINT_AUDIT:
    log.debug(
        "MAINT_AUDIT: ON (interval=%ss, stale=%ss, start_delay=%ss, max_orders_per_tick=%s)",
        MAINT_AUDIT_INTERVAL_SEC, MAINT_AUDIT_STALE_SEC, MAINT_AUDIT_START_DELAY_SEC, MAINT_AUDIT_MAX_ORDERS_PER_TICK
    )
else:
    log.debug("MAINT_AUDIT: OFF")


# 🔸 Главный воркер событий (event-driven)
async def run_trader_maintainer_loop():
    redis = infra.redis_client

    # подготовка CG
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

    # основной цикл чтения/обработки
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
                        # условия достаточности: валидность события
                        if not evt:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                            continue

                        # фильтр winners
                        sid = evt.get("strategy_id")
                        if sid is None or sid not in config.trader_winners:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                            continue

                        # маршрутизация по типу
                        etype = evt["type"]
                        if etype == "tp_harmonize_needed":
                            await _handle_tp_harmonize(evt)
                        elif etype == "post_tp_sl_apply":
                            await _handle_post_tp_sl(evt)
                        elif etype == "final_flatten_force":
                            await _handle_final_flatten_force(evt)
                        elif etype == "cleanup_after_flat":
                            await _handle_cleanup_after_flat(evt)
                        elif etype == "sl_move_to_entry":
                            await _handle_sl_move_to_entry(evt)
                        else:
                            log.debug("ℹ️ Пропуск неизвестного type=%s evt=%s", etype, evt)

                    except Exception:
                        log.exception("❌ Ошибка обработки события maintainer")
                    finally:
                        # ack в любом случае
                        try:
                            await redis.xack(MAINTAINER_STREAM, CG_NAME, record_id)
                        except Exception:
                            log.exception("❌ Не удалось ACK запись maintainer")

        except Exception:
            log.exception("❌ Ошибка в цикле TRADER_MAINTAINER")
            await asyncio.sleep(1.0)

# 🔸 Периодический аудит «гигиены» (опционально)
async def run_trader_maintainer_audit_loop(force: bool = False):
    # условия достаточности: включён ли аудит (ENV или форс) и есть ли ключи
    if not (MAINT_AUDIT or force):
        return
    if TRADER_ORDER_MODE == "off" or not API_KEY or not API_SECRET:
        log.debug("MAINT_AUDIT: пропуск — OFF-режим или нет API-ключей")
        return

    # отложенный старт
    await asyncio.sleep(MAINT_AUDIT_START_DELAY_SEC)
    log.info("🔎 MAINT_AUDIT стартовал")

    while True:
        try:
            # список всех открытых позиций (дальше фильтруем winners)
            rows = await infra.pg_pool.fetch(
                """
                SELECT p.position_uid, p.strategy_id, p.symbol
                FROM public.trader_positions p
                WHERE p.status='open'
                """
            )

            now = datetime.utcnow()
            checked = 0

            for r in rows:
                uid = str(r["position_uid"])
                sid = int(r["strategy_id"])
                if sid not in config.trader_winners:
                    continue

                # остаток позиции
                left_qty = await _calc_left_qty_for_uid(uid)
                if left_qty is None:
                    continue

                # активные ордера по uid (для быстрой оценки защиты)
                active_orders = await infra.pg_pool.fetch(
                    """
                    SELECT id, kind, "type", order_link_id, order_id, symbol, "level", last_ext_event_at, ext_status
                    FROM public.trader_position_orders
                    WHERE position_uid=$1 AND ext_status = ANY($2::text[])
                    """,
                    uid, list(_ACTIVE_EXT)
                )

                # stale: submitted без order_id дольше порога
                for ao in active_orders:
                    # лимит обработки за тик
                    if checked >= MAINT_AUDIT_MAX_ORDERS_PER_TICK:
                        break
                    if (ao["ext_status"] == "submitted") and (not ao["order_id"]):
                        ts = ao["last_ext_event_at"]
                        if ts and (now - ts) > timedelta(seconds=MAINT_AUDIT_STALE_SEC):
                            present = await _is_order_present_on_exchange(
                                symbol=str(ao["symbol"]),
                                link_id=str(ao["order_link_id"]) if ao["order_link_id"] else None,
                                order_id=str(ao["order_id"]) if ao["order_id"] else None
                            )
                            if not present:
                                await _expire_and_recover(uid, sid, ao)
                            checked += 1

                # активен в БД, но отсутствует на бирже
                for ao in active_orders:
                    if checked >= MAINT_AUDIT_MAX_ORDERS_PER_TICK:
                        break
                    present = await _is_order_present_on_exchange(
                        symbol=str(ao["symbol"]),
                        link_id=str(ao["order_link_id"]) if ao["order_link_id"] else None,
                        order_id=str(ao["order_id"]) if ao["order_id"] else None
                    )
                    if not present:
                        await _expire_and_recover(uid, sid, ao)
                    checked += 1

                # нет защиты при left_qty>0: ни активного SL, ни priced TP
                if left_qty > 0:
                    has_sl = any((o["kind"] == "sl") for o in active_orders)
                    has_priced_tp = any((o["kind"] == "tp" and (o["type"] or "").lower() == "limit") for o in active_orders)
                    if not has_sl and not has_priced_tp:
                        ok_rearm = await _rearm_sl(uid, sid)
                        # если SL поставить не удалось — паникуем
                        if not ok_rearm:
                            await _panic_close(uid)

        except Exception:
            log.exception("MAINT_AUDIT: ошибка цикла")

        # интервал аудита
        await asyncio.sleep(MAINT_AUDIT_INTERVAL_SEC)

# 🔸 Гармонизация TP (cancel + recreate)
async def _handle_tp_harmonize(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    order_link_id = evt.get("order_link_id")
    level = _as_int(evt.get("level"))

    # условия достаточности
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

    # dry-run ветка
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] tp_harmonize: cancel+recreate %s L=%s → price=%s qty=%s",
                 symbol, level, _fmt(price_need), _fmt(qty_need))
        return

    ok_c, rc_c, rm_c = await _cancel_by_link(symbol=symbol, link_id=order_link_id)
    log.debug("tp_harmonize: cancel link=%s → ok=%s rc=%s msg=%s", order_link_id, ok_c, rc_c, rm_c)

    ok_t, oid_t, rc_t, rm_t = await _submit_tp(
        symbol=symbol, side=side, price=price_need, qty=qty_need, link_id=order_link_id
    )
    await _mark_order_after_submit(order_link_id=order_link_id, ok=ok_t, order_id=oid_t, retcode=rc_t, retmsg=rm_t)


# 🔸 Применение SL после биржевого TP (policy)
async def _handle_post_tp_sl(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]
    sid = evt["strategy_id"]
    level = _as_int(evt.get("level"))
    left_qty = _as_decimal(evt.get("left_qty") or "0")

    # условия достаточности
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
    entry_side = str(entry["side"] or "").upper()
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

    # точности
    t = config.tickers.get(symbol) or {}
    ticksize = _as_decimal(t.get("ticksize"))
    precision_qty = t.get("precision_qty")

    trigger_price = _compute_sl_after_tp(avg_fill, direction, sl_mode, sl_value, ticksize)
    left_qty = _round_qty(left_qty, precision_qty)
    if left_qty <= 0:
        log.debug("ℹ️ post_tp_sl_apply: остаток уже 0 (uid=%s)", position_uid)
        return

    # dry-run ветка
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] post_tp_sl_apply: %s L=%s → trigger=%s qty=%s",
                 symbol, level, _fmt(trigger_price), _fmt(left_qty))
        return

    # отменяем активные SL и ставим новый
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


# 🔸 Принудительный flatten (reduceOnly market остатка)
async def _handle_final_flatten_force(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    position_uid = evt["position_uid"]

    # входной ордер (для symbol/направления)
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

    # отмена активных TP/SL
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] flatten_force: cancel active TP/SL uid=%s", position_uid)
    else:
        await _cancel_active_orders_for_uid(position_uid=position_uid, symbol=symbol, kinds=("tp", "sl"))

    # первый/второй расчёт остатка
    left_before = await _calc_left_qty_for_uid(position_uid)
    await asyncio.sleep(0.05)
    left_qty = await _calc_left_qty_for_uid(position_uid)
    log.debug("flatten_force: left_before=%s left_after=%s uid=%s", _fmt(left_before), _fmt(left_qty), position_uid)

    # условия достаточности: уже flat
    if not left_qty or left_qty <= 0:
        log.info("flatten_force: уже flat (uid=%s)", position_uid)
        return

    # сабмит reduceOnly market
    link_id = f"{position_uid}-flatten"
    qty = left_qty

    # dry-run ветка
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] flatten_force submit: %s reduceOnly market qty=%s link=%s", symbol, _fmt(qty), link_id)
        return

    # упорный close в БД
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


# 🔸 Перестановка SL на entry (команда sl_move_to_entry от sl_handler)
async def _handle_sl_move_to_entry(evt: Dict[str, Any]) -> None:
    if TRADER_ORDER_MODE == "off":
        return

    uid = evt["position_uid"]
    direction = (evt.get("direction") or "").lower()
    trigger_price = _as_decimal(evt.get("trigger_price"))
    qty_req = _as_decimal(evt.get("qty"))

    # условия достаточности
    if direction not in ("long", "short") or trigger_price is None or qty_req is None or qty_req <= 0:
        log.debug("⚠️ sl_move_to_entry: некорректный evt=%s", evt)
        return

    # symbol из события или из entry
    symbol = evt.get("symbol")
    if not symbol:
        row = await infra.pg_pool.fetchrow(
            """
            SELECT symbol
            FROM public.trader_position_orders
            WHERE position_uid = $1
            ORDER BY id DESC LIMIT 1
            """,
            uid
        )
        if not row or not row["symbol"]:
            log.debug("⚠️ sl_move_to_entry: не найден symbol для uid=%s", uid)
            return
        symbol = str(row["symbol"])

    # если уже flat — выходим
    left_qty = await _calc_left_qty_for_uid(uid)
    if not left_qty or left_qty <= 0:
        log.debug("ℹ️ sl_move_to_entry: uid=%s уже flat", uid)
        return

    # нормализация цены/объёма
    t = config.tickers.get(symbol) or {}
    ticksize = _as_decimal(t.get("ticksize"))
    precision_qty = t.get("precision_qty")
    trig_norm = _round_price(trigger_price, ticksize)
    qty_norm = _round_qty(min(qty_req, left_qty), precision_qty)

    if not qty_norm or qty_norm <= 0:
        log.debug("ℹ️ sl_move_to_entry: qty_norm<=0 (uid=%s)", uid)
        return

    # если уже стоит такой же SL — выходим
    row = await infra.pg_pool.fetchrow(
        """
        SELECT trigger_price, qty
        FROM public.trader_position_orders
        WHERE position_uid = $1 AND kind='sl'
          AND ext_status IN ('submitted','accepted','partially_filled')
        ORDER BY id DESC LIMIT 1
        """,
        uid
    )
    if row:
        cur_trig = _round_price(_as_decimal(row["trigger_price"]), ticksize)
        cur_qty = _round_qty(_as_decimal(row["qty"]) or Decimal("0"), precision_qty)
        if cur_trig == trig_norm and cur_qty == qty_norm:
            log.info("sl_move_to_entry: активный SL уже соответствует (uid=%s)", uid)
            return

    # отменяем активные SL
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] sl_move_to_entry: cancel active SL uid=%s", uid)
    else:
        await _cancel_active_orders_for_uid(position_uid=uid, symbol=symbol, kinds=("sl",))

    # сабмит нового SL reduceOnly на entry (FIX #1 — предварительный upsert)
    new_link = f"{uid}-sl-to-entry"
    if TRADER_ORDER_MODE == "dry_run":
        log.info("[DRY_RUN] sl_move_to_entry submit: %s trigger=%s qty=%s link=%s",
                 symbol, _fmt(trig_norm), _fmt(qty_norm), new_link)
        return

    await _upsert_order(
        position_uid=uid,
        kind="sl",
        level=None,
        exchange="BYBIT",
        symbol=symbol,
        side=_side_word(_opposite(direction)),
        otype="stop_market",
        tif="GTC",
        reduce_only=True,
        price=None,
        trigger_price=trig_norm,
        qty=qty_norm,
        order_link_id=new_link,
        ext_status="planned",
        qty_raw=qty_norm,
        price_raw=None,
        calc_type="manual",
        calc_value=None,
        base_price=None,
        base_kind="fill",
        activation_tp_level=None,
        trigger_by=DEFAULT_TRIGGER_BY,
        supersedes_link_id=None,
    )

    ok_s, oid_s, rc_s, rm_s = await _submit_sl(
        symbol=symbol,
        side=_to_title_side(_side_word(_opposite(direction))),
        trigger_price=trig_norm,
        qty=qty_norm,
        link_id=new_link,
        trigger_direction=_calc_trigger_direction(direction),
    )
    await _mark_order_after_submit(order_link_id=new_link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)
    log.info("sl_move_to_entry: %s → trigger=%s qty=%s ok=%s (uid=%s)",
             symbol, _fmt(trig_norm), _fmt(qty_norm), ok_s, uid)


# 🔸 Expire + восстановление (TP→harmonize, SL→rearm)
async def _expire_and_recover(uid: str, sid: int, ao: Any) -> None:
    order_link_id = str(ao["order_link_id"]) if ao["order_link_id"] else None
    kind = (ao["kind"] or "").lower()
    level = int(ao["level"]) if ao["level"] is not None else None
    symbol = str(ao["symbol"]) if ao["symbol"] else None

    # локально помечаем expired
    await infra.pg_pool.execute(
        """
        UPDATE public.trader_position_orders
        SET ext_status='expired', last_ext_event_at=$2
        WHERE id=$1 AND ext_status = ANY($3::text[])
        """,
        int(ao["id"]), datetime.utcnow(), list(_ACTIVE_EXT)
    )
    log.info("MAINT_AUDIT: expired %s link=%s uid=%s", kind, order_link_id, uid)

    # восстановление по типу
    if kind == "tp" and order_link_id and level is not None:
        try:
            await infra.redis_client.xadd(MAINTAINER_STREAM, {
                "type": "tp_harmonize_needed",
                "position_uid": uid,
                "strategy_id": str(sid),
                "order_link_id": order_link_id,
                "level": str(level),
                "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
                "dedupe": f"{uid}:tp:{level}:harmonize",
            })
            log.info("MAINT_AUDIT → tp_harmonize_needed: uid=%s L=%s", uid, level)
        except Exception:
            log.exception("MAINT_AUDIT: fail emit tp_harmonize_needed uid=%s", uid)
    elif kind == "sl":
        ok = await _rearm_sl(uid, sid)
        if not ok:
            await _panic_close(uid)


# 🔸 Переустановка SL по политике (fallback — на entry)
async def _rearm_sl(uid: str, sid: int) -> bool:
    try:
        # данные по входу
        entry = await infra.pg_pool.fetchrow(
            """
            SELECT symbol, side, avg_fill_price
            FROM public.trader_position_orders
            WHERE position_uid = $1 AND kind='entry'
            ORDER BY id DESC LIMIT 1
            """,
            uid
        )
        if not entry:
            log.debug("MAINT_AUDIT rearm_sl: нет entry для uid=%s", uid)
            return False

        symbol = str(entry["symbol"])
        entry_side = str(entry["side"] or "").upper()
        direction = "long" if entry_side == "BUY" else "short"
        avg_fill = _as_decimal(entry["avg_fill_price"])
        if not avg_fill or avg_fill <= 0:
            log.debug("MAINT_AUDIT rearm_sl: avg_fill пуст uid=%s", uid)
            return False

        # остаток
        left_qty = await _calc_left_qty_for_uid(uid)
        if not left_qty or left_qty <= 0:
            return True  # уже flat

        # точности и политика
        t = config.tickers.get(symbol) or {}
        ticksize = _as_decimal(t.get("ticksize"))
        precision_qty = t.get("precision_qty")
        pol = config.strategy_policy.get(sid) or {}
        base_sl = pol.get("sl") or {}
        sl_type = base_sl.get("type")  # 'percent' | 'atr' | None
        sl_value = _as_decimal(base_sl.get("value"))

        trigger = _compute_sl_after_tp(
            avg_fill=avg_fill,
            direction=direction,
            sl_mode=sl_type if sl_type in ("percent", "atr") else "entry",
            sl_value=sl_value,
            ticksize=ticksize
        )
        qty = _round_qty(left_qty, precision_qty)

        # отменяем активные SL (если есть хвосты)
        await _cancel_active_orders_for_uid(position_uid=uid, symbol=symbol, kinds=("sl",))

        link = f"{uid}-sl-audit-rearm"
        if TRADER_ORDER_MODE == "dry_run":
            log.info("[DRY_RUN] rearm_sl: %s trigger=%s qty=%s link=%s", symbol, _fmt(trigger), _fmt(qty), link)
            return True

        # upsert + submit
        await _upsert_order(
            position_uid=uid,
            kind="sl",
            level=None,
            exchange="BYBIT",
            symbol=symbol,
            side=_side_word(_opposite(direction)),
            otype="stop_market",
            tif="GTC",
            reduce_only=True,
            price=None,
            trigger_price=trigger,
            qty=qty,
            order_link_id=link,
            ext_status="planned",
            qty_raw=qty,
            price_raw=None,
            calc_type=(sl_type if sl_type in ("percent", "atr") else "manual"),
            calc_value=(sl_value if sl_type in ("percent", "atr") else None),
            base_price=avg_fill,
            base_kind="fill",
            activation_tp_level=None,
            trigger_by=DEFAULT_TRIGGER_BY,
            supersedes_link_id=None,
        )
        ok_s, oid_s, rc_s, rm_s = await _submit_sl(
            symbol=symbol,
            side=_to_title_side(_side_word(_opposite(direction))),
            trigger_price=trigger,
            qty=qty,
            link_id=link,
            trigger_direction=_calc_trigger_direction(direction),
        )
        await _mark_order_after_submit(order_link_id=link, ok=ok_s, order_id=oid_s, retcode=rc_s, retmsg=rm_s)
        log.info("MAINT_AUDIT rearm_sl: %s trigger=%s qty=%s ok=%s (uid=%s)", symbol, _fmt(trigger), _fmt(qty), ok_s, uid)
        return bool(ok_s)

    except Exception:
        log.exception("MAINT_AUDIT rearm_sl: ошибка uid=%s", uid)
        return False


# 🔸 Аварийное доведение до нуля (panic-close)
async def _panic_close(uid: str) -> None:
    try:
        sid = await _fetch_sid_for_uid(uid)
        evt = {"type": "final_flatten_force", "position_uid": uid, "strategy_id": sid}
        await _handle_final_flatten_force(evt)
        log.warning("MAINT_AUDIT PANIC_CLOSE: uid=%s", uid)
    except Exception:
        log.exception("MAINT_AUDIT panic_close: ошибка uid=%s", uid)


# 🔸 Вспомогательные выборки/проверки аудита
async def _fetch_sid_for_uid(uid: str) -> Optional[int]:
    row = await infra.pg_pool.fetchrow(
        "SELECT strategy_id FROM public.trader_positions WHERE position_uid=$1",
        uid
    )
    return int(row["strategy_id"]) if row and row["strategy_id"] is not None else None

async def _is_order_present_on_exchange(*, symbol: str, link_id: Optional[str], order_id: Optional[str]) -> bool:
    # условия достаточности: OFF-режим → не считаем отсутствующим
    if TRADER_ORDER_MODE == "off" or not API_KEY or not API_SECRET:
        return True
    try:
        # приоритет поиска — по orderLinkId
        if link_id:
            q = f"category={CATEGORY}&symbol={symbol}&orderLinkId={link_id}"
        elif order_id:
            q = f"category={CATEGORY}&symbol={symbol}&orderId={order_id}"
        else:
            return True
        data = await _bybit_get("/v5/order/realtime", q)
        lst = (data.get("result") or {}).get("list") or []
        return len(lst) > 0
    except Exception:
        log.exception("MAINT_AUDIT: realtime check failed (symbol=%s, link=%s, oid=%s)", symbol, link_id, order_id)
        return True


# 🔸 Bybit REST helpers
def _rest_sign(ts_ms: int, query_or_body: str) -> str:
    import hmac, hashlib
    payload = f"{ts_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)

async def _bybit_get(path: str, query: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}?{query}"
    ts = _now_ms()
    sign = _rest_sign(ts, query)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        try:
            r.raise_for_status()
        except Exception:
            log.warning("⚠️ Bybit GET %s %s: %s", path, r.status_code, r.text)
        try:
            return r.json()
        except Exception:
            return {"retCode": None, "retMsg": "non-json response", "raw": r.text}

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
        "side": side,
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
    side: str,
    trigger_price: Decimal,
    qty: Decimal,
    link_id: str,
    trigger_direction: int,
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
    log.debug("MAINT SL: %s trig=%s dir=%s qty=%s linkId=%s → rc=%s msg=%s oid=%s",
              symbol, _str_price(trigger_price), trigger_direction, _str_qty(qty), link_id, rc, rm, oid)
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
        "side": side,
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


# 🔸 Утилиты БД и форматтеры
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

def _parse_event(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    def _g(k: str) -> Optional[str]:
        v = raw.get(k) if k in raw else raw.get(k.encode(), None)
        return v.decode() if isinstance(v, (bytes, bytearray)) else (v if isinstance(v, str) else None)

    etype = _g("type")
    uid = _g("position_uid")
    sid = _as_int(_g("strategy_id"))
    # условия достаточности: базовая валидация
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
    # условия достаточности: маппинг режима
    if sl_mode == "entry" or sl_mode == "atr":
        price = avg_fill
    elif sl_mode == "percent" and sl_value is not None:
        price = avg_fill * (Decimal("1") - sl_value / Decimal("100")) if direction == "long" \
                else avg_fill * (Decimal("1") + sl_value / Decimal("100"))
    else:
        price = avg_fill
    return _round_price(price, ticksize)

def _str_qty(q: Decimal) -> str:
    return _fmt(q)

def _str_price(p: Decimal) -> str:
    return _fmt(p)

def _extract_order_id(resp: Dict[str, Any]) -> Optional[str]:
    try:
        res = resp.get("result") or {}
        oid = res.get("orderId")
        return str(oid) if oid is not None else None
    except Exception:
        return None

# 🔸 Остаток позиции по uid (entry − tp − close)
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
        c AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='close'
        )
        SELECT e.fq - t.fq - c.fq AS left_qty FROM e,t,c
        """,
        uid
    )
    try:
        return Decimal(str(row["left_qty"])) if row and row["left_qty"] is not None else None
    except Exception:
        return None