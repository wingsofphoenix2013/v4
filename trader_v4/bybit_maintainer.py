# bybit_maintainer.py — сопровождение позиции на бирже «exchange-first»:
# TP1→SL@entry, догрузка SL/TP при частичных fill entry, финализация при SL/size=0/manual; идемпотентные действия

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple, List

import httpx

from trader_infra import infra
from trader_config import config  # для доступа к политике стратегии (volume_percent, sl-параметры)

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
        sid = int(r["strategy_id"]) if r["strategy_id"] is not None else None
        symbol = _as_str(r["symbol"])
        if not uid or not symbol:
            continue

        got = await _with_lock(uid)
        if not got:
            continue

        try:
            # 0) Догрузка SL/TP при росте entry.filled до TP1 (пока TP1 не filled)
            await _reconcile_before_tp1(uid, symbol, sid)

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


# 🔸 Догрузка SL-старт и TP-1 при росте entry.filled (до наступления TP-1)
async def _reconcile_before_tp1(position_uid: str, symbol: str, strategy_id: Optional[int]) -> None:
    # Если TP1 уже имеет filled > 0 — этап догрузки SL-старт заканчивается; управляет ветка SL@entry
    tp1_filled = await _tp1_filled_qty(position_uid)
    if tp1_filled and tp1_filled > Decimal("0"):
        return

    entry_filled = await _entry_filled_qty(position_uid)
    if entry_filled is None or entry_filled <= Decimal("0"):
        return

    # точности по тикеру
    tkr = await _load_ticker_precisions(symbol)
    precision_qty = tkr.get("precision_qty")
    min_qty = tkr.get("min_qty")
    ticksize = tkr.get("ticksize")

    # 0. Добыть политику (volume_percent TP1 и SL-старт)
    tp1_vol_pct = _get_tp1_volume_percent(strategy_id)
    sl_type, sl_value = _get_sl_policy(strategy_id)

    # 1. SL-старт: привести qty к entry_filled (если существует активный SL-старт)
    await _ensure_sl_start_qty(position_uid, symbol, entry_filled, precision_qty, min_qty, ticksize, sl_type, sl_value)

    # 2. TP-1: дожать до target = round(entry_filled * vol_pct)
    target_tp1 = _round_qty((entry_filled * (tp1_vol_pct / Decimal("100"))), precision_qty)
    committed_tp1 = await _tp1_committed_qty(position_uid)  # filled + активные нефинальные qty
    need = target_tp1 - committed_tp1
    if need is not None and need > Decimal("0"):
        if min_qty is not None and need < min_qty:
            return  # слишком мало для дозаливки
        # цена TP1 — по существующему tp-1 (если есть), иначе посчитаем от avg fill
        price_tp1 = await _tp1_price_or_compute(position_uid, symbol, strategy_id, ticksize)
        if price_tp1 is None:
            return
        # создаём дополнительный лимит на недостающее количество
        add_link = await _next_tp1_add_link(position_uid)
        side_title = _to_title_side("SELL" if (await _get_direction(position_uid) or "") == "long" else "BUY")

        # вставка planned в TPO
        await infra.pg_pool.execute(
            """
            INSERT INTO public.trader_position_orders (
              position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
              price, trigger_price, qty, order_link_id, ext_status,
              qty_raw, price_raw, calc_type, calc_value, base_price, base_kind, created_at
            )
            VALUES ($1,'tp',1,'BYBIT',$2,$3,'limit','GTC',true,
                    $4,NULL,$5,$6,'planned',
                    $5,$4,'percent',$7,$8,'fill', $9)
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, symbol, _side_word(await _get_direction(position_uid)),
            price_tp1, need, add_link, Decimal("1"),  # calc_value=1% как параметр цены
            await _fetch_entry_avg_fill_price(position_uid), datetime.utcnow()
        )

        # submit
        if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
            ok, oid, rc, rm = await _submit_tp(symbol=symbol, side=side_title, price=price_tp1, qty=need, link_id=add_link)
            await _mark_order_after_submit(order_link_id=add_link, ok=ok, order_id=oid, retcode=rc, retmsg=rm)
        else:
            await infra.pg_pool.execute(
                "UPDATE public.trader_position_orders SET ext_status='submitted', last_ext_event_at=$2 WHERE order_link_id=$1",
                add_link, datetime.utcnow()
            )
        log.info("[MAINT] TP1 top-up: uid=%s sym=%s committed=%s → target=%s (+%s)",
                 position_uid, symbol, _fmt(committed_tp1), _fmt(target_tp1), _fmt(need))


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
        log.info("[MAINT] skip SL@entry: qty_left(%s) < min_qty(%s) uid=%s", _fmt(target_qty), _fmt(min_qty), position_uid)
        return

    log.info("[MAINT] TP1 detected → activate SL@entry: uid=%s symbol=%s left=%s → target_qty=%s",
             position_uid, symbol, _fmt(left), _fmt(target_qty))

    # отменяем текущие активные SL (если есть)
    for sl in active_sls:
        link = _as_str(sl["order_link_id"])
        if not link:
            continue
        await _intent_mark_cancel(link)
        await _cancel_order_by_link(symbol, link)

    # триггер и linkId
    trig = _as_decimal(sl_after["trigger_price"])
    if trig is None:
        entry_avg = await _fetch_entry_avg_fill_price(position_uid)
        trig = entry_avg

    direction = await _get_direction(position_uid)
    trig_dir = _calc_trigger_direction(direction)

    # берём link из шаблона, при необходимости укорачиваем и обновляем TPO
    old_link = _as_str(sl_after["order_link_id"]) or f"{position_uid}-sl-after-tp-1"
    short_link = await _ensure_short_tpo_link(position_uid, old_link, short_suffix="sla1")

    log.info("[MAINT] submit SL-after-TP1: link=%s trigger=%s qty=%s",
             short_link, _fmt(_round_price(trig, ticksize)), _fmt(target_qty))

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


# 🔸 Приведение SL-старта к текущему объёму до TP1
async def _ensure_sl_start_qty(position_uid: str,
                               symbol: str,
                               entry_filled: Decimal,
                               precision_qty: Optional[int],
                               min_qty: Optional[Decimal],
                               ticksize: Optional[Decimal],
                               sl_type: Optional[str],
                               sl_value: Optional[Decimal]) -> None:
    # активные реальные SL БЕЗ activation_tp_level (стартовый)
    row = await infra.pg_pool.fetchrow(
        """
        SELECT id, order_link_id, qty, trigger_price
        FROM public.trader_position_orders
        WHERE position_uid=$1 AND kind='sl'
          AND activation_tp_level IS NULL
          AND "type" IS NOT NULL
          AND (ext_status IS NULL OR ext_status NOT IN ('canceled','filled','expired','rejected'))
        ORDER BY id DESC LIMIT 1
        """,
        position_uid
    )
    if not row:
        return

    current_qty = _as_decimal(row["qty"]) or Decimal("0")
    desired = _round_qty(entry_filled, precision_qty)
    if desired <= current_qty:
        return
    if min_qty is not None and desired < min_qty:
        return

    # отмена текущего SL-старта
    link = _as_str(row["order_link_id"])
    await _intent_mark_cancel(link)
    await _cancel_order_by_link(symbol, link)

    # вычисление триггера: берём существующий, если есть; иначе от политики
    trig = _as_decimal(row["trigger_price"])
    if trig is None:
        entry_avg = await _fetch_entry_avg_fill_price(position_uid)
        trig = _compute_sl_from_policy(entry_avg, await _get_direction(position_uid), sl_type, sl_value, ticksize)

    # сабмит нового SL-старта с укороченным link
    new_link = await _next_sl_start_link(position_uid)
    side_title = _to_title_side("SELL" if (await _get_direction(position_uid) or "") == "long" else "BUY")
    if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
        ok, oid, rc, rm = await _submit_sl(
            symbol=symbol,
            side=side_title,
            trigger_price=_round_price(trig, ticksize),
            qty=desired,
            link_id=new_link,
            trigger_direction=_calc_trigger_direction(await _get_direction(position_uid)),
        )
        await _mark_order_after_submit(order_link_id=new_link, ok=ok, order_id=oid, retcode=rc, retmsg=rm)
    else:
        # вставим planned строку и пометим submitted
        await infra.pg_pool.execute(
            """
            INSERT INTO public.trader_position_orders (
              position_uid, kind, level, exchange, symbol, side, "type", tif, reduce_only,
              price, trigger_price, qty, order_link_id, ext_status, created_at,
              qty_raw, base_price, base_kind, calc_type, calc_value
            )
            VALUES ($1,'sl',NULL,'BYBIT',$2,$3,'stop_market','GTC',true,
                    NULL,$4,$5,$6,'submitted',$7,
                    $5,$8,'fill',$9,$10)
            ON CONFLICT (order_link_id) DO NOTHING
            """,
            position_uid, symbol, _side_word(await _get_direction(position_uid)),
            trig, desired, new_link, datetime.utcnow(),
            await _fetch_entry_avg_fill_price(position_uid),
            'percent' if sl_type == 'percent' else (sl_type or None),
            sl_value
        )

    log.info("[MAINT] SL-start qty adjusted: uid=%s sym=%s %s → %s", position_uid, symbol, _fmt(current_qty), _fmt(desired))


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


# 🔸 Хелперы политики/расчётов/стоимостей
def _get_tp1_volume_percent(strategy_id: Optional[int]) -> Decimal:
    try:
        pol = config.strategy_policy.get(int(strategy_id)) if strategy_id is not None else None
        if not pol:
            return Decimal("50")
        for t in (pol.get("tp_levels") or []):
            if int(t.get("level", 0)) == 1:
                vp = t.get("volume_percent")
                if vp is None:
                    return Decimal("50")
                return Decimal(str(vp))
        return Decimal("50")
    except Exception:
        return Decimal("50")

def _get_sl_policy(strategy_id: Optional[int]) -> Tuple[Optional[str], Optional[Decimal]]:
    try:
        pol = config.strategy_policy.get(int(strategy_id)) if strategy_id is not None else None
        if not pol:
            return "percent", Decimal("1")
        s = pol.get("sl") or {}
        st = s.get("type")
        sv = s.get("value")
        return (str(st) if st else "percent", Decimal(str(sv)) if sv is not None else Decimal("1"))
    except Exception:
        return "percent", Decimal("1")

async def _entry_filled_qty(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        "SELECT COALESCE(MAX(filled_qty),0) AS fq FROM public.trader_position_orders WHERE position_uid=$1 AND kind='entry'",
        position_uid
    )
    return _as_decimal(row["fq"]) if row else None

async def _tp1_committed_qty(position_uid: str) -> Decimal:
    row = await infra.pg_pool.fetchrow(
        """
        WITH f AS (
          SELECT COALESCE(SUM(filled_qty),0) AS fq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='tp' AND "level"=1
        ),
        a AS (
          SELECT COALESCE(SUM(qty),0) AS qq
          FROM public.trader_position_orders
          WHERE position_uid=$1 AND kind='tp' AND "level"=1
            AND "type"='limit'
            AND (ext_status IS NULL OR ext_status IN ('planned','submitted','accepted','partially_filled'))
        )
        SELECT f.fq + a.qq AS committed
        FROM f,a
        """,
        position_uid
    )
    return _as_decimal(row["committed"]) or Decimal("0")

async def _tp1_price_or_compute(position_uid: str, symbol: str, strategy_id: Optional[int], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    # попробуем взять цену из существующего tp-1
    row = await infra.pg_pool.fetchrow(
        "SELECT price FROM public.trader_position_orders WHERE position_uid=$1 AND kind='tp' AND \"level\"=1 AND price IS NOT NULL ORDER BY id DESC LIMIT 1",
        position_uid
    )
    if row and row["price"] is not None:
        return _round_price(_as_decimal(row["price"]), ticksize)
    # иначе рассчитаем от avg fill и политики (percent/atr)
    avg = await _fetch_entry_avg_fill_price(position_uid)
    if avg is None:
        return None
    # в реверс-шаблоне tp_type всегда percent (+1%), но возьмём из политики для универсальности
    pol = config.strategy_policy.get(int(strategy_id)) if strategy_id is not None else None
    tp_type = None
    tp_value = None
    if pol:
        for t in (pol.get("tp_levels") or []):
            if int(t.get("level", 0)) == 1:
                tp_type = t.get("tp_type")
                tp_value = _as_decimal(t.get("tp_value"))
                break
    if tp_type is None:
        tp_type = "percent"; tp_value = Decimal("1")
    direction = await _get_direction(position_uid)
    return _round_price(_compute_tp_price_from_policy(avg, direction, tp_type, tp_value, ticksize), ticksize)


# 🔸 Финализация позиции: отмена остаточных TP/SL, закрытие в портфеле/БД
async def _finalize_position(position_uid: str, symbol: str, reason: str) -> None:
    now = datetime.utcnow()

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

    left = await _calc_left_qty(position_uid)
    if left is not None and left > Decimal("0"):
        direction = await _get_direction(position_uid)
        side_title = _to_title_side("SELL" if (direction or "").lower() == "long" else "BUY")
        close_link = _make_short_link(position_uid, "cls")
        if TRADER_ORDER_MODE == "on" and API_KEY and API_SECRET:
            ok_c, oid_c, rc_c, rm_c = await _submit_close_market(symbol, side_title, _round_qty(left, await _precision_qty(symbol)), close_link)
            await _mark_order_after_submit(order_link_id=close_link, ok=ok_c, order_id=oid_c, retcode=rc_c, retmsg=rm_c)
        else:
            await infra.pg_pool.execute(
                "INSERT INTO public.trader_position_orders (position_uid, kind, level, exchange, symbol, side, \"type\", tif, reduce_only, price, trigger_price, qty, order_link_id, ext_status, created_at) VALUES ($1,'close',NULL,'BYBIT',$2,NULL,'market','GTC',true,NULL,NULL,$3,$4,'submitted',$5) ON CONFLICT (order_link_id) DO NOTHING",
                position_uid, symbol, _round_qty(left, await _precision_qty(symbol)), close_link, now
            )

    await _set_position_closed(position_uid, reason, now)


# 🔸 Расчётные/REST/локи/форматтеры (общие хелперы)
def _compute_tp_price_from_policy(avg_fill: Decimal, direction: Optional[str], tp_type: Optional[str], tp_value: Optional[Decimal], ticksize: Optional[Decimal]) -> Decimal:
    base = avg_fill
    if (tp_type or "").lower() == "percent" and tp_value is not None:
        if (direction or "").lower() == "long":
            price = base * (Decimal("1") + tp_value / Decimal("100"))
        else:
            price = base * (Decimal("1") - tp_value / Decimal("100"))
    elif (tp_type or "").lower() == "atr":
        price = base
    else:
        price = base
    return _round_price(price, ticksize)

def _compute_sl_from_policy(avg_fill: Optional[Decimal], direction: Optional[str], sl_type: Optional[str], sl_value: Optional[Decimal], ticksize: Optional[Decimal]) -> Optional[Decimal]:
    if avg_fill is None:
        return None
    if (sl_type or "").lower() == "percent" and sl_value is not None:
        if (direction or "").lower() == "long":
            price = avg_fill * (Decimal("1") - sl_value / Decimal("100"))
        else:
            price = avg_fill * (Decimal("1") + sl_value / Decimal("100"))
    else:
        price = avg_fill
    return _round_price(price, ticksize)


# 🔸 Сабмиты/отмена и статусы
async def _submit_tp(*, symbol: str, side: str, price: Decimal, qty: Decimal, link_id: str) -> Tuple[bool, Optional[str], Optional[int], Optional[str]]:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "price": _fmt(price),
        "qty": _fmt(qty),
        "timeInForce": "GTC",
        "reduceOnly": True,
        "orderLinkId": link_id,
    }
    if TRADER_ORDER_MODE != "on" or not API_KEY or not API_SECRET:
        log.info("[DRY_RUN MAINT] submit TP: %s price=%s qty=%s link=%s", symbol, _fmt(price), _fmt(qty), link_id)
        return True, None, None, None
    resp = await _bybit_post("/v5/order/create", body)
    rc, rm = resp.get("retCode"), resp.get("retMsg")
    oid = _extract_order_id(resp)
    log.info("submit TP: %s price=%s qty=%s link=%s → rc=%s msg=%s oid=%s", symbol, _fmt(price), _fmt(qty), link_id, rc, rm, oid)
    return (rc == 0), oid, rc, rm


# 🔸 REST-хелперы
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
    await infra.pg_pool.execute(
        "UPDATE public.trader_position_orders SET order_link_id=$3 WHERE position_uid=$1 AND order_link_id=$2",
        position_uid, current_link, new_link
    )
    log.info("TPO link renamed (too long): %s → %s (uid=%s)", current_link, new_link, position_uid)
    return new_link

async def _next_sl_start_link(position_uid: str) -> str:
    # ищем версии sl / sl-vN
    base = f"{position_uid}-sl"
    rows = await infra.pg_pool.fetch(
        "SELECT order_link_id FROM public.trader_position_orders WHERE position_uid=$1 AND order_link_id LIKE $2",
        position_uid, f"{base}%"
    )
    max_v = 0
    for r in rows:
        lid = _as_str(r["order_link_id"])
        if lid == base:
            max_v = max(max_v, 1)
        elif "-sl-v" in lid:
            try:
                n = int(lid.split("-sl-v", 1)[1]); max_v = max(max_v, n)
            except Exception:
                pass
    link = base if max_v == 0 else f"{position_uid}-sl-v{max_v+1}"
    return _make_short_link(position_uid, link.split(position_uid+"-")[1])

async def _next_tp1_add_link(position_uid: str) -> str:
    base = f"{position_uid}-tp1a"
    rows = await infra.pg_pool.fetch(
        "SELECT order_link_id FROM public.trader_position_orders WHERE position_uid=$1 AND order_link_id LIKE $2",
        position_uid, f"{base}%"
    )
    max_v = 0
    for r in rows:
        lid = _as_str(r["order_link_id"])
        if lid == base:
            max_v = max(max_v, 1)
        elif "-tp1a-v" in lid:
            try:
                n = int(lid.split("-tp1a-v", 1)[1]); max_v = max(max_v, n)
            except Exception:
                pass
    link = base if max_v == 0 else f"{base}-v{max_v+1}"
    return _make_short_link(position_uid, link.split(position_uid+"-")[1])


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

def _side_word(direction: Optional[str]) -> str:
    return "BUY" if (direction or "").lower() == "long" else "SELL"

def _calc_trigger_direction(position_direction: Optional[str]) -> int:
    d = (position_direction or "").lower()
    return 2 if d == "long" else 1