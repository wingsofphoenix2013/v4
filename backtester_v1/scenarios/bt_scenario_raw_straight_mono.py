# bt_scenario_raw_straight_mono.py — raw straight-сценарий (mono): 1 сигнал = 1 позиция, без контроля по тикеру и без общего депозита (run-aware, инкрементально)

import logging
import uuid
import json
from datetime import datetime, timedelta, date
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import get_signal_instance, get_ticker_info

log = logging.getLogger("BT_SCENARIO_RAW_MONO")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
BT_SCENARIOS_READY_STREAM = "bt:scenarios:ready"

# 🔸 Комиссия (как в исходной логике)
COMMISSION_RATE = Decimal("0.0015")  # 0.15% вход+выход

# 🔸 Таймшаги TF (в минутах) для decision_time
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = str(timeframe or "").strip().lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Утилита: обрезка денег/метрик до 4 знаков после запятой
def _q_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Обрезка по precision_price (без глобального 0.0001)
def _quant_price(value: Decimal, precision_price: Optional[int]) -> Decimal:
    if precision_price is None:
        precision_price = 8
    try:
        p_dec = int(precision_price)
    except Exception:
        p_dec = 8
    quant = Decimal("1").scaleb(-p_dec)
    return value.quantize(quant, rounding=ROUND_DOWN)


# 🔸 Приведение цены к precision_price и ticksize
def _round_price(
    price: Decimal,
    precision_price: Optional[int],
    ticksize: Optional[Decimal],
) -> Decimal:
    # сначала обрезка по precision_price
    price = _quant_price(price, precision_price)

    # затем обрезка по ticksize, если есть
    if ticksize is not None and ticksize > Decimal("0"):
        steps = (price / ticksize).to_integral_value(rounding=ROUND_DOWN)
        price = steps * ticksize

    # после снапа к тиксайзу ещё раз приводим к precision_price
    price = _quant_price(price, precision_price)
    return price


# 🔸 Публичная точка входа: backfill для сценария raw_straight_mono по одному окну сигнала (run-aware, инкрементально)
async def run_raw_straight_mono_backfill(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур
) -> None:
    scenario_id = int(scenario.get("id") or 0)
    scenario_key = scenario.get("key")
    scenario_type = scenario.get("type")
    params = scenario.get("params") or {}

    signal_id = int(signal_ctx.get("signal_id") or 0)
    run_id = signal_ctx.get("run_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    # условия достаточности
    if not scenario_id or not signal_id or not isinstance(from_time, datetime) or not isinstance(to_time, datetime):
        log.error(
            "BT_SCENARIO_RAW_MONO: недостаточно данных контекста — scenario_id=%s, signal_id=%s, run_id=%s, from_time=%s, to_time=%s",
            scenario_id,
            signal_id,
            run_id,
            from_time,
            to_time,
        )
        return

    if run_id is None:
        log.error(
            "BT_SCENARIO_RAW_MONO: отсутствует run_id в signal_ctx для scenario_id=%s, signal_id=%s — сценарий не будет выполнен",
            scenario_id,
            signal_id,
        )
        return

    try:
        run_id_i = int(run_id)
    except Exception:
        log.error(
            "BT_SCENARIO_RAW_MONO: некорректный run_id=%s для scenario_id=%s, signal_id=%s — сценарий не будет выполнен",
            run_id,
            scenario_id,
            signal_id,
        )
        return

    # базовые параметры сценария
    try:
        direction_mode = (params["direction"]["value"] or "").strip().lower()
        deposit = Decimal(str(params["deposit"]["value"]))
        leverage = Decimal(str(params["leverage"]["value"]))
        sl_type = (params["sl_type"]["value"] or "").strip().lower()
        sl_value = Decimal(str(params["sl_value"]["value"]))
        tp_type = (params["tp_type"]["value"] or "").strip().lower()
        tp_value = Decimal(str(params["tp_value"]["value"]))
        position_limit = Decimal(str(params["position_limit"]["value"]))
    except Exception as e:
        log.error(
            "BT_SCENARIO_RAW_MONO: сценарий id=%s — некорректные параметры: %s",
            scenario_id,
            e,
            exc_info=True,
        )
        return

    if direction_mode != "mono":
        log.warning(
            "BT_SCENARIO_RAW_MONO: сценарий id=%s ожидает direction='mono', получено '%s'",
            scenario_id,
            direction_mode,
        )

    if sl_type != "percent" or tp_type != "percent":
        log.error(
            "BT_SCENARIO_RAW_MONO: сценарий id=%s поддерживает только sl_type/tp_type='percent', получено sl_type='%s', tp_type='%s'",
            scenario_id,
            sl_type,
            tp_type,
        )
        return

    signal_instance = get_signal_instance(signal_id)
    if not signal_instance:
        log.error(
            "BT_SCENARIO_RAW_MONO: не найден инстанс сигнала id=%s в кеше, сценарий id=%s",
            signal_id,
            scenario_id,
        )
        return

    timeframe = str(signal_instance.get("timeframe") or "").strip().lower()
    if timeframe not in ("m5", "m15", "h1"):
        log.error(
            "BT_SCENARIO_RAW_MONO: scenario_id=%s, signal_id=%s — неподдерживаемый timeframe='%s'",
            scenario_id,
            signal_id,
            timeframe,
        )
        return

    # decision_time = entry_time + TF
    tf_delta = _get_timeframe_timedelta(timeframe)
    if tf_delta <= timedelta(0):
        log.error(
            "BT_SCENARIO_RAW_MONO: неизвестный TF для decision_time (timeframe=%s), scenario_id=%s, signal_id=%s",
            timeframe,
            scenario_id,
            signal_id,
        )
        return

    # допустимые направления (если сигнал моно-направленный — не симулируем статистику для другой стороны)
    allowed_directions: List[str] = ["long", "short"]
    try:
        sig_params = signal_instance.get("params") or {}
        dm_cfg = sig_params.get("direction_mask")
        dm_val = str((dm_cfg or {}).get("value") or "").strip().lower()
        if dm_val in ("long", "short"):
            allowed_directions = [dm_val]
    except Exception:
        allowed_directions = ["long", "short"]

    # граница для инкрементальной досимуляции open позиций (предыдущий run.to_time)
    # для live-mirror сигналов prev_to_time берём по зеркальному backfill сигналу (mirror_signal_id)
    prev_to_time_signal_id = signal_id
    try:
        sig_params = signal_instance.get("params") or {}
        ms_cfg = sig_params.get("mirror_signal_id")
        if ms_cfg is not None:
            ms_val = ms_cfg.get("value")
            if ms_val is not None:
                prev_to_time_signal_id = int(ms_val)
    except Exception:
        prev_to_time_signal_id = signal_id

    prev_to_time = await _load_previous_run_to_time(pg, prev_to_time_signal_id, run_id_i)
    if prev_to_time is None:
        prev_to_time = from_time

    log.debug(
        "BT_SCENARIO_RAW_MONO: старт сценария id=%s (key=%s, type=%s) для signal_id=%s, run_id=%s, TF=%s, окно=[%s .. %s], prev_to_time=%s (from_signal_id=%s)",
        scenario_id,
        scenario_key,
        scenario_type,
        signal_id,
        run_id_i,
        timeframe,
        from_time,
        to_time,
        prev_to_time,
        prev_to_time_signal_id,
    )

    affected_days: Set[date] = set()

    total_open_before = 0
    total_open_closed_now = 0
    total_open_still_open = 0

    total_new_signals = 0
    total_positions_created_open = 0
    total_positions_created_closed = 0
    total_skipped = 0
    total_errors = 0

    # 🔸 1) Досимулируем и закрываем open позиции (если закрылись в новом хвосте)
    for direction in allowed_directions:
        open_positions = await _load_open_positions(pg, scenario_id, signal_id, timeframe, direction)
        total_open_before += len(open_positions)

        for pos in open_positions:
            try:
                closed = await _try_close_open_position(
                    pg=pg,
                    pos=pos,
                    timeframe=timeframe,
                    direction=direction,
                    scan_from_time=prev_to_time,
                    scan_to_time=to_time,
                    run_id=run_id_i,
                )
                if closed is None:
                    total_open_still_open += 1
                    continue

                affected_days.add(closed["exit_time"].date())
                total_open_closed_now += 1

            except Exception as e:
                total_errors += 1
                log.error(
                    "BT_SCENARIO_RAW_MONO: ошибка досимуляции open позиции id=%s: %s",
                    pos.get("id"),
                    e,
                    exc_info=True,
                )

    # 🔸 2) Обрабатываем новые сигналы (необработанные этим сценарием), открываем позицию на каждый сигнал и симулируем до to_time
    signals = await _load_signals_for_scenario(pg, scenario_id, signal_id, timeframe, from_time, to_time)
    total_new_signals = len(signals)

    if signals:
        signals.sort(key=lambda s: s["open_time"])

    # в RAW-режиме: НЕТ контроля "тикер уже в позиции", НЕТ контроля общего депозита/маржи
    for s_row in signals:
        try:
            symbol = s_row["symbol"]
            open_time = s_row["open_time"]
            signal_uuid = s_row["signal_uuid"]
            direction = str(s_row.get("direction") or "").strip().lower()
            raw_message = s_row["raw_message"]

            if direction not in ("long", "short"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: invalid direction")
                total_skipped += 1
                continue

            # если сигнал моно-направленный — пропускаем чужие направления
            if direction not in allowed_directions:
                await _append_log_row(pg, signal_uuid, scenario_id, None, f"skipped: direction not allowed ({direction})")
                total_skipped += 1
                continue

            # decision_time берём из сигнала, если есть; иначе вычисляем
            decision_time = s_row.get("decision_time") or (open_time + tf_delta)

            # цена входа из raw_message
            try:
                if isinstance(raw_message, dict):
                    entry_price_val = raw_message.get("price")
                else:
                    raw_dict = json.loads(raw_message)
                    entry_price_val = raw_dict.get("price")

                entry_price = Decimal(str(entry_price_val))
            except Exception:
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: invalid raw_message price")
                total_skipped += 1
                continue

            if entry_price <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: non-positive entry price")
                total_skipped += 1
                continue

            # загрузка настроек тикера
            ticker_info = get_ticker_info(symbol) or {}
            min_qty_val = ticker_info.get("min_qty")
            precision_qty = ticker_info.get("precision_qty")
            precision_price = ticker_info.get("precision_price")
            ticksize_val = ticker_info.get("ticksize")

            try:
                min_qty = Decimal(str(min_qty_val)) if min_qty_val is not None else Decimal("0")
            except Exception:
                min_qty = Decimal("0")

            try:
                ticksize = Decimal(str(ticksize_val)) if ticksize_val is not None else None
            except Exception:
                ticksize = None

            # выравниваем цену входа по тикеру
            entry_price = _round_price(entry_price, precision_price, ticksize)

            # фиксированная маржа на сделку = position_limit (без учёта общего депозита)
            margin_used = _q_money(position_limit)
            if margin_used <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: position_limit <= 0")
                total_skipped += 1
                continue

            # notional
            entry_notional = margin_used * leverage
            entry_notional = _q_money(entry_notional)

            if entry_notional <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: notional <= 0")
                total_skipped += 1
                continue

            # qty
            qty_raw = entry_notional / entry_price

            if precision_qty is not None:
                try:
                    q_dec = int(precision_qty)
                except Exception:
                    q_dec = 0
                quant = Decimal("1").scaleb(-q_dec)
                entry_qty = qty_raw.quantize(quant, rounding=ROUND_DOWN)
            else:
                entry_qty = qty_raw

            if entry_qty <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: qty <= 0 after rounding")
                total_skipped += 1
                continue

            if entry_qty < min_qty:
                await _append_log_row(
                    pg,
                    signal_uuid,
                    scenario_id,
                    None,
                    f"skipped: qty below min_qty (qty={entry_qty}, min_qty={min_qty})",
                )
                total_skipped += 1
                continue

            # пересчёт notional по округлённому qty (чтобы было консистентно в позиции)
            entry_notional = _q_money(entry_price * entry_qty)
            if entry_notional <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: notional <= 0 after qty rounding")
                total_skipped += 1
                continue

            # расчёт уровней SL/TP в процентах
            sl_price, tp_price = _calc_sl_tp_percent(
                entry_price=entry_price,
                sl_percent=sl_value,
                tp_percent=tp_value,
                direction=direction,
            )

            # приводим цены SL/TP к precision_price и ticksize
            sl_price = _round_price(sl_price, precision_price, ticksize)
            tp_price = _round_price(tp_price, precision_price, ticksize)

            if sl_price <= Decimal("0") or tp_price <= Decimal("0"):
                await _append_log_row(pg, signal_uuid, scenario_id, None, "skipped: invalid SL/TP price after rounding")
                total_skipped += 1
                continue

            # симуляция сделки до to_time
            sim = await _simulate_trade_full(
                pg=pg,
                symbol=symbol,
                timeframe=timeframe,
                direction=direction,
                entry_time=open_time,
                entry_price=entry_price,
                entry_qty=entry_qty,
                entry_notional=entry_notional,
                sl_price=sl_price,
                tp_price=tp_price,
                to_time=to_time,
            )

            position_uid = uuid.uuid4()

            if sim is None:
                # создаём open позицию
                await _insert_position_open(
                    pg=pg,
                    position_uid=position_uid,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    signal_uuid=signal_uuid,
                    created_run_id=run_id_i,
                    symbol=symbol,
                    timeframe=timeframe,
                    direction=direction,
                    entry_time=open_time,
                    decision_time=decision_time,
                    entry_price=entry_price,
                    entry_qty=entry_qty,
                    entry_notional=entry_notional,
                    margin_used=margin_used,
                    sl_price=sl_price,
                    tp_price=tp_price,
                )
                await _append_log_row(pg, signal_uuid, scenario_id, str(position_uid), "position opened (status=open)")
                total_positions_created_open += 1
                continue

            (
                exit_time,
                exit_price,
                exit_reason,
                pnl_abs,
                duration,
                max_fav_pct,
                max_adv_pct,
            ) = sim

            # создаём closed позицию
            await _insert_position_closed(
                pg=pg,
                position_uid=position_uid,
                scenario_id=scenario_id,
                signal_id=signal_id,
                signal_uuid=signal_uuid,
                created_run_id=run_id_i,
                closed_run_id=run_id_i,
                symbol=symbol,
                timeframe=timeframe,
                direction=direction,
                entry_time=open_time,
                decision_time=decision_time,
                entry_price=entry_price,
                entry_qty=entry_qty,
                entry_notional=entry_notional,
                margin_used=margin_used,
                sl_price=sl_price,
                tp_price=tp_price,
                exit_time=exit_time,
                exit_price=exit_price,
                exit_reason=exit_reason,
                pnl_abs=pnl_abs,
                duration=duration,
                max_fav_pct=max_fav_pct,
                max_adv_pct=max_adv_pct,
            )
            await _append_log_row(pg, signal_uuid, scenario_id, str(position_uid), "position opened (status=closed)")
            total_positions_created_closed += 1
            affected_days.add(exit_time.date())

        except Exception as e:
            total_errors += 1
            log.error(
                "BT_SCENARIO_RAW_MONO: ошибка обработки сигнала (scenario_id=%s, signal_id=%s, run_id=%s): %s",
                scenario_id,
                signal_id,
                run_id_i,
                e,
                exc_info=True,
            )

    # 🔸 3) Пересчёт daily по дням закрытия (status='closed' + exit_time::date)
    if affected_days:
        await _recalc_daily_stats(pg, scenario_id, signal_id, deposit, affected_days, allowed_directions)

    # 🔸 4) Пересчёт bt_scenario_stat: all_time + run (по закрытым сделкам)
    await _recalc_total_stats_all_time_and_run(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        deposit=deposit,
        run_id=run_id_i,
        run_from=from_time,
        run_to=to_time,
        directions=allowed_directions,
    )

    log.info(
        "BT_SCENARIO_RAW_MONO: summary scenario_id=%s, signal_id=%s, run_id=%s — "
        "open_before=%s, open_closed_now=%s, open_still_open=%s, new_signals=%s, created_open=%s, created_closed=%s, skipped=%s, errors=%s",
        scenario_id,
        signal_id,
        run_id_i,
        total_open_before,
        total_open_closed_now,
        total_open_still_open,
        total_new_signals,
        total_positions_created_open,
        total_positions_created_closed,
        total_skipped,
        total_errors,
    )

    # 🔸 5) Событие готовности сценария (run-aware)
    finished_at = datetime.utcnow()
    try:
        await redis.xadd(
            BT_SCENARIOS_READY_STREAM,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "run_id": str(run_id_i),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_SCENARIO_RAW_MONO: опубликовано событие готовности сценария в стрим '%s' для scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s",
            BT_SCENARIOS_READY_STREAM,
            scenario_id,
            signal_id,
            run_id_i,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SCENARIO_RAW_MONO: не удалось опубликовать событие в стрим '%s' для scenario_id=%s, signal_id=%s, run_id=%s: %s",
            BT_SCENARIOS_READY_STREAM,
            scenario_id,
            signal_id,
            run_id_i,
            e,
            exc_info=True,
        )


# 🔸 Получить to_time предыдущего успешного run для данного signal_id
async def _load_previous_run_to_time(pg, signal_id: int, run_id: int) -> Optional[datetime]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT to_time
            FROM bt_signal_backfill_runs
            WHERE signal_id = $1
              AND id < $2
              AND status = 'success'
            ORDER BY id DESC
            LIMIT 1
            """,
            signal_id,
            run_id,
        )
    if not row:
        return None
    return row["to_time"]


# 🔸 Загрузка сигналов для сценария (только необработанные этим сценарием)
async def _load_signals_for_scenario(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                v.signal_uuid,
                v.symbol,
                v.timeframe,
                v.open_time,
                v.decision_time,
                v.direction,
                v.raw_message
            FROM bt_signals_values v
            LEFT JOIN bt_signals_log l
              ON l.signal_uuid = v.signal_uuid
             AND l.scenario_id = $2
            WHERE v.signal_id = $1
              AND v.timeframe = $3
              AND v.open_time BETWEEN $4 AND $5
              AND l.id IS NULL
            ORDER BY v.open_time
            """,
            signal_id,
            scenario_id,
            timeframe,
            from_time,
            to_time,
        )

    signals: List[Dict[str, Any]] = []
    for r in rows:
        signals.append(
            {
                "signal_uuid": r["signal_uuid"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "open_time": r["open_time"],
                "decision_time": r["decision_time"],
                "direction": r["direction"],
                "raw_message": r["raw_message"],
            }
        )

    log.debug(
        "BT_SCENARIO_RAW_MONO: загружено новых сигналов для scenario_id=%s, signal_id=%s, TF=%s в окне [%s .. %s]: %s",
        scenario_id,
        signal_id,
        timeframe,
        from_time,
        to_time,
        len(signals),
    )
    return signals


# 🔸 Загрузка open позиций (для досимуляции)
async def _load_open_positions(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                position_uid,
                symbol,
                entry_time,
                decision_time,
                entry_price,
                entry_qty,
                entry_notional,
                sl_price,
                tp_price
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND timeframe   = $3
              AND direction   = $4
              AND status      = 'open'
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            timeframe,
            direction,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "position_uid": r["position_uid"],
                "symbol": r["symbol"],
                "entry_time": r["entry_time"],
                "decision_time": r["decision_time"],
                "entry_price": Decimal(str(r["entry_price"])),
                "entry_qty": Decimal(str(r["entry_qty"])),
                "entry_notional": Decimal(str(r["entry_notional"])),
                "sl_price": Decimal(str(r["sl_price"])),
                "tp_price": Decimal(str(r["tp_price"])),
            }
        )
    return out


# 🔸 Расчёт SL/TP в процентах от цены входа
def _calc_sl_tp_percent(
    entry_price: Decimal,
    sl_percent: Decimal,
    tp_percent: Decimal,
    direction: str,
) -> Tuple[Decimal, Decimal]:
    if direction == "long":
        sl_price = entry_price * (Decimal("1") - sl_percent / Decimal("100"))
        tp_price = entry_price * (Decimal("1") + tp_percent / Decimal("100"))
    else:
        sl_price = entry_price * (Decimal("1") + sl_percent / Decimal("100"))
        tp_price = entry_price * (Decimal("1") - tp_percent / Decimal("100"))

    return sl_price, tp_price


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m5":
        return "ohlcv_bb_m5"
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None


# 🔸 Найти закрытие сделки (TP/SL) в диапазоне (scan_from .. scan_to]
async def _find_exit_in_range(
    pg,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_price: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    scan_from: datetime,
    scan_to: datetime,
) -> Optional[Tuple[datetime, Decimal, str]]:
    table_name = _ohlcv_table_for_timeframe(timeframe)
    if not table_name:
        return None

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, high, low
            FROM {table_name}
            WHERE symbol = $1
              AND open_time > $2
              AND open_time <= $3
            ORDER BY open_time
            """,
            symbol,
            scan_from,
            scan_to,
        )

    if not rows:
        return None

    for r in rows:
        otime = r["open_time"]
        high = Decimal(str(r["high"]))
        low = Decimal(str(r["low"]))

        if direction == "long":
            touched_sl = low <= sl_price
            touched_tp = high >= tp_price

            if touched_sl and touched_tp:
                return otime, sl_price, "sl_after_tp"
            if touched_sl:
                return otime, sl_price, "full_sl_hit"
            if touched_tp:
                return otime, tp_price, "full_tp_hit"

        else:
            touched_sl = high >= sl_price
            touched_tp = low <= tp_price

            if touched_sl and touched_tp:
                return otime, sl_price, "sl_after_tp"
            if touched_sl:
                return otime, sl_price, "full_sl_hit"
            if touched_tp:
                return otime, tp_price, "full_tp_hit"

    return None


# 🔸 Посчитать PnL/MFE/MAE/duration для уже известного закрытия (полный диапазон от entry_time до exit_time)
async def _compute_closed_trade_stats(
    pg,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    exit_time: datetime,
    exit_price: Decimal,
    exit_reason: str,
) -> Tuple[Decimal, timedelta, Decimal, Decimal]:
    table_name = _ohlcv_table_for_timeframe(timeframe)
    if not table_name:
        raw_pnl = Decimal("0")
        commission = _q_money(entry_notional * COMMISSION_RATE)
        pnl_abs = _q_money(raw_pnl - commission)
        duration = exit_time - entry_time
        return pnl_abs, duration, Decimal("0"), Decimal("0")

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, high, low
            FROM {table_name}
            WHERE symbol = $1
              AND open_time > $2
              AND open_time <= $3
            ORDER BY open_time
            """,
            symbol,
            entry_time,
            exit_time,
        )

    max_fav = Decimal("0")
    max_adv = Decimal("0")

    for r in rows:
        high = Decimal(str(r["high"]))
        low = Decimal(str(r["low"]))

        if direction == "long":
            fav_move = high - entry_price
            adv_move = low - entry_price
        else:
            fav_move = entry_price - low
            adv_move = entry_price - high

        if fav_move > max_fav:
            max_fav = fav_move
        if adv_move < max_adv:
            max_adv = adv_move

    if direction == "long":
        raw_pnl = (exit_price - entry_price) * entry_qty
    else:
        raw_pnl = (entry_price - exit_price) * entry_qty

    raw_pnl = _q_money(raw_pnl)

    commission = _q_money(entry_notional * COMMISSION_RATE)
    pnl_abs = _q_money(raw_pnl - commission)

    duration = exit_time - entry_time

    if entry_price > Decimal("0"):
        max_fav_pct = _q_money((max_fav / entry_price) * Decimal("100"))
        max_adv_pct = _q_money((max_adv / entry_price) * Decimal("100"))
    else:
        max_fav_pct = Decimal("0")
        max_adv_pct = Decimal("0")

    return pnl_abs, duration, max_fav_pct, max_adv_pct


# 🔸 Полная симуляция сделки до to_time (для новых сигналов)
async def _simulate_trade_full(
    pg,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    to_time: datetime,
) -> Optional[Tuple[datetime, Decimal, str, Decimal, timedelta, Decimal, Decimal]]:
    exit_info = await _find_exit_in_range(
        pg=pg,
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        entry_price=entry_price,
        sl_price=sl_price,
        tp_price=tp_price,
        scan_from=entry_time,
        scan_to=to_time,
    )

    if exit_info is None:
        return None

    exit_time, exit_price, exit_reason = exit_info

    pnl_abs, duration, max_fav_pct, max_adv_pct = await _compute_closed_trade_stats(
        pg=pg,
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        entry_time=entry_time,
        entry_price=entry_price,
        entry_qty=entry_qty,
        entry_notional=entry_notional,
        sl_price=sl_price,
        tp_price=tp_price,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=exit_reason,
    )

    return exit_time, exit_price, exit_reason, pnl_abs, duration, max_fav_pct, max_adv_pct


# 🔸 Попытка закрыть open позицию (сканируем только хвост; при закрытии считаем полные метрики и обновляем БД)
async def _try_close_open_position(
    pg,
    pos: Dict[str, Any],
    timeframe: str,
    direction: str,
    scan_from_time: datetime,
    scan_to_time: datetime,
    run_id: int,
) -> Optional[Dict[str, Any]]:
    pos_id = int(pos["id"])
    symbol = str(pos["symbol"])
    entry_time: datetime = pos["entry_time"]

    entry_price: Decimal = pos["entry_price"]
    entry_qty: Decimal = pos["entry_qty"]
    entry_notional: Decimal = pos["entry_notional"]
    sl_price: Decimal = pos["sl_price"]
    tp_price: Decimal = pos["tp_price"]

    # scan_from_time не должен быть раньше entry_time
    if scan_from_time < entry_time:
        scan_from_time = entry_time

    exit_info = await _find_exit_in_range(
        pg=pg,
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        entry_price=entry_price,
        sl_price=sl_price,
        tp_price=tp_price,
        scan_from=scan_from_time,
        scan_to=scan_to_time,
    )

    if exit_info is None:
        return None

    exit_time, exit_price, exit_reason = exit_info

    pnl_abs, duration, max_fav_pct, max_adv_pct = await _compute_closed_trade_stats(
        pg=pg,
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        entry_time=entry_time,
        entry_price=entry_price,
        entry_qty=entry_qty,
        entry_notional=entry_notional,
        sl_price=sl_price,
        tp_price=tp_price,
        exit_time=exit_time,
        exit_price=exit_price,
        exit_reason=exit_reason,
    )

    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE bt_scenario_positions
            SET status = 'closed',
                closed_run_id = $2,
                exit_time = $3,
                exit_price = $4,
                exit_reason = $5,
                pnl_abs = $6,
                duration = $7,
                max_favorable_excursion = $8,
                max_adverse_excursion = $9
            WHERE id = $1
            """,
            pos_id,
            int(run_id),
            exit_time,
            exit_price,
            exit_reason,
            pnl_abs,
            duration,
            max_fav_pct,
            max_adv_pct,
        )

    return {
        "id": pos_id,
        "symbol": symbol,
        "exit_time": exit_time,
        "exit_reason": exit_reason,
        "pnl_abs": pnl_abs,
    }


# 🔸 Вставка open позиции
async def _insert_position_open(
    pg,
    position_uid: uuid.UUID,
    scenario_id: int,
    signal_id: int,
    signal_uuid: uuid.UUID,
    created_run_id: int,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    decision_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    margin_used: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bt_scenario_positions (
                position_uid,
                scenario_id,
                signal_id,
                signal_uuid,
                created_run_id,
                symbol,
                timeframe,
                direction,
                entry_time,
                decision_time,
                entry_price,
                entry_qty,
                entry_notional,
                margin_used,
                sl_price,
                tp_price,
                status,
                postproc,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10,
                $11, $12, $13, $14, $15, $16,
                'open',
                false,
                now()
            )
            """,
            str(position_uid),
            scenario_id,
            signal_id,
            signal_uuid,
            created_run_id,
            symbol,
            timeframe,
            direction,
            entry_time,
            decision_time,
            entry_price,
            entry_qty,
            entry_notional,
            margin_used,
            sl_price,
            tp_price,
        )


# 🔸 Вставка closed позиции
async def _insert_position_closed(
    pg,
    position_uid: uuid.UUID,
    scenario_id: int,
    signal_id: int,
    signal_uuid: uuid.UUID,
    created_run_id: int,
    closed_run_id: int,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    decision_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    margin_used: Decimal,
    sl_price: Decimal,
    tp_price: Decimal,
    exit_time: datetime,
    exit_price: Decimal,
    exit_reason: str,
    pnl_abs: Decimal,
    duration: timedelta,
    max_fav_pct: Decimal,
    max_adv_pct: Decimal,
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bt_scenario_positions (
                position_uid,
                scenario_id,
                signal_id,
                signal_uuid,
                created_run_id,
                closed_run_id,
                symbol,
                timeframe,
                direction,
                entry_time,
                decision_time,
                entry_price,
                entry_qty,
                entry_notional,
                margin_used,
                sl_price,
                tp_price,
                status,
                exit_time,
                exit_price,
                exit_reason,
                pnl_abs,
                duration,
                max_favorable_excursion,
                max_adverse_excursion,
                postproc,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11,
                $12, $13, $14, $15, $16, $17,
                'closed',
                $18, $19, $20,
                $21, $22, $23, $24,
                false,
                now()
            )
            """,
            str(position_uid),
            scenario_id,
            signal_id,
            signal_uuid,
            created_run_id,
            closed_run_id,
            symbol,
            timeframe,
            direction,
            entry_time,
            decision_time,
            entry_price,
            entry_qty,
            entry_notional,
            margin_used,
            sl_price,
            tp_price,
            exit_time,
            exit_price,
            exit_reason,
            pnl_abs,
            duration,
            max_fav_pct,
            max_adv_pct,
        )


# 🔸 Запись строки в bt_signals_log (маркер обработки сигнала сценарием)
async def _append_log_row(
    pg,
    signal_uuid: uuid.UUID,
    scenario_id: int,
    position_uid: Optional[str],
    report: str,
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bt_signals_log (signal_uuid, scenario_id, position_uid, report, created_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (scenario_id, signal_uuid) DO NOTHING
            """,
            signal_uuid,
            scenario_id,
            position_uid,
            report,
        )


# 🔸 Пересчёт суточной статистики по затронутым дням (day = exit_time::date, только closed)
async def _recalc_daily_stats(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Decimal,
    days: Set[date],
    directions: List[str],
) -> None:
    if not days:
        return

    async with pg.acquire() as conn:
        for d in sorted(days):
            for direction in directions:
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*)                                         AS trades,
                        COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                        COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total,
                        COALESCE(AVG(max_favorable_excursion), 0)        AS mfe_avg,
                        COALESCE(AVG(max_adverse_excursion), 0)          AS mae_avg
                    FROM bt_scenario_positions
                    WHERE scenario_id = $1
                      AND signal_id   = $2
                      AND status      = 'closed'
                      AND exit_time::date = $3
                      AND direction   = $4
                    """,
                    scenario_id,
                    signal_id,
                    d,
                    direction,
                )

                trades = int(row["trades"] or 0) if row else 0
                if trades <= 0:
                    continue

                wins = int(row["wins"] or 0)
                pnl_abs_total = Decimal(str(row["pnl_abs_total"]))
                mfe_avg = Decimal(str(row["mfe_avg"]))
                mae_avg = Decimal(str(row["mae_avg"]))

                winrate = _q_money(Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")
                roi = _q_money(pnl_abs_total / deposit) if deposit != 0 else Decimal("0")

                await conn.execute(
                    """
                    INSERT INTO bt_scenario_daily (
                        scenario_id,
                        signal_id,
                        day,
                        direction,
                        trades,
                        pnl_abs,
                        winrate,
                        roi,
                        max_favorable_excursion_avg,
                        max_adverse_excursion_avg,
                        raw_stat,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10,
                        NULL,
                        now()
                    )
                    ON CONFLICT (scenario_id, signal_id, day, direction) DO UPDATE
                    SET
                        trades                      = EXCLUDED.trades,
                        pnl_abs                     = EXCLUDED.pnl_abs,
                        winrate                     = EXCLUDED.winrate,
                        roi                         = EXCLUDED.roi,
                        max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                        max_adverse_excursion_avg   = EXCLUDED.max_adverse_excursion_avg,
                        updated_at                  = now()
                    """,
                    scenario_id,
                    signal_id,
                    d,
                    direction,
                    trades,
                    _q_money(pnl_abs_total),
                    winrate,
                    roi,
                    _q_money(mfe_avg),
                    _q_money(mae_avg),
                )


# 🔸 Пересчёт bt_scenario_stat: all_time + run (только закрытые сделки)
async def _recalc_total_stats_all_time_and_run(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Decimal,
    run_id: int,
    run_from: datetime,
    run_to: datetime,
    directions: List[str],
) -> None:
    async with pg.acquire() as conn:
        for direction in directions:
            # run-stat: закрыто в этом окне (по entry_time в окне и статусу closed)
            row_run = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)                                         AS trades,
                    COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                    COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total,
                    COALESCE(AVG(max_favorable_excursion), 0)        AS mfe_avg,
                    COALESCE(AVG(max_adverse_excursion), 0)          AS mae_avg
                FROM bt_scenario_positions
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND direction   = $3
                  AND status      = 'closed'
                  AND entry_time BETWEEN $4 AND $5
                """,
                scenario_id,
                signal_id,
                direction,
                run_from,
                run_to,
            )

            trades_run = int(row_run["trades"] or 0) if row_run else 0
            wins_run = int(row_run["wins"] or 0) if row_run else 0
            pnl_run = Decimal(str(row_run["pnl_abs_total"])) if row_run else Decimal("0")
            mfe_run = Decimal(str(row_run["mfe_avg"])) if row_run else Decimal("0")
            mae_run = Decimal(str(row_run["mae_avg"])) if row_run else Decimal("0")

            winrate_run = _q_money(Decimal(wins_run) / Decimal(trades_run)) if trades_run > 0 else Decimal("0")
            roi_run = _q_money(pnl_run / deposit) if deposit != 0 else Decimal("0")

            await conn.execute(
                """
                INSERT INTO bt_scenario_stat (
                    scenario_id,
                    signal_id,
                    direction,
                    stat_kind,
                    run_id,
                    window_from,
                    window_to,
                    first_run_id,
                    last_run_id,
                    trades,
                    pnl_abs,
                    winrate,
                    roi,
                    max_favorable_excursion_avg,
                    max_adverse_excursion_avg,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3,
                    'run',
                    $4,
                    $5,
                    $6,
                    $4,
                    $4,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    NULL,
                    now()
                )
                ON CONFLICT (scenario_id, signal_id, direction, stat_kind) DO UPDATE
                SET
                    run_id                      = EXCLUDED.run_id,
                    window_from                 = EXCLUDED.window_from,
                    window_to                   = EXCLUDED.window_to,
                    first_run_id                = EXCLUDED.first_run_id,
                    last_run_id                 = EXCLUDED.last_run_id,
                    trades                      = EXCLUDED.trades,
                    pnl_abs                     = EXCLUDED.pnl_abs,
                    winrate                     = EXCLUDED.winrate,
                    roi                         = EXCLUDED.roi,
                    max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                    max_adverse_excursion_avg   = EXCLUDED.max_adverse_excursion_avg,
                    raw_stat                    = EXCLUDED.raw_stat,
                    updated_at                  = now()
                """,
                scenario_id,
                signal_id,
                direction,
                run_id,
                run_from,
                run_to,
                trades_run,
                _q_money(pnl_run),
                winrate_run,
                roi_run,
                _q_money(mfe_run),
                _q_money(mae_run),
            )

            # all-time: все закрытые
            row_all = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)                                         AS trades,
                    COUNT(*) FILTER (WHERE pnl_abs > 0)              AS wins,
                    COALESCE(SUM(pnl_abs), 0)                        AS pnl_abs_total,
                    COALESCE(AVG(max_favorable_excursion), 0)        AS mfe_avg,
                    COALESCE(AVG(max_adverse_excursion), 0)          AS mae_avg,
                    COALESCE(MIN(created_run_id), $4)                AS first_run_id
                FROM bt_scenario_positions
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND direction   = $3
                  AND status      = 'closed'
                """,
                scenario_id,
                signal_id,
                direction,
                run_id,
            )

            trades_all = int(row_all["trades"] or 0) if row_all else 0
            wins_all = int(row_all["wins"] or 0) if row_all else 0
            pnl_all = Decimal(str(row_all["pnl_abs_total"])) if row_all else Decimal("0")
            mfe_all = Decimal(str(row_all["mfe_avg"])) if row_all else Decimal("0")
            mae_all = Decimal(str(row_all["mae_avg"])) if row_all else Decimal("0")
            first_run_id = int(row_all["first_run_id"] or run_id) if row_all else run_id

            # window_from берём по первому run_id (если не найден — используем run_from)
            first_bounds = await conn.fetchrow(
                """
                SELECT from_time
                FROM bt_signal_backfill_runs
                WHERE id = $1
                """,
                first_run_id,
            )
            all_from = first_bounds["from_time"] if first_bounds and first_bounds["from_time"] is not None else run_from
            all_to = run_to

            winrate_all = _q_money(Decimal(wins_all) / Decimal(trades_all)) if trades_all > 0 else Decimal("0")
            roi_all = _q_money(pnl_all / deposit) if deposit != 0 else Decimal("0")

            await conn.execute(
                """
                INSERT INTO bt_scenario_stat (
                    scenario_id,
                    signal_id,
                    direction,
                    stat_kind,
                    run_id,
                    window_from,
                    window_to,
                    first_run_id,
                    last_run_id,
                    trades,
                    pnl_abs,
                    winrate,
                    roi,
                    max_favorable_excursion_avg,
                    max_adverse_excursion_avg,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3,
                    'all_time',
                    NULL,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    $13,
                    NULL,
                    now()
                )
                ON CONFLICT (scenario_id, signal_id, direction, stat_kind) DO UPDATE
                SET
                    run_id                      = EXCLUDED.run_id,
                    window_from                 = EXCLUDED.window_from,
                    window_to                   = EXCLUDED.window_to,
                    first_run_id                = EXCLUDED.first_run_id,
                    last_run_id                 = EXCLUDED.last_run_id,
                    trades                      = EXCLUDED.trades,
                    pnl_abs                     = EXCLUDED.pnl_abs,
                    winrate                     = EXCLUDED.winrate,
                    roi                         = EXCLUDED.roi,
                    max_favorable_excursion_avg = EXCLUDED.max_favorable_excursion_avg,
                    max_adverse_excursion_avg   = EXCLUDED.max_adverse_excursion_avg,
                    raw_stat                    = EXCLUDED.raw_stat,
                    updated_at                  = now()
                """,
                scenario_id,
                signal_id,
                direction,
                all_from,
                all_to,
                first_run_id,
                run_id,
                trades_all,
                _q_money(pnl_all),
                winrate_all,
                roi_all,
                _q_money(mfe_all),
                _q_money(mae_all),
            )