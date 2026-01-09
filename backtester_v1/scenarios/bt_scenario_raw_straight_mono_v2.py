# bt_scenario_raw_straight_mono_v2.py — raw straight-сценарий (mono) v2: 1 event = 1 позиция-объект (positions_v2) + membership_v2 (run-aware), без stat/daily

import logging
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import get_signal_instance, get_ticker_info

log = logging.getLogger("BT_SCENARIO_RAW_MONO_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Стрим готовности сценария v2
BT_SCENARIOS_READY_STREAM_V2 = "bt:scenarios:ready_v2"

# 🔸 Комиссия (0.2% вход+выход, как в v1: списываем на entry_notional)
COMMISSION_RATE = Decimal("0.002")

# 🔸 Таблицы v2
BT_POSITIONS_V2_TABLE = "bt_scenario_positions_v2"
BT_MEMBERSHIP_V2_TABLE = "bt_scenario_membership_v2"

# 🔸 Таблицы сигналов (входной датасет)
BT_SIGNAL_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_SIGNAL_EVENTS_TABLE = "bt_signals_values"

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
            str(symbol),
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


# 🔸 Посчитать PnL/MFE/MAE/duration (полный диапазон от entry_time до exit_time)
async def _compute_closed_trade_stats(
    pg,
    symbol: str,
    timeframe: str,
    direction: str,
    entry_time: datetime,
    entry_price: Decimal,
    entry_qty: Decimal,
    entry_notional: Decimal,
    exit_time: datetime,
    exit_price: Decimal,
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
            str(symbol),
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


# 🔸 Загрузка events (входной датасет сигналов) через membership(run_id, signal_id)
async def _load_signal_events_for_run(
    pg,
    signal_id: int,
    run_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                e.id            AS signal_value_id,
                e.symbol        AS symbol,
                e.timeframe     AS timeframe,
                e.open_time     AS open_time,
                e.decision_time AS decision_time,
                e.direction     AS direction,
                e.price         AS price
            FROM {BT_SIGNAL_MEMBERSHIP_TABLE} m
            JOIN {BT_SIGNAL_EVENTS_TABLE} e
              ON e.id = m.signal_value_id
            WHERE m.run_id = $1
              AND m.signal_id = $2
              AND e.timeframe = $3
              AND e.open_time BETWEEN $4 AND $5
            ORDER BY e.open_time
            """,
            int(run_id),
            int(signal_id),
            str(timeframe),
            from_time,
            to_time,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "signal_value_id": int(r["signal_value_id"]),
                "symbol": str(r["symbol"]),
                "timeframe": str(r["timeframe"]),
                "open_time": r["open_time"],
                "decision_time": r["decision_time"],
                "direction": str(r["direction"]),
                "price": r["price"],
            }
        )
    return out


# 🔸 Создание позиции-объекта v2 (если не существует) и получение её id
async def _create_or_get_position_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    signal_value_id: int,
    opened_run_id: int,
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
) -> Tuple[int, bool]:
    position_uid = uuid.uuid4()

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {BT_POSITIONS_V2_TABLE} (
                position_uid,
                scenario_id,
                signal_id,
                signal_value_id,
                opened_run_id,
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
                raw_stat,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10,
                $11, $12, $13, $14, $15, $16,
                'open',
                NULL,
                now()
            )
            ON CONFLICT (scenario_id, signal_id, signal_value_id) DO NOTHING
            RETURNING id
            """,
            str(position_uid),
            int(scenario_id),
            int(signal_id),
            int(signal_value_id),
            int(opened_run_id),
            str(symbol),
            str(timeframe),
            str(direction),
            entry_time,
            decision_time,
            entry_price,
            entry_qty,
            entry_notional,
            margin_used,
            sl_price,
            tp_price,
        )

        if row and row["id"] is not None:
            return int(row["id"]), True

        # конфликт — достаём существующую позицию
        existing_id = await conn.fetchval(
            f"""
            SELECT id
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE scenario_id = $1
              AND signal_id = $2
              AND signal_value_id = $3
            """,
            int(scenario_id),
            int(signal_id),
            int(signal_value_id),
        )

    return int(existing_id), False


# 🔸 Загрузка open позиций v2 для сценария/сигнала (для прогрессивного закрытия)
async def _load_open_positions_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    directions: List[str],
) -> List[Dict[str, Any]]:
    if not directions:
        return []

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                id,
                symbol,
                timeframe,
                direction,
                entry_time,
                entry_price,
                entry_qty,
                entry_notional,
                sl_price,
                tp_price
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE scenario_id = $1
              AND signal_id = $2
              AND timeframe = $3
              AND direction = ANY($4::text[])
              AND status = 'open'
            ORDER BY entry_time
            """,
            int(scenario_id),
            int(signal_id),
            str(timeframe),
            [str(d) for d in directions],
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "symbol": str(r["symbol"]),
                "timeframe": str(r["timeframe"]),
                "direction": str(r["direction"]),
                "entry_time": r["entry_time"],
                "entry_price": Decimal(str(r["entry_price"])),
                "entry_qty": Decimal(str(r["entry_qty"])),
                "entry_notional": Decimal(str(r["entry_notional"])),
                "sl_price": Decimal(str(r["sl_price"])),
                "tp_price": Decimal(str(r["tp_price"])),
            }
        )
    return out


# 🔸 Попытка закрыть open позицию v2 в пределах доступного окна (.. run_to]
async def _try_close_position_v2(
    pg,
    run_id: int,
    position: Dict[str, Any],
    run_to_time: datetime,
) -> Optional[Dict[str, Any]]:
    pos_id = int(position["id"])
    symbol = str(position["symbol"])
    timeframe = str(position["timeframe"])
    direction = str(position["direction"])

    entry_time: datetime = position["entry_time"]
    entry_price: Decimal = position["entry_price"]
    entry_qty: Decimal = position["entry_qty"]
    entry_notional: Decimal = position["entry_notional"]
    sl_price: Decimal = position["sl_price"]
    tp_price: Decimal = position["tp_price"]

    # условия достаточности
    if run_to_time <= entry_time:
        return None

    exit_info = await _find_exit_in_range(
        pg=pg,
        symbol=symbol,
        timeframe=timeframe,
        direction=direction,
        sl_price=sl_price,
        tp_price=tp_price,
        scan_from=entry_time,
        scan_to=run_to_time,
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
        exit_time=exit_time,
        exit_price=exit_price,
    )

    # закрываем только если всё ещё open (идемпотентность)
    async with pg.acquire() as conn:
        updated = await conn.execute(
            f"""
            UPDATE {BT_POSITIONS_V2_TABLE}
            SET status = 'closed',
                closed_run_id = $2,
                exit_time = $3,
                exit_price = $4,
                exit_reason = $5,
                pnl_abs = $6,
                duration = $7,
                max_favorable_excursion = $8,
                max_adverse_excursion = $9,
                updated_at = now()
            WHERE id = $1
              AND status = 'open'
            """,
            pos_id,
            int(run_id),
            exit_time,
            exit_price,
            str(exit_reason),
            pnl_abs,
            duration,
            max_fav_pct,
            max_adv_pct,
        )

    # если строка не обновилась — значит позиция уже закрыта параллельно/ранее
    if not isinstance(updated, str) or not updated.startswith("UPDATE"):
        return None

    return {
        "id": pos_id,
        "exit_time": exit_time,
        "exit_reason": str(exit_reason),
        "pnl_abs": pnl_abs,
    }


# 🔸 Получить актуальные статусы позиций (для заполнения membership.status_at_end)
async def _load_positions_status_by_ids(
    pg,
    position_ids: List[int],
) -> Dict[int, str]:
    if not position_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, status
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE id = ANY($1::bigint[])
            """,
            [int(pid) for pid in position_ids],
        )

    out: Dict[int, str] = {}
    for r in rows:
        out[int(r["id"])] = str(r["status"])
    return out


# 🔸 Вставка membership_v2 для текущего run (идемпотентно)
async def _insert_membership_v2(
    pg,
    run_id: int,
    rows: List[Tuple[int, bool, bool, str]],
) -> int:
    # rows: (position_id, opened_in_run, closed_in_run, status_at_end)
    if not rows:
        return 0

    run_ids: List[int] = []
    position_ids: List[int] = []
    opened_flags: List[bool] = []
    closed_flags: List[bool] = []
    statuses: List[str] = []

    for (pid, opened_in_run, closed_in_run, status_at_end) in rows:
        run_ids.append(int(run_id))
        position_ids.append(int(pid))
        opened_flags.append(bool(opened_in_run))
        closed_flags.append(bool(closed_in_run))
        statuses.append(str(status_at_end))

    async with pg.acquire() as conn:
        inserted = await conn.fetch(
            f"""
            INSERT INTO {BT_MEMBERSHIP_V2_TABLE}
                (run_id, position_id, opened_in_run, closed_in_run, status_at_end, created_at)
            SELECT
                u.run_id,
                u.position_id,
                u.opened_in_run,
                u.closed_in_run,
                u.status_at_end,
                now()
            FROM unnest(
                $1::bigint[],
                $2::bigint[],
                $3::bool[],
                $4::bool[],
                $5::text[]
            ) AS u(
                run_id,
                position_id,
                opened_in_run,
                closed_in_run,
                status_at_end
            )
            ON CONFLICT (run_id, position_id) DO NOTHING
            RETURNING id
            """,
            run_ids,
            position_ids,
            opened_flags,
            closed_flags,
            statuses,
        )

    return len(inserted)


# 🔸 Публичная точка входа: backfill для сценария raw_straight_mono_v2 по одному окну датасета сигналов (membership run-aware)
async def run_raw_straight_mono_backfill_v2(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    scenario_id = int(scenario.get("id") or 0)
    scenario_key = scenario.get("key")
    scenario_type = scenario.get("type")
    params = scenario.get("params") or {}

    signal_id = int(signal_ctx.get("signal_id") or 0)
    run_id_raw = signal_ctx.get("run_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    # условия достаточности
    if not scenario_id or not signal_id or not isinstance(from_time, datetime) or not isinstance(to_time, datetime):
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: недостаточно данных контекста — scenario_id=%s, signal_id=%s, run_id=%s, from_time=%s, to_time=%s",
            scenario_id,
            signal_id,
            run_id_raw,
            from_time,
            to_time,
        )
        return

    try:
        run_id = int(run_id_raw)
    except Exception:
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: некорректный run_id=%s для scenario_id=%s, signal_id=%s — сценарий не будет выполнен",
            run_id_raw,
            scenario_id,
            signal_id,
        )
        return

    # базовые параметры сценария (stat/daily не считаем, но параметры нужны для симуляции)
    try:
        direction_mode = (params["direction"]["value"] or "").strip().lower()
        deposit = Decimal(str(params.get("deposit", {}).get("value", "0")))  # не используется в v2
        leverage = Decimal(str(params["leverage"]["value"]))
        sl_type = (params["sl_type"]["value"] or "").strip().lower()
        sl_value = Decimal(str(params["sl_value"]["value"]))
        tp_type = (params["tp_type"]["value"] or "").strip().lower()
        tp_value = Decimal(str(params["tp_value"]["value"]))
        position_limit = Decimal(str(params["position_limit"]["value"]))
    except Exception as e:
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: сценарий id=%s — некорректные параметры: %s",
            scenario_id,
            e,
            exc_info=True,
        )
        return

    # direction=mono ожидается, но не жёстко блокируем
    if direction_mode != "mono":
        log.warning(
            "BT_SCENARIO_RAW_MONO_V2: сценарий id=%s ожидает direction='mono', получено '%s'",
            scenario_id,
            direction_mode,
        )

    if sl_type != "percent" or tp_type != "percent":
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: сценарий id=%s поддерживает только sl_type/tp_type='percent', получено sl_type='%s', tp_type='%s'",
            scenario_id,
            sl_type,
            tp_type,
        )
        return

    signal_instance = get_signal_instance(signal_id)
    if not signal_instance:
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: не найден инстанс сигнала id=%s в кеше, сценарий id=%s",
            signal_id,
            scenario_id,
        )
        return

    timeframe = str(signal_instance.get("timeframe") or "").strip().lower()
    if timeframe not in ("m5", "m15", "h1"):
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: scenario_id=%s, signal_id=%s — неподдерживаемый timeframe='%s'",
            scenario_id,
            signal_id,
            timeframe,
        )
        return

    # decision_time = entry_time + TF (если в event нет decision_time)
    tf_delta = _get_timeframe_timedelta(timeframe)
    if tf_delta <= timedelta(0):
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: неизвестный TF для decision_time (timeframe=%s), scenario_id=%s, signal_id=%s",
            timeframe,
            scenario_id,
            signal_id,
        )
        return

    # допустимые направления (поддерживаем моно-направленные сигналы)
    allowed_directions: List[str] = ["long", "short"]
    try:
        sig_params = signal_instance.get("params") or {}
        dm_cfg = sig_params.get("direction_mask")
        dm_val = str((dm_cfg or {}).get("value") or "").strip().lower()
        if dm_val in ("long", "short"):
            allowed_directions = [dm_val]
    except Exception:
        allowed_directions = ["long", "short"]

    log.debug(
        "BT_SCENARIO_RAW_MONO_V2: старт scenario_id=%s (key=%s, type=%s) signal_id=%s run_id=%s TF=%s window=[%s..%s] allowed_directions=%s",
        scenario_id,
        scenario_key,
        scenario_type,
        signal_id,
        run_id,
        timeframe,
        from_time,
        to_time,
        allowed_directions,
    )

    total_events = 0
    positions_created = 0
    positions_existing = 0
    positions_closed_now = 0
    membership_inserted = 0
    skipped = 0
    errors = 0

    # 🔸 1) Загружаем входной датасет (events через signals membership)
    try:
        events = await _load_signal_events_for_run(
            pg=pg,
            signal_id=signal_id,
            run_id=run_id,
            timeframe=timeframe,
            from_time=from_time,
            to_time=to_time,
        )
        total_events = len(events)
    except Exception as e:
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: ошибка загрузки events для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )
        return

    # позиция -> opened_in_run (для membership)
    opened_in_run_by_pos: Dict[int, bool] = {}
    # позиции, которые нужно зафиксировать в membership текущего run
    positions_in_run: Set[int] = set()

    # 🔸 2) Создаём позиции-объекты (если нужно) по событиям датасета
    for ev in events:
        try:
            symbol = str(ev["symbol"])
            open_time: datetime = ev["open_time"]
            signal_value_id = int(ev["signal_value_id"])

            direction = str(ev.get("direction") or "").strip().lower()
            if direction not in ("long", "short"):
                skipped += 1
                continue

            # если сигнал моно-направленный — пропускаем чужие направления
            if direction not in allowed_directions:
                skipped += 1
                continue

            # decision_time берём из event, если есть; иначе вычисляем
            decision_time = ev.get("decision_time") or (open_time + tf_delta)

            # entry_price берём из event.price (numeric)
            price_val = ev.get("price")
            if price_val is None:
                skipped += 1
                continue

            entry_price = Decimal(str(price_val))
            if entry_price <= Decimal("0"):
                skipped += 1
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

            # фиксированная маржа на сделку = position_limit
            margin_used = _q_money(position_limit)
            if margin_used <= Decimal("0"):
                skipped += 1
                continue

            # notional
            entry_notional = _q_money(margin_used * leverage)
            if entry_notional <= Decimal("0"):
                skipped += 1
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
                skipped += 1
                continue

            if entry_qty < min_qty:
                skipped += 1
                continue

            # пересчёт notional по округлённому qty
            entry_notional = _q_money(entry_price * entry_qty)
            if entry_notional <= Decimal("0"):
                skipped += 1
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
                skipped += 1
                continue

            # создаём (или получаем) позицию-объект
            pos_id, created_now = await _create_or_get_position_v2(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                signal_value_id=signal_value_id,
                opened_run_id=run_id,
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

            positions_in_run.add(int(pos_id))
            opened_in_run_by_pos[int(pos_id)] = bool(created_now)

            if created_now:
                positions_created += 1
            else:
                positions_existing += 1

        except Exception as e:
            errors += 1
            log.error(
                "BT_SCENARIO_RAW_MONO_V2: ошибка обработки event (scenario_id=%s, signal_id=%s, run_id=%s): %s",
                scenario_id,
                signal_id,
                run_id,
                e,
                exc_info=True,
            )

    # 🔸 3) Прогрессивное закрытие open позиций (без lookahead; закрываем, если exit попал в (entry_time..to_time])
    closed_in_run_by_pos: Dict[int, bool] = {}

    try:
        open_positions = await _load_open_positions_v2(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            timeframe=timeframe,
            directions=allowed_directions,
        )

        for pos in open_positions:
            try:
                closed = await _try_close_position_v2(
                    pg=pg,
                    run_id=run_id,
                    position=pos,
                    run_to_time=to_time,
                )
                if closed is None:
                    continue

                pos_id = int(pos["id"])
                closed_in_run_by_pos[pos_id] = True
                positions_closed_now += 1

                # если позиция закрылась в этом run, зафиксируем её в membership этого run
                positions_in_run.add(pos_id)

            except Exception as e:
                errors += 1
                log.error(
                    "BT_SCENARIO_RAW_MONO_V2: ошибка прогрессивного закрытия позиции id=%s (run_id=%s): %s",
                    pos.get("id"),
                    run_id,
                    e,
                    exc_info=True,
                )

    except Exception as e:
        errors += 1
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: ошибка загрузки/обработки open позиций для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )

    # 🔸 4) Записываем membership_v2 для текущего run
    try:
        pos_ids_list = sorted(list(positions_in_run))
        status_by_id = await _load_positions_status_by_ids(pg, pos_ids_list)

        membership_rows: List[Tuple[int, bool, bool, str]] = []
        for pid in pos_ids_list:
            opened_flag = bool(opened_in_run_by_pos.get(pid, False))
            closed_flag = bool(closed_in_run_by_pos.get(pid, False))
            status_at_end = str(status_by_id.get(pid, "open")).strip().lower()
            if status_at_end not in ("open", "closed"):
                status_at_end = "open"

            membership_rows.append((pid, opened_flag, closed_flag, status_at_end))

        membership_inserted = await _insert_membership_v2(
            pg=pg,
            run_id=run_id,
            rows=membership_rows,
        )

    except Exception as e:
        errors += 1
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: ошибка записи membership_v2 для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )

    # 🔸 5) Итог и событие готовности сценария v2
    log.info(
        "BT_SCENARIO_RAW_MONO_V2: summary scenario_id=%s signal_id=%s run_id=%s TF=%s window=[%s..%s] — "
        "events=%s positions_created=%s positions_existing=%s positions_closed_now=%s membership_inserted=%s skipped=%s errors=%s",
        scenario_id,
        signal_id,
        run_id,
        timeframe,
        from_time,
        to_time,
        total_events,
        positions_created,
        positions_existing,
        positions_closed_now,
        membership_inserted,
        skipped,
        errors,
    )

    finished_at = datetime.utcnow()
    try:
        await redis.xadd(
            BT_SCENARIOS_READY_STREAM_V2,
            {
                "scenario_id": str(int(scenario_id)),
                "signal_id": str(int(signal_id)),
                "run_id": str(int(run_id)),
                "finished_at": finished_at.isoformat(),
                "events": str(int(total_events)),
                "positions_created": str(int(positions_created)),
                "positions_closed_now": str(int(positions_closed_now)),
                "membership_inserted": str(int(membership_inserted)),
                "skipped": str(int(skipped)),
                "errors": str(int(errors)),
            },
        )
        log.debug(
            "BT_SCENARIO_RAW_MONO_V2: опубликовано bt:scenarios:ready_v2 scenario_id=%s signal_id=%s run_id=%s finished_at=%s",
            scenario_id,
            signal_id,
            run_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SCENARIO_RAW_MONO_V2: не удалось опубликовать bt:scenarios:ready_v2 для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )