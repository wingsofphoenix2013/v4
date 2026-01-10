# bt_scenario_basic_straight_mono_v2.py — basic straight-сценарий (mono) v2: депозит/маржа в памяти, в БД фиксируем только сделки open+close внутри run + bt_signals_log_v2 (upsert)

# 🔸 Базовые импорты
import logging
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import get_signal_instance, get_ticker_info

# 🔸 Логгер модуля
log = logging.getLogger("BT_SCENARIO_BASIC_MONO_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Стрим готовности сценария v2
BT_SCENARIOS_READY_STREAM_V2 = "bt:scenarios:ready_v2"

# 🔸 Комиссия (0.2% вход+выход, списываем на entry_notional)
COMMISSION_RATE = Decimal("0.002")

# 🔸 Таблицы v2
BT_POSITIONS_V2_TABLE = "bt_scenario_positions_v2"
BT_MEMBERSHIP_V2_TABLE = "bt_scenario_membership_v2"

# 🔸 Таблица логов v2
BT_SIGNALS_LOG_V2_TABLE = "bt_signals_log_v2"

# 🔸 Таблицы сигналов (входной датасет)
BT_SIGNAL_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_SIGNAL_EVENTS_TABLE = "bt_signals_values"

# 🔸 Параллелизм/батчи (в этом сценарии обработка событий последовательная из-за маржи)
LOGS_BATCH_SIZE = 1000

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


# 🔸 Загрузка существующих позиций v2 по набору signal_value_id (для skip: already closed)
async def _load_existing_positions_by_signal_values_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    signal_value_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    # условия достаточности
    if not signal_value_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                id,
                signal_value_id,
                status,
                opened_run_id,
                closed_run_id
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE scenario_id = $1
              AND signal_id = $2
              AND signal_value_id = ANY($3::int[])
            """,
            int(scenario_id),
            int(signal_id),
            [int(x) for x in signal_value_ids],
        )

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        svid = int(r["signal_value_id"])
        out[svid] = {
            "id": int(r["id"]),
            "status": str(r["status"] or "").strip().lower(),
            "opened_run_id": int(r["opened_run_id"] or 0),
            "closed_run_id": int(r["closed_run_id"] or 0) if r["closed_run_id"] is not None else 0,
        }
    return out


# 🔸 Upsert сигнал-логов v2 (bulk, report накапливаем через перенос строки)
async def _upsert_signals_log_v2_bulk(
    pg,
    rows: List[Tuple[int, int, int, int, Optional[int], str]],
) -> int:
    # rows: (run_id, scenario_id, signal_id, signal_value_id, position_id, report)
    # условия достаточности
    if not rows:
        return 0

    run_ids: List[int] = []
    scenario_ids: List[int] = []
    signal_ids: List[int] = []
    value_ids: List[int] = []
    position_ids: List[Optional[int]] = []
    reports: List[str] = []

    for (run_id, scenario_id, signal_id, signal_value_id, position_id, report) in rows:
        run_ids.append(int(run_id))
        scenario_ids.append(int(scenario_id))
        signal_ids.append(int(signal_id))
        value_ids.append(int(signal_value_id))
        position_ids.append(None if position_id is None else int(position_id))
        reports.append(str(report))

    async with pg.acquire() as conn:
        affected = await conn.fetch(
            f"""
            INSERT INTO {BT_SIGNALS_LOG_V2_TABLE}
                (run_id, scenario_id, signal_id, signal_value_id, position_id, report, report_json, created_at)
            SELECT
                u.run_id,
                u.scenario_id,
                u.signal_id,
                u.signal_value_id,
                u.position_id,
                u.report,
                NULL::jsonb,
                now()
            FROM unnest(
                $1::bigint[],
                $2::int[],
                $3::int[],
                $4::int[],
                $5::bigint[],
                $6::text[]
            ) AS u(
                run_id,
                scenario_id,
                signal_id,
                signal_value_id,
                position_id,
                report
            )
            ON CONFLICT (scenario_id, run_id, signal_value_id)
            DO UPDATE SET
                position_id = COALESCE(EXCLUDED.position_id, {BT_SIGNALS_LOG_V2_TABLE}.position_id),
                report = CASE
                    WHEN {BT_SIGNALS_LOG_V2_TABLE}.report IS NULL OR {BT_SIGNALS_LOG_V2_TABLE}.report = ''
                    THEN EXCLUDED.report
                    ELSE {BT_SIGNALS_LOG_V2_TABLE}.report || E'\\n' || EXCLUDED.report
                END
            RETURNING id
            """,
            run_ids,
            scenario_ids,
            signal_ids,
            value_ids,
            position_ids,
            reports,
        )

    return len(affected)


# 🔸 Создание позиции v2 сразу в статусе closed (идемпотентно по (scenario_id, signal_id, signal_value_id))
async def _create_or_get_closed_position_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    signal_value_id: int,
    run_id: int,
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
) -> Tuple[Optional[int], bool, bool]:
    """
    Returns:
      position_id, created_now, belongs_to_this_run

    belongs_to_this_run:
      True  -> позиция относится к текущему run (opened_run_id=run_id AND closed_run_id=run_id AND status='closed')
      False -> позиция уже существует, но относится к другому run или имеет невалидный статус для "closed in run"
    """
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
                raw_stat,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11,
                $12, $13, $14, $15, $16, $17,
                'closed',
                $18, $19, $20,
                $21, $22, $23, $24,
                NULL,
                now(),
                now()
            )
            ON CONFLICT (scenario_id, signal_id, signal_value_id) DO NOTHING
            RETURNING id
            """,
            str(position_uid),
            int(scenario_id),
            int(signal_id),
            int(signal_value_id),
            int(run_id),
            int(run_id),
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
            exit_time,
            exit_price,
            str(exit_reason),
            pnl_abs,
            duration,
            max_fav_pct,
            max_adv_pct,
        )

        if row and row["id"] is not None:
            return int(row["id"]), True, True

        existing = await conn.fetchrow(
            f"""
            SELECT id, status, opened_run_id, closed_run_id
            FROM {BT_POSITIONS_V2_TABLE}
            WHERE scenario_id = $1
              AND signal_id = $2
              AND signal_value_id = $3
            """,
            int(scenario_id),
            int(signal_id),
            int(signal_value_id),
        )

    # условий достаточности
    if not existing:
        return None, False, False

    try:
        ex_id = int(existing["id"])
    except Exception:
        return None, False, False

    ex_status = str(existing["status"] or "").strip().lower()
    try:
        ex_opened_run_id = int(existing["opened_run_id"] or 0)
    except Exception:
        ex_opened_run_id = 0
    try:
        ex_closed_run_id = int(existing["closed_run_id"] or 0)
    except Exception:
        ex_closed_run_id = 0

    belongs = (ex_status == "closed" and ex_opened_run_id == int(run_id) and ex_closed_run_id == int(run_id))
    return ex_id, False, belongs


# 🔸 Вставка membership_v2 для текущего run (идемпотентно)
async def _insert_membership_v2(
    pg,
    run_id: int,
    rows: List[Tuple[int, bool, bool, str]],
) -> int:
    # rows: (position_id, opened_in_run, closed_in_run, status_at_end)
    # условия достаточности
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


# 🔸 Получение активных "виртуальных" позиций на момент T (entry_time <= T < exit_time)
def _get_active_virtual_positions(
    virtual_positions: List[Dict[str, Any]],
    current_time: datetime,
) -> List[Dict[str, Any]]:
    active: List[Dict[str, Any]] = []
    for p in virtual_positions:
        et = p["entry_time"]
        xt = p["exit_time"]
        if et <= current_time < xt:
            active.append(p)
    return active


# 🔸 Публичная точка входа: backfill для сценария basic_straight_mono_v2 по одному окну датасета сигналов (membership)
async def run_basic_straight_mono_backfill_v2(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    t0 = time.perf_counter()

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
            "BT_SCENARIO_BASIC_MONO_V2: недостаточно данных контекста — scenario_id=%s, signal_id=%s, run_id=%s, from_time=%s, to_time=%s",
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
            "BT_SCENARIO_BASIC_MONO_V2: некорректный run_id=%s для scenario_id=%s, signal_id=%s — сценарий не будет выполнен",
            run_id_raw,
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
            "BT_SCENARIO_BASIC_MONO_V2: сценарий id=%s — некорректные параметры: %s",
            scenario_id,
            e,
            exc_info=True,
        )
        return

    if direction_mode != "mono":
        log.warning(
            "BT_SCENARIO_BASIC_MONO_V2: сценарий id=%s ожидает direction='mono', получено '%s'",
            scenario_id,
            direction_mode,
        )

    if sl_type != "percent" or tp_type != "percent":
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: сценарий id=%s поддерживает только sl_type/tp_type='percent', получено sl_type='%s', tp_type='%s'",
            scenario_id,
            sl_type,
            tp_type,
        )
        return

    if deposit <= Decimal("0") or leverage <= Decimal("0") or position_limit <= Decimal("0"):
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: некорректные deposit/leverage/position_limit — scenario_id=%s deposit=%s leverage=%s position_limit=%s",
            scenario_id,
            deposit,
            leverage,
            position_limit,
        )
        return

    signal_instance = get_signal_instance(signal_id)
    if not signal_instance:
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: не найден инстанс сигнала id=%s в кеше, сценарий id=%s",
            signal_id,
            scenario_id,
        )
        return

    timeframe = str(signal_instance.get("timeframe") or "").strip().lower()
    if timeframe not in ("m5", "m15", "h1"):
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: scenario_id=%s, signal_id=%s — неподдерживаемый timeframe='%s'",
            scenario_id,
            signal_id,
            timeframe,
        )
        return

    tf_delta = _get_timeframe_timedelta(timeframe)
    if tf_delta <= timedelta(0):
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: неизвестный TF для decision_time (timeframe=%s), scenario_id=%s, signal_id=%s",
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
        "BT_SCENARIO_BASIC_MONO_V2: старт scenario_id=%s (key=%s, type=%s) signal_id=%s run_id=%s TF=%s window=[%s..%s] "
        "deposit=%s leverage=%s position_limit=%s directions=%s",
        scenario_id,
        scenario_key,
        scenario_type,
        signal_id,
        run_id,
        timeframe,
        from_time,
        to_time,
        deposit,
        leverage,
        position_limit,
        allowed_directions,
    )

    # 🔸 1) Загружаем входной датасет сигналов
    t_load0 = time.perf_counter()
    try:
        events = await _load_signal_events_for_run(
            pg=pg,
            signal_id=signal_id,
            run_id=run_id,
            timeframe=timeframe,
            from_time=from_time,
            to_time=to_time,
        )
    except Exception as e:
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: ошибка загрузки events для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )
        return

    total_events = len(events)
    t_load_ms = int((time.perf_counter() - t_load0) * 1000)

    # 🔸 2) Префетчим уже существующие позиции по этим signal_value_id (чтобы не “пересимулировать” их в марже)
    t_pref0 = time.perf_counter()
    try:
        existing_pos_map = await _load_existing_positions_by_signal_values_v2(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            signal_value_ids=[int(e["signal_value_id"]) for e in events],
        )
    except Exception as e:
        existing_pos_map = {}
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: ошибка префетча позиций v2 (scenario_id=%s signal_id=%s run_id=%s): %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )
    t_pref_ms = int((time.perf_counter() - t_pref0) * 1000)

    # 🔸 Счётчики
    closed_in_run_total = 0
    positions_created = 0
    positions_existing = 0
    unresolved = 0
    skipped = 0
    errors = 0
    membership_inserted = 0
    logs_upserted = 0
    skipped_by_margin = 0
    skipped_by_ticker_lock = 0

    # 🔸 Буферы для bulk-вставок
    membership_rows: List[Tuple[int, bool, bool, str]] = []
    log_rows: List[Tuple[int, int, int, int, Optional[int], str]] = []

    # 🔸 3) Обработка событий (последовательно, т.к. маржа зависит от порядка)
    t_proc0 = time.perf_counter()

    # virtual_positions_by_dir: direction -> list of {symbol, entry_time, exit_time, margin_used}
    virtual_positions_by_dir: Dict[str, List[Dict[str, Any]]] = {d: [] for d in allowed_directions}

    for direction in allowed_directions:
        dir_events = [e for e in events if str(e.get("direction") or "").strip().lower() == direction]
        if not dir_events:
            continue

        for ev in dir_events:
            try:
                signal_value_id = int(ev.get("signal_value_id") or 0)
                symbol = str(ev.get("symbol") or "")
                open_time = ev.get("open_time")

                # условия достаточности
                if signal_value_id <= 0 or not symbol or not isinstance(open_time, datetime):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, "skip:bad_event_fields")
                    )
                    skipped += 1
                    continue

                # если по этому signal_value уже есть позиция — не учитываем в марже и не создаём новую
                ex = existing_pos_map.get(signal_value_id)
                if ex is not None and str(ex.get("status") or "") == "closed":
                    pos_id = int(ex.get("id") or 0) or None
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, pos_id, "skip:already_closed_previous_run")
                    )
                    skipped += 1
                    continue

                # decision_time берём из event, если есть; иначе вычисляем
                decision_time = ev.get("decision_time") or (open_time + tf_delta)

                # активные виртуальные позиции на момент сигнала
                vlist = virtual_positions_by_dir.get(direction) or []
                active_positions = _get_active_virtual_positions(vlist, open_time)

                # тикер уже в позиции по этому направлению
                if any(str(p.get("symbol") or "") == symbol for p in active_positions):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:ticker_in_position symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    skipped_by_ticker_lock += 1
                    continue

                # свободная маржа на момент сигнала
                used_margin_now = Decimal("0")
                for p in active_positions:
                    try:
                        used_margin_now += Decimal(str(p.get("margin_used") or "0"))
                    except Exception:
                        continue

                free_margin = deposit - used_margin_now
                if free_margin <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:no_free_margin symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    skipped_by_margin += 1
                    continue

                # ограничение маржи на одну позицию
                max_margin_for_trade = free_margin if free_margin < position_limit else position_limit
                max_margin_for_trade = _q_money(max_margin_for_trade)

                if max_margin_for_trade <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:no_per_position_margin symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    skipped_by_margin += 1
                    continue

                # entry_price берём из event.price
                price_val = ev.get("price")
                if price_val is None:
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:no_price symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                entry_price = Decimal(str(price_val))
                if entry_price <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_price symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # настройки тикера
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

                # округляем entry_price по тикеру
                entry_price = _round_price(entry_price, precision_price, ticksize)

                # максимальный notional под сделку
                max_notional_for_trade = _q_money(max_margin_for_trade * leverage)
                if max_notional_for_trade <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_notional symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # qty
                qty_raw = max_notional_for_trade / entry_price
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
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_qty symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                if entry_qty < min_qty:
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:qty_lt_min symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # notional и фактическая маржа (после округления qty)
                entry_notional = _q_money(entry_price * entry_qty)
                if entry_notional <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_notional_after_qty symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                margin_used = _q_money(entry_notional / leverage)
                if margin_used <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_margin_used symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # SL/TP
                sl_price, tp_price = _calc_sl_tp_percent(
                    entry_price=entry_price,
                    sl_percent=sl_value,
                    tp_percent=tp_value,
                    direction=direction,
                )
                sl_price = _round_price(sl_price, precision_price, ticksize)
                tp_price = _round_price(tp_price, precision_price, ticksize)

                if sl_price <= Decimal("0") or tp_price <= Decimal("0"):
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_sl_tp symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # сканируем выход в окне текущего run
                if to_time <= open_time:
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"skip:bad_window symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                exit_info = await _find_exit_in_range(
                    pg=pg,
                    symbol=symbol,
                    timeframe=timeframe,
                    direction=direction,
                    sl_price=sl_price,
                    tp_price=tp_price,
                    scan_from=open_time,
                    scan_to=to_time,
                )

                if exit_info is None:
                    # позиция удерживает маржу до конца окна run (в памяти), но в БД не пишем
                    vlist.append(
                        {
                            "symbol": symbol,
                            "entry_time": open_time,
                            "exit_time": to_time,
                            "margin_used": margin_used,
                        }
                    )
                    virtual_positions_by_dir[direction] = vlist

                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"result:open_unresolved_in_run symbol={symbol} dir={direction}")
                    )
                    unresolved += 1
                    continue

                exit_time, exit_price, exit_reason = exit_info

                pnl_abs, duration, max_fav_pct, max_adv_pct = await _compute_closed_trade_stats(
                    pg=pg,
                    symbol=symbol,
                    timeframe=timeframe,
                    direction=direction,
                    entry_time=open_time,
                    entry_price=entry_price,
                    entry_qty=entry_qty,
                    entry_notional=entry_notional,
                    exit_time=exit_time,
                    exit_price=exit_price,
                )

                pos_id, created_now, belongs = await _create_or_get_closed_position_v2(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    signal_value_id=signal_value_id,
                    run_id=run_id,
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

                if pos_id is None or int(pos_id) <= 0:
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, None, f"result:error_position_missing symbol={symbol} dir={direction}")
                    )
                    errors += 1
                    continue

                if not belongs:
                    # позиция уже существует, но относится к другому run -> не учитываем в run-результатах и не влияeм на маржу
                    log_rows.append(
                        (run_id, scenario_id, signal_id, signal_value_id, int(pos_id), f"skip:position_exists_other_run symbol={symbol} dir={direction}")
                    )
                    skipped += 1
                    continue

                # учитываем маржу только до exit_time (внутри run)
                vlist.append(
                    {
                        "symbol": symbol,
                        "entry_time": open_time,
                        "exit_time": exit_time,
                        "margin_used": margin_used,
                    }
                )
                virtual_positions_by_dir[direction] = vlist

                if created_now:
                    positions_created += 1
                else:
                    positions_existing += 1

                closed_in_run_total += 1
                membership_rows.append((int(pos_id), True, True, "closed"))

                log_rows.append(
                    (run_id, scenario_id, signal_id, signal_value_id, int(pos_id), f"result:closed_in_run symbol={symbol} dir={direction} exit_reason={exit_reason} pnl_abs={pnl_abs}")
                )

            except Exception as e:
                errors += 1
                signal_value_id = int(ev.get("signal_value_id") or 0) if isinstance(ev, dict) else 0
                log.error(
                    "BT_SCENARIO_BASIC_MONO_V2: ошибка обработки event (scenario_id=%s, signal_id=%s, run_id=%s): %s",
                    scenario_id,
                    signal_id,
                    run_id,
                    e,
                    exc_info=True,
                )
                log_rows.append((run_id, scenario_id, signal_id, signal_value_id, None, "result:error"))

    t_proc_ms = int((time.perf_counter() - t_proc0) * 1000)

    # 🔸 4) Membership_v2 (фиксируем только сделки, закрывшиеся в run)
    t_memb0 = time.perf_counter()
    try:
        membership_inserted = await _insert_membership_v2(pg=pg, run_id=run_id, rows=membership_rows)
    except Exception as e:
        membership_inserted = 0
        errors += 1
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: ошибка записи membership_v2 для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )
    t_memb_ms = int((time.perf_counter() - t_memb0) * 1000)

    # 🔸 5) bt_signals_log_v2 (bulk upsert)
    t_log0 = time.perf_counter()
    try:
        total_upsert = 0
        for i in range(0, len(log_rows), LOGS_BATCH_SIZE):
            chunk = log_rows[i : i + LOGS_BATCH_SIZE]
            total_upsert += await _upsert_signals_log_v2_bulk(pg=pg, rows=chunk)
        logs_upserted = total_upsert
    except Exception as e:
        logs_upserted = 0
        errors += 1
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: ошибка записи bt_signals_log_v2 для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )
    t_log_ms = int((time.perf_counter() - t_log0) * 1000)

    total_ms = int((time.perf_counter() - t0) * 1000)

    log.info(
        "BT_SCENARIO_BASIC_MONO_V2: summary scenario_id=%s signal_id=%s run_id=%s TF=%s window=[%s..%s] — "
        "events=%s closed_in_run=%s created=%s existing=%s unresolved=%s skipped=%s (margin=%s ticker_lock=%s) errors=%s "
        "memb_inserted=%s logs_upserted=%s timing_ms(load=%s prefetch=%s proc=%s memb=%s logs=%s total=%s)",
        scenario_id,
        signal_id,
        run_id,
        timeframe,
        from_time,
        to_time,
        total_events,
        closed_in_run_total,
        positions_created,
        positions_existing,
        unresolved,
        skipped,
        skipped_by_margin,
        skipped_by_ticker_lock,
        errors,
        membership_inserted,
        logs_upserted,
        t_load_ms,
        t_pref_ms,
        t_proc_ms,
        t_memb_ms,
        t_log_ms,
        total_ms,
    )

    # 🔸 6) Событие готовности сценария v2
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
                "positions_existing": str(int(positions_existing)),
                "positions_closed_now": str(int(closed_in_run_total)),
                "positions_unresolved": str(int(unresolved)),
                "membership_inserted": str(int(membership_inserted)),
                "skipped": str(int(skipped)),
                "errors": str(int(errors)),
                "logs_upserted": str(int(logs_upserted)),
                "timing_ms_load": str(int(t_load_ms)),
                "timing_ms_prefetch": str(int(t_pref_ms)),
                "timing_ms_proc": str(int(t_proc_ms)),
                "timing_ms_membership": str(int(t_memb_ms)),
                "timing_ms_logs": str(int(t_log_ms)),
                "timing_ms_total": str(int(total_ms)),
            },
        )
        log.debug(
            "BT_SCENARIO_BASIC_MONO_V2: опубликовано bt:scenarios:ready_v2 scenario_id=%s signal_id=%s run_id=%s finished_at=%s",
            scenario_id,
            signal_id,
            run_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SCENARIO_BASIC_MONO_V2: не удалось опубликовать bt:scenarios:ready_v2 для scenario_id=%s signal_id=%s run_id=%s: %s",
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )