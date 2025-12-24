# bt_signals_lr_universal.py — воркер backfill для LR-bounce сигналов (trend / counter / agnostic) на m5 (с фиксацией run_id)

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import get_all_ticker_symbols, get_ticker_info

# 🔸 Константы и логгер
BT_SIGNALS_READY_STREAM = "bt:signals:ready"
log = logging.getLogger("BT_SIG_LR_UNI")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
}


# 🔸 Длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса LR-сигнала
async def run_lr_universal_backfill(
    signal: Dict[str, Any],
    pg,
    redis,
    run_id: Optional[int] = None,
    window_from_time: Optional[datetime] = None,
    window_to_time: Optional[datetime] = None,
) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # читаем инстанс LR на m5 (обязательный)
    try:
        lr_cfg = params["indicator"]
        lr_m5_instance_id = int(lr_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') — некорректные параметры indicator (LR instance_id m5): %s",
            signal_id,
            name,
            e,
        )
        return

    if backfill_days <= 0 and (window_from_time is None or window_to_time is None):
        log.warning(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') имеет backfill_days=%s и окно не передано, backfill пропущен",
            signal_id,
            name,
            backfill_days,
        )
        return

    # маска направлений: 'long' / 'short' / 'both' (по умолчанию both)
    dir_mask_cfg = params.get("direction_mask")
    if dir_mask_cfg:
        mask_val_raw = dir_mask_cfg.get("value") or ""
        mask_val = str(mask_val_raw).strip().lower()
    else:
        mask_val = "both"

    if mask_val == "long":
        allowed_directions: Set[str] = {"long"}
    elif mask_val == "short":
        allowed_directions = {"short"}
    else:
        allowed_directions = {"long", "short"}

    # режим работы по тренду: trend / counter / agnostic
    trend_cfg = params.get("trend_type")
    if trend_cfg:
        trend_raw = trend_cfg.get("value") or ""
        trend_type = str(trend_raw).strip().lower()
    else:
        trend_type = "agnostic"

    if trend_type not in ("trend", "counter", "agnostic"):
        log.warning(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') имеет неизвестный trend_type=%s, используется 'agnostic'",
            signal_id,
            name,
            trend_type,
        )
        trend_type = "agnostic"

    # флаг: нужно ли следить, чтобы после отскока цена оставалась в своей половине канала
    keep_half_cfg = params.get("keep_half")
    if keep_half_cfg:
        keep_half_raw = keep_half_cfg.get("value") or ""
        keep_half = str(keep_half_raw).strip().lower() == "true"
    else:
        keep_half = False

    # параметр зоны у границы канала: доля высоты канала (0.0 .. 0.5)
    zone_k = _get_float_param(params, "zone_k", 0.0)
    if zone_k < 0.0:
        zone_k = 0.0
    if zone_k > 0.5:
        zone_k = 0.5

    # паттерн — только bounce
    pattern = "bounce"

    # рабочее окно по времени
    if window_from_time is not None and window_to_time is not None:
        from_time = window_from_time
        to_time = window_to_time
    else:
        now = datetime.utcnow()
        from_time = now - timedelta(days=int(backfill_days))
        to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_LR_UNI: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_LR_UNI: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=[%s..%s], "
        "тикеров=%s, direction_mask=%s, lr_m5_instance_id=%s, pattern=%s, trend_type=%s, zone_k=%.3f, keep_half=%s, run_id=%s",
        signal_id,
        name,
        signal_key,
        timeframe,
        from_time,
        to_time,
        len(symbols),
        mask_val,
        lr_m5_instance_id,
        pattern,
        trend_type,
        zone_k,
        keep_half,
        run_id,
    )

    # загружаем уже существующие события сигнала в окне, чтобы избежать дублей
    existing_events = await _load_existing_events(pg, signal_id, timeframe, from_time, to_time)

    sema = asyncio.Semaphore(5)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            _process_symbol(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                lr_m5_instance_id=lr_m5_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                pattern=pattern,
                trend_type=trend_type,
                zone_k=zone_k,
                keep_half=keep_half,
                run_id=run_id,
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_inserted = 0
    total_skipped_existing = 0
    total_skipped_duplicate = 0

    total_long = 0
    total_short = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        ins, longs, shorts, skipped_existing, skipped_duplicate = res
        total_inserted += ins
        total_long += longs
        total_short += shorts
        total_skipped_existing += skipped_existing
        total_skipped_duplicate += skipped_duplicate

    log.info(
        "BT_SIG_LR_UNI: итоги backfill — signal_id=%s, TF=%s, window=[%s..%s], run_id=%s, "
        "inserted=%s (long=%s, short=%s), skipped_existing=%s, skipped_duplicate=%s",
        signal_id,
        timeframe,
        from_time,
        to_time,
        run_id,
        total_inserted,
        total_long,
        total_short,
        total_skipped_existing,
        total_skipped_duplicate,
    )

    # отправляем уведомление в Redis Stream о готовности сигналов (с run_id)
    finished_at = datetime.utcnow()

    try:
        payload = {
            "signal_id": str(signal_id),
            "from_time": from_time.isoformat(),
            "to_time": to_time.isoformat(),
            "finished_at": finished_at.isoformat(),
        }
        if run_id is not None:
            payload["run_id"] = str(int(run_id))

        await redis.xadd(BT_SIGNALS_READY_STREAM, payload)

        log.debug(
            "BT_SIG_LR_UNI: опубликовано событие готовности в стрим '%s' для signal_id=%s, run_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            run_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI: не удалось опубликовать событие в стрим '%s' для signal_id=%s: %s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Загрузка уже существующих событий сигнала в окне (для идемпотентности)
async def _load_existing_events(
    pg,
    signal_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> set[Tuple[str, datetime, str]]:
    existing: set[Tuple[str, datetime, str]] = set()
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, open_time, direction
            FROM bt_signals_values
            WHERE signal_id = $1
              AND timeframe = $2
              AND open_time BETWEEN $3 AND $4
            """,
            signal_id,
            timeframe,
            from_time,
            to_time,
        )
    for r in rows:
        existing.add((r["symbol"], r["open_time"], r["direction"]))
    log.debug(
        "BT_SIG_LR_UNI: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск LR bounce-сигналов и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    lr_m5_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    pattern: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    run_id: Optional[int],
) -> Tuple[int, int, int, int, int]:
    async with sema:
        try:
            return await _process_symbol_inner(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                lr_m5_instance_id=lr_m5_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                pattern=pattern,
                trend_type=trend_type,
                zone_k=zone_k,
                keep_half=keep_half,
                run_id=run_id,
            )
        except Exception as e:
            log.error(
                "BT_SIG_LR_UNI: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
                symbol,
                signal_id,
                name,
                e,
                exc_info=True,
            )
            return 0, 0, 0, 0, 0


# 🔸 Внутренняя логика обработки символа без семафора
async def _process_symbol_inner(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    lr_m5_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    pattern: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    run_id: Optional[int],
) -> Tuple[int, int, int, int, int]:
    # загружаем LR-канал на m5
    lr_m5_series = await _load_lr_series(pg, lr_m5_instance_id, symbol, from_time, to_time)
    if not lr_m5_series or len(lr_m5_series) < 2:
        return 0, 0, 0, 0, 0

    # загружаем OHLCV для m5 (для цен)
    ohlcv_series = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
    if not ohlcv_series:
        return 0, 0, 0, 0, 0

    # работаем по общим временным точкам LR m5 + OHLCV
    times = sorted(set(lr_m5_series.keys()) & set(ohlcv_series.keys()))
    if len(times) < 2:
        return 0, 0, 0, 0, 0

    # precision цены для raw_message
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    # decision_time = open_time + TF (момент закрытия бара)
    tf_delta = _get_timeframe_timedelta(timeframe)
    if tf_delta <= timedelta(0):
        return 0, 0, 0, 0, 0

    to_insert = []
    long_count = 0
    short_count = 0

    skipped_existing = 0
    skipped_duplicate = 0

    # перебираем пары (prev_ts, ts) для поиска bounce-паттерна
    for i in range(1, len(times)):
        prev_ts = times[i - 1]
        ts = times[i]

        lr_prev = lr_m5_series.get(prev_ts)
        lr_curr = lr_m5_series.get(ts)
        if lr_prev is None or lr_curr is None:
            continue

        ohlcv_prev = ohlcv_series.get(prev_ts)
        ohlcv_curr = ohlcv_series.get(ts)
        if ohlcv_prev is None or ohlcv_curr is None:
            continue

        _, _, _, close_prev = ohlcv_prev
        _, _, _, close_curr = ohlcv_curr

        if close_curr is None or close_curr == 0:
            continue

        angle_m5 = lr_curr.get("angle")
        upper_curr = lr_curr.get("upper")
        lower_curr = lr_curr.get("lower")
        upper_prev = lr_prev.get("upper")
        lower_prev = lr_prev.get("lower")
        center_curr = lr_curr.get("center")

        if (
            angle_m5 is None
            or upper_curr is None
            or lower_curr is None
            or upper_prev is None
            or lower_prev is None
        ):
            continue

        # если keep_half включён, но нет center_curr — не можем применить правило половины
        if keep_half and center_curr is None:
            continue

        try:
            angle_m5_f = float(angle_m5)
            upper_curr_f = float(upper_curr)
            lower_curr_f = float(lower_curr)
            upper_prev_f = float(upper_prev)
            lower_prev_f = float(lower_prev)
            close_prev_f = float(close_prev)
            close_curr_f = float(close_curr)
            center_curr_f = float(center_curr) if center_curr is not None else 0.0
        except Exception:
            continue

        # высота канала
        H = upper_prev_f - lower_prev_f
        if H <= 0:
            continue

        # определяем условия по тренду
        if trend_type == "trend":
            long_trend_ok = angle_m5_f > 0.0
            short_trend_ok = angle_m5_f < 0.0
        elif trend_type == "counter":
            long_trend_ok = angle_m5_f < 0.0
            short_trend_ok = angle_m5_f > 0.0
        else:
            long_trend_ok = True
            short_trend_ok = True

        direction: Optional[str] = None

        # LONG bounce: отскок от нижней границы
        if "long" in allowed_directions and long_trend_ok:
            if zone_k == 0.0:
                # поведение: любой close_prev ниже/на границе
                in_zone_prev = close_prev_f <= lower_prev_f
            else:
                zone_up = zone_k * H
                threshold = lower_prev_f + zone_up
                # позволяем глубокие выносы ниже lower_prev, но не слишком далеко выше
                in_zone_prev = close_prev_f <= threshold

            if in_zone_prev and close_curr_f > lower_prev_f:
                # если keep_half включён — цена после отскока должна быть в нижней половине канала
                if keep_half and not (close_curr_f <= center_curr_f):
                    continue
                direction = "long"

        # SHORT bounce: отскок от верхней границы
        if direction is None and "short" in allowed_directions and short_trend_ok:
            if zone_k == 0.0:
                # поведение: любой close_prev выше/на границе
                in_zone_prev = close_prev_f >= upper_prev_f
            else:
                zone_down = zone_k * H
                threshold = upper_prev_f - zone_down
                # позволяем глубокие выносы выше upper_prev, но не слишком далеко ниже
                in_zone_prev = close_prev_f >= threshold

            if in_zone_prev and close_curr_f < upper_prev_f:
                # если keep_half включён — цена после отскока должна быть в верхней половине канала
                if keep_half and not (close_curr_f >= center_curr_f):
                    continue
                direction = "short"

        if direction is None:
            continue

        key_event = (symbol, ts, direction)
        if key_event in existing_events:
            skipped_existing += 1
            continue

        # округляем цену для raw_message
        try:
            price_rounded = float(f"{close_curr_f:.{precision_price}f}")
        except Exception:
            price_rounded = close_curr_f

        signal_uuid = uuid.uuid4()
        if direction == "long":
            message = "LR_UNI_BOUNCE_LONG"
        else:
            message = "LR_UNI_BOUNCE_SHORT"

        # decision_time = close_time бара, по которому сформирован сигнал
        decision_time = ts + tf_delta

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "decision_time": decision_time.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "pattern": pattern,
            "angle_m5": angle_m5_f,
            "upper_prev": upper_prev_f,
            "lower_prev": lower_prev_f,
            "upper_curr": upper_curr_f,
            "lower_curr": lower_curr_f,
            "center_curr": center_curr_f,
            "zone_k": float(zone_k),
            "trend_type": trend_type,
            "keep_half": keep_half,
            "lr_m5_instance_id": lr_m5_instance_id,
        }

        to_insert.append(
            (
                str(signal_uuid),
                signal_id,
                symbol,
                timeframe,
                ts,
                decision_time,
                direction,
                message,
                json.dumps(raw_message),
                int(run_id) if run_id is not None else None,
            )
        )

        if direction == "long":
            long_count += 1
        else:
            short_count += 1

    if not to_insert:
        return 0, 0, 0, skipped_existing, 0

    # вставка: события не перезаписываем, фиксируем first_backfill_run_id только при первом появлении
    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_signals_values
                (signal_uuid, signal_id, symbol, timeframe, open_time, decision_time, direction, message, raw_message, first_backfill_run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
            ON CONFLICT (signal_id, symbol, timeframe, open_time, direction)
            DO NOTHING
            """,
            to_insert,
        )

    inserted = len(to_insert)
    return inserted, long_count, short_count, skipped_existing, skipped_duplicate


# 🔸 Загрузка LR-серии (angle/upper/lower/center) для одного инстанса / символа / окна
async def _load_lr_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Dict[str, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, param_name, value
            FROM indicator_values_v4
            WHERE instance_id = $1
              AND symbol      = $2
              AND open_time  BETWEEN $3 AND $4
            ORDER BY open_time
            """,
            instance_id,
            symbol,
            from_time,
            to_time,
        )

    series: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        ts = r["open_time"]
        pname = str(r["param_name"] or "")
        val = r["value"]

        entry = series.setdefault(ts, {})

        pname_l = pname.lower()
        try:
            fval = float(val)
        except Exception:
            continue

        if pname_l.endswith("_angle"):
            entry["angle"] = fval
        elif pname_l.endswith("_upper"):
            entry["upper"] = fval
        elif pname_l.endswith("_lower"):
            entry["lower"] = fval
        elif pname_l.endswith("_center"):
            entry["center"] = fval

    return series


# 🔸 Загрузка OHLCV-серии для одного символа / TF / окна
async def _load_ohlcv_series(
    pg,
    symbol: str,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Tuple[float, float, float, float]]:
    if timeframe != "m5":
        return {}

    table_name = "ohlcv_bb_m5"

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, open, high, low, close
            FROM {table_name}
            WHERE symbol = $1
              AND open_time BETWEEN $2 AND $3
            ORDER BY open_time
            """,
            symbol,
            from_time,
            to_time,
        )

    series: Dict[datetime, Tuple[float, float, float, float]] = {}
    for r in rows:
        try:
            series[r["open_time"]] = (
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
            )
        except Exception:
            continue
    return series


# 🔸 Вспомогательная функция: безопасное чтение float-параметров сигнала
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default