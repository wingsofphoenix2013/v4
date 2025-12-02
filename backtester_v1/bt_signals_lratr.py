# bt_signals_lratr.py — воркер backfill для псевдо-сигналов семейства LR+ATR (канал + режим волатильности)

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
log = logging.getLogger("BT_SIG_LR_ATR")

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


# 🔸 Поиск индекса последнего бара с open_time <= cutoff_time
def _find_index_leq(series: List[Tuple[datetime, Any]], cutoff_time: datetime) -> Optional[int]:
    # series отсортирован по времени
    lo = 0
    hi = len(series) - 1
    idx = None

    while lo <= hi:
        mid = (lo + hi) // 2
        t = series[mid][0]
        if t <= cutoff_time:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return idx


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса LR+ATR-сигнала
async def run_lratr_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_LR_ATR: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем инстанс LR (обязательный)
    try:
        lr_cfg = params["lr_instance_id"]
        lr_instance_id = int(lr_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_LR_ATR: сигнал id=%s ('%s') — некорректные параметры lr_instance_id: %s",
            signal_id,
            name,
            e,
        )
        return

    # считываем инстанс ATR (обязательный)
    try:
        atr_cfg = params["atr_instance_id"]
        atr_instance_id = int(atr_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_LR_ATR: сигнал id=%s ('%s') — некорректные параметры atr_instance_id: %s",
            signal_id,
            name,
            e,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_LR_ATR: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # паттерн: breakout / bounce
    pattern_cfg = params.get("pattern")
    if pattern_cfg:
        pattern_raw = pattern_cfg.get("value") or ""
        pattern = str(pattern_raw).strip().lower()
    else:
        pattern = "breakout"

    if pattern not in ("breakout", "bounce"):
        log.warning(
            "BT_SIG_LR_ATR: сигнал id=%s ('%s') — pattern=%s пока не поддерживается, будет использован 'breakout'",
            signal_id,
            name,
            pattern,
        )
        pattern = "breakout"

    # минимальный модуль угла (наклон LR), чтобы отсеять совсем плоские каналы
    angle_min_abs = _get_float_param(params, "angle_min_abs", 0.0)

    # ATR-фильтры по нормализованному ATR (опциональны)
    atr_min_norm = _get_float_param(params, "atr_min_norm", 0.0)   # 0.0 → без нижнего фильтра
    atr_max_norm = _get_float_param(params, "atr_max_norm", 0.0)   # 0.0 → без верхнего фильтра

    # рабочее окно по времени
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_LR_ATR: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_LR_ATR: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, lr_instance_id=%s, atr_instance_id=%s, "
        "pattern=%s, angle_min_abs=%.5f, atr_min_norm=%.5f, atr_max_norm=%.5f",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        lr_instance_id,
        atr_instance_id,
        pattern,
        angle_min_abs,
        atr_min_norm,
        atr_max_norm,
    )

    # загружаем уже существующие события сигнала в окне, чтобы избежать дублей
    existing_events = await _load_existing_events(pg, signal_id, timeframe, from_time, to_time)

    sema = asyncio.Semaphore(5)
    tasks = []
    for symbol in symbols:
        tasks.append(
            _process_symbol(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                lr_instance_id=lr_instance_id,
                atr_instance_id=atr_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                pattern=pattern,
                angle_min_abs=angle_min_abs,
                atr_min_norm=atr_min_norm,
                atr_max_norm=atr_max_norm,
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_inserted = 0
    total_long = 0
    total_short = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        ins, longs, shorts = res
        total_inserted += ins
        total_long += longs
        total_short += shorts

    log.info(
        "BT_SIG_LR_ATR: backfill завершён для сигнала id=%s ('%s', key=%s): "
        "вставлено событий=%s, long=%s, short=%s, окно=[%s .. %s]",
        signal_id,
        name,
        signal_key,
        total_inserted,
        total_long,
        total_short,
        from_time,
        to_time,
    )

    # отправляем уведомление в Redis Stream о готовности сигналов
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            BT_SIGNALS_READY_STREAM,
            {
                "signal_id": str(signal_id),
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_SIG_LR_ATR: опубликовано событие готовности в стрим '%s' "
            "для signal_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_ATR: не удалось опубликовать событие в стрим '%s' "
            "для signal_id=%s: %s",
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
        "BT_SIG_LR_ATR: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск LR+ATR сигналов и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    lr_instance_id: int,
    atr_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    pattern: str,
    angle_min_abs: float,
    atr_min_norm: float,
    atr_max_norm: float,
) -> Tuple[int, int, int]:
    async with sema:
        try:
            return await _process_symbol_inner(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                lr_instance_id=lr_instance_id,
                atr_instance_id=atr_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                pattern=pattern,
                angle_min_abs=angle_min_abs,
                atr_min_norm=atr_min_norm,
                atr_max_norm=atr_max_norm,
            )
        except Exception as e:
            log.error(
                "BT_SIG_LR_ATR: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
                symbol,
                signal_id,
                name,
                e,
                exc_info=True,
            )
            return 0, 0, 0


# 🔸 Внутренняя логика обработки символа без семафора
async def _process_symbol_inner(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    lr_instance_id: int,
    atr_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    pattern: str,
    angle_min_abs: float,
    atr_min_norm: float,
    atr_max_norm: float,
) -> Tuple[int, int, int]:
    # загружаем LR-канал на m5
    lr_series = await _load_lr_series(pg, lr_instance_id, symbol, from_time, to_time)
    if not lr_series or len(lr_series) < 2:
        log.debug(
            "BT_SIG_LR_ATR: недостаточно данных LR для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем ATR на m5
    atr_series = await _load_atr_series(pg, atr_instance_id, symbol, from_time, to_time)
    if not atr_series:
        log.debug(
            "BT_SIG_LR_ATR: нет данных ATR для %s (atr_instance_id=%s) в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            atr_instance_id,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем OHLCV для m5 (для цен)
    ohlcv_series = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
    if not ohlcv_series:
        log.debug(
            "BT_SIG_LR_ATR: нет OHLCV для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # работаем по общим временным точкам LR + ATR + OHLCV
    times = sorted(set(lr_series.keys()) & set(atr_series.keys()) & set(ohlcv_series.keys()))
    if len(times) < 2:
        log.debug(
            "BT_SIG_LR_ATR: нет достаточного пересечения LR/ATR/OHLCV для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # precision цены для raw_message
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    to_insert = []
    long_count = 0
    short_count = 0

    # длительность TF (для возможного расширения, пока ATR и LR на m5)
    sig_tf_delta = _get_timeframe_timedelta(timeframe)

    # подготовим серию для поиска по времени (если понадобится расширение)
    time_series = [(t, None) for t in times]

    # перебираем пары (prev_ts, ts) для поиска паттерна
    for i in range(1, len(times)):
        prev_ts = times[i - 1]
        ts = times[i]

        lr_prev = lr_series.get(prev_ts)
        lr_curr = lr_series.get(ts)
        if lr_prev is None or lr_curr is None:
            continue

        atr_val = atr_series.get(ts)
        ohlcv_prev = ohlcv_series.get(prev_ts)
        ohlcv_curr = ohlcv_series.get(ts)
        if atr_val is None or ohlcv_prev is None or ohlcv_curr is None:
            continue

        _, _, _, close_prev = ohlcv_prev
        _, _, _, close_curr = ohlcv_curr

        if close_curr is None or close_curr == 0:
            continue

        angle = lr_curr.get("angle")
        upper_curr = lr_curr.get("upper")
        lower_curr = lr_curr.get("lower")
        upper_prev = lr_prev.get("upper")
        lower_prev = lr_prev.get("lower")

        if angle is None or upper_curr is None or lower_curr is None or upper_prev is None or lower_prev is None:
            continue

        try:
            angle_f = float(angle)
            upper_curr_f = float(upper_curr)
            lower_curr_f = float(lower_curr)
            upper_prev_f = float(upper_prev)
            lower_prev_f = float(lower_prev)
            atr_f = float(atr_val)
            close_prev_f = float(close_prev)
            close_curr_f = float(close_curr)
        except Exception:
            continue

        # фильтр по наклону (если задан модуль)
        if angle_min_abs > 0.0 and abs(angle_f) < angle_min_abs:
            continue

        # нормализованный ATR
        try:
            atr_norm = atr_f / close_curr_f if close_curr_f != 0 else 0.0
        except Exception:
            atr_norm = 0.0

        # фильтр по ATR, если включён
        if atr_min_norm > 0.0 and atr_norm < atr_min_norm:
            continue
        if atr_max_norm > 0.0 and atr_norm > atr_max_norm:
            continue

        direction: Optional[str] = None

        # паттерн breakout: выход за канал по направлению наклона
        if pattern == "breakout":
            # LONG breakout: тренд вверх, пробой верха
            if "long" in allowed_directions and angle_f > 0.0:
                if close_prev_f <= upper_prev_f and close_curr_f > upper_curr_f:
                    direction = "long"

            # SHORT breakout: тренд вниз, пробой низа
            if direction is None and "short" in allowed_directions and angle_f < 0.0:
                if close_prev_f >= lower_prev_f and close_curr_f < lower_curr_f:
                    direction = "short"

        # паттерн bounce: отскок от границы канала по направлению наклона
        elif pattern == "bounce":
            # LONG bounce: тренд вверх, отскок от нижней границы
            if "long" in allowed_directions and angle_f > 0.0:
                # предыдущий close был ниже/у lower, текущий поднялся выше lower
                if close_prev_f <= lower_prev_f and close_curr_f > lower_prev_f:
                    direction = "long"

            # SHORT bounce: тренд вниз, отскок от верхней границы
            if direction is None and "short" in allowed_directions and angle_f < 0.0:
                if close_prev_f >= upper_prev_f and close_curr_f < upper_prev_f:
                    direction = "short"

        if direction is None:
            continue

        key_event = (symbol, ts, direction)
        if key_event in existing_events:
            continue

        # округляем цену для raw_message
        try:
            price_rounded = float(f"{close_curr_f:.{precision_price}f}")
        except Exception:
            price_rounded = close_curr_f

        signal_uuid = uuid.uuid4()
        message = "LR_ATR_LONG" if direction == "long" else "LR_ATR_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "pattern": pattern,
            "angle": angle_f,
            "upper_prev": upper_prev_f,
            "lower_prev": lower_prev_f,
            "upper_curr": upper_curr_f,
            "lower_curr": lower_curr_f,
            "atr": atr_f,
            "atr_norm": float(atr_norm),
            "atr_min_norm": float(atr_min_norm),
            "atr_max_norm": float(atr_max_norm),
            "angle_min_abs": float(angle_min_abs),
            "lr_instance_id": lr_instance_id,
            "atr_instance_id": atr_instance_id,
        }

        to_insert.append(
            (
                str(signal_uuid),
                signal_id,
                symbol,
                timeframe,
                ts,
                direction,
                message,
                json.dumps(raw_message),
            )
        )

        if direction == "long":
            long_count += 1
        else:
            short_count += 1

    if not to_insert:
        log.debug(
            "BT_SIG_LR_ATR: сигналов не найдено для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_signals_values
                (signal_uuid, signal_id, symbol, timeframe, open_time, direction, message, raw_message)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            to_insert,
        )

    inserted = len(to_insert)
    log.debug(
        "BT_SIG_LR_ATR: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
        symbol,
        inserted,
        long_count,
        short_count,
        signal_id,
        name,
    )
    return inserted, long_count, short_count


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


# 🔸 Загрузка ATR-серии для одного инстанса / символа / окна
async def _load_atr_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, float]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, value
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

    series: Dict[datetime, float] = {}
    for r in rows:
        try:
            series[r["open_time"]] = float(r["value"])
        except Exception:
            continue
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