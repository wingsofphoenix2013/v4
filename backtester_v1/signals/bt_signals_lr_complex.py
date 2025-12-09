# bt_signals_lr_complex.py — упрощённый воркер backfill для LR-сигналов (bounce по каналу с zone_k + фильтр по бинам)

import asyncio
import logging
import uuid
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Кеши backtester_v1
from backtester_config import get_all_ticker_symbols, get_ticker_info

# 🔸 Константы и логгер
BT_SIGNALS_READY_STREAM = "bt:signals:ready"
log = logging.getLogger("BT_SIG_LR_COMPLEX")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
}


# 🔸 Длительность таймфрейма в виде timedelta (на будущее, пока не используется)
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного LR-сигнала
async def run_lr_complex_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    # проверяем поддерживаемый TF
    if timeframe != "m5":
        log.warning(
            "BT_SIG_LR_COMPLEX: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_LR_COMPLEX: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
            signal_id,
            name,
            backfill_days,
        )
        return

    # читаем инстанс LR на m5 (обязательный)
    try:
        lr_cfg = params["lr_instance_id"]
        lr_m5_instance_id = int(lr_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_LR_COMPLEX: сигнал id=%s ('%s') — некорректные параметры lr_instance_id (m5): %s",
            signal_id,
            name,
            e,
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

    # зона у границы канала: доля высоты канала (0.0 .. 0.5)
    zone_k = _get_float_param(params, "zone_k", 0.0)
    if zone_k < 0.0:
        zone_k = 0.0
    if zone_k > 0.5:
        zone_k = 0.5

    # паттерн фиксируем как "bounce"
    pattern = "bounce"

    # запрещённые бины по направлению (для текущего TF)
    forbidden_bins_long = _parse_forbidden_bins(params, timeframe, "long")
    forbidden_bins_short = _parse_forbidden_bins(params, timeframe, "short")

    # рабочее окно по времени
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_LR_COMPLEX: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_LR_COMPLEX: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, lr_m5_instance_id=%s, pattern=%s, zone_k=%.3f, "
        "forbidden_bins_long=%s, forbidden_bins_short=%s",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        lr_m5_instance_id,
        pattern,
        zone_k,
        sorted(forbidden_bins_long),
        sorted(forbidden_bins_short),
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
                lr_m5_instance_id=lr_m5_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                pattern=pattern,
                zone_k=zone_k,
                forbidden_bins_long=forbidden_bins_long,
                forbidden_bins_short=forbidden_bins_short,
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
        "BT_SIG_LR_COMPLEX: backfill завершён для сигнала id=%s ('%s', key=%s): "
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
            "BT_SIG_LR_COMPLEX: опубликовано событие готовности в стрим '%s' "
            "для signal_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_COMPLEX: не удалось опубликовать событие в стрим '%s' "
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
        "BT_SIG_LR_COMPLEX: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск LR-bounce сигналов и запись в bt_signals_values
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
    zone_k: float,
    forbidden_bins_long: Set[str],
    forbidden_bins_short: Set[str],
) -> Tuple[int, int, int]:
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
                zone_k=zone_k,
                forbidden_bins_long=forbidden_bins_long,
                forbidden_bins_short=forbidden_bins_short,
            )
        except Exception as e:
            log.error(
                "BT_SIG_LR_COMPLEX: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    lr_m5_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    pattern: str,
    zone_k: float,
    forbidden_bins_long: Set[str],
    forbidden_bins_short: Set[str],
) -> Tuple[int, int, int]:
    # загружаем LR-канал на m5
    lr_m5_series = await _load_lr_series(pg, lr_m5_instance_id, symbol, from_time, to_time)
    if not lr_m5_series or len(lr_m5_series) < 2:
        log.debug(
            "BT_SIG_LR_COMPLEX: недостаточно данных LR m5 для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем OHLCV для m5 (для цен)
    ohlcv_series = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
    if not ohlcv_series:
        log.debug(
            "BT_SIG_LR_COMPLEX: нет OHLCV для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # работаем по общим временным точкам LR m5 + OHLCV
    times = sorted(set(lr_m5_series.keys()) & set(ohlcv_series.keys()))
    if len(times) < 2:
        log.debug(
            "BT_SIG_LR_COMPLEX: нет достаточного пересечения LR/OHLCV для %s, сигнал id=%s ('%s')",
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

        if (
            angle_m5 is None
            or upper_curr is None
            or lower_curr is None
            or upper_prev is None
            or lower_prev is None
        ):
            continue

        try:
            angle_m5_f = float(angle_m5)
            upper_curr_f = float(upper_curr)
            lower_curr_f = float(lower_curr)
            upper_prev_f = float(upper_prev)
            lower_prev_f = float(lower_prev)
            close_prev_f = float(close_prev)
            close_curr_f = float(close_curr)
        except Exception:
            continue

        # высота канала на предыдущем баре
        H = upper_prev_f - lower_prev_f
        if H <= 0.0:
            continue

        direction: Optional[str] = None

        # LONG bounce: отскок от нижней границы
        if "long" in allowed_directions and angle_m5_f > 0.0:
            # зона у нижней границы
            if zone_k == 0.0:
                in_zone_prev = (close_prev_f <= lower_prev_f)
            else:
                zone_up = zone_k * H
                threshold = lower_prev_f + zone_up
                in_zone_prev = (close_prev_f <= threshold)

            if in_zone_prev and close_curr_f > lower_prev_f:
                direction = "long"

        # SHORT bounce: отскок от верхней границы
        if direction is None and "short" in allowed_directions and angle_m5_f < 0.0:
            # зона у верхней границы
            if zone_k == 0.0:
                in_zone_prev = (close_prev_f >= upper_prev_f)
            else:
                zone_down = zone_k * H
                threshold = upper_prev_f - zone_down
                in_zone_prev = (close_prev_f >= threshold)

            if in_zone_prev and close_curr_f < upper_prev_f:
                direction = "short"

        if direction is None:
            continue

        # бин по цене входа (close текущего бара) относительно LR-канала на этом же баре (ts)
        if direction == "long":
            forbidden_bins = forbidden_bins_long
        else:
            forbidden_bins = forbidden_bins_short

        bin_name = _compute_lr_bin(
            price_f=close_curr_f,
            upper_f=upper_curr_f,
            lower_f=lower_curr_f,
        )
        if bin_name is None:
            continue

        if bin_name in forbidden_bins:
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
        message = "LR_BOUNCE_LONG" if direction == "long" else "LR_BOUNCE_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "pattern": pattern,
            "zone_k": float(zone_k),
            "angle_m5": angle_m5_f,
            "upper_prev": upper_prev_f,
            "lower_prev": lower_prev_f,
            "upper_curr": upper_curr_f,
            "lower_curr": lower_curr_f,
            "lr_m5_instance_id": lr_m5_instance_id,
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
            "BT_SIG_LR_COMPLEX: сигналов не найдено для %s в окне [%s..%s], сигнал id=%s ('%s')",
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
        "BT_SIG_LR_COMPLEX: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
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


# 🔸 Вспомогательная функция: парсинг списка запрещённых бинов из параметров
def _parse_forbidden_bins(
    params: Dict[str, Any],
    timeframe: str,
    direction: str,
) -> Set[str]:
    param_name = f"forbidden_bins_{timeframe}_{direction}"
    cfg = params.get(param_name)
    if cfg is None:
        return set()

    raw = cfg.get("value")
    if raw is None:
        return set()

    text = str(raw).strip()
    if not text:
        return set()

    # разделители: запятая, точка с запятой, пробелы
    tokens = re.split(r"[,\s;]+", text)
    bins: Set[str] = set()

    for tok in tokens:
        t = tok.strip().lower()
        if not t:
            continue
        # допускаем форматы "bin_0", "0"
        if t.startswith("bin_"):
            suffix = t.split("_", 1)[1]
            bin_name = f"bin_{suffix}"
        else:
            bin_name = f"bin_{t}"
        bins.add(bin_name)

    return bins


# 🔸 Вспомогательная функция: расчёт bin_X для цены относительно LR-канала
def _compute_lr_bin(
    price_f: float,
    upper_f: float,
    lower_f: float,
) -> Optional[str]:
    try:
        price = Decimal(str(price_f))
        upper = Decimal(str(upper_f))
        lower = Decimal(str(lower_f))
    except (InvalidOperation, TypeError, ValueError):
        return None

    H = upper - lower
    if H <= Decimal("0"):
        return None

    # выше верхней границы
    if price > upper:
        return "bin_0"

    # ниже нижней границы
    if price < lower:
        return "bin_9"

    # внутри канала [lower, upper]
    rel = (upper - price) / H  # 0 → верх, 1 → низ

    if rel < Decimal("0"):
        rel = Decimal("0")
    if rel > Decimal("1"):
        rel = Decimal("1")

    # 8 полос внутри: rel ∈ [0,1] → idx ∈ [0,7]
    idx = int((rel * Decimal("8")).quantize(Decimal("0"), rounding=ROUND_DOWN))
    if idx >= 8:
        idx = 7

    bin_idx = 1 + idx  # bin_1..bin_8
    return f"bin_{bin_idx}"