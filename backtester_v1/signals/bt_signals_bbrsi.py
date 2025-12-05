# bt_signals_bbrsi.py — воркер backfill для псевдо-сигналов семейства BB+RSI reversion

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
log = logging.getLogger("BT_SIG_BB_RSI")


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса BB+RSI-сигнала
async def run_bbrsi_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_BB_RSI: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем идентификаторы BB- и RSI-инстансов из параметров сигнала
    try:
        bb_cfg = params["bb_instance_id"]
        rsi_cfg = params["rsi_instance_id"]
        bb_instance_id = int(bb_cfg["value"])
        rsi_instance_id = int(rsi_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_BB_RSI: сигнал id=%s ('%s') — некорректные параметры BB/RSI-инстансов: %s",
            signal_id,
            name,
            e,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_BB_RSI: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # пороги RSI
    rsi_long_threshold = _get_float_param(params, "rsi_long_threshold", 30.0)
    rsi_short_threshold = _get_float_param(params, "rsi_short_threshold", 70.0)

    # глубина выхода за полосу BB в долях ширины (0.0 = достаточно просто выйти за границу)
    band_break_k = _get_float_param(params, "band_break_k", 0.0)
    if band_break_k < 0.0:
        band_break_k = 0.0

    # рабочее окно по времени
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_BB_RSI: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_BB_RSI: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, rsi_long_threshold=%.2f, rsi_short_threshold=%.2f, band_break_k=%.3f",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        rsi_long_threshold,
        rsi_short_threshold,
        band_break_k,
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
                bb_instance_id=bb_instance_id,
                rsi_instance_id=rsi_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                rsi_long_threshold=rsi_long_threshold,
                rsi_short_threshold=rsi_short_threshold,
                band_break_k=band_break_k,
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

    log.debug(
        "BT_SIG_BB_RSI: backfill завершён для сигнала id=%s ('%s'): вставлено событий=%s, "
        "long=%s, short=%s, direction_mask=%s",
        signal_id,
        name,
        total_inserted,
        total_long,
        total_short,
        mask_val,
    )

    log.info(
        "BT_SIG_BB_RSI: итоги backfill сигнала id=%s ('%s'): событий=%s, long=%s, short=%s, "
        "rsi_long_threshold=%.2f, rsi_short_threshold=%.2f, band_break_k=%.3f",
        signal_id,
        name,
        total_inserted,
        total_long,
        total_short,
        rsi_long_threshold,
        rsi_short_threshold,
        band_break_k,
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
            "BT_SIG_BB_RSI: опубликовано событие готовности в стрим '%s' для signal_id=%s, "
            "окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        # ошибки стрима не должны ломать основной backfill
        log.error(
            "BT_SIG_BB_RSI: не удалось опубликовать событие в стрим '%s' для signal_id=%s: %s",
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
        "BT_SIG_BB_RSI: уже существующих событий в окне [%s .. %s] для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск сигналов BB+RSI и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    bb_instance_id: int,
    rsi_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    rsi_long_threshold: float,
    rsi_short_threshold: float,
    band_break_k: float,
) -> Tuple[int, int, int]:
    async with sema:
        try:
            return await _process_symbol_inner(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                bb_instance_id=bb_instance_id,
                rsi_instance_id=rsi_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                rsi_long_threshold=rsi_long_threshold,
                rsi_short_threshold=rsi_short_threshold,
                band_break_k=band_break_k,
            )
        except Exception as e:
            log.error(
                "BT_SIG_BB_RSI: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    bb_instance_id: int,
    rsi_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    rsi_long_threshold: float,
    rsi_short_threshold: float,
    band_break_k: float,
) -> Tuple[int, int, int]:
    # загружаем BB и RSI серии
    bb_series = await _load_bb_series(pg, bb_instance_id, symbol, from_time, to_time)
    rsi_series = await _load_rsi_series(pg, rsi_instance_id, symbol, from_time, to_time)

    if not bb_series or not rsi_series:
        log.debug(
            "BT_SIG_BB_RSI: недостаточно данных BB/RSI для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # работаем только по общим временным точкам (без подглядывания — только закрытые бары)
    times = sorted(set(bb_series.keys()) & set(rsi_series.keys()))
    if len(times) < 1:
        log.debug(
            "BT_SIG_BB_RSI: нет общих баров BB/RSI для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # подгружаем цены close для найденных баров
    prices = await _load_close_prices(pg, symbol, timeframe, times)
    if not prices:
        log.debug(
            "BT_SIG_BB_RSI: нет цен close для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    to_insert = []
    long_count = 0
    short_count = 0

    # precision цены для логирования/аналитики
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    for ts in times:
        bb = bb_series.get(ts)
        rsi_val = rsi_series.get(ts)
        price = prices.get(ts)

        if not bb or rsi_val is None or price is None:
            continue

        lower = bb.get("lower")
        upper = bb.get("upper")
        center = bb.get("center")

        if lower is None or upper is None or center is None:
            continue

        # ширина полосы
        band_width_lower = center - lower
        band_width_upper = upper - center

        # защита от некорректной геометрии BB
        if band_width_lower <= 0 or band_width_upper <= 0:
            continue

        # базовые пороги выхода за полосу с учётом band_break_k
        # band_break_k=0.0 → достаточно просто выйти за границу
        long_threshold = lower - band_break_k * band_width_lower
        short_threshold = upper + band_break_k * band_width_upper

        direction: Optional[str] = None

        # условия long
        if "long" in allowed_directions:
            if price < long_threshold and rsi_val <= rsi_long_threshold:
                direction = "long"

        # условия short
        if direction is None and "short" in allowed_directions:
            if price > short_threshold and rsi_val >= rsi_short_threshold:
                direction = "short"

        if direction is None:
            continue

        # идемпотентность: пропускаем, если уже есть такое событие
        key = (symbol, ts, direction)
        if key in existing_events:
            continue

        signal_uuid = uuid.uuid4()
        message = "BB_RSI_REVERT_LONG" if direction == "long" else "BB_RSI_REVERT_SHORT"

        # округляем цену для raw_message (только для красоты и консистентности)
        try:
            price_rounded = float(f"{price:.{precision_price}f}")
        except Exception:
            price_rounded = float(price)

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "bb_lower": float(lower),
            "bb_upper": float(upper),
            "bb_center": float(center),
            "rsi": float(rsi_val),
            "rsi_long_threshold": float(rsi_long_threshold),
            "rsi_short_threshold": float(rsi_short_threshold),
            "band_break_k": float(band_break_k),
            "bb_instance_id": bb_instance_id,
            "rsi_instance_id": rsi_instance_id,
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
        "BT_SIG_BB_RSI: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
        symbol,
        inserted,
        long_count,
        short_count,
        signal_id,
        name,
    )
    return inserted, long_count, short_count


# 🔸 Загрузка BB-серии (lower/upper/center) для одного инстанса / символа / окна
async def _load_bb_series(
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
              AND symbol = $2
              AND open_time BETWEEN $3 AND $4
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
        param_name = r["param_name"]
        val = float(r["value"])

        entry = series.setdefault(ts, {})

        # распознаём нижнюю/верхнюю/центральную полосу по суффиксу param_name
        pname = str(param_name or "")
        if pname.endswith("_lower"):
            entry["lower"] = val
        elif pname.endswith("_upper"):
            entry["upper"] = val
        elif pname.endswith("_center") or "center" in pname:
            entry["center"] = val

    return series


# 🔸 Загрузка RSI-серии для одного инстанса / символа / окна
async def _load_rsi_series(
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
              AND symbol = $2
              AND open_time BETWEEN $3 AND $4
            ORDER BY open_time
            """,
            instance_id,
            symbol,
            from_time,
            to_time,
        )

    series: Dict[datetime, float] = {}
    for r in rows:
        series[r["open_time"]] = float(r["value"])
    return series


# 🔸 Загрузка цен close для набора open_time
async def _load_close_prices(
    pg,
    symbol: str,
    timeframe: str,
    open_times: List[datetime],
) -> Dict[datetime, float]:
    if not open_times:
        return {}

    # сейчас поддерживаем только m5
    if timeframe != "m5":
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, "close"
            FROM ohlcv_bb_m5
            WHERE symbol = $1
              AND open_time = ANY($2::timestamp[])
            """,
            symbol,
            open_times,
        )

    prices: Dict[datetime, float] = {}
    for r in rows:
        prices[r["open_time"]] = float(r["close"])
    return prices


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