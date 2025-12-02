# bt_signals_rsiatr.py — воркер backfill для псевдо-сигналов семейства RSI-momentum (+ опциональный ATR-фильтр)

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
log = logging.getLogger("BT_SIG_RSI_ATR")


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса сигнала
async def run_rsiatr_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_RSI_ATR: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем идентификатор RSI-инстанса
    try:
        rsi_cfg = params["rsi_instance_id"]
        rsi_instance_id = int(rsi_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_RSI_ATR: сигнал id=%s ('%s') — некорректные параметры RSI-инстанса: %s",
            signal_id,
            name,
            e,
        )
        return

    # ATR-инстанс опционален (для Шага 1 может отсутствовать)
    atr_instance_id: Optional[int] = None
    atr_cfg = params.get("atr_instance_id")
    if atr_cfg is not None:
        try:
            atr_instance_id = int(atr_cfg["value"])
        except Exception:
            atr_instance_id = None

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_RSI_ATR: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # уровень пересечения RSI (по умолчанию 50.0)
    rsi_level = _get_float_param(params, "rsi_level", 50.0)

    # сила свечи (насколько близко к high/low, 0.0..1.0), по умолчанию 0.6
    candle_strength = _get_float_param(params, "candle_strength", 0.6)
    if candle_strength < 0.0:
        candle_strength = 0.0
    if candle_strength > 1.0:
        candle_strength = 1.0

    # ATR-фильтры по нормализованному ATR (опциональны, для Шага 1 могут не использоваться)
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
            "BT_SIG_RSI_ATR: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_RSI_ATR: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, rsi_level=%.2f, candle_strength=%.2f, atr_instance_id=%s, "
        "atr_min_norm=%.5f, atr_max_norm=%.5f",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        rsi_level,
        candle_strength,
        atr_instance_id,
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
                rsi_instance_id=rsi_instance_id,
                atr_instance_id=atr_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                rsi_level=rsi_level,
                candle_strength=candle_strength,
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

    # логируем суммарный результат
    log.info(
        "BT_SIG_RSI_ATR: backfill завершён для сигнала id=%s ('%s', key=%s): "
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
            "BT_SIG_RSI_ATR: опубликовано событие готовности в стрим '%s' "
            "для signal_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        # ошибки стрима не должны ломать основной backfill
        log.error(
            "BT_SIG_RSI_ATR: не удалось опубликовать событие в стрим '%s' "
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
        "BT_SIG_RSI_ATR: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск RSI-momentum сигналов и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    rsi_instance_id: int,
    atr_instance_id: Optional[int],
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    rsi_level: float,
    candle_strength: float,
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
                rsi_instance_id=rsi_instance_id,
                atr_instance_id=atr_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                rsi_level=rsi_level,
                candle_strength=candle_strength,
                atr_min_norm=atr_min_norm,
                atr_max_norm=atr_max_norm,
            )
        except Exception as e:
            log.error(
                "BT_SIG_RSI_ATR: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    rsi_instance_id: int,
    atr_instance_id: Optional[int],
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    rsi_level: float,
    candle_strength: float,
    atr_min_norm: float,
    atr_max_norm: float,
) -> Tuple[int, int, int]:
    # загружаем RSI-серию
    rsi_series = await _load_rsi_series(pg, rsi_instance_id, symbol, from_time, to_time)

    if not rsi_series or len(rsi_series) < 2:
        log.debug(
            "BT_SIG_RSI_ATR: недостаточно данных RSI для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем OHLCV для свечей
    ohlcv_series = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
    if not ohlcv_series:
        log.debug(
            "BT_SIG_RSI_ATR: нет OHLCV для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # ATR-серия по желанию (для Шага 1 может не использоваться)
    atr_series: Dict[datetime, float] = {}
    if atr_instance_id is not None:
        atr_series = await _load_scalar_series(pg, atr_instance_id, symbol, from_time, to_time)

    # работаем по общим временным точкам RSI + OHLCV
    times = sorted(set(rsi_series.keys()) & set(ohlcv_series.keys()))
    if len(times) < 2:
        log.debug(
            "BT_SIG_RSI_ATR: нет достаточного пересечения RSI/OHLCV для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    to_insert = []
    long_count = 0
    short_count = 0

    # precision цены для красоты/аналитики в raw_message
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    # перебираем пары (prev_ts, ts) для поиска пересечения RSI уровня rsi_level
    for i in range(1, len(times)):
        prev_ts = times[i - 1]
        ts = times[i]

        rsi_prev = rsi_series.get(prev_ts)
        rsi_curr = rsi_series.get(ts)
        ohlcv = ohlcv_series.get(ts)

        if rsi_prev is None or rsi_curr is None or ohlcv is None:
            continue

        open_price, high, low, close = ohlcv
        if high is None or low is None or close is None:
            continue

        # защита от нулевого диапазона бара
        bar_range = high - low
        if bar_range <= 0:
            continue

        # ATR-фильтр, если включён
        if atr_series and (atr_min_norm > 0.0 or atr_max_norm > 0.0):
            atr_val = atr_series.get(ts)
            if atr_val is None:
                continue
            try:
                atr_norm = float(atr_val) / float(close) if close != 0 else 0.0
            except Exception:
                atr_norm = 0.0

            if atr_min_norm > 0.0 and atr_norm < atr_min_norm:
                continue
            if atr_max_norm > 0.0 and atr_norm > atr_max_norm:
                continue
        else:
            atr_norm = 0.0

        direction: Optional[str] = None

        # условия LONG: RSI пересекает уровень снизу вверх, свеча закрывается ближе к high
        if "long" in allowed_directions:
            if rsi_prev < rsi_level <= rsi_curr:
                rel_pos = (close - low) / bar_range
                if rel_pos >= candle_strength:
                    direction = "long"

        # условия SHORT: RSI пересекает уровень сверху вниз, свеча закрывается ближе к low
        if direction is None and "short" in allowed_directions:
            if rsi_prev > rsi_level >= rsi_curr:
                rel_pos = (high - close) / bar_range
                if rel_pos >= candle_strength:
                    direction = "short"

        if direction is None:
            continue

        # идемпотентность: пропускаем, если уже есть такое событие
        key = (symbol, ts, direction)
        if key in existing_events:
            continue

        # округляем цену для raw_message
        try:
            price_rounded = float(f"{close:.{precision_price}f}")
        except Exception:
            price_rounded = float(close)

        signal_uuid = uuid.uuid4()
        message = "RSI_ATR_MOMENTUM_LONG" if direction == "long" else "RSI_ATR_MOMENTUM_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "rsi": float(rsi_curr),
            "rsi_prev": float(rsi_prev),
            "rsi_level": float(rsi_level),
            "candle_strength": float(candle_strength),
            "rsi_instance_id": rsi_instance_id,
            "atr": float(atr_series.get(ts)) if atr_series and atr_series.get(ts) is not None else None,
            "atr_norm": float(atr_norm),
            "atr_min_norm": float(atr_min_norm),
            "atr_max_norm": float(atr_max_norm),
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
            "BT_SIG_RSI_ATR: сигналов не найдено для %s в окне [%s..%s], сигнал id=%s ('%s')",
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
        "BT_SIG_RSI_ATR: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
        symbol,
        inserted,
        long_count,
        short_count,
        signal_id,
        name,
    )
    return inserted, long_count, short_count


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


# 🔸 Загрузка скалярной серии (например ATR) для одного инстанса / символа / окна
async def _load_scalar_series(
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
        series[r["open_time"]] = (
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
        )
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