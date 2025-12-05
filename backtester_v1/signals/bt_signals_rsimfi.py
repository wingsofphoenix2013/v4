# bt_signals_rsimfi.py — воркер backfill для псевдо-сигналов семейства RSI+MFI в боковике (ADX low)

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
log = logging.getLogger("BT_SIG_RSI_MFI")


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса сигнала
async def run_rsimfi_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_RSI_MFI: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем идентификаторы инстансов RSI, MFI и ADX-DMI из параметров сигнала
    try:
        rsi_cfg = params["rsi_instance_id"]
        mfi_cfg = params["mfi_instance_id"]
        adx_cfg = params["adx_instance_id"]
        rsi_instance_id = int(rsi_cfg["value"])
        mfi_instance_id = int(mfi_cfg["value"])
        adx_instance_id = int(adx_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_RSI_MFI: сигнал id=%s ('%s') — некорректные параметры RSI/MFI/ADX-инстансов: %s",
            signal_id,
            name,
            e,
            exc_info=True,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_RSI_MFI: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # пороги MFI
    mfi_long_threshold = _get_float_param(params, "mfi_long_threshold", 30.0)
    mfi_short_threshold = _get_float_param(params, "mfi_short_threshold", 70.0)

    # параметры ADX (режим боковика)
    adx_max = _get_float_param(params, "adx_max", 20.0)
    adx_low_bars = _get_int_param(params, "adx_low_bars", 1)
    if adx_low_bars < 1:
        adx_low_bars = 1

    # рабочее окно по времени
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_RSI_MFI: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_RSI_MFI: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, rsi_long_threshold=%.2f, rsi_short_threshold=%.2f, "
        "mfi_long_threshold=%.2f, mfi_short_threshold=%.2f, adx_max=%.2f, adx_low_bars=%s",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        rsi_long_threshold,
        rsi_short_threshold,
        mfi_long_threshold,
        mfi_short_threshold,
        adx_max,
        adx_low_bars,
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
                mfi_instance_id=mfi_instance_id,
                adx_instance_id=adx_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                rsi_long_threshold=rsi_long_threshold,
                rsi_short_threshold=rsi_short_threshold,
                mfi_long_threshold=mfi_long_threshold,
                mfi_short_threshold=mfi_short_threshold,
                adx_max=adx_max,
                adx_low_bars=adx_low_bars,
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
        "BT_SIG_RSI_MFI: backfill завершён для сигнала id=%s ('%s'): "
        "вставлено событий=%s, long=%s, short=%s, direction_mask=%s, "
        "rsi_long_threshold=%.2f, rsi_short_threshold=%.2f, "
        "mfi_long_threshold=%.2f, mfi_short_threshold=%.2f, "
        "adx_max=%.2f, adx_low_bars=%s",
        signal_id,
        name,
        total_inserted,
        total_long,
        total_short,
        mask_val,
        rsi_long_threshold,
        rsi_short_threshold,
        mfi_long_threshold,
        mfi_short_threshold,
        adx_max,
        adx_low_bars,
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
            "BT_SIG_RSI_MFI: опубликовано событие готовности в стрим '%s' для signal_id=%s, "
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
            "BT_SIG_RSI_MFI: не удалось опубликовать событие в стрим '%s' для signal_id=%s: %s",
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
        "BT_SIG_RSI_MFI: уже существующих событий в окне [%s .. %s] для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск RSI+MFI range-сигналов и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    rsi_instance_id: int,
    mfi_instance_id: int,
    adx_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    rsi_long_threshold: float,
    rsi_short_threshold: float,
    mfi_long_threshold: float,
    mfi_short_threshold: float,
    adx_max: float,
    adx_low_bars: int,
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
                mfi_instance_id=mfi_instance_id,
                adx_instance_id=adx_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                rsi_long_threshold=rsi_long_threshold,
                rsi_short_threshold=rsi_short_threshold,
                mfi_long_threshold=mfi_long_threshold,
                mfi_short_threshold=mfi_short_threshold,
                adx_max=adx_max,
                adx_low_bars=adx_low_bars,
            )
        except Exception as e:
            log.error(
                "BT_SIG_RSI_MFI: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    mfi_instance_id: int,
    adx_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    rsi_long_threshold: float,
    rsi_short_threshold: float,
    mfi_long_threshold: float,
    mfi_short_threshold: float,
    adx_max: float,
    adx_low_bars: int,
) -> Tuple[int, int, int]:
    # загружаем серии RSI, MFI, ADX
    rsi_series = await _load_indicator_series(pg, rsi_instance_id, symbol, from_time, to_time)
    mfi_series = await _load_indicator_series(pg, mfi_instance_id, symbol, from_time, to_time)
    adx_series = await _load_adx_series(pg, adx_instance_id, symbol, from_time, to_time)

    if not rsi_series or not mfi_series or not adx_series:
        log.debug(
            "BT_SIG_RSI_MFI: недостаточно данных RSI/MFI/ADX для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # пересечение временных точек
    times = sorted(set(rsi_series.keys()) & set(mfi_series.keys()) & set(adx_series.keys()))
    if not times:
        log.debug(
            "BT_SIG_RSI_MFI: нет общих баров по RSI/MFI/ADX для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем цены close для этих баров
    close_prices = await _load_close_prices(pg, symbol, timeframe, times)
    if not close_prices:
        log.debug(
            "BT_SIG_RSI_MFI: нет цен close для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
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

    for idx, ts in enumerate(times):
        if ts < from_time or ts > to_time:
            continue

        rsi_val = rsi_series.get(ts)
        mfi_val = mfi_series.get(ts)
        adx_val = adx_series.get(ts)
        price = close_prices.get(ts)

        if rsi_val is None or mfi_val is None or adx_val is None or price is None:
            continue

        # режим рейнджа: ADX <= adx_max и (опционально) последние adx_low_bars баров также были низкими
        if adx_val > adx_max:
            continue

        if adx_low_bars > 1:
            start_idx = max(0, idx - adx_low_bars + 1)
            window_times = times[start_idx : idx + 1]
            all_low = True
            for t_win in window_times:
                v = adx_series.get(t_win)
                if v is None or v > adx_max:
                    all_low = False
                    break
            if not all_low:
                continue

        direction: Optional[str] = None

        # условия long
        if "long" in allowed_directions:
            if rsi_val <= rsi_long_threshold and mfi_val <= mfi_long_threshold:
                direction = "long"

        # условия short
        if direction is None and "short" in allowed_directions:
            if rsi_val >= rsi_short_threshold and mfi_val >= mfi_short_threshold:
                direction = "short"

        if direction is None:
            continue

        # идемпотентность
        key = (symbol, ts, direction)
        if key in existing_events:
            continue

        signal_uuid = uuid.uuid4()
        message = "RSI_MFI_RANGE_LONG" if direction == "long" else "RSI_MFI_RANGE_SHORT"

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
            "rsi": float(rsi_val),
            "mfi": float(mfi_val),
            "adx": float(adx_val),
            "rsi_long_threshold": float(rsi_long_threshold),
            "rsi_short_threshold": float(rsi_short_threshold),
            "mfi_long_threshold": float(mfi_long_threshold),
            "mfi_short_threshold": float(mfi_short_threshold),
            "adx_max": float(adx_max),
            "adx_low_bars": int(adx_low_bars),
            "rsi_instance_id": rsi_instance_id,
            "mfi_instance_id": mfi_instance_id,
            "adx_instance_id": adx_instance_id,
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
        "BT_SIG_RSI_MFI: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
        symbol,
        inserted,
        long_count,
        short_count,
        signal_id,
        name,
    )
    return inserted, long_count, short_count


# 🔸 Загрузка серии одного параметра индикатора (RSI/MFI) для инстанса / символа / окна
async def _load_indicator_series(
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


# 🔸 Загрузка ADX-серии (только adx) для одного инстанса / символа / окна
async def _load_adx_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, float]:
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

    series: Dict[datetime, float] = {}
    for r in rows:
        ts = r["open_time"]
        pname = str(r["param_name"] or "")
        val = float(r["value"])

        # по DDL adx_dmi: adx_dmi{length}_adx
        if "_adx" in pname:
            series[ts] = val

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


# 🔸 Вспомогательные функции для чтения параметров сигнала
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default


def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return int(str(raw))
    except Exception:
        return default