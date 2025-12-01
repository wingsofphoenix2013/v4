# bt_signals_bbadx.py — воркер backfill для псевдо-сигналов семейства BB-squeeze + ADX breakout

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
log = logging.getLogger("BT_SIG_BB_ADX")


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса BB+ADX-сигнала
async def run_bbadx_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_BB_ADX: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем идентификаторы BB- и ADX-инстансов из параметров сигнала
    try:
        bb_cfg = params["bb_instance_id"]
        adx_cfg = params["adx_instance_id"]
        bb_instance_id = int(bb_cfg["value"])
        adx_instance_id = int(adx_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_BB_ADX: сигнал id=%s ('%s') — некорректные параметры BB/ADX-инстансов: %s",
            signal_id,
            name,
            e,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_BB_ADX: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # параметры squeeze
    squeeze_window_bars = _get_int_param(params, "squeeze_window_bars", 288)  # ~1 день по m5
    if squeeze_window_bars < 10:
        squeeze_window_bars = 10

    squeeze_percentile = _get_float_param(params, "squeeze_percentile", 0.2)
    if squeeze_percentile <= 0.0:
        squeeze_percentile = 0.1
    if squeeze_percentile >= 1.0:
        squeeze_percentile = 0.9

    # параметры ADX/DI
    adx_min = _get_float_param(params, "adx_min", 20.0)
    di_dom_min = _get_float_param(params, "di_dom_min", 0.0)
    adx_rise_required = _get_bool_param(params, "adx_rise_required", False)

    # глубина выхода за полосу (0.0 = достаточно просто выйти за upper/lower)
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
            "BT_SIG_BB_ADX: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_BB_ADX: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, squeeze_window_bars=%s, squeeze_percentile=%.3f, "
        "adx_min=%.2f, di_dom_min=%.2f, adx_rise_required=%s, band_break_k=%.3f",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        squeeze_window_bars,
        squeeze_percentile,
        adx_min,
        di_dom_min,
        adx_rise_required,
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
                adx_instance_id=adx_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                squeeze_window_bars=squeeze_window_bars,
                squeeze_percentile=squeeze_percentile,
                adx_min=adx_min,
                di_dom_min=di_dom_min,
                adx_rise_required=adx_rise_required,
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

    log.info(
        "BT_SIG_BB_ADX: backfill завершён для сигнала id=%s ('%s'): "
        "вставлено событий=%s, long=%s, short=%s, direction_mask=%s, "
        "squeeze_window_bars=%s, squeeze_percentile=%.3f, adx_min=%.2f, di_dom_min=%.2f, "
        "adx_rise_required=%s, band_break_k=%.3f",
        signal_id,
        name,
        total_inserted,
        total_long,
        total_short,
        mask_val,
        squeeze_window_bars,
        squeeze_percentile,
        adx_min,
        di_dom_min,
        adx_rise_required,
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
            "BT_SIG_BB_ADX: опубликовано событие готовности в стрим '%s' для signal_id=%s, "
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
            "BT_SIG_BB_ADX: не удалось опубликовать событие в стрим '%s' для signal_id=%s: %s",
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
        "BT_SIG_BB_ADX: уже существующих событий в окне [%s .. %s] для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск BB-squeeze + ADX breakout и запись сигналов
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    bb_instance_id: int,
    adx_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    squeeze_window_bars: int,
    squeeze_percentile: float,
    adx_min: float,
    di_dom_min: float,
    adx_rise_required: bool,
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
                adx_instance_id=adx_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                squeeze_window_bars=squeeze_window_bars,
                squeeze_percentile=squeeze_percentile,
                adx_min=adx_min,
                di_dom_min=di_dom_min,
                adx_rise_required=adx_rise_required,
                band_break_k=band_break_k,
            )
        except Exception as e:
            log.error(
                "BT_SIG_BB_ADX: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    adx_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    squeeze_window_bars: int,
    squeeze_percentile: float,
    adx_min: float,
    di_dom_min: float,
    adx_rise_required: bool,
    band_break_k: float,
) -> Tuple[int, int, int]:
    # загружаем BB и ADX/DMI серии
    bb_series = await _load_bb_series(pg, bb_instance_id, symbol, from_time, to_time)
    adx_series = await _load_adx_series(pg, adx_instance_id, symbol, from_time, to_time)

    if not bb_series or not adx_series:
        log.debug(
            "BT_SIG_BB_ADX: недостаточно данных BB/ADX для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # подгружаем цены close для всех потенциальных баров
    all_times = sorted(set(bb_series.keys()) & set(adx_series.keys()))
    close_prices = await _load_close_prices(pg, symbol, timeframe, all_times)
    if not close_prices:
        log.debug(
            "BT_SIG_BB_ADX: нет цен close для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # финальный набор временных точек — пересечение BB, ADX и close
    times = sorted(set(all_times) & set(close_prices.keys()))
    if len(times) <= squeeze_window_bars + 1:
        log.debug(
            "BT_SIG_BB_ADX: недостаточно баров для squeeze_window_bars=%s для %s, сигнал id=%s ('%s')",
            squeeze_window_bars,
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # перетаскиваем BB в массивы для вычисления squeeze
    centers: List[float] = []
    lowers: List[float] = []
    uppers: List[float] = []
    rel_widths: List[float] = []

    for ts in times:
        bb = bb_series.get(ts)
        if not bb:
            centers.append(0.0)
            lowers.append(0.0)
            uppers.append(0.0)
            rel_widths.append(0.0)
            continue

        center = float(bb.get("center") or 0.0)
        lower = float(bb.get("lower") or 0.0)
        upper = float(bb.get("upper") or 0.0)

        centers.append(center)
        lowers.append(lower)
        uppers.append(upper)

        width = upper - lower
        if center > 0:
            rel_width = width / center
        else:
            rel_width = width if width > 0 else 0.0
        rel_widths.append(rel_width)

    # precision цены для raw_message
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    to_insert = []
    long_count = 0
    short_count = 0

    # перебираем бары, которые попадают в окно [from_time..to_time]
    # и имеют достаточно истории для squeeze_window_bars
    for i in range(1, len(times)):
        ts = times[i]
        if ts < from_time or ts > to_time:
            continue

        prev_idx = i - 1
        if prev_idx < squeeze_window_bars:
            continue

        # окно для оценки squeeze — последние squeeze_window_bars значений rel_width до prev_idx
        window_start = prev_idx - squeeze_window_bars
        window_end = prev_idx  # включительно
        window = rel_widths[window_start:window_end + 1]
        if not window:
            continue

        # порог перцентиля для rel_width(prev_idx)
        threshold = _calc_percentile(window, squeeze_percentile)
        if threshold is None:
            continue

        prev_rel_width = rel_widths[prev_idx]
        squeeze_ok = prev_rel_width <= threshold
        if not squeeze_ok:
            continue

        # breakout на текущем баре ts
        center = centers[i]
        lower = lowers[i]
        upper = uppers[i]
        price = close_prices.get(ts)

        if price is None or center <= 0 or lower <= 0 or upper <= 0:
            continue

        width = upper - lower
        if width <= 0:
            continue

        # уровни для breakout с учётом band_break_k
        long_break_threshold = upper + band_break_k * width
        short_break_threshold = lower - band_break_k * width

        # ADX/DMI на текущем баре
        adx_info = adx_series.get(ts)
        if not adx_info:
            continue

        adx_val = float(adx_info.get("adx") or 0.0)
        plus_di = float(adx_info.get("plus_di") or 0.0)
        minus_di = float(adx_info.get("minus_di") or 0.0)

        if adx_val < adx_min:
            continue

        # при необходимости проверяем рост ADX относительно предыдущего бара
        if adx_rise_required:
            prev_adx_info = adx_series.get(times[prev_idx])
            if not prev_adx_info:
                continue
            prev_adx_val = float(prev_adx_info.get("adx") or 0.0)
            if adx_val <= prev_adx_val:
                continue

        direction: Optional[str] = None

        # условия long-breakout
        if "long" in allowed_directions:
            di_dom_long = plus_di - minus_di
            if di_dom_long >= di_dom_min and price > long_break_threshold:
                direction = "long"

        # условия short-breakout
        if direction is None and "short" in allowed_directions:
            di_dom_short = minus_di - plus_di
            if di_dom_short >= di_dom_min and price < short_break_threshold:
                direction = "short"

        if direction is None:
            continue

        # идемпотентность: пропускаем, если уже есть такое событие
        key = (symbol, ts, direction)
        if key in existing_events:
            continue

        signal_uuid = uuid.uuid4()
        message = "BB_ADX_BREAKOUT_LONG" if direction == "long" else "BB_ADX_BREAKOUT_SHORT"

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
            "bb_center": float(center),
            "bb_lower": float(lower),
            "bb_upper": float(upper),
            "bb_width": float(width),
            "rel_width": float(prev_rel_width),
            "squeeze_window_bars": int(squeeze_window_bars),
            "squeeze_percentile": float(squeeze_percentile),
            "adx": float(adx_val),
            "plus_di": float(plus_di),
            "minus_di": float(minus_di),
            "adx_min": float(adx_min),
            "di_dom_min": float(di_dom_min),
            "adx_rise_required": bool(adx_rise_required),
            "band_break_k": float(band_break_k),
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
        "BT_SIG_BB_ADX: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
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

        pname = str(param_name or "")
        if pname.endswith("_lower"):
            entry["lower"] = val
        elif pname.endswith("_upper"):
            entry["upper"] = val
        elif pname.endswith("_center") or "center" in pname:
            entry["center"] = val

    return series


# 🔸 Загрузка ADX/DMI-серии для одного инстанса / символа / окна
async def _load_adx_series(
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
        param_name = str(r["param_name"] or "")
        val = float(r["value"])

        entry = series.setdefault(ts, {})

        if "_adx" in param_name:
            entry["adx"] = val
        elif "plus_di" in param_name:
            entry["plus_di"] = val
        elif "minus_di" in param_name:
            entry["minus_di"] = val

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


# 🔸 Вспомогательная функция: безопасное чтение int-параметров сигнала
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return int(str(raw))
    except Exception:
        return default


# 🔸 Вспомогательная функция: безопасное чтение bool-параметров сигнала
def _get_bool_param(params: Dict[str, Any], name: str, default: bool) -> bool:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = str(cfg.get("value") or "").strip().lower()
    if raw in ("1", "true", "yes", "y", "on"):
        return True
    if raw in ("0", "false", "no", "n", "off"):
        return False
    return default


# 🔸 Расчёт перцентиля по окну значений
def _calc_percentile(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None

    vals = sorted(values)
    n = len(vals)
    # ограничиваем percentile в [0,1)
    if percentile <= 0.0:
        idx = 0
    elif percentile >= 1.0:
        idx = n - 1
    else:
        idx = int(n * percentile)
        if idx >= n:
            idx = n - 1
        if idx < 0:
            idx = 0

    return vals[idx]