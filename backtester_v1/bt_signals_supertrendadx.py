# bt_signals_supertrendadx.py — воркер backfill для псевдо-сигналов семейства Supertrend+ADX

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
log = logging.getLogger("BT_SIG_SUPERTREND_ADX")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
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


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса Supertrend+ADX-сигнала
async def run_supertrendadx_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # считываем инстанс Supertrend (обязательный)
    try:
        st_cfg = params["supertrend_instance_id"]
        st_instance_id = int(st_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') — некорректные параметры supertrend_instance_id: %s",
            signal_id,
            name,
            e,
        )
        return

    # считываем инстансы ADX по TF (опционально, но хотя бы один нужен)
    adx_m5_instance_id: Optional[int] = None
    adx_m15_instance_id: Optional[int] = None
    adx_h1_instance_id: Optional[int] = None

    adx_m5_cfg = params.get("adx_m5_instance_id")
    if adx_m5_cfg is not None:
        try:
            adx_m5_instance_id = int(adx_m5_cfg["value"])
        except Exception:
            adx_m5_instance_id = None

    adx_m15_cfg = params.get("adx_m15_instance_id")
    if adx_m15_cfg is not None:
        try:
            adx_m15_instance_id = int(adx_m15_cfg["value"])
        except Exception:
            adx_m15_instance_id = None

    adx_h1_cfg = params.get("adx_h1_instance_id")
    if adx_h1_cfg is not None:
        try:
            adx_h1_instance_id = int(adx_h1_cfg["value"])
        except Exception:
            adx_h1_instance_id = None

    # выбираем TF для ADX (m5/m15/h1)
    adx_tf_cfg = params.get("adx_tf")
    if adx_tf_cfg is not None:
        adx_tf_raw = adx_tf_cfg.get("value")
        adx_tf = str(adx_tf_raw or "").strip().lower()
    else:
        adx_tf = "m15"  # по умолчанию используем m15 как фон

    if adx_tf not in ("m5", "m15", "h1"):
        log.error(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') — неподдерживаемый adx_tf=%s",
            signal_id,
            name,
            adx_tf,
        )
        return

    # выбираем instance_id для выбранного TF
    if adx_tf == "m5":
        adx_instance_id = adx_m5_instance_id
    elif adx_tf == "m15":
        adx_instance_id = adx_m15_instance_id
    else:
        adx_instance_id = adx_h1_instance_id

    if adx_instance_id is None:
        log.error(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') — не задан instance_id для ADX на TF=%s",
            signal_id,
            name,
            adx_tf,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
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

    # режим триггера supertrend (пока поддерживаем только flip)
    trigger_mode_cfg = params.get("trigger_mode")
    if trigger_mode_cfg:
        trigger_mode_raw = trigger_mode_cfg.get("value") or ""
        trigger_mode = str(trigger_mode_raw).strip().lower()
    else:
        trigger_mode = "flip"

    if trigger_mode != "flip":
        log.warning(
            "BT_SIG_SUPERTREND_ADX: сигнал id=%s ('%s') — trigger_mode=%s пока не поддерживается, будет использован 'flip'",
            signal_id,
            name,
            trigger_mode,
        )
        trigger_mode = "flip"

    # порог ADX
    adx_min = _get_float_param(params, "adx_min", 0.0)

    # флаг использования DI-фильтра
    use_di_cfg = params.get("use_di_filter")
    if use_di_cfg is not None:
        use_di_raw = str(use_di_cfg.get("value") or "").strip().lower()
        use_di_filter = use_di_raw in ("1", "true", "yes", "y")
    else:
        use_di_filter = False

    # рабочее окно по времени
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_SUPERTREND_ADX: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_SUPERTREND_ADX: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=%s дней, "
        "тикеров=%s, direction_mask=%s, supertrend_instance_id=%s, adx_tf=%s, adx_instance_id=%s, "
        "adx_min=%.4f, use_di_filter=%s, trigger_mode=%s",
        signal_id,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        st_instance_id,
        adx_tf,
        adx_instance_id,
        adx_min,
        use_di_filter,
        trigger_mode,
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
                st_instance_id=st_instance_id,
                adx_instance_id=adx_instance_id,
                adx_tf=adx_tf,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                adx_min=adx_min,
                use_di_filter=use_di_filter,
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
        "BT_SIG_SUPERTREND_ADX: backfill завершён для сигнала id=%s ('%s', key=%s): "
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
            "BT_SIG_SUPERTREND_ADX: опубликовано событие готовности в стрим '%s' "
            "для signal_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_SIG_SUPERTREND_ADX: не удалось опубликовать событие в стрим '%s' "
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
        "BT_SIG_SUPERTREND_ADX: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск Supertrend+ADX сигналов и запись в bt_signals_values
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    st_instance_id: int,
    adx_instance_id: int,
    adx_tf: str,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    adx_min: float,
    use_di_filter: bool,
) -> Tuple[int, int, int]:
    async with sema:
        try:
            return await _process_symbol_inner(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                st_instance_id=st_instance_id,
                adx_instance_id=adx_instance_id,
                adx_tf=adx_tf,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                adx_min=adx_min,
                use_di_filter=use_di_filter,
            )
        except Exception as e:
            log.error(
                "BT_SIG_SUPERTREND_ADX: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    st_instance_id: int,
    adx_instance_id: int,
    adx_tf: str,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    adx_min: float,
    use_di_filter: bool,
) -> Tuple[int, int, int]:
    # загружаем Supertrend-тренд на m5
    st_trend_series = await _load_supertrend_trend_series(pg, st_instance_id, symbol, from_time, to_time)
    if not st_trend_series or len(st_trend_series) < 2:
        log.debug(
            "BT_SIG_SUPERTREND_ADX: недостаточно данных Supertrend для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем ADX/DMI на выбранном TF
    adx_series = await _load_adx_series(pg, adx_instance_id, symbol, from_time, to_time)
    if not adx_series:
        log.debug(
            "BT_SIG_SUPERTREND_ADX: нет данных ADX для %s (adx_instance_id=%s) в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            adx_instance_id,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # загружаем OHLCV для m5 (для цен входа)
    ohlcv_series = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
    if not ohlcv_series:
        log.debug(
            "BT_SIG_SUPERTREND_ADX: нет OHLCV для %s в окне [%s..%s], сигнал id=%s ('%s')",
            symbol,
            from_time,
            to_time,
            signal_id,
            name,
        )
        return 0, 0, 0

    # sorted m5 times для supertrend
    times = sorted(st_trend_series.keys())
    if len(times) < 2:
        return 0, 0, 0

    # подготовка для lookup ADX без подглядывания
    adx_times = sorted(adx_series.keys())
    if not adx_times:
        return 0, 0, 0

    adx_tf_delta = _get_timeframe_timedelta(adx_tf)
    sig_tf_delta = _get_timeframe_timedelta(timeframe)

    # precision цены для raw_message
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    to_insert = []
    long_count = 0
    short_count = 0

    # перебираем пары (prev_ts, ts) для поиска flip Supertrend
    for i in range(1, len(times)):
        prev_ts = times[i - 1]
        ts = times[i]

        prev_trend = st_trend_series.get(prev_ts)
        curr_trend = st_trend_series.get(ts)
        if prev_trend is None or curr_trend is None:
            continue

        # проверяем наличие цены close для ts
        ohlcv = ohlcv_series.get(ts)
        if not ohlcv:
            continue
        _, _, _, close_price = ohlcv

        # вычисляем момент принятия решения и cutoff по старшему TF
        if adx_tf_delta.total_seconds() > 0 and sig_tf_delta.total_seconds() > 0:
            decision_time = ts + sig_tf_delta
            cutoff_time = decision_time - adx_tf_delta
        else:
            cutoff_time = ts

        # ищем последний ADX-бар, который успел закрыться к decision_time
        adx_idx = _find_index_leq([(t, None) for t in adx_times], cutoff_time)
        if adx_idx is None:
            continue

        adx_time = adx_times[adx_idx]
        adx_entry = adx_series.get(adx_time) or {}
        adx_val = adx_entry.get("adx")
        plus_di = adx_entry.get("plus_di")
        minus_di = adx_entry.get("minus_di")

        if adx_val is None or plus_di is None or minus_di is None:
            continue

        try:
            adx_f = float(adx_val)
            plus_di_f = float(plus_di)
            minus_di_f = float(minus_di)
        except Exception:
            continue

        # фильтр по порогу ADX
        if adx_min > 0.0 and adx_f < adx_min:
            continue

        direction: Optional[str] = None

        # flip вниз→вверх → LONG
        if "long" in allowed_directions and prev_trend <= 0 and curr_trend > 0:
            # фильтр по DI
            if use_di_filter and plus_di_f <= minus_di_f:
                pass
            else:
                direction = "long"

        # flip вверх→вниз → SHORT
        if direction is None and "short" in allowed_directions and prev_trend >= 0 and curr_trend < 0:
            if use_di_filter and minus_di_f <= plus_di_f:
                pass
            else:
                direction = "short"

        if direction is None:
            continue

        key_event = (symbol, ts, direction)
        if key_event in existing_events:
            continue

        # округляем цену для raw_message
        try:
            price_rounded = float(f"{close_price:.{precision_price}f}")
        except Exception:
            price_rounded = float(close_price)

        signal_uuid = uuid.uuid4()
        message = "SUPERTREND_ADX_LONG" if direction == "long" else "SUPERTREND_ADX_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "supertrend_trend_prev": float(prev_trend),
            "supertrend_trend_curr": float(curr_trend),
            "adx_tf": adx_tf,
            "adx_time": adx_time.isoformat(),
            "adx": adx_f,
            "plus_di": plus_di_f,
            "minus_di": minus_di_f,
            "adx_min": float(adx_min),
            "use_di_filter": use_di_filter,
            "st_instance_id": st_instance_id,
            "adx_instance_id": adx_instance_id,
        }

        to_insert.append(
            (
                str(signal_uuid),
                signal_id,
            ])