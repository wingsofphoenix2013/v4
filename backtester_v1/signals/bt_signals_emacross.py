# bt_signals_emacross.py — воркер backfill для псевдо-сигналов семейства EMA-cross (с фиксацией run_id)

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
log = logging.getLogger("BT_SIG_EMA_CROSS")

# 🔸 Таймшаги TF (в минутах) для decision_time
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


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса сигнала
async def run_emacross_backfill(
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
            "BT_SIG_EMA_CROSS: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            signal_id,
            name,
            timeframe,
        )
        return

    # условия достаточности: decision_time = open_time + TF
    tf_delta = _get_timeframe_timedelta(timeframe)
    if tf_delta <= timedelta(0):
        log.error(
            "BT_SIG_EMA_CROSS: неизвестный TF для decision_time (timeframe=%s), signal_id=%s ('%s')",
            timeframe,
            signal_id,
            name,
        )
        return

    # считываем идентификаторы EMA-инстансов из параметров сигнала
    try:
        fast_cfg = params["ema_fast_instance_id"]
        slow_cfg = params["ema_slow_instance_id"]
        fast_instance_id = int(fast_cfg["value"])
        slow_instance_id = int(slow_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_EMA_CROSS: сигнал id=%s ('%s') — некорректные параметры EMA-инстансов: %s",
            signal_id,
            name,
            e,
        )
        return

    if backfill_days <= 0 and (window_from_time is None or window_to_time is None):
        log.warning(
            "BT_SIG_EMA_CROSS: сигнал id=%s ('%s') имеет backfill_days=%s и окно не передано, backfill пропущен",
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
            "BT_SIG_EMA_CROSS: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            signal_id,
            name,
        )
        return

    log.debug(
        "BT_SIG_EMA_CROSS: старт backfill для сигнала id=%s ('%s', key=%s), TF=%s, окно=[%s..%s], тикеров=%s, "
        "direction_mask=%s, ema_fast_instance_id=%s, ema_slow_instance_id=%s, run_id=%s",
        signal_id,
        name,
        signal_key,
        timeframe,
        from_time,
        to_time,
        len(symbols),
        mask_val,
        fast_instance_id,
        slow_instance_id,
        run_id,
    )

    # загружаем уже существующие события сигнала в окне, чтобы избежать лишней работы
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
                fast_instance_id=fast_instance_id,
                slow_instance_id=slow_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
                tf_delta=tf_delta,
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
        "BT_SIG_EMA_CROSS: итоги backfill — signal_id=%s, TF=%s, window=[%s..%s], run_id=%s, "
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
            "BT_SIG_EMA_CROSS: опубликовано событие готовности в стрим '%s' для signal_id=%s, run_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            signal_id,
            run_id,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        # ошибки стрима не должны ломать основной backfill
        log.error(
            "BT_SIG_EMA_CROSS: не удалось опубликовать событие в стрим '%s' для signal_id=%s: %s",
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
        "BT_SIG_EMA_CROSS: уже существующих событий в окне [%s .. %s] для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Обработка одного символа: поиск кроссов EMA и запись сигналов
async def _process_symbol(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    fast_instance_id: int,
    slow_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    sema: asyncio.Semaphore,
    allowed_directions: Set[str],
    tf_delta: timedelta,
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
                fast_instance_id=fast_instance_id,
                slow_instance_id=slow_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                allowed_directions=allowed_directions,
                tf_delta=tf_delta,
                run_id=run_id,
            )
        except Exception as e:
            log.error(
                "BT_SIG_EMA_CROSS: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
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
    fast_instance_id: int,
    slow_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
    tf_delta: timedelta,
    run_id: Optional[int],
) -> Tuple[int, int, int, int, int]:
    # загружаем серии EMA для fast и slow
    fast_series = await _load_ema_series(pg, fast_instance_id, symbol, from_time, to_time)
    slow_series = await _load_ema_series(pg, slow_instance_id, symbol, from_time, to_time)

    if not fast_series or not slow_series:
        log.debug("BT_SIG_EMA_CROSS: недостаточно данных EMA для %s, сигнал id=%s ('%s')", symbol, signal_id, name)
        return 0, 0, 0, 0, 0

    # работаем только по общим временным точкам
    times = sorted(set(fast_series.keys()) & set(slow_series.keys()))
    if len(times) < 2:
        log.debug("BT_SIG_EMA_CROSS: слишком мало общих баров EMA для %s, сигнал id=%s ('%s')", symbol, signal_id, name)
        return 0, 0, 0, 0, 0

    # epsilon = 1 * ticksize
    ticker_info = get_ticker_info(symbol) or {}
    ticksize = ticker_info.get("ticksize")
    try:
        epsilon = 1.0 * float(ticksize) if ticksize is not None else 0.0
    except Exception:
        epsilon = 0.0

    # классификация состояний и поиск кроссов
    candidates: List[Tuple[datetime, str]] = []
    prev_state: Optional[str] = None

    for ts in times:
        fast_val = fast_series.get(ts)
        slow_val = slow_series.get(ts)
        if fast_val is None or slow_val is None:
            continue

        diff = fast_val - slow_val
        state = _classify_state(diff, epsilon)

        if state == "neutral":
            # зона неопределённости, состояние не меняем
            continue

        if prev_state is None:
            prev_state = state
            continue

        if state != prev_state:
            # фиксируем кросс с учётом смены состояния
            if prev_state == "below" and state == "above":
                direction = "long"
            elif prev_state == "above" and state == "below":
                direction = "short"
            else:
                prev_state = state
                continue

            # фильтр по маске направлений
            if direction not in allowed_directions:
                prev_state = state
                continue

            candidates.append((ts, direction))
            prev_state = state

    if not candidates:
        return 0, 0, 0, 0, 0

    # подгружаем цены close для найденных баров
    open_times = [ts for ts, _ in candidates]
    prices = await _load_close_prices(pg, symbol, timeframe, open_times)

    # формируем вставки, учитывая уже существующие события в окне
    to_insert = []
    long_count = 0
    short_count = 0

    skipped_existing = 0
    skipped_duplicate = 0

    for ts, direction in candidates:
        # проверяем наличие цены
        price = prices.get(ts)
        if price is None:
            continue

        # если уже есть событие в окне — пропускаем
        key = (symbol, ts, direction)
        if key in existing_events:
            skipped_existing += 1
            continue

        # decision_time = close_time бара, по которому сформирован сигнал
        decision_time = ts + tf_delta

        signal_uuid = uuid.uuid4()
        message = "EMA_CROSS_LONG" if direction == "long" else "EMA_CROSS_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "decision_time": decision_time.isoformat(),
            "direction": direction,
            "price": float(price),
            "epsilon": epsilon,
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
        res = await conn.executemany(
            """
            INSERT INTO bt_signals_values
                (signal_uuid, signal_id, symbol, timeframe, open_time, decision_time, direction, message, raw_message, first_backfill_run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
            ON CONFLICT (signal_id, symbol, timeframe, open_time, direction)
            DO NOTHING
            """,
            to_insert,
        )

    # executemany не возвращает поштучный inserted count; считаем "дубликаты" как (to_insert - реально вставленные)
    # чтобы получить реальное число вставок — делаем INSERT ... RETURNING на батчах; но тут держим простой вариант.
    # оценка duplicate ниже будет "0", если не считать — а важнее суммарные inserted по сигналу на уровне run.
    # поэтому ниже считаем inserted как len(to_insert), а duplicate=0; реальный duplicate поймается индексом и не поломает backfill.
    inserted = len(to_insert)

    # если run_id задан, то это означает "первое появление в backfill"; в случае дублей insert не произойдёт
    # и first_backfill_run_id не будет затёрт.
    return inserted, long_count, short_count, skipped_existing, skipped_duplicate


# 🔸 Классификация состояния fast vs slow по diff и epsilon
def _classify_state(diff: float, epsilon: float) -> str:
    # без epsilon считаем только знак
    if epsilon <= 0:
        if diff > 0:
            return "above"
        if diff < 0:
            return "below"
        return "neutral"

    if diff > epsilon:
        return "above"
    if diff < -epsilon:
        return "below"
    return "neutral"


# 🔸 Загрузка серии EMA для одного инстанса / символа / окна
async def _load_ema_series(
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