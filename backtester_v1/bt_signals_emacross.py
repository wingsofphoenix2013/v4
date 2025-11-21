# bt_signals_emacross.py — воркер backfill для псевдо-сигналов семейства EMA-cross

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


# 🔸 Публичная точка входа: backfill по окну backfill_days для одного инстанса сигнала
async def run_emacross_backfill(signal: Dict[str, Any], pg, redis) -> None:
    signal_id = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            f"BT_SIG_EMA_CROSS: сигнал id={signal_id} ('{name}') имеет неподдерживаемый timeframe={timeframe}, "
            f"ожидается 'm5'"
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
            f"BT_SIG_EMA_CROSS: сигнал id={signal_id} ('{name}') — некорректные параметры EMA-инстансов: {e}"
        )
        return

    if backfill_days <= 0:
        log.warning(
            f"BT_SIG_EMA_CROSS: сигнал id={signal_id} ('{name}') имеет backfill_days={backfill_days}, "
            f"ожидается > 0"
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
    now = datetime.utcnow()
    from_time = now - timedelta(days=backfill_days)
    to_time = now

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(f"BT_SIG_EMA_CROSS: нет активных тикеров для обработки, сигнал id={signal_id} ('{name}')")
        return

    log.debug(
        f"BT_SIG_EMA_CROSS: старт backfill для сигнала id={signal_id} ('{name}', key={signal_key}), "
        f"TF={timeframe}, окно={backfill_days} дней, тикеров={len(symbols)}, "
        f"direction_mask={mask_val}"
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
                fast_instance_id=fast_instance_id,
                slow_instance_id=slow_instance_id,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                pg=pg,
                sema=sema,
                allowed_directions=allowed_directions,
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
        f"BT_SIG_EMA_CROSS: backfill завершён для сигнала id={signal_id} ('{name}'): "
        f"вставлено событий={total_inserted}, long={total_long}, short={total_short}, "
        f"direction_mask={mask_val}"
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
            f"BT_SIG_EMA_CROSS: опубликовано событие готовности в стрим '{BT_SIGNALS_READY_STREAM}' "
            f"для signal_id={signal_id}, окно=[{from_time} .. {to_time}], finished_at={finished_at}"
        )
    except Exception as e:
        # ошибки стрима не должны ломать основной backfill
        log.error(
            f"BT_SIG_EMA_CROSS: не удалось опубликовать событие в стрим '{BT_SIGNALS_READY_STREAM}' "
            f"для signal_id={signal_id}: {e}",
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
        f"BT_SIG_EMA_CROSS: уже существующих событий в окне [{from_time} .. {to_time}] "
        f"для signal_id={signal_id}, TF={timeframe}: {len(existing)}"
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
) -> Tuple[int, int, int]:
    async with sema:
        try:
            return await _process_symbol_inner(
                signal_id,
                signal_key,
                name,
                timeframe,
                symbol,
                fast_instance_id,
                slow_instance_id,
                from_time,
                to_time,
                existing_events,
                pg,
                allowed_directions,
            )
        except Exception as e:
            log.error(
                f"BT_SIG_EMA_CROSS: ошибка обработки символа {symbol} для сигнала id={signal_id} ('{name}'): {e}",
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
    fast_instance_id: int,
    slow_instance_id: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    pg,
    allowed_directions: Set[str],
) -> Tuple[int, int, int]:
    # загружаем серии EMA для fast и slow
    fast_series = await _load_ema_series(pg, fast_instance_id, symbol, from_time, to_time)
    slow_series = await _load_ema_series(pg, slow_instance_id, symbol, from_time, to_time)

    if not fast_series or not slow_series:
        log.debug(
            f"BT_SIG_EMA_CROSS: недостаточно данных EMA для {symbol}, сигнал id={signal_id} ('{name}')"
        )
        return 0, 0, 0

    # работаем только по общим временным точкам
    times = sorted(set(fast_series.keys()) & set(slow_series.keys()))
    if len(times) < 2:
        log.debug(
            f"BT_SIG_EMA_CROSS: слишком мало общих баров EMA для {symbol}, сигнал id={signal_id} ('{name}')"
        )
        return 0, 0, 0

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
        log.debug(
            f"BT_SIG_EMA_CROSS: кроссов EMA9/21 не найдено для {symbol} в окне [{from_time}..{to_time}]"
        )
        return 0, 0, 0

    # подгружаем цены close для найденных баров
    open_times = [ts for ts, _ in candidates]
    prices = await _load_close_prices(pg, symbol, timeframe, open_times)

    # формируем вставки, учитывая уже существующие события
    to_insert = []
    long_count = 0
    short_count = 0

    for ts, direction in candidates:
        # проверяем наличие цены
        price = prices.get(ts)
        if price is None:
            continue

        # идемпотентность: пропускаем, если уже есть такое событие
        key = (symbol, ts, direction)
        if key in existing_events:
            continue

        signal_uuid = uuid.uuid4()
        message = "EMA_CROSS_LONG" if direction == "long" else "EMA_CROSS_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
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
                direction,
                message,
                json.dumps(raw_message),  # сериализуем dict в JSON-строку
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
        f"BT_SIG_EMA_CROSS: {symbol} → вставлено событий={inserted} (long={long_count}, short={short_count}) "
        f"для сигнала id={signal_id} ('{name}')"
    )
    return inserted, long_count, short_count


# 🔸 Классификация состояния fast vs slow по diff и epsilon
def _classify_state(diff: float, epsilon: float) -> str:
    if epsilon <= 0:
        # без epsilon считаем только знак
        if diff > 0:
            return "above"
        elif diff < 0:
            return "below"
        else:
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