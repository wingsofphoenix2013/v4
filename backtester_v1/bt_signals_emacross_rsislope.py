# bt_signals_emacross_rsislope.py — воркер псевдо-сигналов EMA-cross с фильтром RSI-slope

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set

from decimal import Decimal

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_all_ticker_symbols,
    get_ticker_info,
    get_analysis_instance,
)

# 🔸 Анализатор RSI (для резолва instance_id и индексирования по времени)
from bt_analysis_rsi import _resolve_rsi_instance_id, _find_index_leq

# 🔸 Утилиты анализа фич (для feature_name)
from bt_analysis_utils import resolve_feature_name

log = logging.getLogger("BT_SIG_EMA_CROSS_RSISLOPE")

# 🔸 Имя таблицы кандидатов good-бинов
BT_ANALYSIS_CANDIDATES_TABLE = "bt_analysis_candidates"

# 🔸 Имя стрима готовности псевдо-сигналов
BT_SIGNALS_READY_STREAM = "bt:signals:ready"


# 🔸 Публичная точка входа: backfill EMA-cross с фильтром по RSI-slope для одного инстанса сигнала
async def run_emacross_rsislope_backfill(
    signal: Dict[str, Any],
    pg,
    redis,
    trigger_ctx: Optional[Dict[str, Any]] = None,
) -> None:
    sid = signal.get("id")
    signal_key = signal.get("key")
    name = signal.get("name")
    timeframe = signal.get("timeframe")
    backfill_days = signal.get("backfill_days") or 0
    params = signal.get("params") or {}

    if timeframe != "m5":
        log.warning(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, ожидается 'm5'",
            sid,
            name,
            timeframe,
        )
        return

    # читаем ema_fast/slow так же, как в обычном emacross
    try:
        fast_cfg = params["ema_fast_instance_id"]
        slow_cfg = params["ema_slow_instance_id"]
        fast_instance_id = int(fast_cfg["value"])
        slow_instance_id = int(slow_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') — некорректные параметры EMA-инстансов: %s",
            sid,
            name,
            e,
        )
        return

    if backfill_days <= 0:
        log.warning(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') имеет backfill_days=%s, ожидается > 0",
            sid,
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

    # рабочее окно по времени: используем либо finished_at из триггера, либо now()
    if trigger_ctx and trigger_ctx.get("finished_at"):
        to_time = trigger_ctx["finished_at"]
        if isinstance(to_time, str):
            try:
                to_time = datetime.fromisoformat(to_time)
            except Exception:
                to_time = datetime.utcnow()
    else:
        to_time = datetime.utcnow()

    from_time = to_time - timedelta(days=backfill_days)

    # читаем параметры привязки к анализу
    trigger_scenario_id = _get_int_param(params, "trigger_scenario_id", default=None)
    trigger_base_signal_id = _get_int_param(params, "trigger_base_signal_id", default=None)
    trigger_family_key = _get_str_param(params, "trigger_family_key", default=None)
    trigger_analysis_id = _get_int_param(params, "trigger_analysis_id", default=None)
    trigger_version = _get_str_param(params, "trigger_version", default=None)

    if trigger_analysis_id is None:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') не имеет корректного trigger_analysis_id",
            sid,
            name,
        )
        return

    # загружаем анализатор, чтобы взять timeframe/source_key/slope_k
    analysis_inst = get_analysis_instance(trigger_analysis_id)
    if not analysis_inst:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: для сигнала id=%s ('%s') не найден анализатор analysis_id=%s",
            sid,
            name,
            trigger_analysis_id,
        )
        return

    analysis_family = (analysis_inst.get("family_key") or "").lower()
    analysis_key = (analysis_inst.get("key") or "").lower()
    analysis_params = analysis_inst.get("params") or {}

    # лёгкая проверка family_key
    if trigger_family_key and analysis_family and trigger_family_key.lower() != analysis_family:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') ожидает family_key=%s, "
            "но анализатор analysis_id=%s имеет family_key=%s",
            sid,
            name,
            trigger_family_key,
            trigger_analysis_id,
            analysis_family,
        )
        return

    # читаем timeframe/source_key RSI из анализатора
    tf_cfg = analysis_params.get("timeframe")
    source_cfg = analysis_params.get("source_key")

    rsi_timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "h1"
    rsi_source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi21"

    # читаем slope_k так же, как в анализаторе: из params или дефолт 3
    slope_k = _get_int_param_from_analysis(analysis_params, "slope_k", default=3)

    # резолвим instance_id RSI через ту же функцию, что и анализатор
    rsi_instance_id = _resolve_rsi_instance_id(rsi_timeframe, rsi_source_key)
    if rsi_instance_id is None:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: не удалось найти RSI instance_id для timeframe=%s, source_key=%s "
            "(analysis_id=%s, signal_id=%s)",
            rsi_timeframe,
            rsi_source_key,
            trigger_analysis_id,
            sid,
        )
        return

    # имя фичи для кандидатов (строго так же, как в анализаторе)
    feature_name = resolve_feature_name(
        family_key="rsi",
        key="rsi_slope",
        timeframe=rsi_timeframe,
        source_key=rsi_source_key,
    )

    # читаем good-бинчики из bt_analysis_candidates для текущего направления
    # direction здесь = направление самого сигнала (long-only / short-only)
    signal_direction = "long" if "long" in allowed_directions and "short" not in allowed_directions else \
        "short" if "short" in allowed_directions and "long" not in allowed_directions else None

    if signal_direction is None:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: сигнал id=%s ('%s') имеет неоднозначный direction_mask=%s "
            "(ожидается только 'long' или только 'short' для rsislope-сигнала)",
            sid,
            name,
            mask_val,
        )
        return

    candidate_ranges = await _load_rsi_slope_candidate_ranges(
        pg=pg,
        scenario_id=trigger_scenario_id,
        base_signal_id=trigger_base_signal_id,
        analysis_id=trigger_analysis_id,
        family_key="rsi",
        key="rsi_slope",
        direction=signal_direction,
        timeframe=rsi_timeframe,
        feature_name=feature_name,
        version=trigger_version,
    )

    if not candidate_ranges:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: для сигнала id=%s ('%s') нет кандидатов в %s по направлению=%s, "
            "работа зависимого сигнала пропущена",
            sid,
            name,
            BT_ANALYSIS_CANDIDATES_TABLE,
            signal_direction,
        )
        return

    # список активных тикеров
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: нет активных тикеров для обработки, сигнал id=%s ('%s')",
            sid,
            name,
        )
        return

    log.debug(
        "BT_SIG_EMA_CROSS_RSISLOPE: старт backfill для сигнала id=%s ('%s', key=%s), "
        "TF=%s, окно=%s дней, тикеров=%s, direction_mask=%s, rsi_timeframe=%s, rsi_source_key=%s, "
        "slope_k=%s, кандидатов_диапазонов=%s",
        sid,
        name,
        signal_key,
        timeframe,
        backfill_days,
        len(symbols),
        mask_val,
        rsi_timeframe,
        rsi_source_key,
        slope_k,
        len(candidate_ranges),
    )

    # загружаем уже существующие события этого сигналa в окне, чтобы избежать дублей
    existing_events = await _load_existing_events(
        pg=pg,
        signal_id=sid,
        timeframe=timeframe,
        from_time=from_time,
        to_time=to_time,
    )

    sema = asyncio.Semaphore(5)
    tasks = []
    for symbol in symbols:
        tasks.append(
            _process_symbol_rsislope(
                signal_id=sid,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                fast_instance_id=fast_instance_id,
                slow_instance_id=slow_instance_id,
                rsi_instance_id=rsi_instance_id,
                rsi_timeframe=rsi_timeframe,
                rsi_source_key=rsi_source_key,
                slope_k=slope_k,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                candidate_ranges=candidate_ranges,
                allowed_directions=allowed_directions,
                pg=pg,
                sema=sema,
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
        "BT_SIG_EMA_CROSS_RSISLOPE: backfill завершён для сигнала id=%s ('%s'): "
        "вставлено событий=%s, long=%s, short=%s",
        sid,
        name,
        total_inserted,
        total_long,
        total_short,
    )

    # отправляем уведомление в Redis Stream о готовности сигналов
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            BT_SIGNALS_READY_STREAM,
            {
                "signal_id": str(sid),
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: опубликовано событие готовности в стрим '%s' "
            "для signal_id=%s, окно=[%s .. %s], finished_at=%s",
            BT_SIGNALS_READY_STREAM,
            sid,
            from_time,
            to_time,
            finished_at,
        )
    except Exception as e:
        # ошибки стрима не должны ломать основной backfill
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: не удалось опубликовать событие в стрим '%s' "
            "для signal_id=%s: %s",
            BT_SIGNALS_READY_STREAM,
            sid,
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
        "BT_SIG_EMA_CROSS_RSISLOPE: уже существующих событий в окне [%s .. %s] "
        "для signal_id=%s, TF=%s: %s",
        from_time,
        to_time,
        signal_id,
        timeframe,
        len(existing),
    )
    return existing


# 🔸 Загрузка good-диапазонов rsi_slope из bt_analysis_candidates
async def _load_rsi_slope_candidate_ranges(
    pg,
    scenario_id: Optional[int],
    base_signal_id: Optional[int],
    analysis_id: int,
    family_key: str,
    key: str,
    direction: str,
    timeframe: str,
    feature_name: str,
    version: Optional[str],
) -> List[Tuple[float, float]]:
    ranges: List[Tuple[float, float]] = []

    if scenario_id is None or base_signal_id is None:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE: невозможно загрузить кандидатов rsi_slope — "
            "scenario_id или base_signal_id не заданы (scenario_id=%s, base_signal_id=%s)",
            scenario_id,
            base_signal_id,
        )
        return ranges

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT bin_from, bin_to, trades, winrate, bin_roi, coverage
            FROM {BT_ANALYSIS_CANDIDATES_TABLE}
            WHERE scenario_id  = $1
              AND signal_id    = $2
              AND analysis_id  = $3
              AND family_key   = $4
              AND "key"        = $5
              AND direction    = $6
              AND timeframe    = $7
              AND feature_name = $8
              AND version      = $9
            """,
            scenario_id,
            base_signal_id,
            analysis_id,
            family_key,
            key,
            direction,
            timeframe,
            feature_name,
            version or "v2",
        )

    if not rows:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: в %s нет строк для scenario_id=%s, base_signal_id=%s, "
            "analysis_id=%s, family_key=%s, key=%s, direction=%s, timeframe=%s, feature_name=%s, version=%s",
            BT_ANALYSIS_CANDIDATES_TABLE,
            scenario_id,
            base_signal_id,
            analysis_id,
            family_key,
            key,
            direction,
            timeframe,
            feature_name,
            version,
        )
        return ranges

    for r in rows:
        b_from = r["bin_from"]
        b_to = r["bin_to"]
        try:
            f_from = float(b_from) if b_from is not None else float("-inf")
            f_to = float(b_to) if b_to is not None else float("inf")
            ranges.append((f_from, f_to))
        except Exception:
            continue

    log.debug(
        "BT_SIG_EMA_CROSS_RSISLOPE: загружено кандидатов rsi_slope=%s для scenario_id=%s, base_signal_id=%s, "
        "analysis_id=%s, direction=%s, timeframe=%s",
        len(ranges),
        scenario_id,
        base_signal_id,
        analysis_id,
        direction,
        timeframe,
    )
    return ranges


# 🔸 Обработка одного символа: EMA-cross + фильтр по RSI-slope и запись сигналов
async def _process_symbol_rsislope(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    fast_instance_id: int,
    slow_instance_id: int,
    rsi_instance_id: int,
    rsi_timeframe: str,
    rsi_source_key: str,
    slope_k: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    candidate_ranges: List[Tuple[float, float]],
    allowed_directions: Set[str],
    pg,
    sema: asyncio.Semaphore,
) -> Tuple[int, int, int]:
    async with sema:
        try:
            return await _process_symbol_rsislope_inner(
                signal_id=signal_id,
                signal_key=signal_key,
                name=name,
                timeframe=timeframe,
                symbol=symbol,
                fast_instance_id=fast_instance_id,
                slow_instance_id=slow_instance_id,
                rsi_instance_id=rsi_instance_id,
                rsi_timeframe=rsi_timeframe,
                rsi_source_key=rsi_source_key,
                slope_k=slope_k,
                from_time=from_time,
                to_time=to_time,
                existing_events=existing_events,
                candidate_ranges=candidate_ranges,
                allowed_directions=allowed_directions,
                pg=pg,
            )
        except Exception as e:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE: ошибка обработки символа %s для сигнала id=%s ('%s'): %s",
                symbol,
                signal_id,
                name,
                e,
                exc_info=True,
            )
            return 0, 0, 0


# 🔸 Внутренняя логика обработки символа без семафора
async def _process_symbol_rsislope_inner(
    signal_id: int,
    signal_key: str,
    name: str,
    timeframe: str,
    symbol: str,
    fast_instance_id: int,
    slow_instance_id: int,
    rsi_instance_id: int,
    rsi_timeframe: str,
    rsi_source_key: str,
    slope_k: int,
    from_time: datetime,
    to_time: datetime,
    existing_events: set[Tuple[str, datetime, str]],
    candidate_ranges: List[Tuple[float, float]],
    allowed_directions: Set[str],
    pg,
) -> Tuple[int, int, int]:
    # загружаем серии EMA для fast и slow
    fast_series = await _load_indicator_series(pg, fast_instance_id, symbol, from_time, to_time)
    slow_series = await _load_indicator_series(pg, slow_instance_id, symbol, from_time, to_time)

    if not fast_series or not slow_series:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: недостаточно данных EMA для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # работаем только по общим временным точкам
    times = sorted(set(fast_series.keys()) & set(slow_series.keys()))
    if len(times) < 2:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: слишком мало общих баров EMA для %s, сигнал id=%s ('%s')",
            symbol,
            signal_id,
            name,
        )
        return 0, 0, 0

    # epsilon = 1 * ticksize (как в обычном emacross)
    ticker_info = get_ticker_info(symbol) or {}
    ticksize = ticker_info.get("ticksize")
    try:
        epsilon = 1.0 * float(ticksize) if ticksize is not None else 0.0
    except Exception:
        epsilon = 0.0

    # классификация состояний и поиск кроссов EMA
    ema_candidates: List[Tuple[datetime, str]] = []
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

            ema_candidates.append((ts, direction))
            prev_state = state

    if not ema_candidates:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: EMA-кроссов не найдено для %s в окне [%s..%s]",
            symbol,
            from_time,
            to_time,
        )
        return 0, 0, 0

    # загружаем ряд RSI для расчёта slope
    # для slope_k баров назад нужен небольшой запас по времени
    rsi_from_time = from_time - timedelta(minutes=60 * slope_k) if rsi_timeframe.lower() == "h1" else from_time
    rsi_series_list = await _load_indicator_series_list(
        pg,
        rsi_instance_id,
        symbol,
        rsi_from_time,
        to_time,
    )
    if not rsi_series_list:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE: нет истории RSI для %s (instance_id=%s) в окне [%s..%s]",
            symbol,
            rsi_instance_id,
            rsi_from_time,
            to_time,
        )
        return 0, 0, 0

    # подгружаем цены close для найденных EMA-кандидатов
    open_times = [ts for ts, _ in ema_candidates]
    prices = await _load_close_prices(pg, symbol, timeframe, open_times)

    to_insert = []
    long_count = 0
    short_count = 0

    for ts, direction in ema_candidates:
        # проверяем наличие цены
        price = prices.get(ts)
        if price is None:
            continue

        # проверяем идемпотентность
        key_tuple = (symbol, ts, direction)
        if key_tuple in existing_events:
            continue

        # ищем индекс RSI-бара <= ts
        idx = _find_index_leq(rsi_series_list, ts)
        if idx is None:
            continue

        if idx - slope_k < 0:
            # недостаточно истории для slope
            continue

        rsi_t = rsi_series_list[idx][1]
        rsi_prev = rsi_series_list[idx - slope_k][1]
        slope = rsi_t - rsi_prev

        # проверяем попадание slope в один из кандидатных диапазонов
        if not _slope_in_ranges(slope, candidate_ranges):
            continue

        signal_uuid = uuid.uuid4()
        message = "EMA_CROSS_RSISLOPE_LONG" if direction == "long" else "EMA_CROSS_RSISLOPE_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "direction": direction,
            "price": float(price),
            "rsi_instance_id": rsi_instance_id,
            "rsi_timeframe": rsi_timeframe,
            "rsi_source_key": rsi_source_key,
            "slope_k": slope_k,
            "rsi_t": float(rsi_t),
            "rsi_prev": float(rsi_prev),
            "slope": float(slope),
            "candidate_ranges": [
                {"from": r[0], "to": r[1]} for r in candidate_ranges
            ],
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
        "BT_SIG_EMA_CROSS_RSISLOPE: %s → вставлено событий=%s (long=%s, short=%s) для сигнала id=%s ('%s')",
        symbol,
        inserted,
        long_count,
        short_count,
        signal_id,
        name,
    )
    return inserted, long_count, short_count


# 🔸 Классификация состояния fast vs slow по diff и epsilon (та же логика, что в bt_signals_emacross.py)
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


# 🔸 Загрузка серии индикатора для одного инстанса / символа / окна (dict по open_time)
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


# 🔸 Загрузка серии индикатора в виде списка (для _find_index_leq)
async def _load_indicator_series_list(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> List[Tuple[datetime, float]]:
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

    series: List[Tuple[datetime, float]] = []
    for r in rows:
        try:
            series.append((r["open_time"], float(r["value"])))
        except Exception:
            continue
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


# 🔸 Проверка попадания slope в один из кандидатов диапазонов
def _slope_in_ranges(slope: float, ranges: List[Tuple[float, float]]) -> bool:
    for b_from, b_to in ranges:
        # используем полуоткрытый интервал [from, to), кроме случая inf
        if slope >= b_from and slope < b_to:
            return True
    return False


# 🔸 Вспомогательные функции чтения параметров сигнала/анализатора
def _get_int_param(params: Dict[str, Any], name: str, default: Optional[int]) -> Optional[int]:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    try:
        return int(str(raw))
    except Exception:
        return default


def _get_str_param(params: Dict[str, Any], name: str, default: Optional[str]) -> Optional[str]:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw)


def _get_int_param_from_analysis(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default
    try:
        # в analysis_params значение лежит внутри {"value": ...}
        return int(str(cfg.get("value")))
    except Exception:
        return default