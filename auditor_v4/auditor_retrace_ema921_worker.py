# 🔸 auditor_retrace_ema921_worker.py — воркер анализа качества отката для EMA9/21 (m5)

# 🔸 Импорты
import logging
import datetime as dt
from collections import defaultdict
from typing import Dict, Any, List, Tuple

import auditor_infra as infra


# 🔸 Логгер
log = logging.getLogger("AUD_RETRACE_EMA921")


# 🔸 Константы воркера
TIMEFRAME = "m5"
BATCH_SIZE = 200
WINDOW_SIZE_BARS = 30  # N баров назад от бара сигнала

# инстансы индикаторов (m5), по данным пользователя
INSTANCE_ID_EMA9 = 1
INSTANCE_ID_EMA21 = 2
INSTANCE_ID_ATR14 = 6

# Пороговые параметры (примерные, должны быть вынесены в конфиг и подобраны статистически)
LOWER_ATR_THRESHOLD = 0.2        # retracement_ratio_atr < 0.2 → малый откат
LOWER_IMPULSE_THRESHOLD = 0.15   # retracement_ratio_impulse < 0.15 → малый откат

UPPER_ATR_THRESHOLD = 1.2        # retracement_ratio_atr > 1.2 → глубокий откат
UPPER_IMPULSE_THRESHOLD = 0.5    # retracement_ratio_impulse > 0.5 → глубокий откат

MIN_SPREAD_EMA = 0.0005          # доля цены для проверки EMA9 vs EMA21 (пример: 0.05%)
DEEP_SKEW_FACTOR = 0.8           # skew для "слишком близко к предыдущему экстремуму"


# 🔸 Вспомогательные функции

def _calc_retrace_flags(
    direction: str,
    entry_price: float,
    swing_high: float,
    swing_low_prev: float,
    atr14: float | None,
    ema9_series: List[float],
    ema21_series: List[float],
    swing_idx: int,
    signal_idx: int,
) -> Tuple[float | None, float | None, int, int, int]:
    """
    Расчёт retracement_ratio_atr / retracement_ratio_impulse и флагов ban_small / ban_deep / allow_retrace
    direction: 'long'/'short'
    ema*_series: список по тем же барам, что и OHLC (индекс соответствует позиции в окне)
    swing_idx: индекс swing_high (long) или swing_low (short) в окне
    signal_idx: индекс бара сигнала в окне
    """

    # защита от некорректных индексов
    if swing_idx < 0 or signal_idx <= swing_idx or signal_idx >= len(ema9_series):
        return None, None, 0, 0, 0

    # расчёт retracement_size и impulse_size
    if direction == "long":
        swing_high_price = swing_high
        swing_low_prev_price = swing_low_prev

        retracement_size = swing_high_price - entry_price
        impulse_size = swing_high_price - swing_low_prev_price

        # расстояния для проверки "слишком глубокий"
        distance_to_low = entry_price - swing_low_prev_price
        distance_to_high = swing_high_price - entry_price

        # диапазон ретрейса: (swing_idx+1 .. signal_idx)
        ema_spreads = [
            ema9_series[i] - ema21_series[i]
            for i in range(swing_idx + 1, signal_idx + 1)
            if ema9_series[i] is not None and ema21_series[i] is not None
        ]
        min_ema_spread = min(ema_spreads) if ema_spreads else None

    elif direction == "short":
        swing_low_price = swing_high  # для short мы передаём swing_low в swing_high аргументе
        swing_high_prev_price = swing_low_prev  # и наоборот

        retracement_size = entry_price - swing_low_price
        impulse_size = swing_high_prev_price - swing_low_price

        distance_to_low = entry_price - swing_low_price
        distance_to_high = swing_high_prev_price - entry_price

        ema_spreads = [
            ema9_series[i] - ema21_series[i]
            for i in range(swing_idx + 1, signal_idx + 1)
            if ema9_series[i] is not None and ema21_series[i] is not None
        ]
        max_ema_spread = max(ema_spreads) if ema_spreads else None

    else:
        return None, None, 0, 0, 0

    # защита от нереальных значений
    if retracement_size <= 0 or impulse_size <= 0:
        return None, None, 0, 0, 0

    # нормировки
    retracement_ratio_atr = None
    if atr14 and atr14 > 0:
        retracement_ratio_atr = float(retracement_size) / float(atr14)

    retracement_ratio_impulse = float(retracement_size) / float(impulse_size)

    # --- ban_small ---
    small_conditions = []

    if retracement_ratio_atr is not None:
        small_conditions.append(retracement_ratio_atr < LOWER_ATR_THRESHOLD)

    small_conditions.append(retracement_ratio_impulse < LOWER_IMPULSE_THRESHOLD)

    if direction == "long":
        if min_ema_spread is not None:
            # EMA9 почти не уходила ниже EMA21
            small_conditions.append(min_ema_spread > -MIN_SPREAD_EMA * entry_price)
    else:  # short
        if ema_spreads:
            max_ema_spread = max_ema_spread  # уже посчитан выше
            if max_ema_spread is not None:
                small_conditions.append(max_ema_spread < MIN_SPREAD_EMA * entry_price)

    ban_small = 1 if any(small_conditions) else 0

    # --- ban_deep ---
    deep_conditions = []

    if retracement_ratio_atr is not None:
        deep_conditions.append(retracement_ratio_atr > UPPER_ATR_THRESHOLD)

    deep_conditions.append(retracement_ratio_impulse > UPPER_IMPULSE_THRESHOLD)

    # геометрия "слишком глубоко к предыдущему низу"
    if distance_to_low is not None and distance_to_high is not None and distance_to_high > 0:
        deep_conditions.append(distance_to_low < distance_to_high * DEEP_SKEW_FACTOR)

    ban_deep = 1 if any(deep_conditions) else 0

    # итоговый флаг
    allow_retrace = 1 if (ban_small == 0 and ban_deep == 0) else 0

    return retracement_ratio_atr, retracement_ratio_impulse, ban_small, ban_deep, allow_retrace


def _find_swing_indices_long(highs: List[float], lows: List[float]) -> Tuple[int | None, int | None]:
    """
    По списку high/low (по окну) для long:
    - swing_high_idx: последний локальный максимум до сигнала,
    - swing_low_prev_idx: последний локальный минимум перед swing_high_idx.
    """
    n = len(highs)
    if n < 3:
        return None, None

    # последний локальный максимум перед последним баром (сигналом)
    swing_high_idx = None
    for i in range(n - 2, 0, -1):
        if highs[i] is None:
            continue
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            swing_high_idx = i
            break

    if swing_high_idx is None:
        return None, None

    swing_low_prev_idx = None
    for i in range(swing_high_idx - 1, 0, -1):
        if lows[i] is None:
            continue
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            swing_low_prev_idx = i
            break

    if swing_low_prev_idx is None:
        return None, None

    return swing_high_idx, swing_low_prev_idx


def _find_swing_indices_short(highs: List[float], lows: List[float]) -> Tuple[int | None, int | None]:
    """
    По списку high/low (по окну) для short:
    - swing_low_idx: последний локальный минимум до сигнала,
    - swing_high_prev_idx: последний локальный максимум перед swing_low_idx.
    """
    n = len(highs)
    if n < 3:
        return None, None

    swing_low_idx = None
    for i in range(n - 2, 0, -1):
        if lows[i] is None:
            continue
        if lows[i] < lows[i - 1] and lows[i] < lows[i + 1]:
            swing_low_idx = i
            break

    if swing_low_idx is None:
        return None, None

    swing_high_prev_idx = None
    for i in range(swing_low_idx - 1, 0, -1):
        if highs[i] is None:
            continue
        if highs[i] > highs[i - 1] and highs[i] > highs[i + 1]:
            swing_high_prev_idx = i
            break

    if swing_high_prev_idx is None:
        return None, None

    return swing_low_idx, swing_high_prev_idx


# 🔸 Загрузка стратегий (все enabled, не archived, timeframe='m5')
async def _load_m5_strategies(conn) -> Dict[int, Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT id, deposit
        FROM strategies_v4
        WHERE enabled = true
          AND (archived IS NOT TRUE)
          AND timeframe = 'm5'
        """
    )

    strategies: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        sid = int(r["id"])
        strategies[sid] = {
            "id": sid,
            "deposit": r["deposit"],
        }

    log.info("🔍 AUD_RETRACE_EMA921: найдено m5-стратегий для анализа: %d", len(strategies))
    return strategies


# 🔸 Обработка позиций одной стратегии батчами
async def _process_strategy_positions(
    conn,
    strategy_id: int,
    calc_at: dt.datetime,
) -> Tuple[int, int, int]:
    last_id = 0
    total_positions = 0
    used_positions = 0
    inserted_rows = 0

    while True:
        # выборка батча позиций (все closed, обе стороны)
        rows = await conn.fetch(
            """
            SELECT id, position_uid, symbol, direction, entry_price, closed_at
            FROM positions_v4
            WHERE status = 'closed'
              AND strategy_id = $1
              AND id > $2
            ORDER BY id
            LIMIT $3
            """,
            strategy_id,
            last_id,
            BATCH_SIZE,
        )

        if not rows:
            break

        positions_batch = []
        position_uids: list[str] = []

        for r in rows:
            pid = int(r["id"])
            position_uid = str(r["position_uid"])
            symbol = str(r["symbol"])
            direction = str(r["direction"])
            entry_price = r["entry_price"]
            closed_at = r["closed_at"]

            positions_batch.append(
                {
                    "id": pid,
                    "position_uid": position_uid,
                    "symbol": symbol,
                    "direction": direction,
                    "entry_price": entry_price,
                    "closed_at": closed_at,
                    "strategy_id": strategy_id,
                }
            )
            position_uids.append(position_uid)

            if pid > last_id:
                last_id = pid

        total_positions += len(positions_batch)

        if not position_uids:
            continue

        # получение bar_time (open_time m5 бара сигнала) из indicator_position_stat
        ips_rows = await conn.fetch(
            """
            SELECT position_uid, open_time
            FROM indicator_position_stat
            WHERE position_uid = ANY($1::text[])
              AND timeframe = 'm5'
              AND param_type = 'indicator'
              AND param_base = 'ema'
              AND param_name = 'ema9'
              AND status = 'ok'
            """,
            position_uids,
        )

        bar_time_map: Dict[str, dt.datetime] = {}
        for r in ips_rows:
            puid = str(r["position_uid"])
            open_time = r["open_time"]
            bar_time_map[puid] = open_time

        # подготовка вставок
        insert_rows: list[tuple] = []

        # обработка каждой позиции
        for pos in positions_batch:
            puid = pos["position_uid"]
            symbol = pos["symbol"]
            direction = pos["direction"]
            entry_price = pos["entry_price"]
            closed_at = pos["closed_at"]

            # условия достаточности
            if puid not in bar_time_map:
                continue
            if entry_price is None:
                continue
            if closed_at is None:
                trade_date = None
            else:
                trade_date = closed_at.date() if isinstance(closed_at, dt.datetime) else closed_at

            bar_time = bar_time_map[puid]

            # загрузка окна OHLC
            ohlc_rows = await conn.fetch(
                """
                SELECT open_time, high, low, close
                FROM ohlcv_bb_m5
                WHERE symbol = $1
                  AND open_time <= $2
                ORDER BY open_time DESC
                LIMIT $3
                """,
                symbol,
                bar_time,
                WINDOW_SIZE_BARS,
            )

            if len(ohlc_rows) < 5:
                continue

            # приводим к списку в порядке возрастания времени
            ohlc_rows = list(reversed(ohlc_rows))
            times = [r["open_time"] for r in ohlc_rows]
            highs = [float(r["high"]) for r in ohlc_rows]
            lows = [float(r["low"]) for r in ohlc_rows]
            closes = [float(r["close"]) for r in ohlc_rows]

            # индекс сигнального бара — последний в окне (по bar_time)
            try:
                signal_idx = times.index(bar_time)
            except ValueError:
                # если точного совпадения нет — берём последний бар
                signal_idx = len(times) - 1

            # загрузка EMA9/EMA21/ATR14 для этого окна
            min_time = times[0]
            ind_rows = await conn.fetch(
                """
                SELECT instance_id, open_time, param_name, value
                FROM indicator_values_v4
                WHERE symbol = $1
                  AND open_time BETWEEN $2 AND $3
                  AND instance_id IN ($4, $5, $6)
                """,
                symbol,
                min_time,
                bar_time,
                INSTANCE_ID_EMA9,
                INSTANCE_ID_EMA21,
                INSTANCE_ID_ATR14,
            )

            ema9_map: Dict[dt.datetime, float] = {}
            ema21_map: Dict[dt.datetime, float] = {}
            atr_map: Dict[dt.datetime, float] = {}

            for r in ind_rows:
                inst = int(r["instance_id"])
                ot = r["open_time"]
                val = float(r["value"])
                if inst == INSTANCE_ID_EMA9:
                    ema9_map[ot] = val
                elif inst == INSTANCE_ID_EMA21:
                    ema21_map[ot] = val
                elif inst == INSTANCE_ID_ATR14:
                    atr_map[ot] = val

            ema9_series = [ema9_map.get(t) for t in times]
            ema21_series = [ema21_map.get(t) for t in times]
            atr_signal = atr_map.get(bar_time)

            # ищем swing'и
            if direction == "long":
                swing_high_idx, swing_low_prev_idx = _find_swing_indices_long(highs, lows)
                if swing_high_idx is None or swing_low_prev_idx is None:
                    continue
                swing_high_price = highs[swing_high_idx]
                swing_low_prev_price = lows[swing_low_prev_idx]

                retr_atr, retr_imp, ban_small, ban_deep, allow_retrace = _calc_retrace_flags(
                    direction,
                    float(entry_price),
                    float(swing_high_price),
                    float(swing_low_prev_price),
                    atr_signal,
                    ema9_series,
                    ema21_series,
                    swing_high_idx,
                    signal_idx,
                )

            elif direction == "short":
                swing_low_idx, swing_high_prev_idx = _find_swing_indices_short(highs, lows)
                if swing_low_idx is None or swing_high_prev_idx is None:
                    continue
                swing_low_price = lows[swing_low_idx]
                swing_high_prev_price = highs[swing_high_prev_idx]

                retr_atr, retr_imp, ban_small, ban_deep, allow_retrace = _calc_retrace_flags(
                    direction,
                    float(entry_price),
                    float(swing_low_price),        # как swing_high аргумент
                    float(swing_high_prev_price),  # как swing_low_prev аргумент
                    atr_signal,
                    ema9_series,
                    ema21_series,
                    swing_low_idx,
                    signal_idx,
                )

            else:
                continue

            if retr_atr is None or retr_imp is None:
                continue

            # расчёт "сырых" величин для сохранения
            if direction == "long":
                retracement_size = float(highs[swing_high_idx] - float(entry_price))
                impulse_size = float(highs[swing_high_idx] - float(lows[swing_low_prev_idx]))
            else:
                retracement_size = float(float(entry_price) - lows[swing_low_idx])
                impulse_size = float(highs[swing_high_prev_idx] - lows[swing_low_idx])

            insert_rows.append(
                (
                    calc_at,
                    puid,
                    strategy_id,
                    symbol,
                    direction,
                    TIMEFRAME,
                    bar_time,
                    closed_at,
                    trade_date,
                    retracement_size,
                    impulse_size,
                    atr_signal,
                    retr_atr,
                    retr_imp,
                    ban_small,
                    ban_deep,
                    allow_retrace,
                )
            )
            used_positions += 1

        # вставка батча в БД
        if insert_rows:
            await conn.executemany(
                """
                INSERT INTO auditor_retrace_ema921 (
                    calc_at,
                    position_uid,
                    strategy_id,
                    symbol,
                    direction,
                    timeframe,
                    bar_time,
                    closed_at,
                    trade_date,
                    retracement_size,
                    impulse_size,
                    atr14,
                    retracement_ratio_atr,
                    retracement_ratio_impulse,
                    ban_small,
                    ban_deep,
                    allow_retrace
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,
                    $7,$8,$9,
                    $10,$11,$12,
                    $13,$14,
                    $15,$16,$17
                )
                ON CONFLICT (position_uid, timeframe) DO NOTHING
                """,
                insert_rows,
            )
            inserted_rows += len(insert_rows)

    log.info(
        "🔍 AUD_RETRACE_EMA921: стратегия %d — позиций всего=%d, обработано=%d, записано строк=%d",
        strategy_id,
        total_positions,
        used_positions,
        inserted_rows,
    )

    return total_positions, used_positions, inserted_rows


# 🔸 Основная корутина воркера
async def run_retrace_ema921_worker():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ AUD_RETRACE_EMA921: пропуск воркера — PG не инициализирован")
        return

    calc_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    log.info("🚀 AUD_RETRACE_EMA921: старт расчёта качества отката EMA9/21 (calc_at=%s)", calc_at)

    async with infra.pg_pool.acquire() as conn:
        strategies = await _load_m5_strategies(conn)
        if not strategies:
            log.info("❌ AUD_RETRACE_EMA921: нет m5-стратегий для анализа — выход")
            return

        total_positions_all = 0
        used_positions_all = 0
        inserted_all = 0

        for strategy_id in sorted(strategies.keys()):
            log.info("🔧 AUD_RETRACE_EMA921: стратегия %d — старт обработки", strategy_id)

            total_pos, used_pos, inserted_rows = await _process_strategy_positions(
                conn,
                strategy_id,
                calc_at,
            )

            total_positions_all += total_pos
            used_positions_all += used_pos
            inserted_all += inserted_rows

        log.info(
            "✅ AUD_RETRACE_EMA921: завершено — стратегий=%d, позиций_всего=%d, обработано=%d, записано строк=%d",
            len(strategies),
            total_positions_all,
            used_positions_all,
            inserted_all,
        )