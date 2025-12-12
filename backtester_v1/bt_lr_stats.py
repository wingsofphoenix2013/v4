# bt_lr_stats.py — периодический снимок распределения позиций по биннам LR-канала

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_LR_STATS")

# 🔸 Таймфреймы, по которым смотрим LR в raw_stat
LR_TFS = ["m5", "m15", "h1"]

# 🔸 Длины LR, которые анализируем
LR_LENGTHS = [50, 100]

# 🔸 Имена indicator_param для каждой длины
LR_INDICATOR_PARAMS: Dict[int, str] = {
    50: "lr50",
    100: "lr100",
}

# 🔸 Настройки периодичности
INITIAL_DELAY_SEC = 60
SLEEP_BETWEEN_RUNS_SEC = 3600  # 1 час


# 🔸 Публичная точка входа: периодический запуск LR-статистики по всем сигналам
async def run_bt_lr_stats_worker(pg) -> None:
    log.debug(
        "BT_LR_STATS: воркер запущен, первый проход будет выполнен через %s секунд",
        INITIAL_DELAY_SEC,
    )
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        started_at = datetime.utcnow()
        try:
            await _run_single_pass(pg)
        except Exception as e:
            log.error(
                "BT_LR_STATS: ошибка при выполнении прохода: %s",
                e,
                exc_info=True,
            )

        finished_at = datetime.utcnow()
        log.info(
            "BT_LR_STATS: проход завершён, время_старта=%s, время_окончания=%s, "
            "следующий запуск через %s секунд",
            started_at,
            finished_at,
            SLEEP_BETWEEN_RUNS_SEC,
        )

        await asyncio.sleep(SLEEP_BETWEEN_RUNS_SEC)


# 🔸 Один проход по всем (scenario_id, signal_id)
async def _run_single_pass(pg) -> None:
    log.debug("BT_LR_STATS: старт одиночного прохода по bt_scenario_positions")

    # очищаем только LR-строки в bt_analysys_rsimfi (по indicator_param)
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysys_rsimfi
            WHERE indicator_param IN ('lr50', 'lr100')
               OR indicator_param IS NULL
            """
        )
    log.debug(
        "BT_LR_STATS: строки с indicator_param IN ('lr50','lr100') очищены из bt_analysys_rsimfi"
    )

    pairs = await _load_distinct_scenario_signal_pairs(pg)
    if not pairs:
        log.info("BT_LR_STATS: в bt_scenario_positions нет позиций, проход завершён")
        return

    log.debug(
        "BT_LR_STATS: найдено различных пар (scenario_id, signal_id): %s",
        len(pairs),
    )

    run_at = datetime.utcnow()

    for scenario_id, signal_id in pairs:
        await _process_pair(pg, scenario_id, signal_id, run_at)


# 🔸 Загрузка уникальных (scenario_id, signal_id) из bt_scenario_positions
async def _load_distinct_scenario_signal_pairs(pg) -> List[tuple]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT scenario_id, signal_id
            FROM bt_scenario_positions
            ORDER BY scenario_id, signal_id
            """
        )
    return [(int(r["scenario_id"]), int(r["signal_id"])) for r in rows]


# 🔸 Обработка одной пары (scenario_id, signal_id): считаем гистограммы по TF и длинам LR и записываем в bt_analysys_rsimfi
async def _process_pair(pg, scenario_id: int, signal_id: int, run_at: datetime) -> None:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                entry_price,
                raw_stat
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            """,
            scenario_id,
            signal_id,
        )

    if not rows:
        log.debug(
            "BT_LR_STATS: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
            scenario_id,
            signal_id,
        )
        return

    # структура:
    #   hist[tf][indicator_param][bin_name] = count
    #   missing[tf][indicator_param] = count_missing
    #   total[tf] = количество позиций (для пары) по данному tf
    hist: Dict[str, Dict[str, Dict[str, int]]] = {
        tf: {LR_INDICATOR_PARAMS[length]: {} for length in LR_LENGTHS}
        for tf in LR_TFS
    }
    missing_by_tf: Dict[str, Dict[str, int]] = {
        tf: {LR_INDICATOR_PARAMS[length]: 0 for length in LR_LENGTHS}
        for tf in LR_TFS
    }
    total_by_tf: Dict[str, int] = {tf: 0 for tf in LR_TFS}

    for r in rows:
        raw = r["raw_stat"]
        entry_price = r["entry_price"]

        # приводим raw_stat к dict при необходимости
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        price = _safe_float(entry_price)
        if price is None:
            # если нет цены входа — позиция не участвует в LR-статистике
            for tf in LR_TFS:
                total_by_tf[tf] += 1
                for length in LR_LENGTHS:
                    ind_param = LR_INDICATOR_PARAMS[length]
                    missing_by_tf[tf][ind_param] += 1
            continue

        # обрабатываем по каждому TF
        for tf in LR_TFS:
            total_by_tf[tf] += 1

            for length in LR_LENGTHS:
                ind_param = LR_INDICATOR_PARAMS[length]

                upper, lower = _extract_lr_bounds(raw, tf, length)
                if upper is None or lower is None:
                    missing_by_tf[tf][ind_param] += 1
                    continue

                bin_name = _lr_position_to_bin(price, upper, lower)
                if bin_name is None:
                    missing_by_tf[tf][ind_param] += 1
                    continue

                tf_hist = hist[tf][ind_param]
                tf_hist[bin_name] = tf_hist.get(bin_name, 0) + 1

    # формируем строки для вставки
    rows_to_insert: List[tuple] = []

    for tf in LR_TFS:
        total = total_by_tf[tf]

        for length in LR_LENGTHS:
            ind_param = LR_INDICATOR_PARAMS[length]
            missing = missing_by_tf[tf][ind_param]
            with_data = total - missing
            bins = hist[tf][ind_param]

            if not bins:
                # нет ни одной валидной точки данных для этой длины LR на данном TF
                rows_to_insert.append(
                    (
                        run_at,
                        scenario_id,
                        signal_id,
                        tf,
                        "NONE",
                        total,
                        with_data,
                        missing,
                        0,
                        ind_param,
                    )
                )
                bins_repr = ""
            else:
                for bin_name, count in sorted(bins.items()):
                    rows_to_insert.append(
                        (
                            run_at,
                            scenario_id,
                            signal_id,
                            tf,
                            bin_name,
                            total,
                            with_data,
                            missing,
                            count,
                            ind_param,
                        )
                    )

                bins_repr = ", ".join(
                    f"{bin_name}: {count}"
                    for bin_name, count in sorted(bins.items())
                )

            log.info(
                "BT_LR_STATS: scenario_id=%s, signal_id=%s, tf=%s, indicator=%s — "
                "позиций_всего=%s, с_данными=%s, без_данных=%s, распределение={%s}",
                scenario_id,
                signal_id,
                tf,
                ind_param,
                total,
                with_data,
                missing,
                bins_repr,
            )

    if rows_to_insert:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_analysys_rsimfi (
                    run_at,
                    scenario_id,
                    signal_id,
                    timeframe,
                    zone_label,
                    positions_total,
                    positions_with_data,
                    positions_missing,
                    count_in_zone,
                    indicator_param
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5, $6, $7, $8, $9, $10
                )
                """,
                rows_to_insert,
            )

    log.debug(
        "BT_LR_STATS: записано строк в bt_analysys_rsimfi для scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(rows_to_insert),
    )


# 🔸 Извлечение верхней и нижней границ LR-канала из raw_stat по TF и длине
def _extract_lr_bounds(
    raw_stat: Any,
    tf: str,
    length: int,
) -> Tuple[Optional[float], Optional[float]]:
    if raw_stat is None:
        return None, None

    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None, None

    if not isinstance(raw_stat, dict):
        return None, None

    tf_block = (raw_stat.get("tf") or {}).get(tf)
    if not isinstance(tf_block, dict):
        return None, None

    indicators = tf_block.get("indicators") or {}
    if not isinstance(indicators, dict):
        return None, None

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None, None

    prefix = f"lr{length}"
    upper_val = lr_family.get(f"{prefix}_upper")
    lower_val = lr_family.get(f"{prefix}_lower")

    upper = _safe_float(upper_val)
    lower = _safe_float(lower_val)

    return upper, lower


# 🔸 Маппинг цены относительно LR-канала в бин bin_0..bin_5
def _lr_position_to_bin(
    price: float,
    upper: float,
    lower: float,
) -> Optional[str]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except (TypeError, ValueError):
        return None

    H = u - l
    if H <= 0:
        return None

    # выше верхней границы
    if p > u:
        return "bin_0"

    # ниже нижней границы
    if p < l:
        return "bin_5"

    # внутри канала: делим на 4 зоны сверху вниз
    # rel = 0 → на верхней границе, rel = 1 → на нижней
    rel = (u - p) / H

    if rel < 0:
        rel = 0.0
    if rel > 1:
        rel = 1.0

    idx = int(rel * 4)  # 0..3
    if idx < 0:
        idx = 0
    if idx > 3:
        idx = 3

    # idx=0 → верхняя четверть, idx=3 → нижняя четверть
    return f"bin_{1 + idx}"


# 🔸 Вспомогательная функция: безопасное приведение к float
def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None