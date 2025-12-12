# bt_rsimfi_stats.py — периодический снимок распределения позиций по биннам RSI/MFI

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("BT_RSIMFI_STATS")

# 🔸 Таймфреймы, по которым смотрим RSI/MFI в raw_stat
RSIMFI_TFS = ["m5", "m15", "h1"]

# 🔸 Настройки периодичности
INITIAL_DELAY_SEC = 60
SLEEP_BETWEEN_RUNS_SEC = 3600  # 1 час

# 🔸 Индикаторы и их param_name в raw_stat + имя для indicator_param
INDICATORS: List[Tuple[str, str, str]] = [
    ("rsi", "rsi14", "rsi14"),
    ("rsi", "rsi21", "rsi21"),
    ("mfi", "mfi14", "mfi14"),
    ("mfi", "mfi21", "mfi21"),
]


# 🔸 Публичная точка входа: периодический запуск анализа RSI/MFI по всем сигналам
async def run_bt_rsimfi_stats_worker(pg) -> None:
    log.debug(
        "BT_RSIMFI_STATS: воркер запущен, первый проход будет выполнен через %s секунд",
        INITIAL_DELAY_SEC,
    )
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        started_at = datetime.utcnow()
        try:
            await _run_single_pass(pg)
        except Exception as e:
            log.error(
                "BT_RSIMFI_STATS: ошибка при выполнении прохода: %s",
                e,
                exc_info=True,
            )

        finished_at = datetime.utcnow()
        log.info(
            "BT_RSIMFI_STATS: проход завершён, время_старта=%s, время_окончания=%s, следующий запуск через %s секунд",
            started_at,
            finished_at,
            SLEEP_BETWEEN_RUNS_SEC,
        )

        await asyncio.sleep(SLEEP_BETWEEN_RUNS_SEC)


# 🔸 Один проход по всем (scenario_id, signal_id)
async def _run_single_pass(pg) -> None:
    log.debug("BT_RSIMFI_STATS: старт одиночного прохода по bt_scenario_positions")

    # очищаем таблицу bt_analysys_rsimfi целиком
    async with pg.acquire() as conn:
        await conn.execute("DELETE FROM bt_analysys_rsimfi")
    log.debug("BT_RSIMFI_STATS: таблица bt_analysys_rsimfi очищена перед новым проходом")

    pairs = await _load_distinct_scenario_signal_pairs(pg)
    if not pairs:
        log.info("BT_RSIMFI_STATS: в bt_scenario_positions нет позиций, проход завершён")
        return

    log.debug(
        "BT_RSIMFI_STATS: найдено различных пар (scenario_id, signal_id): %s",
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


# 🔸 Обработка одной пары (scenario_id, signal_id): считаем гистограммы по TF и индикаторам и записываем в bt_analysys_rsimfi
async def _process_pair(pg, scenario_id: int, signal_id: int, run_at: datetime) -> None:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
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
            "BT_RSIMFI_STATS: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
            scenario_id,
            signal_id,
        )
        return

    # структура:
    #   hist[tf][indicator_param][bin_name] = count
    #   missing_by_tf[tf][indicator_param] = count_missing
    hist: Dict[str, Dict[str, Dict[str, int]]] = {
        tf: {ind_param: {} for _, _, ind_param in INDICATORS}
        for tf in RSIMFI_TFS
    }
    missing_by_tf: Dict[str, Dict[str, int]] = {
        tf: {ind_param: 0 for _, _, ind_param in INDICATORS}
        for tf in RSIMFI_TFS
    }
    total_by_tf: Dict[str, int] = {tf: 0 for tf in RSIMFI_TFS}

    for r in rows:
        raw = r["raw_stat"]

        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        for tf in RSIMFI_TFS:
            total_by_tf[tf] += 1

            for family, param_name, ind_param in INDICATORS:
                value = _extract_indicator_value(raw, tf, family, param_name)
                if value is None:
                    missing_by_tf[tf][ind_param] += 1
                    continue

                bin_name = _value_to_bin(value)
                if bin_name is None:
                    missing_by_tf[tf][ind_param] += 1
                    continue

                tf_hist = hist[tf][ind_param]
                tf_hist[bin_name] = tf_hist.get(bin_name, 0) + 1

    # формируем строки для вставки
    rows_to_insert: List[tuple] = []

    for tf in RSIMFI_TFS:
        total = total_by_tf[tf]

        for _, _, ind_param in INDICATORS:
            missing = missing_by_tf[tf][ind_param]
            with_data = total - missing
            bins = hist[tf][ind_param]

            if not bins:
                # нет ни одной валидной точки данных для этого индикатора на данном TF
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
                "BT_RSIMFI_STATS: scenario_id=%s, signal_id=%s, tf=%s, indicator=%s — "
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
        "BT_RSIMFI_STATS: записано строк в bt_analysys_rsimfi для scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(rows_to_insert),
    )


# 🔸 Извлечение значения индикатора family/param_name из raw_stat для TF
def _extract_indicator_value(
    raw_stat: Any,
    tf: str,
    family: str,
    param_name: str,
) -> Optional[float]:
    if raw_stat is None:
        return None

    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_block = (raw_stat.get("tf") or {}).get(tf)
    if not isinstance(tf_block, dict):
        return None

    indicators = tf_block.get("indicators") or {}
    if not isinstance(indicators, dict):
        return None

    fam = indicators.get(family) or {}
    if not isinstance(fam, dict):
        return None

    value = fam.get(param_name)
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# 🔸 Разбиение значения 0–100 по биннам bin_0..bin_4 (шаг 20)
def _value_to_bin(value: float) -> Optional[str]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    # допускаем небольшие выходы за пределы, но квантуем в 0..4
    idx = int(v // 20)
    if idx < 0:
        idx = 0
    if idx > 4:
        idx = 4

    return f"bin_{idx}"