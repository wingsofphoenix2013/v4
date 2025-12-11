# bt_rsimfi_stats.py — периодический снимок распределения позиций по корзинкам RSI/MFI

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

log = logging.getLogger("BT_RSIMFI_STATS")

# 🔸 Таймфреймы, по которым смотрим RSI/MFI в raw_stat
RSIMFI_TFS = ["m5", "m15", "h1"]

# 🔸 Настройки периодичности
INITIAL_DELAY_SEC = 60
SLEEP_BETWEEN_RUNS_SEC = 3600  # 1 час

# 🔸 Пороги RSI/MFI (можно будет потом вынести в параметры)
RSI_LOW = 30.0
RSI_HIGH = 70.0
MFI_LOW = 30.0
MFI_HIGH = 70.0


# 🔸 Публичная точка входа: периодический запуск анализа RSI/MFI по всем сигналам
async def run_bt_rsimfi_stats_worker(pg) -> None:
    log.debug("BT_RSIMFI_STATS: воркер запущен, первый проход будет выполнен через %s секунд", INITIAL_DELAY_SEC)
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


# 🔸 Обработка одной пары (scenario_id, signal_id): считаем гистограммы по TF и записываем в bt_analysys_rsimfi
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

    # структура: tf -> зона -> count
    hist: Dict[str, Dict[str, int]] = {tf: {} for tf in RSIMFI_TFS}
    missing_by_tf: Dict[str, int] = {tf: 0 for tf in RSIMFI_TFS}
    total_by_tf: Dict[str, int] = {tf: 0 for tf in RSIMFI_TFS}

    for r in rows:
        raw = r["raw_stat"]

        # raw_stat может быть либо jsonb, либо строка
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        for tf in RSIMFI_TFS:
            total_by_tf[tf] += 1

            rsi_val = _extract_indicator_value(raw, tf, "rsi", "rsi14")
            mfi_val = _extract_indicator_value(raw, tf, "mfi", "mfi14")

            if rsi_val is None or mfi_val is None:
                missing_by_tf[tf] += 1
                continue

            zone = _classify_rsi_mfi(rsi_val, mfi_val)
            if zone is None:
                missing_by_tf[tf] += 1
                continue

            hist[tf][zone] = hist[tf].get(zone, 0) + 1

    # формируем строки для вставки
    rows_to_insert: List[tuple] = []

    for tf in RSIMFI_TFS:
        total = total_by_tf[tf]
        missing = missing_by_tf[tf]
        with_data = total - missing

        # чтобы было видно, что TF вообще есть, даже если ни одна зона не набралась
        if not hist[tf]:
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
                )
            )
        else:
            for zone_label, count in hist[tf].items():
                rows_to_insert.append(
                    (
                        run_at,
                        scenario_id,
                        signal_id,
                        tf,
                        zone_label,
                        total,
                        with_data,
                        missing,
                        count,
                    )
                )

        bins_repr = ", ".join(
            f"{zone}: {count}"
            for zone, count in hist[tf].items()
        )

        log.info(
            "BT_RSIMFI_STATS: scenario_id=%s, signal_id=%s, tf=%s — позиций_всего=%s, с_данными=%s, без_данных=%s, "
            "распределение={%s}",
            scenario_id,
            signal_id,
            tf,
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
                    count_in_zone
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5, $6, $7, $8, $9
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


# 🔸 Классификация RSI/MFI в одну из 5 корзинок
def _classify_rsi_mfi(rsi: float, mfi: float) -> Optional[str]:
    # сначала классифицируем RSI/MFI по трём уровням
    r_zone = _level_3(rsi, RSI_LOW, RSI_HIGH)
    m_zone = _level_3(mfi, MFI_LOW, MFI_HIGH)

    if r_zone is None or m_zone is None:
        return None

    # Z1: подтверждённый сильный тренд (оба в LOW или оба в HIGH)
    if (r_zone == "LOW" and m_zone == "LOW") or (r_zone == "HIGH" and m_zone == "HIGH"):
        return "Z1_CONFIRMED"

    # Z2: цена в экстремуме, деньги не до конца подтверждают
    if (r_zone == "HIGH" and m_zone in ("MID", "LOW")) or (r_zone == "LOW" and m_zone in ("MID", "HIGH")):
        return "Z2_PRICE_EXTREME"

    # Z3: деньги сильные, цена ещё средняя
    if r_zone == "MID" and m_zone in ("HIGH", "LOW"):
        return "Z3_FLOW_LEADS"

    # Z4: жёсткая дивергенция (цена и деньги на противоположных полюсах)
    if (r_zone == "HIGH" and m_zone == "LOW") or (r_zone == "LOW" and m_zone == "HIGH"):
        return "Z4_DIVERGENCE"

    # Z5: оба в середине (нейтрально)
    if r_zone == "MID" and m_zone == "MID":
        return "Z5_NEUTRAL"

    # теоретически сюда не попадём, но пусть будет
    return "Z5_NEUTRAL"


# 🔸 Классификация значения в LOW/MID/HIGH по двум порогам
def _level_3(value: float, low: float, high: float) -> Optional[str]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    if v <= low:
        return "LOW"
    if v >= high:
        return "HIGH"
    return "MID"