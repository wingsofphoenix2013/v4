# bt_lr50_angle.py — периодический хистограмм-анализ углов LR50 по m15/h1 для всех сигналов

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Логгер модуля
log = logging.getLogger("BT_LR50_ANGLE")

# 🔸 Таймфреймы, по которым смотрим lr50_angle в raw_stat (m5 пока исключаем)
LR_TFS = ["m15", "h1"]

# 🔸 Настройки периодичности
INITIAL_DELAY_SEC = 60
SLEEP_BETWEEN_RUNS_SEC = 3600  # 1 час


# 🔸 Вспомогательная функция форматирования Decimal с 2 знаками
def _fmt2(d: Decimal) -> str:
    return str(d.quantize(Decimal("0.00")))


# 🔸 Биннинг для m15 и h1: 5 логических зон по углу LR50
# 1) angle <= -0.10          → сильный нисходящий тренд
# 2) -0.10 < angle < -0.02   → слабый нисходящий
# 3) -0.02 <= angle <= 0.02  → флэт / почти горизонт
# 4) 0.02 < angle < 0.10     → слабый восходящий
# 5) angle >= 0.10           → сильный восходящий тренд
def _build_5zone_bins() -> List[Tuple[Optional[Decimal], Optional[Decimal], str]]:
    bins: List[Tuple[Optional[Decimal], Optional[Decimal], str]] = []
    D = Decimal

    # angle <= -0.10
    bins.append((None, D("-0.10"), "<= -0.10"))

    # -0.10 < angle < -0.02  → (-0.10; -0.02)
    lo = D("-0.10")
    hi = D("-0.02")
    bins.append((lo, hi, "(-0.10; -0.02)"))

    # -0.02 <= angle <= 0.02  → [-0.02; 0.02]
    lo = D("-0.02")
    hi = D("0.02")
    bins.append((lo, hi, "[-0.02; 0.02]"))

    # 0.02 < angle < 0.10     → (0.02; 0.10)
    lo = D("0.02")
    hi = D("0.10")
    bins.append((lo, hi, "(0.02; 0.10)"))

    # angle >= 0.10
    bins.append((D("0.10"), None, ">= 0.10"))

    return bins


# 🔸 Схемы бинов по TF (для m15 и h1 одинаковые 5 зон)
ANGLE_BINS_BY_TF: Dict[str, List[Tuple[Optional[Decimal], Optional[Decimal], str]]] = {
    "m15": _build_5zone_bins(),
    "h1": _build_5zone_bins(),
}


# 🔸 Получить список биннов для конкретного TF
def _get_angle_bins_for_tf(tf: str) -> List[Tuple[Optional[Decimal], Optional[Decimal], str]]:
    # по умолчанию тоже 5-зонная схема, если вдруг tf незнаком
    return ANGLE_BINS_BY_TF.get(tf, _build_5zone_bins())


# 🔸 Публичная точка входа: периодический запуск хистограмм по всем сигналам
async def run_bt_lr50_angle_worker(pg) -> None:
    log.debug(
        "BT_LR50_ANGLE: воркер запущен, первый проход будет выполнен через %s секунд",
        INITIAL_DELAY_SEC,
    )
    await asyncio.sleep(INITIAL_DELAY_SEC)

    while True:
        started_at = datetime.utcnow()
        try:
            await _run_single_pass(pg)
        except Exception as e:
            log.error(
                "BT_LR50_ANGLE: ошибка при выполнении прохода: %s",
                e,
                exc_info=True,
            )

        finished_at = datetime.utcnow()
        log.info(
            "BT_LR50_ANGLE: проход завершён, время_старта=%s, время_окончания=%s, следующий запуск через %s секунд",
            started_at,
            finished_at,
            SLEEP_BETWEEN_RUNS_SEC,
        )

        await asyncio.sleep(SLEEP_BETWEEN_RUNS_SEC)


# 🔸 Один проход по всем signal_id, присутствующим в bt_scenario_positions
async def _run_single_pass(pg) -> None:
    log.debug("BT_LR50_ANGLE: старт одиночного прохода по bt_scenario_positions")

    # очищаем таблицу bt_analysis_angle целиком
    async with pg.acquire() as conn:
        await conn.execute("DELETE FROM bt_analysis_angle")
    log.debug("BT_LR50_ANGLE: таблица bt_analysis_angle очищена перед новым проходом")

    signal_ids = await _load_distinct_signal_ids(pg)
    if not signal_ids:
        log.info("BT_LR50_ANGLE: в bt_scenario_positions нет сигналов, проход завершён")
        return

    log.debug(
        "BT_LR50_ANGLE: найдено различных сигналов в bt_scenario_positions: %s",
        len(signal_ids),
    )

    run_at = datetime.utcnow()

    for signal_id in signal_ids:
        await _process_signal(pg, signal_id, run_at)


# 🔸 Загрузка списка уникальных signal_id из bt_scenario_positions
async def _load_distinct_signal_ids(pg) -> List[int]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT signal_id
            FROM bt_scenario_positions
            ORDER BY signal_id
            """
        )
    return [int(r["signal_id"]) for r in rows]


# 🔸 Обработка одного signal_id:
#     - считаем гистограммы по m15/h1
#     - для h1-бинов ещё считаем распределение m15-зон и кладём в raw_stat
async def _process_signal(pg, signal_id: int, run_at: datetime) -> None:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                direction,
                raw_stat
            FROM bt_scenario_positions
            WHERE signal_id = $1
            """,
            signal_id,
        )

    if not rows:
        log.debug(
            "BT_LR50_ANGLE: для signal_id=%s нет позиций в bt_scenario_positions",
            signal_id,
        )
        return

    # структура: tf -> label -> count
    hist: Dict[str, Dict[str, int]] = {}
    missing_by_tf: Dict[str, int] = {}
    total_by_tf: Dict[str, int] = {}

    for tf in LR_TFS:
        bins = _get_angle_bins_for_tf(tf)
        hist[tf] = {label: 0 for _, _, label in bins}
        missing_by_tf[tf] = 0
        total_by_tf[tf] = 0

    # для h1: раскладка по m15-зонам внутри каждого h1-бина
    # h1_label -> (m15_label -> count)
    h1_m15_cross: Dict[str, Dict[str, int]] = {}

    for r in rows:
        raw = r["raw_stat"]

        # приводим jsonb к dict, если он строкой
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        # извлекаем углы сразу для обоих TF, чтобы использовать их в cross-статистике
        angle_m15 = _extract_lr50_angle(raw, "m15")
        angle_h1 = _extract_lr50_angle(raw, "h1")

        # m15
        total_by_tf["m15"] += 1
        m15_label: Optional[str] = None
        if angle_m15 is None:
            missing_by_tf["m15"] += 1
        else:
            m15_label = _assign_angle_bin(angle_m15, "m15")
            if m15_label is None:
                missing_by_tf["m15"] += 1
            else:
                hist["m15"][m15_label] += 1

        # h1
        total_by_tf["h1"] += 1
        h1_label: Optional[str] = None
        if angle_h1 is None:
            missing_by_tf["h1"] += 1
        else:
            h1_label = _assign_angle_bin(angle_h1, "h1")
            if h1_label is None:
                missing_by_tf["h1"] += 1
            else:
                hist["h1"][h1_label] += 1

        # cross: считаем m15-зоны внутри каждого h1-бина, если оба есть
        if h1_label is not None and m15_label is not None:
            m15_map = h1_m15_cross.setdefault(h1_label, {})
            m15_map[m15_label] = m15_map.get(m15_label, 0) + 1

    # формируем строки для вставки в bt_analysis_angle
    rows_to_insert: List[Tuple[Any, ...]] = []

    for tf in LR_TFS:
        total = total_by_tf[tf]
        missing = missing_by_tf[tf]
        used = total - missing
        bins = _get_angle_bins_for_tf(tf)

        for lo, hi, label in bins:
            count = hist[tf].get(label, 0)

            # raw_stat:
            #  - для m15 оставляем NULL
            #  - для h1 кладём JSON-раскладку по m15-зонам, если есть
            if tf == "h1":
                m15_dist = h1_m15_cross.get(label) or {}
                if m15_dist:
                    raw_obj = {"m15": m15_dist}
                    raw_stat_json = json.dumps(raw_obj, ensure_ascii=False)
                else:
                    raw_stat_json = None
            else:
                raw_stat_json = None

            rows_to_insert.append(
                (
                    run_at,
                    signal_id,
                    tf,
                    lo,
                    hi,
                    label,
                    total,
                    used,
                    missing,
                    count,
                    raw_stat_json,
                )
            )

        bins_repr = ", ".join(
            f"{label}: {count}"
            for label, count in hist[tf].items()
            if count > 0
        )

        log.info(
            "BT_LR50_ANGLE: signal_id=%s, tf=%s — позиций_всего=%s, с_углом=%s, без_угла=%s, гистограмма={%s}",
            signal_id,
            tf,
            total,
            used,
            missing,
            bins_repr,
        )

    # записываем в bt_analysis_angle
    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_analysis_angle (
                run_at,
                signal_id,
                timeframe,
                bin_lo,
                bin_hi,
                bin_label,
                positions_total,
                positions_with_angle,
                positions_missing,
                count_in_bin,
                raw_stat
            )
            VALUES (
                $1, $2, $3,
                $4, $5, $6,
                $7, $8, $9,
                $10, $11
            )
            """,
            rows_to_insert,
        )

    log.debug(
        "BT_LR50_ANGLE: в bt_analysis_angle записано строк=%s для signal_id=%s",
        len(rows_to_insert),
        signal_id,
    )


# 🔸 Извлечение lr50_angle из raw_stat для заданного TF
def _extract_lr50_angle(raw_stat: Any, tf: str) -> Optional[Decimal]:
    if raw_stat is None:
        return None

    # если raw_stat пришёл строкой из jsonb — парсим
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

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    value = lr_family.get("lr50_angle")
    if value is None:
        return None

    return _safe_decimal(value)


# 🔸 Определение бина для угла LR50 с учётом TF
def _assign_angle_bin(angle: Decimal, tf: str) -> Optional[str]:
    val = angle
    bins = _get_angle_bins_for_tf(tf)

    for lo, hi, label in bins:
        # левая куча: angle < hi
        if lo is None and hi is not None:
            if val < hi:
                return label
        # правая куча: angle >= lo
        elif lo is not None and hi is None:
            if val >= lo:
                return label
        # обычный интервал [lo, hi) или [lo, hi] в центральной зоне — нам хватит [lo; hi)
        elif lo is not None and hi is not None:
            if lo <= val < hi:
                return label

    return None


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")