# bt_lr50_angle.py — периодический хистограмм-анализ углов LR50 по всем сигналам

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Логгер модуля
log = logging.getLogger("BT_LR50_ANGLE")

# 🔸 Таймфреймы, по которым смотрим lr50_angle в raw_stat
LR_TFS = ["m5", "m15", "h1"]

# 🔸 Настройки периодичности
INITIAL_DELAY_SEC = 60
SLEEP_BETWEEN_RUNS_SEC = 3600

# 🔸 Биннинг для углов LR50: от 0 в обе стороны с шагом 0.5 до -5/5, остальное в крайние кучи
# порядок важен для аккуратного логирования
def _build_angle_bins() -> List[Tuple[Optional[float], Optional[float], str]]:
    bins: List[Tuple[Optional[float], Optional[float], str]] = []

    # левая "куча": angle < -5.0
    bins.append((None, -5.0, "< -5.0"))

    # промежутки от -5.0 до 0.0, шаг 0.5: [-5.0,-4.5), [-4.5,-4.0), ..., [-0.5,0.0)
    step = 0.5
    v = -5.0
    while v < 0.0:
        lo = v
        hi = v + step
        # последний отрезок до 0.0 не включительно
        label = f"[{lo:.1f}; {hi:.1f})"
        bins.append((lo, hi, label))
        v = hi

    # от 0.0 до 5.0: [0.0,0.5), [0.5,1.0), ..., [4.5,5.0)
    v = 0.0
    while v < 5.0:
        lo = v
        hi = v + step
        label = f"[{lo:.1f}; {hi:.1f})"
        bins.append((lo, hi, label))
        v = hi

    # правая "куча": angle >= 5.0
    bins.append((5.0, None, ">= 5.0"))

    return bins


ANGLE_BINS = _build_angle_bins()


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

    signal_ids = await _load_distinct_signal_ids(pg)
    if not signal_ids:
        log.info("BT_LR50_ANGLE: в bt_scenario_positions нет сигналов, проход завершён")
        return

    log.debug(
        "BT_LR50_ANGLE: найдено различных сигналов в bt_scenario_positions: %s",
        len(signal_ids),
    )

    for signal_id in signal_ids:
        await _process_signal(pg, signal_id)


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


# 🔸 Обработка одного signal_id: подсчёт гистограмм по tf ∈ {m5, m15, h1}
async def _process_signal(pg, signal_id: int) -> None:
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

    # готовим счётчики по TF и бинам
    # структура: tf -> label -> count
    hist: Dict[str, Dict[str, int]] = {tf: {label: 0 for _, _, label in ANGLE_BINS} for tf in LR_TFS}
    missing_by_tf: Dict[str, int] = {tf: 0 for tf in LR_TFS}
    total_by_tf: Dict[str, int] = {tf: 0 for tf in LR_TFS}

    for r in rows:
        raw = r["raw_stat"]

        # приводим jsonb к dict, если он строкой
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        for tf in LR_TFS:
            total_by_tf[tf] += 1
            angle = _extract_lr50_angle(raw, tf)
            if angle is None:
                missing_by_tf[tf] += 1
                continue

            label = _assign_angle_bin(angle)
            if label is None:
                # на всякий случай считаем это как missing
                missing_by_tf[tf] += 1
                continue

            hist[tf][label] += 1

    # логируем итоговую статистику по каждому TF
    for tf in LR_TFS:
        total = total_by_tf[tf]
        missing = missing_by_tf[tf]
        used = total - missing

        # формируем компактное представление гистограммы
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


# 🔸 Определение бина для угла LR50
def _assign_angle_bin(angle: Decimal) -> Optional[str]:
    try:
        val = float(angle)
    except (TypeError, InvalidOperation, ValueError):
        return None

    for lo, hi, label in ANGLE_BINS:
        # левая "куча": angle < hi
        if lo is None and hi is not None:
            if val < hi:
                return label
        # правая "куча": angle >= lo
        elif lo is not None and hi is None:
            if val >= lo:
                return label
        # обычный полузакрытый интервал [lo, hi)
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