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
SLEEP_BETWEEN_RUNS_SEC = 3600  # 1 час


# 🔸 Вспомогательная функция форматирования Decimal с 2 знаками
def _fmt2(d: Decimal) -> str:
    return str(d.quantize(Decimal("0.00")))


# 🔸 Биннинг углов LR50:
#     - левая куча: angle < -0.20
#     - [-0.20; -0.10)
#     - диапазон [-0.10; 0.10) с шагом 0.02
#     - [0.10; 0.20)
#     - правая куча: angle >= 0.20
def _build_angle_bins() -> List[Tuple[Optional[Decimal], Optional[Decimal], str]]:
    bins: List[Tuple[Optional[Decimal], Optional[Decimal], str]] = []

    D = Decimal

    # левая куча: angle < -0.20
    bins.append((None, D("-0.20"), "< -0.20"))

    # [-0.20; -0.10)
    lo = D("-0.20")
    hi = D("-0.10")
    bins.append((lo, hi, f"[{_fmt2(lo)}; {_fmt2(hi)})"))

    # [-0.10; 0.10) с шагом 0.02
    step = D("0.02")
    v = D("-0.10")
    while v < D("0.10"):
        lo = v
        hi = v + step
        bins.append((lo, hi, f"[{_fmt2(lo)}; {_fmt2(hi)})"))
        v = hi

    # [0.10; 0.20)
    lo = D("0.10")
    hi = D("0.20")
    bins.append((lo, hi, f"[{_fmt2(lo)}; {_fmt2(hi)})"))

    # правая куча: angle >= 0.20
    bins.append((D("0.20"), None, ">= 0.20"))

    return bins


ANGLE_BINS: List[Tuple[Optional[Decimal], Optional[Decimal], str]] = _build_angle_bins()


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

    # сначала очищаем таблицу bt_analysis_angle целиком
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


# 🔸 Обработка одного signal_id: подсчёт гистограмм по tf ∈ {m5, m15, h1} и запись в bt_analysis_angle
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
    hist: Dict[str, Dict[str, int]] = {
        tf: {label: 0 for _, _, label in ANGLE_BINS} for tf in LR_TFS
    }
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
                missing_by_tf[tf] += 1
                continue

            hist[tf][label] += 1

    # формируем строки для вставки в bt_analysis_angle
    rows_to_insert: List[Tuple[Any, ...]] = []

    for tf in LR_TFS:
        total = total_by_tf[tf]
        missing = missing_by_tf[tf]
        used = total - missing

        for lo, hi, label in ANGLE_BINS:
            count = hist[tf].get(label, 0)
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
                count_in_bin
            )
            VALUES (
                $1, $2, $3,
                $4, $5, $6,
                $7, $8, $9, $10
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


# 🔸 Определение бина для угла LR50
def _assign_angle_bin(angle: Decimal) -> Optional[str]:
    # работаем в Decimal, без перевода в float, чтобы не ловить артефакты
    val = angle

    for lo, hi, label in ANGLE_BINS:
        # левая куча: angle < hi
        if lo is None and hi is not None:
            if val < hi:
                return label
        # правая куча: angle >= lo
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