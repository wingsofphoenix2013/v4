# bt_analysis_stability.py — расчёт индекса стабильности анализаторов по суточной аналитике

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1
from backtester_config import get_analysis_instance

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_STABILITY")

# 🔸 Константы стрима готовности суточной аналитики
DAILY_READY_STREAM_KEY = "bt:analysis:daily:ready"
STABILITY_CONSUMER_GROUP = "bt_analysis_stability"
STABILITY_CONSUMER_NAME = "bt_analysis_stability_main"

# 🔸 Настройки чтения стрима bt:analysis:daily:ready
STABILITY_STREAM_BATCH_SIZE = 10
STABILITY_STREAM_BLOCK_MS = 5000

# 🔸 Окна для расчёта стабильности (в днях)
STABILITY_WINDOWS = [14, 28]

# 🔸 Порог минимального количества дней в окне
MIN_DAYS_PER_WINDOW = {
    14: 7,
    28: 14,
}

# 🔸 Порог минимального количества базовых сделок в окне
MIN_BASE_TRADES_PER_WINDOW = {
    14: 200,
    28: 400,
}

# 🔸 Поддерживаемые семейства анализаторов
SUPPORTED_FAMILIES_CALIB = {"rsi", "adx", "ema", "atr", "supertrend"}

# 🔸 Квантование до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Расчёт средних и стандартного отклонения по списку Decimal
def _mean_std(values: List[Decimal]) -> Tuple[Decimal, Decimal]:
    if not values:
        return Decimal("0"), Decimal("0")
    n = len(values)
    mean = sum(values) / Decimal(n)
    if n < 2:
        return mean, Decimal("0")
    var = sum((v - mean) * (v - mean) for v in values) / Decimal(n - 1)
    # защита от отрицательных артефактов из-за округлений
    var = var if var >= 0 else Decimal("0")
    std = var.sqrt()
    return mean, std


# 🔸 Расчёт наклона тренда по ΔROI (t = 0..n-1)
def _trend_slope(values: List[Decimal]) -> Decimal:
    n = len(values)
    if n < 2:
        return Decimal("0")

    # преобразуем в Decimal индексы
    t_vals = [Decimal(i) for i in range(n)]
    mean_t = sum(t_vals) / Decimal(n)
    mean_v = sum(values) / Decimal(n)

    num = sum((t_vals[i] - mean_t) * (values[i] - mean_v) for i in range(n))
    den = sum((t_vals[i] - mean_t) * (t_vals[i] - mean_t) for i in range(n))

    if den == 0:
        return Decimal("0")

    return num / den


# 🔸 Проверка/создание consumer group для стрима bt:analysis:daily:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=DAILY_READY_STREAM_KEY,
            groupname=STABILITY_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_STABILITY: создана consumer group '%s' для стрима '%s'",
            STABILITY_CONSUMER_GROUP,
            DAILY_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_STABILITY: consumer group '%s' для стрима '%s' уже существует",
                STABILITY_CONSUMER_GROUP,
                DAILY_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_STABILITY: ошибка при создании consumer group '%s': %s",
                STABILITY_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:daily:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=STABILITY_CONSUMER_GROUP,
        consumername=STABILITY_CONSUMER_NAME,
        streams={DAILY_READY_STREAM_KEY: ">"},
        count=STABILITY_STREAM_BATCH_SIZE,
        block=STABILITY_STREAM_BLOCK_MS,
    )

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:analysis:daily:ready
def _parse_daily_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        raw_ids = [s.strip() for s in analysis_ids_str.split(",") if s.strip()]
        analysis_ids: List[int] = []
        for s in raw_ids:
            try:
                analysis_ids.append(int(s))
            except Exception:
                continue

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "analysis_ids": analysis_ids,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_STABILITY: ошибка разбора сообщения стрима bt:analysis:daily:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Расчёт стабильности по одному анализатору, версии и окну
async def _process_analysis_stability_for_window(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_id: int,
    version: str,
    window_days: int,
) -> int:
    inst = get_analysis_instance(analysis_id)
    if not inst:
        log.warning(
            "BT_ANALYSIS_STABILITY: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return 0

    inst_family = inst.get("family_key")
    key = inst.get("key")

    if inst_family != family_key:
        return 0

    # загружаем максимум даты day для этого анализатора и версии
    async with pg.acquire() as conn:
        max_day_row = await conn.fetchrow(
            """
            SELECT MAX(day) AS max_day
            FROM bt_analysis_daily
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND analysis_id = $3
              AND version     = $4
            """,
            scenario_id,
            signal_id,
            analysis_id,
            version,
        )

    if not max_day_row or max_day_row["max_day"] is None:
        log.debug(
            "BT_ANALYSIS_STABILITY: нет daily для scenario_id=%s, signal_id=%s, analysis_id=%s, version=%s",
            scenario_id,
            signal_id,
            analysis_id,
            version,
        )
        return 0

    max_day = max_day_row["max_day"]
    from_day = max_day - timedelta(days=window_days - 1)

    # загружаем suточные записи в окне для этого анализатора/версии
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                timeframe,
                day,
                base_trades,
                base_pnl_abs,
                base_winrate,
                base_roi,
                selected_trades,
                selected_pnl_abs,
                selected_winrate,
                selected_roi,
                coverage
            FROM bt_analysis_daily
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND analysis_id = $3
              AND version     = $4
              AND day BETWEEN $5 AND $6
            """,
            scenario_id,
            signal_id,
            analysis_id,
            version,
            from_day,
            max_day,
        )

    if not rows:
        log.debug(
            "BT_ANALYSIS_STABILITY: нет daily-строк в окне [%s..%s] для scenario_id=%s, signal_id=%s, "
            "analysis_id=%s, version=%s",
            from_day,
            max_day,
            scenario_id,
            signal_id,
            analysis_id,
            version,
        )
        return 0

    # группируем по (direction, timeframe)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        direction = r["direction"]
        timeframe = r["timeframe"]
        if direction is None or timeframe is None:
            continue
        key_dt = (direction, timeframe)
        grouped.setdefault(key_dt, []).append(
            {
                "day": r["day"],
                "base_trades": int(r["base_trades"]),
                "base_pnl_abs": Decimal(str(r["base_pnl_abs"])),
                "base_winrate": Decimal(str(r["base_winrate"])),
                "base_roi": Decimal(str(r["base_roi"])),
                "selected_trades": int(r["selected_trades"]),
                "selected_pnl_abs": Decimal(str(r["selected_pnl_abs"])),
                "selected_winrate": Decimal(str(r["selected_winrate"])),
                "selected_roi": Decimal(str(r["selected_roi"])),
                "coverage": Decimal(str(r["coverage"])),
            }
        )

    rows_written = 0

    # обрабатываем каждую комбинацию direction+timeframe отдельно
    for (direction, timeframe), day_rows in grouped.items():
        days_count = len(day_rows)
        min_days = MIN_DAYS_PER_WINDOW.get(window_days, 0)
        if days_count < min_days:
            log.debug(
                "BT_ANALYSIS_STABILITY: недостаточно дней (%s) для окна %s "
                "scenario_id=%s, signal_id=%s, analysis_id=%s, dir=%s, tf=%s, version=%s",
                days_count,
                window_days,
                scenario_id,
                signal_id,
                analysis_id,
                direction,
                timeframe,
                version,
            )
            continue

        # агрегаты по базовой линии и selected
        base_trades_total = 0
        base_pnl_abs_total = Decimal("0")

        # для оценок uplift по дням
        uplift_winrate_list: List[Decimal] = []
        uplift_roi_list: List[Decimal] = []
        coverage_list: List[Decimal] = []

        good_winrate_days = 0
        good_roi_days = 0

        for drow in day_rows:
            base_trades_d = drow["base_trades"]
            if base_trades_d <= 0:
                continue

            base_trades_total += base_trades_d
            base_pnl_abs_total += drow["base_pnl_abs"]

            base_winrate_d = drow["base_winrate"]
            base_roi_d = drow["base_roi"]

            selected_trades_d = drow["selected_trades"]
            selected_winrate_d = drow["selected_winrate"]
            selected_roi_d = drow["selected_roi"]
            coverage_d = drow["coverage"]

            # дневной uplift по winrate/ROI
            uplift_w_d = selected_winrate_d - base_winrate_d
            uplift_r_d = selected_roi_d - base_roi_d

            uplift_winrate_list.append(uplift_w_d)
            uplift_roi_list.append(uplift_r_d)
            coverage_list.append(coverage_d)

            if uplift_w_d > 0:
                good_winrate_days += 1
            if uplift_r_d > 0:
                good_roi_days += 1

        min_trades = MIN_BASE_TRADES_PER_WINDOW.get(window_days, 0)
        if base_trades_total < min_trades:
            log.debug(
                "BT_ANALYSIS_STABILITY: недостаточно базовых сделок (%s) для окна %s "
                "scenario_id=%s, signal_id=%s, analysis_id=%s, dir=%s, tf=%s, version=%s",
                base_trades_total,
                window_days,
                scenario_id,
                signal_id,
                analysis_id,
                direction,
                timeframe,
                version,
            )
            continue

        if not uplift_winrate_list or not uplift_roi_list:
            continue

        # базовый winrate/roi как среднее по окну (взвешивание по сделкам можно усложнить позже)
        if base_trades_total > 0:
            # простое среднее по дневным метрикам
            base_winrate_mean, _ = _mean_std([r["base_winrate"] for r in day_rows])
            base_roi_mean, _ = _mean_std([r["base_roi"] for r in day_rows])
        else:
            base_winrate_mean = Decimal("0")
            base_roi_mean = Decimal("0")

        # средний uplift и std
        avg_uplift_w, std_uplift_w = _mean_std(uplift_winrate_list)
        avg_uplift_r, std_uplift_r = _mean_std(uplift_roi_list)

        # доля "хороших" дней
        total_days_for_uplift = len(uplift_winrate_list)
        good_days_winrate_ratio = _safe_div(Decimal(good_winrate_days), Decimal(total_days_for_uplift))
        good_days_roi_ratio = _safe_div(Decimal(good_roi_days), Decimal(total_days_for_uplift))

        # среднее coverage
        avg_coverage, _ = _mean_std(coverage_list)

        # тренд по uplift ROI
        trend_uplift_roi = _trend_slope(uplift_roi_list)

        # расчёт финального stability_score (простая, но адекватная формула)
        # нормализуем эффект: высокий средний uplift при маленьком std => выше score
        eps = Decimal("0.0001")
        effect_component = avg_uplift_r / (std_uplift_r + eps)
        # ограничиваем effect_component мягко, чтобы не было диких всплесков
        # (для старта можно просто оставить как есть и обрезать в разумном диапазоне)
        # интегрируем частоту успеха и покрытие
        raw_score = effect_component * avg_coverage * good_days_roi_ratio

        # штрафуем за негативный тренд (если trend сильно отрицательный)
        if trend_uplift_roi < 0:
            penalty = Decimal("1") / (Decimal("1") + (-trend_uplift_roi))
            stability_score = raw_score * penalty
        else:
            stability_score = raw_score

        # аккуратное квантование
        base_winrate_q = _q4(base_winrate_mean)
        base_roi_q = _q4(base_roi_mean)
        avg_uplift_winrate_q = _q4(avg_uplift_w)
        std_uplift_winrate_q = _q4(std_uplift_w)
        avg_uplift_roi_q = _q4(avg_uplift_r)
        std_uplift_roi_q = _q4(std_uplift_r)
        good_days_winrate_ratio_q = _q4(good_days_winrate_ratio)
        good_days_roi_ratio_q = _q4(good_days_roi_ratio)
        avg_coverage_q = _q4(avg_coverage)
        trend_uplift_roi_q = _q4(trend_uplift_roi)
        stability_score_q = _q4(stability_score)

        # перед записью удаляем старую запись для этого анализатора/окна
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_analysis_stability
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND analysis_id = $3
                  AND direction   = $4
                  AND timeframe   = $5
                  AND version     = $6
                  AND window_days = $7
                """,
                scenario_id,
                signal_id,
                analysis_id,
                direction,
                timeframe,
                version,
                window_days,
            )

            await conn.execute(
                """
                INSERT INTO bt_analysis_stability (
                    scenario_id,
                    signal_id,
                    analysis_id,
                    family_key,
                    key,
                    direction,
                    timeframe,
                    version,
                    window_days,
                    days_count,
                    base_trades,
                    base_winrate,
                    base_roi,
                    avg_uplift_winrate,
                    std_uplift_winrate,
                    avg_uplift_roi,
                    std_uplift_roi,
                    good_days_winrate_ratio,
                    good_days_roi_ratio,
                    avg_coverage,
                    trend_uplift_roi,
                    stability_score,
                    raw_stat,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13,
                    $14, $15, $16, $17,
                    $18, $19, $20, $21,
                    $22, NULL, now(), NULL
                )
                """,
                scenario_id,
                signal_id,
                analysis_id,
                inst_family,
                key,
                direction,
                timeframe,
                version,
                window_days,
                days_count,
                base_trades_total,
                base_winrate_q,
                base_roi_q,
                avg_uplift_winrate_q,
                std_uplift_winrate_q,
                avg_uplift_roi_q,
                std_uplift_roi_q,
                good_days_winrate_ratio_q,
                good_days_roi_ratio_q,
                avg_coverage_q,
                trend_uplift_roi_q,
                stability_score_q,
            )

        rows_written += 1

        log.debug(
            "BT_ANALYSIS_STABILITY: записана стабильность для scenario_id=%s, signal_id=%s, "
            "analysis_id=%s, dir=%s, tf=%s, version=%s, window_days=%s, score=%s",
            scenario_id,
            signal_id,
            analysis_id,
            direction,
            timeframe,
            version,
            window_days,
            stability_score_q,
        )

    return rows_written


# 🔸 Публичная точка входа: воркер стабильности анализаторов
async def run_bt_analysis_stability(pg, redis):
    log.info("BT_ANALYSIS_STABILITY: воркер расчёта стабильности запущен")

    # подготавливаем consumer group для стрима bt:analysis:daily:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и обработки
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_rows_written = 0

            for stream_key, entries in messages:
                if stream_key != DAILY_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_daily_ready_message(fields)
                    if not ctx:
                        await redis.xack(DAILY_READY_STREAM_KEY, STABILITY_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]

                    log.info(
                        "BT_ANALYSIS_STABILITY: получено сообщение о готовности daily-аналитики "
                        "scenario_id=%s, signal_id=%s, family=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        analysis_ids,
                        entry_id,
                    )

                    # пока работаем только с поддерживаемыми семействами
                    if family_key not in SUPPORTED_FAMILIES:
                        log.debug(
                            "BT_ANALYSIS_STABILITY: family_key=%s пока не поддерживается, "
                            "scenario_id=%s, signal_id=%s",
                            family_key,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(DAILY_READY_STREAM_KEY, STABILITY_CONSUMER_GROUP, entry_id)
                        continue

                    if not analysis_ids:
                        log.debug(
                            "BT_ANALYSIS_STABILITY: для scenario_id=%s, signal_id=%s, family=%s нет analysis_ids",
                            scenario_id,
                            signal_id,
                            family_key,
                        )
                        await redis.xack(DAILY_READY_STREAM_KEY, STABILITY_CONSUMER_GROUP, entry_id)
                        continue

                    rows_written_pair = 0

                    # по каждому анализатору считаем стабильность для v1 и v2, по окнам 14 и 28
                    for aid in analysis_ids:
                        for version in ("v1", "v2"):
                            for window_days in STABILITY_WINDOWS:
                                rows = await _process_analysis_stability_for_window(
                                    pg=pg,
                                    scenario_id=scenario_id,
                                    signal_id=signal_id,
                                    family_key=family_key,
                                    analysis_id=aid,
                                    version=version,
                                    window_days=window_days,
                                )
                                rows_written_pair += rows

                    total_pairs += 1
                    total_rows_written += rows_written_pair

                    # помечаем сообщение как обработанное
                    await redis.xack(DAILY_READY_STREAM_KEY, STABILITY_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_ANALYSIS_STABILITY: сообщение stream_id=%s для scenario_id=%s, signal_id=%s "
                        "обработано, строк_в_bt_analysis_stability=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        rows_written_pair,
                    )

            log.info(
                "BT_ANALYSIS_STABILITY: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "строк_в_bt_analysis_stability=%s",
                total_msgs,
                total_pairs,
                total_rows_written,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_STABILITY: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)