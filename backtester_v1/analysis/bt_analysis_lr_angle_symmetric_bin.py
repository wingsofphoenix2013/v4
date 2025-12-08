# bt_analysis_lr_angle_symmetric_bin.py — анализатор симметричных биннов по модулю угла LR вокруг нуля

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN")


# 🔸 Публичная точка входа анализатора LR/angle_symmetric_bin
async def run_lr_angle_symmetric_bin_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> Dict[str, Any]:
    analysis_id = analysis.get("id")
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")

    params = analysis.get("params") or {}
    scenario_id = analysis_ctx.get("scenario_id")
    signal_id = analysis_ctx.get("signal_id")

    # базовые параметры анализатора
    tf = _get_str_param(params, "tf", default="m5")  # TF из raw_stat["tf"][tf]
    angle_param_name = _get_str_param(params, "param_name", "lr50_angle")  # например lr50_angle

    # квантильные уровни по модулю угла (по умолчанию 0.2, 0.4, 0.6, 0.8)
    p1 = _get_float_param(params, "p1", 0.2)
    p2 = _get_float_param(params, "p2", 0.4)
    p3 = _get_float_param(params, "p3", 0.6)
    p4 = _get_float_param(params, "p4", 0.8)

    # условия достаточности и упорядоченности квантилей
    ps = sorted([p1, p2, p3, p4])
    p1, p2, p3, p4 = ps

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, angle_param_name=%s, p1=%.3f, p2=%.3f, p3=%.3f, p4=%.3f",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        angle_param_name,
        p1,
        p2,
        p3,
        p4,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
            },
        }

    positions_total = len(positions)
    valid_positions: List[Dict[str, Any]] = []
    magnitudes: List[Decimal] = []
    angles: List[Decimal] = []

    # первый проход: извлекаем углы и модули
    for p in positions:
        raw_stat = p["raw_stat"]
        angle = _extract_angle_from_raw_stat(raw_stat, tf, angle_param_name)
        if angle is None:
            p["angle"] = None
            continue

        p["angle"] = angle
        valid_positions.append(p)
        mag = abs(angle)
        magnitudes.append(mag)
        angles.append(angle)

    if not valid_positions:
        log.info(
            "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: анализатор id=%s (family=%s, key=%s, name=%s) "
            "для scenario_id=%s, signal_id=%s — нет валидных углов LR для анализа "
            "(positions_total=%s)",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
            positions_total,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": positions_total,
                "positions_used": 0,
                "positions_skipped": positions_total,
            },
        }

    # сортируем модули углов для расчёта квантилей
    magnitudes_sorted = sorted(magnitudes)
    total_valid = len(magnitudes_sorted)

    # защита от вырожденных случаев
    if total_valid == 0:
        log.info(
            "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: нет валидных модулей углов LR, "
            "analysis_id=%s, scenario_id=%s, signal_id=%s",
            analysis_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": positions_total,
                "positions_used": 0,
                "positions_skipped": positions_total,
            },
        }

    # квантильные уровни по |angle|
    q1 = _quantile(magnitudes_sorted, p1)
    q2 = _quantile(magnitudes_sorted, p2)
    q3 = _quantile(magnitudes_sorted, p3)
    q4 = _quantile(magnitudes_sorted, p4)

    # если какие-то квантили не определены, деградируем к max_abs
    max_abs = max(magnitudes_sorted)
    if q1 is None:
        q1 = max_abs
    if q2 is None:
        q2 = max_abs
    if q3 is None:
        q3 = max_abs
    if q4 is None:
        q4 = max_abs

    # на всякий случай упорядочим пороги
    thresholds = sorted([q1, q2, q3, q4])
    q1, q2, q3, q4 = thresholds

    min_angle = min(angles)
    max_angle = max(angles)

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: раскладываем по симметричным бинам
    for p in valid_positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        angle = p["angle"]

        # модуль угла
        mag = abs(angle)

        # определяем бин по знаку и модулю
        bin_name = _assign_symmetric_bin(angle, mag, q1, q2, q3, q4)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": angle,   # сам угол LR
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, валидных=%s, использовано=%s, "
        "пропущено=%s, строк_в_результате=%s, min_angle=%s, max_angle=%s, "
        "q1=%s, q2=%s, q3=%s, q4=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        total_valid,
        positions_used,
        positions_skipped,
        len(rows),
        str(min_angle),
        str(max_angle),
        str(q1),
        str(q2),
        str(q3),
        str(q4),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
        },
    }


# 🔸 Загрузка позиций сценария/сигнала с postproc=true
async def _load_positions_for_analysis(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                timeframe,
                direction,
                pnl_abs,
                raw_stat
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        raw = r["raw_stat"]

        # приводим jsonb к dict, если он пришёл строкой
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        positions.append(
            {
                "position_uid": r["position_uid"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_SYMMETRIC_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение значения угла LR из raw_stat по TF и имени параметра (например, 'lr50_angle')
def _extract_angle_from_raw_stat(
    raw_stat: Any,
    tf: str,
    angle_param_name: str,
) -> Optional[Decimal]:
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
    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    value = lr_family.get(angle_param_name)
    if value is None:
        return None

    return _safe_decimal(value)


# 🔸 Определение имени бина по симметричной схеме вокруг нуля
def _assign_symmetric_bin(
    angle: Decimal,
    mag: Decimal,
    q1: Decimal,
    q2: Decimal,
    q3: Decimal,
    q4: Decimal,
) -> Optional[str]:
    # совпадение всех порогов → деградация к одному уровню
    if q1 <= 0 and q2 <= 0 and q3 <= 0 and q4 <= 0:
        if angle < 0:
            return "bin_2"
        elif angle > 0:
            return "bin_7"
        else:
            return "bin_4"

    # отрицательные углы (канал вниз)
    if angle < 0:
        # bin_0: очень сильный тренд вниз
        if mag > q4:
            return "bin_0"
        # bin_1: сильный тренд вниз
        if mag > q3:
            return "bin_1"
        # bin_2: средний тренд вниз
        if mag > q2:
            return "bin_2"
        # bin_3: слабый тренд вниз
        if mag > q1:
            return "bin_3"
        # bin_4: почти флэт вниз
        return "bin_4"

    # положительные углы (канал вверх)
    if angle > 0:
        # bin_9: очень сильный тренд вверх
        if mag > q4:
            return "bin_9"
        # bin_8: сильный тренд вверх
        if mag > q3:
            return "bin_8"
        # bin_7: средний тренд вверх
        if mag > q2:
            return "bin_7"
        # bin_6: слабый тренд вверх
        if mag > q1:
            return "bin_6"
        # bin_5: почти флэт вверх
        return "bin_5"

    # угол ровно 0 — считаем как почти флэт, условно "вверх"
    return "bin_5"


# 🔸 Расчёт квантиля по отсортированному списку magnitudes
def _quantile(
    sorted_values: List[Decimal],
    q: float,
) -> Optional[Decimal]:
    # q ∈ [0,1]
    if not sorted_values:
        return None

    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]

    n = len(sorted_values)
    # индекс квантиля по рангу
    idx = int(q * (n - 1))
    if idx < 0:
        idx = 0
    if idx >= n:
        idx = n - 1

    return sorted_values[idx]


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное чтение float-параметра
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return float(str(raw))
    except (ValueError, TypeError):
        return default


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")