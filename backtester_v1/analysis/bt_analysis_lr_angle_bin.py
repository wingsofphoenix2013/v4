# bt_analysis_lr_angle_bin.py — анализатор распределения позиций по биннам угла LR

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_BIN")


# 🔸 Публичная точка входа анализатора LR/angle_bin (адаптивные бины по углу)
async def run_lr_angle_bin_analysis(
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

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, angle_param_name=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        angle_param_name,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

    # первый проход: собираем углы по всем позициям, находим min/max
    positions_total = len(positions)
    valid_angles: List[Decimal] = []

    for p in positions:
        raw_stat = p["raw_stat"]
        angle = _extract_angle_from_raw_stat(raw_stat, tf, angle_param_name)
        if angle is None:
            p["angle"] = None
            continue

        p["angle"] = angle
        valid_angles.append(angle)

    if not valid_angles:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_BIN: анализатор id=%s (family=%s, key=%s, name=%s) "
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

    min_angle = min(valid_angles)
    max_angle = max(valid_angles)

    # строим адаптивные бины по углу в диапазоне [min_angle .. max_angle]
    bins = _build_angle_bins(min_angle, max_angle, bins_count=10)

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = 0

    # второй проход: раскладываем позиции по биннам
    for p in positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        angle = p.get("angle")

        if angle is None:
            positions_skipped += 1
            continue

        # на всякий случай клипуем угол в диапазон [min_angle, max_angle]
        if angle < min_angle:
            angle = min_angle
        if angle > max_angle:
            angle = max_angle

        bin_name = _assign_bin(bins, angle)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": angle,   # значение угла LR на входе
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, использовано=%s, пропущено=%s, "
        "строк_в_результате=%s, min_angle=%s, max_angle=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        str(min_angle),
        str(max_angle),
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
        "BT_ANALYSIS_LR_ANGLE_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
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


# 🔸 Построение адаптивных бинов по углу LR
def _build_angle_bins(
    min_angle: Decimal,
    max_angle: Decimal,
    bins_count: int = 10,
) -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []

    # если диапазон вырожденный — делаем 10 одинаковых бинов
    if max_angle <= min_angle:
        for i in range(bins_count):
            name = f"bin_{i}"
            bins.append(
                {
                    "name": name,
                    "min": min_angle,
                    "max": max_angle,
                }
            )
        return bins

    total_range = max_angle - min_angle
    step = total_range / Decimal(bins_count)

    # первые bins_count-1 бинов [min, max)
    for i in range(bins_count - 1):
        lo = min_angle + step * Decimal(i)
        hi = min_angle + step * Decimal(i + 1)
        name = f"bin_{i}"
        bins.append(
            {
                "name": name,
                "min": lo,
                "max": hi,
            }
        )

    # последний бин [min_last, max_angle] включительно
    lo_last = min_angle + step * Decimal(bins_count - 1)
    bins.append(
        {
            "name": f"bin_{bins_count - 1}",
            "min": lo_last,
            "max": max_angle,
        }
    )

    return bins


# 🔸 Определение имени бина для значения угла
def _assign_bin(
    bins: List[Dict[str, Decimal]],
    value: Decimal,
) -> Optional[str]:
    # все бины кроме последнего: [min, max)
    # последний бин: [min, max] (включая верхнюю границу)
    if not bins:
        return None

    last_index = len(bins) - 1

    for idx, b in enumerate(bins):
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")

        if lo is None or hi is None or name is None:
            continue

        if idx < last_index:
            if lo <= value < hi:
                return str(name)
        else:
            # последний бин включительно по верхней границе
            if lo <= value <= hi:
                return str(name)

    return None


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков (если понадобится)
def _q_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)