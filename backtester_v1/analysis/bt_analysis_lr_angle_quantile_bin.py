# bt_analysis_lr_angle_quantile_bin.py — анализатор распределения позиций по квантильным бинам угла LR

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN")


# 🔸 Публичная точка входа анализатора LR/angle_quantile_bin
async def run_lr_angle_quantile_bin_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")               # TF из raw_stat["tf"][tf]
    angle_param_name = _get_str_param(params, "param_name", "lr50_angle")  # например lr50_angle

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
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
            "BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
    valid_angles: List[Decimal] = []

    # первый проход: извлекаем углы, помечаем валидные позиции
    for p in positions:
        raw_stat = p["raw_stat"]
        angle = _extract_angle_from_raw_stat(raw_stat, tf, angle_param_name)
        if angle is None:
            p["angle"] = None
            continue

        p["angle"] = angle
        valid_positions.append(p)
        valid_angles.append(angle)

    if not valid_positions:
        log.info(
            "BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN: анализатор id=%s (family=%s, key=%s, name=%s) "
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

    # сортируем валидные позиции по углу
    valid_positions.sort(key=lambda p: p["angle"])
    min_angle = min(valid_angles)
    max_angle = max(valid_angles)

    bins_count = 20
    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: назначаем бины по рангу (квантильные бины)
    total_valid = len(valid_positions)
    if total_valid > 0:
        for idx, p in enumerate(valid_positions):
            position_uid = p["position_uid"]
            direction = p["direction"]
            pnl_abs = p["pnl_abs"]
            angle = p["angle"]

            # индекс бина по рангу: примерно одинаковое число позиций в каждом
            bin_idx = (idx * bins_count) // total_valid
            if bin_idx >= bins_count:
                bin_idx = bins_count - 1
            bin_name = f"bin_{bin_idx}"

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

    log.info(
        "BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, валидных=%s, использовано=%s, "
        "пропущено=%s, строк_в_результате=%s, min_angle=%s, max_angle=%s",
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
        "BT_ANALYSIS_LR_ANGLE_QUANTILE_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
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