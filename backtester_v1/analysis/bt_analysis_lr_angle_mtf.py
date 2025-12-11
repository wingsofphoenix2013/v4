# bt_analysis_lr_angle_mtf.py — анализатор распределения позиций по комбинациям углов LR50 на h1 и m15

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_MTF")


# 🔸 Публичная точка входа анализатора LR50/angle MTF (h1 + m15)
async def run_lr_angle_mtf_analysis(
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

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, params=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        params,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (postproc=true)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_MTF: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

    rows: List[Dict[str, Any]] = []
    positions_total = 0
    positions_used = 0
    positions_skipped = 0

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # извлекаем зоны углов LR50 для m15 и h1
        zone_m15 = _extract_lr50_zone(raw_stat, "m15")
        zone_h1 = _extract_lr50_zone(raw_stat, "h1")

        # если не удалось определить хотя бы одну зону — позицию пропускаем
        if zone_m15 is None or zone_h1 is None:
            positions_skipped += 1
            continue

        # бин — комбинация зон h1 и m15
        # пример: "H_SD|M_FLAT", "H_MU|M_MD" и т.п.
        bin_name = f"H_{zone_h1}|M_{zone_m15}"

        # value — числовое поле, для этого анализатора не несёт отдельного смысла
        # используем 0 как константу
        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": "mtf",
                "direction": direction,
                "bin_name": bin_name,
                "value": 0,      # numeric NOT NULL в bt_analysis_positions_raw
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_LR_ANGLE_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
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
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение зоны LR50-угла для заданного TF (m15 или h1)
def _extract_lr50_zone(
    raw_stat: Any,
    tf: str,
) -> Optional[str]:
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

    angle = _safe_decimal(value)

    # зоны:
    #  - SD  (strong_down): angle <= -0.10
    #  - MD  (mild_down):   -0.10 < angle < -0.02
    #  - FLAT:              -0.02 <= angle <= 0.02
    #  - MU  (mild_up):      0.02 < angle < 0.10
    #  - SU  (strong_up):    angle >= 0.10
    return _angle_to_zone(angle)


# 🔸 Преобразование угла в короткий код зоны
def _angle_to_zone(angle: Decimal) -> Optional[str]:
    try:
        val = float(angle)
    except (TypeError, InvalidOperation, ValueError):
        return None

    if val <= -0.10:
        return "SD"   # strong_down
    if -0.10 < val < -0.02:
        return "MD"   # mild_down
    if -0.02 <= val <= 0.02:
        return "FLAT"
    if 0.02 < val < 0.10:
        return "MU"   # mild_up
    if val >= 0.10:
        return "SU"   # strong_up

    # теоретически сюда не попадём, но пусть будет
    return None


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")