# bt_analysis_lr_angle_mtf.py — анализатор распределения позиций по комбинациям углов LR (h1/m15 + квантиль m5)

import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_MTF")

# 🔸 Константы квантилей и дефолтов
ANGLE_QUANTILES = 5
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LR_PREFIX = "lr50"


# 🔸 Публичная точка входа анализатора LR/angle MTF (h1 + m15 + квантиль m5)
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

    # параметры анализатора
    lr_prefix = _get_str_param(params, "lr_prefix", DEFAULT_LR_PREFIX)
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)

    log.debug(
        "BT_ANALYSIS_LR_ANGLE_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, lr_prefix=%s, min_share=%s, params=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        lr_prefix,
        min_share,
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
        summary = {
            "positions_total": 0,
            "positions_used": 0,
            "positions_skipped": 0,
        }
        return {
            "rows": [],
            "summary": summary,
        }

    # 🔸 Первый проход: считаем зоны по h1/m15 + угол m5, группируем по (H_zone, M_zone)
    positions_total = 0
    positions_skipped = 0

    # структура: group_key -> список позиций
    # group_key = (zone_h1, zone_m15)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # зоны LR для h1 и m15
        zone_m15 = _extract_lr_zone(raw_stat, "m15", lr_prefix)
        zone_h1 = _extract_lr_zone(raw_stat, "h1", lr_prefix)
        # угол LR для m5
        angle_m5 = _extract_lr_angle(raw_stat, "m5", lr_prefix)

        # если чего-то нет — позицию пропускаем
        if zone_m15 is None or zone_h1 is None or angle_m5 is None:
            positions_skipped += 1
            continue

        key = (zone_h1, zone_m15)
        grouped.setdefault(key, []).append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "angle_m5": angle_m5,
                "zone_h1": zone_h1,
                "zone_m15": zone_m15,
            }
        )

    # 🔸 Второй проход: внутри каждой группы (H|M) делаем квантильную разбивку по углу m5
    if positions_total > 0:
        total_for_share = Decimal(positions_total - positions_skipped)
    else:
        total_for_share = Decimal(0)

    rows: List[Dict[str, Any]] = []
    positions_used = 0

    for (zone_h1, zone_m15), plist in grouped.items():
        group_n = len(plist)
        if total_for_share <= 0:
            # если по факту все позиции скипнуты — ничего не делаем
            continue

        share = Decimal(group_n) / total_for_share

        # если группа меньше min_share или слишком маленькая по количеству — не делим на квантиль, присваиваем Q0
        if share < min_share or group_n <= ANGLE_QUANTILES:
            for rec in plist:
                bin_name = f"H_{zone_h1}|M_{zone_m15}|Q0"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": 0,
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
                positions_used += 1
            continue

        # иначе делим на квантиль по angle_m5 с учётом направления
        # готовим список (sort_key, rec)
        sortable: List[Tuple[float, Dict[str, Any]]] = []
        for rec in plist:
            angle = rec["angle_m5"]
            try:
                angle_f = float(angle)
            except (TypeError, InvalidOperation, ValueError):
                continue

            direction = str(rec["direction"] or "").lower()
            # для шорта инвертируем знак, чтобы "хорошие/плохие" углы были сопоставимыми
            if direction == "short":
                sort_key = -angle_f
            else:
                sort_key = angle_f

            sortable.append((sort_key, rec))

        if not sortable:
            continue

        sortable.sort(key=lambda x: x[0])
        n = len(sortable)

        for idx, (_, rec) in enumerate(sortable):
            # NTILE(Q): q_idx = floor(i * Q / n) + 1
            q_idx = (idx * ANGLE_QUANTILES) // n + 1
            if q_idx < 1:
                q_idx = 1
            if q_idx > ANGLE_QUANTILES:
                q_idx = ANGLE_QUANTILES

            bin_name = f"H_{zone_h1}|M_{zone_m15}|Q{q_idx}"
            rows.append(
                {
                    "position_uid": rec["position_uid"],
                    "timeframe": "mtf",
                    "direction": rec["direction"],
                    "bin_name": bin_name,
                    "value": 0,
                    "pnl_abs": rec["pnl_abs"],
                }
            )
            positions_used += 1

    log.info(
        "BT_ANALYSIS_LR_ANGLE_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s, lr_prefix=%s, min_share=%s — позиций всего=%s, "
        "использовано=%s, пропущено=%s, групп=%s, строк_в_результате=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        lr_prefix,
        min_share,
        positions_total,
        positions_used,
        positions_skipped,
        len(grouped),
        len(rows),
    )

    summary = {
        "positions_total": positions_total,
        "positions_used": positions_used,
        "positions_skipped": positions_skipped,
    }

    return {
        "rows": rows,
        "summary": summary,
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


# 🔸 Извлечение зоны LR-угла для заданного TF (m15 или h1)
def _extract_lr_zone(
    raw_stat: Any,
    tf: str,
    lr_prefix: str,
) -> Optional[str]:
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

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    key = f"{lr_prefix}_angle"
    value = lr_family.get(key)
    if value is None:
        return None

    angle = _safe_decimal(value)
    return _angle_to_zone(angle)


# 🔸 Извлечение угла LR для заданного TF (m5)
def _extract_lr_angle(
    raw_stat: Any,
    tf: str,
    lr_prefix: str,
) -> Optional[Decimal]:
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

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None

    key = f"{lr_prefix}_angle"
    value = lr_family.get(key)
    if value is None:
        return None

    return _safe_decimal(value)


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
    return None


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное чтение Decimal-параметра
def _get_decimal_param(params: Dict[str, Any], name: str, default: Decimal) -> Decimal:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return default