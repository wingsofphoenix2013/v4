# bt_analysis_lr_angle_mtf.py — анализатор распределения позиций по комбинациям углов LR50 на h1/m15 + квантиль m5

import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation
from datetime import datetime  # для стрима bt:analysis:angle

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_ANGLE_MTF")

# 🔸 Количество квантилей по углу m5 внутри каждой MTF-группы (H|M)
ANGLE_QUANTILES = 5

# 🔸 Порог для “малых” групп: если доля позиций в группе < 1%, даём Q0 вместо квантилей
MIN_SHARE = Decimal("0.01")


# 🔸 Публичная точка входа анализатора LR50/angle MTF (h1 + m15 + квантиль m5)
async def run_lr_angle_mtf_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,
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
        summary = {
            "positions_total": 0,
            "positions_used": 0,
            "positions_skipped": 0,
        }
        await _publish_angle_ready(redis, analysis_id, scenario_id, signal_id, summary)
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

        # зоны LR50 для h1 и m15
        zone_m15 = _extract_lr50_zone(raw_stat, "m15")
        zone_h1 = _extract_lr50_zone(raw_stat, "h1")
        # угол LR50 для m5
        angle_m5 = _extract_lr50_angle(raw_stat, "m5")

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

    # 🔸 Второй проход: внутри каждой группы (H|M) делаем квантильную разбивку по m5 угол
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

        # если группа меньше 1% — не делим на квантиль, присваиваем Q0
        if share < MIN_SHARE or group_n <= ANGLE_QUANTILES:
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

    summary = {
        "positions_total": positions_total,
        "positions_used": positions_used,
        "positions_skipped": positions_skipped,
    }

    # 🔸 Публикуем событие в Redis stream bt:analysis:angle (возможно, ещё пригодится)
    await _publish_angle_ready(
        redis=redis,
        analysis_id=analysis_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        summary=summary,
    )

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
    return _angle_to_zone(angle)


# 🔸 Извлечение угла LR50 для заданного TF (m5)
def _extract_lr50_angle(
    raw_stat: Any,
    tf: str,
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

    value = lr_family.get("lr50_angle")
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


# 🔸 Публикация события готовности MTF-углового анализа в bt:analysis:angle (временный стрим)
async def _publish_angle_ready(
    redis,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    summary: Dict[str, Any],
) -> None:
    if redis is None:
        return

    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            "bt:analysis:angle",
            {
                "analysis_id": str(analysis_id),
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "positions_total": str(summary.get("positions_total", 0)),
                "positions_used": str(summary.get("positions_used", 0)),
                "positions_skipped": str(summary.get("positions_skipped", 0)),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_LR_ANGLE_MTF: опубликовано событие в стрим 'bt:analysis:angle' "
            "для analysis_id=%s, scenario_id=%s, signal_id=%s, positions_total=%s, "
            "positions_used=%s, positions_skipped=%s, finished_at=%s",
            analysis_id,
            scenario_id,
            signal_id,
            summary.get("positions_total", 0),
            summary.get("positions_used", 0),
            summary.get("positions_skipped", 0),
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_LR_ANGLE_MTF: не удалось опубликовать событие в стрим 'bt:analysis:angle' "
            "для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")