# bt_analysis_rsimfi_mtf.py — анализатор распределения позиций по комбинациям зон RSI/MFI на h1 и m15

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_RSIMFI_MTF")

# 🔸 Пороги RSI/MFI (должны совпадать с bt_rsimfi_stats)
RSI_LOW = 40.0
RSI_HIGH = 60.0
MFI_LOW = 40.0
MFI_HIGH = 60.0


# 🔸 Публичная точка входа анализатора RSI/MFI MTF (h1 + m15, без m5)
async def run_rsimfi_mtf_analysis(
    analysis: Dict[str, Any],
    analysis_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости, здесь не используется
) -> Dict[str, Any]:
    analysis_id = analysis.get("id")
    family_key = str(analysis.get("family_key") or "").strip()
    analysis_key = str(analysis.get("key") or "").strip()
    name = analysis.get("name")

    params = analysis.get("params") or {}
    scenario_id = analysis_ctx.get("scenario_id")
    signal_id = analysis_ctx.get("signal_id")

    log.debug(
        "BT_ANALYSIS_RSIMFI_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
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
            "BT_ANALYSIS_RSIMFI_MTF: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

        # зоны RSI/MFI для h1 и m15
        zone_h1 = _extract_rsimfi_zone(raw_stat, "h1")
        zone_m15 = _extract_rsimfi_zone(raw_stat, "m15")

        # если не удалось определить хотя бы одну зону — позицию пропускаем
        if zone_h1 is None or zone_m15 is None:
            positions_skipped += 1
            continue

        # бин — комбинация зон h1 и m15
        # пример: "H_Z1_CONFIRMED|M_Z3_FLOW" или коротко "H_Z1|M_Z3"
        bin_name = f"H_{zone_h1}|M_{zone_m15}"

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
        "BT_ANALYSIS_RSIMFI_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
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
        "BT_ANALYSIS_RSIMFI_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение зоны RSI/MFI для заданного TF (m15 или h1)
def _extract_rsimfi_zone(
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

    rsi_family = indicators.get("rsi") or {}
    mfi_family = indicators.get("mfi") or {}
    if not isinstance(rsi_family, dict) or not isinstance(mfi_family, dict):
        return None

    rsi_val = rsi_family.get("rsi14")
    mfi_val = mfi_family.get("mfi14")
    if rsi_val is None or mfi_val is None:
        return None

    try:
        rsi_f = float(rsi_val)
        mfi_f = float(mfi_val)
    except (TypeError, ValueError):
        return None

    zone = _classify_rsi_mfi(rsi_f, mfi_f)
    return zone


# 🔸 Классификация RSI/MFI в одну из 5 корзинок (взаимоисключающие зоны)
# zони называем так же, как в bt_rsimfi_stats: Z1_CONFIRMED, Z2_PRICE_EXTREME, Z3_FLOW_LEADS, Z4_DIVERGENCE, Z5_NEUTRAL
def _classify_rsi_mfi(rsi: float, mfi: float) -> Optional[str]:
    r_zone = _level_3(rsi, RSI_LOW, RSI_HIGH)
    m_zone = _level_3(mfi, MFI_LOW, MFI_HIGH)

    if r_zone is None or m_zone is None:
        return None

    # Z1: подтверждённый сильный тренд (оба в LOW или оба в HIGH)
    if (r_zone == "LOW" and m_zone == "LOW") or (r_zone == "HIGH" and m_zone == "HIGH"):
        return "Z1_CONFIRMED"

    # Z4: жёсткая дивергенция (цена и деньги на противоположных полюсах)
    if (r_zone == "HIGH" and m_zone == "LOW") or (r_zone == "LOW" and m_zone == "HIGH"):
        return "Z4_DIVERGENCE"

    # Z2: цена в экстремуме, деньги в середине (EXTREME PRICE, NEUTRAL FLOW)
    if r_zone in ("LOW", "HIGH") and m_zone == "MID":
        return "Z2_PRICE_EXTREME"

    # Z3: деньги сильные, цена в середине (FLOW LEADS)
    if m_zone in ("LOW", "HIGH") and r_zone == "MID":
        return "Z3_FLOW_LEADS"

    # Z5: оба в середине (нейтрально)
    if r_zone == "MID" and m_zone == "MID":
        return "Z5_NEUTRAL"

    # fallback, теоретически сюда не должны попасть
    return "Z5_NEUTRAL"


# 🔸 Классификация значения в LOW/MID/HIGH по двум порогам
def _level_3(value: float, low: float, high: float) -> Optional[str]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None

    if v <= low:
        return "LOW"
    if v >= high:
        return "HIGH"
    return "MID"


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")