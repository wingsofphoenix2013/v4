# bt_analysis_rsimfi_mtf.py — анализатор распределения позиций по комбинациям зон RSI/MFI на h1/m15/m5

import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_RSIMFI_MTF")

# 🔸 Дефолтные пороги и параметры (должны совпадать с bt_rsimfi_stats по умолчанию)
DEFAULT_RSIMFI_LOW = Decimal("40.0")
DEFAULT_RSIMFI_HIGH = Decimal("60.0")
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LENGTH = 14


# 🔸 Публичная точка входа анализатора RSI/MFI MTF (h1 + m15 + m5)
async def run_rsimfi_mtf_analysis(
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
    rsimfi_low = _get_decimal_param(params, "rsimfi_low", DEFAULT_RSIMFI_LOW)
    rsimfi_high = _get_decimal_param(params, "rsimfi_high", DEFAULT_RSIMFI_HIGH)
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)
    length = _get_int_param(params, "length", DEFAULT_LENGTH)

    log.debug(
        "BT_ANALYSIS_RSIMFI_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, rsimfi_low=%s, rsimfi_high=%s, "
        "min_share=%s, length=%s, params=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        rsimfi_low,
        rsimfi_high,
        min_share,
        length,
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

    positions_total = 0
    positions_skipped = 0

    # 🔸 Сначала собираем все зоны по h1/m15/m5 для каждой позиции
    # Используем короткие коды зон: Z1..Z5; Z0 будем использовать для агрегированных хвостов
    base_list: List[Dict[str, Any]] = []

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        zone_h1 = _extract_rsimfi_zone(raw_stat, "h1", length, rsimfi_low, rsimfi_high)
        zone_m15 = _extract_rsimfi_zone(raw_stat, "m15", length, rsimfi_low, rsimfi_high)
        zone_m5 = _extract_rsimfi_zone(raw_stat, "m5", length, rsimfi_low, rsimfi_high)

        # если не удалось определить полную тройку зон — пропускаем позицию
        if zone_h1 is None or zone_m15 is None or zone_m5 is None:
            positions_skipped += 1
            continue

        # сократим имена зон до Z1..Z5 (отбрасываем суффиксы вроде _NEUTRAL)
        z_h1 = zone_h1.split("_")[0]   # "Z1_CONFIRMED" -> "Z1"
        z_m15 = zone_m15.split("_")[0]
        z_m5 = zone_m5.split("_")[0]

        base_list.append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "zone_h1": z_h1,
                "zone_m15": z_m15,
                "zone_m5": z_m5,
            }
        )

    positions_used = len(base_list)

    if positions_used == 0:
        log.debug(
            "BT_ANALYSIS_RSIMFI_MTF: после фильтрации зон нет позиций для анализа scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": positions_total,
                "positions_used": 0,
                "positions_skipped": positions_skipped,
            },
        }

    total_for_share = Decimal(positions_used)

    # 🔸 Фильтр min_share после h1: группируем по zone_h1
    by_h1: Dict[str, List[Dict[str, Any]]] = {}
    for rec in base_list:
        z_h1 = rec["zone_h1"]
        by_h1.setdefault(z_h1, []).append(rec)

    rows: List[Dict[str, Any]] = []

    for z_h1, h1_group in by_h1.items():
        group_n_h1 = len(h1_group)
        share_h1 = Decimal(group_n_h1) / total_for_share

        # если доля по h1 < min_share — весь этот хвост складываем в H_Zh|M15_Z0|M5_Z0
        if share_h1 < min_share:
            bin_name = f"H_{z_h1}|M15_Z0|M5_Z0"
            for rec in h1_group:
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
            continue

        # 🔸 Внутри этой h1-зоны — фильтр по m15
        by_m15: Dict[str, List[Dict[str, Any]]] = {}
        for rec in h1_group:
            z_m15 = rec["zone_m15"]
            by_m15.setdefault(z_m15, []).append(rec)

        for z_m15, m15_group in by_m15.items():
            group_n_m15 = len(m15_group)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # если доля по этой (h1,m15)-зоне < min_share — кладём в H_Zh|M15_Zm|M5_Z0
            if share_m15 < min_share:
                bin_name = f"H_{z_h1}|M15_{z_m15}|M5_Z0"
                for rec in m15_group:
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
                continue

            # 🔸 Остальные — разбиваем ещё и по m5: H_Zh|M15_Zm|M5_Zk
            for rec in m15_group:
                z_m5 = rec["zone_m5"]
                bin_name = f"H_{z_h1}|M15_{z_m15}|M5_{z_m5}"
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

    log.debug(
        "BT_ANALYSIS_RSIMFI_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s, rsimfi_low=%s, rsimfi_high=%s, min_share=%s, length=%s — "
        "позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        rsimfi_low,
        rsimfi_high,
        min_share,
        length,
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
        "BT_ANALYSIS_RSIMFI_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение зоны RSI/MFI для заданного TF (m5, m15 или h1)
def _extract_rsimfi_zone(
    raw_stat: Any,
    tf: str,
    length: int,
    rsimfi_low: Decimal,
    rsimfi_high: Decimal,
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

    rsi_key = f"rsi{length}"
    mfi_key = f"mfi{length}"

    rsi_val = rsi_family.get(rsi_key)
    mfi_val = mfi_family.get(mfi_key)
    if rsi_val is None or mfi_val is None:
        return None

    try:
        rsi_f = float(rsi_val)
        mfi_f = float(mfi_val)
    except (TypeError, ValueError):
        return None

    # используем ту же логику зон, что в bt_rsimfi_stats, но возвращаем имена типа "Z1_CONFIRMED"
    zone = _classify_rsi_mfi(rsi_f, mfi_f, float(rsimfi_low), float(rsimfi_high))
    return zone


# 🔸 Классификация RSI/MFI в одну из 5 корзинок (взаимоисключающие зоны)
def _classify_rsi_mfi(
    rsi: float,
    mfi: float,
    low: float,
    high: float,
) -> Optional[str]:
    r_zone = _level_3(rsi, low, high)
    m_zone = _level_3(mfi, low, high)

    if r_zone is None or m_zone is None:
        return None

    # Z1: подтверждённый сильный тренд (оба в LOW или оба в HIGH)
    if (r_zone == "LOW" and m_zone == "LOW") or (r_zone == "HIGH" and m_zone == "HIGH"):
        return "Z1_CONFIRMED"

    # Z4: жёсткая дивергенция (цена и деньги на противоположных полюсах)
    if (r_zone == "HIGH" and m_zone == "LOW") or (r_zone == "LOW" and m_zone == "HIGH"):
        return "Z4_DIVERGENCE"

    # Z2: цена в экстремуме, деньги в середине
    if r_zone in ("LOW", "HIGH") and m_zone == "MID":
        return "Z2_PRICE_EXTREME"

    # Z3: деньги сильные, цена в середине
    if m_zone in ("LOW", "HIGH") and r_zone == "MID":
        return "Z3_FLOW_LEADS"

    # Z5: оба в середине (нейтрально)
    if r_zone == "MID" and m_zone == "MID":
        return "Z5_NEUTRAL"

    # fallback
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


# 🔸 Вспомогательная функция: безопасное чтение int-параметра
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return default