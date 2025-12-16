# bt_analysis_rsimfi_mtf.py — анализатор распределения позиций по комбинациям зон RSI/MFI на h1/m15/m5 через bt_analysis_bins_dict

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_RSIMFI_MTF")

# 🔸 Дефолтные пороги и параметры (должны совпадать с bt_rsimfi_stats по умолчанию)
DEFAULT_RSIMFI_LOW = Decimal("40.0")
DEFAULT_RSIMFI_HIGH = Decimal("60.0")
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LENGTH = 14

# 🔸 Маппинг зон Z* -> bin_* (контракт)
ZONE_TO_BIN_IDX = {
    "Z1": 0,
    "Z2": 1,
    "Z3": 2,
    "Z4": 3,
    "Z5": 4,
}


# 🔸 Публичная точка входа анализатора RSI/MFI MTF (h1 + m15 + m5) через словарь биннов
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

    if analysis_id is None:
        log.info(
            "BT_ANALYSIS_RSIMFI_MTF: анализ пропущен (нет analysis_id) scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
                "skipped_reason": "no_analysis_id",
            },
        }

    log.debug(
        "BT_ANALYSIS_RSIMFI_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, rsimfi_low=%s, rsimfi_high=%s, min_share=%s, length=%s",
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
    )

    # загружаем компонентные бины из словаря (по каждому TF отдельно)
    bins_h1_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "h1")
    bins_m15_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "m15")
    bins_m5_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "m5")

    # условия достаточности словаря
    if not bins_h1_by_dir or not bins_m15_by_dir or not bins_m5_by_dir:
        log.info(
            "BT_ANALYSIS_RSIMFI_MTF: анализ пропущен (нет биннов в bt_analysis_bins_dict) "
            "analysis_id=%s, scenario_id=%s, signal_id=%s, bins_h1=%s, bins_m15=%s, bins_m5=%s",
            analysis_id,
            scenario_id,
            signal_id,
            bool(bins_h1_by_dir),
            bool(bins_m15_by_dir),
            bool(bins_m5_by_dir),
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
                "skipped_reason": "no_bins_dict",
            },
        }

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (postproc=true)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.info(
            "BT_ANALYSIS_RSIMFI_MTF: нет позиций для анализа analysis_id=%s, scenario_id=%s, signal_id=%s",
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

    # сначала собираем компонентные бин-коды по h1/m15/m5 для каждой позиции
    base_list: List[Dict[str, Any]] = []

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # бины зависят от направления
        bins_h1 = bins_h1_by_dir.get(direction)
        bins_m15 = bins_m15_by_dir.get(direction)
        bins_m5 = bins_m5_by_dir.get(direction)
        if not bins_h1 or not bins_m15 or not bins_m5:
            positions_skipped += 1
            continue

        # определяем bin_idx (0..4) по зонам Z1..Z5 на каждом TF
        idx_h1 = _extract_rsimfi_bin_idx(raw_stat, "h1", length, rsimfi_low, rsimfi_high)
        idx_m15 = _extract_rsimfi_bin_idx(raw_stat, "m15", length, rsimfi_low, rsimfi_high)
        idx_m5 = _extract_rsimfi_bin_idx(raw_stat, "m5", length, rsimfi_low, rsimfi_high)

        # если не удалось определить полную тройку — пропускаем позицию
        if idx_h1 is None or idx_m15 is None or idx_m5 is None:
            positions_skipped += 1
            continue

        # бин-имена берём из словаря через интервальный маппинг (0..4)
        h_bin = _assign_bin(bins_h1, Decimal(idx_h1))
        m15_bin = _assign_bin(bins_m15, Decimal(idx_m15))
        m5_bin = _assign_bin(bins_m5, Decimal(idx_m5))

        if h_bin is None or m15_bin is None or m5_bin is None:
            positions_skipped += 1
            continue

        base_list.append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "h_bin": h_bin,        # например "H1_bin_0"
                "m15_bin": m15_bin,    # например "M15_bin_2"
                "m5_bin": m5_bin,      # например "M5_bin_4"
            }
        )

    positions_used = len(base_list)

    if positions_used == 0:
        log.info(
            "BT_ANALYSIS_RSIMFI_MTF: после фильтрации нет позиций для анализа analysis_id=%s, scenario_id=%s, signal_id=%s "
            "(total=%s, skipped=%s)",
            analysis_id,
            scenario_id,
            signal_id,
            positions_total,
            positions_skipped,
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

    # фильтр min_share после h1: группируем по h_bin
    by_h1: Dict[str, List[Dict[str, Any]]] = {}
    for rec in base_list:
        h_bin = rec["h_bin"]
        by_h1.setdefault(h_bin, []).append(rec)

    rows: List[Dict[str, Any]] = []

    for h_bin, h1_group in by_h1.items():
        group_n_h1 = len(h1_group)
        share_h1 = Decimal(group_n_h1) / total_for_share

        # если доля по h1 < min_share — весь этот хвост складываем в H1_bin_X|M15_0|M5_0
        if share_h1 < min_share:
            bin_name = f"{h_bin}|M15_0|M5_0"
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

        # внутри этой h1-группы — фильтр по m15_bin
        by_m15: Dict[str, List[Dict[str, Any]]] = {}
        for rec in h1_group:
            m15_bin = rec["m15_bin"]
            by_m15.setdefault(m15_bin, []).append(rec)

        for m15_bin, m15_group in by_m15.items():
            group_n_m15 = len(m15_group)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # если доля по этой (h1,m15) < min_share — кладём в H1_bin_X|M15_bin_Y|M5_0
            if share_m15 < min_share:
                bin_name = f"{h_bin}|{m15_bin}|M5_0"
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

            # остальные — полноценная MTF-матрица: H1_bin_X|M15_bin_Y|M5_bin_Z
            for rec in m15_group:
                bin_name = f"{h_bin}|{m15_bin}|{rec['m5_bin']}"
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

    # итоговый лог по результатам
    log.info(
        "BT_ANALYSIS_RSIMFI_MTF: завершено analysis_id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "rsimfi_low=%s, rsimfi_high=%s, length=%s, min_share=%s, pos_total=%s, pos_used=%s, pos_skipped=%s, rows=%s, H1_groups=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        rsimfi_low,
        rsimfi_high,
        length,
        min_share,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        len(by_h1),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
        },
    }


# 🔸 Загрузка биннов из bt_analysis_bins_dict для analysis_id + tf (группировка по direction)
async def _load_bins_dict_for_analysis(
    pg,
    analysis_id: int,
    timeframe: str,
) -> Dict[str, List[Dict[str, Any]]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                bin_order,
                bin_name,
                val_from,
                val_to,
                to_inclusive
            FROM bt_analysis_bins_dict
            WHERE analysis_id = $1
              AND timeframe   = $2
              AND bin_type    = 'bins'
            ORDER BY direction, bin_order
            """,
            analysis_id,
            timeframe,
        )

    if not rows:
        return {}

    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        direction = str(r["direction"] or "").strip().lower()
        if not direction:
            continue

        out.setdefault(direction, []).append(
            {
                "name": str(r["bin_name"]),
                "min": _safe_decimal(r["val_from"]),
                "max": _safe_decimal(r["val_to"]),
                "to_inclusive": bool(r["to_inclusive"]),
            }
        )

    return out


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


# 🔸 Извлечение bin_idx (0..4) по зоне RSI/MFI для заданного TF
def _extract_rsimfi_bin_idx(
    raw_stat: Any,
    tf: str,
    length: int,
    rsimfi_low: Decimal,
    rsimfi_high: Decimal,
) -> Optional[int]:
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

    rsi_dec = _safe_decimal(rsi_val)
    mfi_dec = _safe_decimal(mfi_val)

    # клипуем RSI/MFI в [0, 100]
    rsi_dec = _clip_0_100(rsi_dec)
    mfi_dec = _clip_0_100(mfi_dec)

    # классифицируем в зону Z1..Z5 (возвращает "Z*_SUFFIX")
    zone_full = _classify_rsi_mfi(
        float(rsi_dec),
        float(mfi_dec),
        float(rsimfi_low),
        float(rsimfi_high),
    )
    if zone_full is None:
        return None

    # берём только префикс "Z1"
    z = zone_full.split("_")[0].strip().upper()
    idx = ZONE_TO_BIN_IDX.get(z)
    if idx is None:
        return None

    return int(idx)


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


# 🔸 Определение имени бина для значения (границы из bt_analysis_bins_dict)
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    for b in bins:
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
        if to_inclusive:
            if lo <= value <= hi:
                return str(name)
        else:
            if lo <= value < hi:
                return str(name)

    return None


# 🔸 Клипование значения в диапазон [0, 100]
def _clip_0_100(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("100"):
        return Decimal("100")
    return value


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