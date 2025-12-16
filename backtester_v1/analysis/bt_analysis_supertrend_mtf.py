# bt_analysis_supertrend_mtf.py — анализатор согласованности позиций с Supertrend по трём ТФ (H1 + M15 + M5) через словарь bt_analysis_bins_dict

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_SUPERTREND_MTF")


# 🔸 Публичная точка входа анализатора Supertrend/mtf
async def run_supertrend_mtf_analysis(
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
    st_param_name = _get_str_param(params, "param_name", default="supertrend10_3_0_trend")

    # порядок TF в MTF-формате: от старшего к младшему
    tf_order = ["h1", "m15", "m5"]

    if analysis_id is None:
        log.debug(
            "BT_ANALYSIS_SUPERTREND_MTF: analysis_id отсутствует (family=%s, key=%s, name=%s), "
            "scenario_id=%s, signal_id=%s — анализ пропущен",
            family_key,
            analysis_key,
            name,
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

    # загружаем словарь биннов (bin_order -> bin_name) для timeframe='mtf'
    bins_map = await _load_bins_dict_for_mtf(pg, int(analysis_id))
    if not bins_map:
        log.debug(
            "BT_ANALYSIS_SUPERTREND_MTF: нет биннов в bt_analysis_bins_dict для analysis_id=%s, timeframe='mtf' "
            "(family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — анализ пропущен",
            analysis_id,
            family_key,
            analysis_key,
            name,
            scenario_id,
            signal_id,
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

    log.debug(
        "BT_ANALYSIS_SUPERTREND_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, st_param_name=%s, tf_order=%s, bins_loaded=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        st_param_name,
        tf_order,
        {d: len(m) for d, m in bins_map.items()},
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_SUPERTREND_MTF: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s",
            analysis_id,
            family_key,
            analysis_key,
            name,
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
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]

        # извлекаем бинарный вектор согласованности по трём ТФ (H1, M15, M5)
        bits = _build_supertrend_bits_vector(
            raw_stat=raw_stat,
            tf_order=tf_order,
            st_param_name=st_param_name,
            direction=direction,
        )
        if bits is None:
            positions_skipped += 1
            continue

        # числовой код бина: H1/M15/M5, например [1,0,1] -> 101
        value_numeric = _bits_to_numeric(bits)
        bin_order = int(value_numeric)

        # достаём имя бина из словаря
        dir_bins = bins_map.get(direction)
        if not dir_bins:
            positions_skipped += 1
            continue

        bin_name = dir_bins.get(bin_order)
        if not bin_name:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": "mtf",
                "direction": direction,
                "bin_name": bin_name,
                "value": value_numeric,
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_SUPERTREND_MTF: анализатор id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "positions_total=%s, used=%s, skipped=%s, rows=%s",
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


# 🔸 Загрузка словаря биннов для timeframe='mtf' из bt_analysis_bins_dict (map direction -> bin_order -> bin_name)
async def _load_bins_dict_for_mtf(
    pg,
    analysis_id: int,
) -> Dict[str, Dict[int, str]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                bin_order,
                bin_name
            FROM bt_analysis_bins_dict
            WHERE analysis_id = $1
              AND timeframe   = 'mtf'
              AND bin_type    = 'bins'
            ORDER BY direction, bin_order
            """,
            analysis_id,
        )

    if not rows:
        return {}

    out: Dict[str, Dict[int, str]] = {}
    for r in rows:
        direction = str(r["direction"] or "").strip().lower()
        if not direction:
            continue

        try:
            order = int(r["bin_order"])
        except Exception:
            continue

        name = r["bin_name"]
        if name is None:
            continue

        out.setdefault(direction, {})[order] = str(name)

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
        "BT_ANALYSIS_SUPERTREND_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Построение бинарного вектора согласованности по Supertrend для позиции (в порядке tf_order)
def _build_supertrend_bits_vector(
    raw_stat: Any,
    tf_order: List[str],
    st_param_name: str,
    direction: str,
) -> Optional[List[int]]:
    # если raw_stat пришёл строкой из jsonb — парсим
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_block = raw_stat.get("tf") or {}
    if not isinstance(tf_block, dict):
        return None

    dir_norm = str(direction or "").strip().lower()
    if dir_norm not in ("long", "short"):
        return None

    bits: List[int] = []

    for tf in tf_order:
        tf_info = tf_block.get(tf)
        if not isinstance(tf_info, dict):
            return None

        indicators = tf_info.get("indicators") or {}
        if not isinstance(indicators, dict):
            return None

        st_family = indicators.get("supertrend") or {}
        if not isinstance(st_family, dict):
            return None

        trend_raw = st_family.get(st_param_name)
        if trend_raw is None:
            return None

        trend = _safe_decimal(trend_raw)

        # нормализуем знак тренда: >0 -> +1, <0 -> -1, 0 считаем бессмысленным
        if trend > 0:
            st_sign = 1
        elif trend < 0:
            st_sign = -1
        else:
            return None

        # условия согласованности:
        # long  + supertrend +1 -> 1, иначе 0
        # short + supertrend -1 -> 1, иначе 0
        if dir_norm == "long":
            bit = 1 if st_sign == 1 else 0
        else:
            bit = 1 if st_sign == -1 else 0

        bits.append(bit)

    if len(bits) != len(tf_order):
        return None

    return bits


# 🔸 Преобразование битов [1,0,1] в числовой код 101 (numeric)
def _bits_to_numeric(bits: List[int]) -> Decimal:
    digits = "".join(str(int(b)) for b in bits)
    if not digits:
        return Decimal("0")
    try:
        return Decimal(int(digits))
    except (InvalidOperation, ValueError, TypeError):
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


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")