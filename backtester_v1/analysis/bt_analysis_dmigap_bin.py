# bt_analysis_dmigap_bin.py — анализатор распределения позиций по биннам DMI-gap (+DI − −DI)

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_DMIGAP_BIN")


# 🔸 Публичная точка входа анализатора DMI-gap/bin (линейные бины по диапазону)
async def run_dmigap_bin_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")                 # TF из raw_stat["tf"][tf]
    base_param_name = _get_str_param(params, "param_name", "adx_dmi14")  # например adx_dmi14

    log.debug(
        "BT_ANALYSIS_DMIGAP_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, base_param_name=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        base_param_name,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_DMIGAP_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
    valid_values: List[Decimal] = []
    valid_positions: List[Dict[str, Any]] = []

    # первый проход: считаем dmi_gap для каждой позиции
    for p in positions:
        raw_stat = p["raw_stat"]
        dmi_gap = _extract_dmigap_from_raw_stat(raw_stat, tf, base_param_name)
        if dmi_gap is None:
            p["dmi_gap"] = None
            continue

        p["dmi_gap"] = dmi_gap
        valid_values.append(dmi_gap)
        valid_positions.append(p)

    if not valid_positions:
        log.info(
            "BT_ANALYSIS_DMIGAP_BIN: анализатор id=%s (family=%s, key=%s, name=%s) "
            "для scenario_id=%s, signal_id=%s — нет валидных значений DMI-gap для анализа "
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

    min_gap = min(valid_values)
    max_gap = max(valid_values)

    # строим линейные бины по диапазону [min_gap .. max_gap]
    bins_count = 10
    bins = _build_dmigap_bins(min_gap, max_gap, bins_count=bins_count)

    rows: List[Dict[str, Any]] = []
    positions_used = 0
    positions_skipped = positions_total - len(valid_positions)

    # второй проход: раскладываем позиции по бинам
    for p in valid_positions:
        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        dmi_gap = p.get("dmi_gap")

        if dmi_gap is None:
            positions_skipped += 1
            continue

        # клипуем значение в [min_gap, max_gap] на всякий случай
        if dmi_gap < min_gap:
            dmi_gap = min_gap
        if dmi_gap > max_gap:
            dmi_gap = max_gap

        bin_name = _assign_bin(bins, dmi_gap)
        if bin_name is None:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": dmi_gap,   # DMI-gap (+DI - -DI)
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_ANALYSIS_DMIGAP_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, валидных=%s, использовано=%s, "
        "пропущено=%s, строк_в_результате=%s, min_gap=%s, max_gap=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        len(valid_positions),
        positions_used,
        positions_skipped,
        len(rows),
        str(min_gap),
        str(max_gap),
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
        "BT_ANALYSIS_DMIGAP_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение DMI-gap (+DI − −DI) из raw_stat по TF и базовому имени (например, 'adx_dmi14')
def _extract_dmigap_from_raw_stat(
    raw_stat: Any,
    tf: str,
    base_param_name: str,
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
    dmi_family = indicators.get("adx_dmi") or {}
    if not isinstance(dmi_family, dict):
        return None

    plus_key = f"{base_param_name}_plus_di"
    minus_key = f"{base_param_name}_minus_di"

    plus_val = dmi_family.get(plus_key)
    minus_val = dmi_family.get(minus_key)

    if plus_val is None or minus_val is None:
        return None

    plus_dec = _safe_decimal(plus_val)
    minus_dec = _safe_decimal(minus_val)

    return plus_dec - minus_dec


# 🔸 Построение линейных бинов по диапазону DMI-gap
def _build_dmigap_bins(
    min_val: Decimal,
    max_val: Decimal,
    bins_count: int = 10,
) -> List[Dict[str, Decimal]]:
    bins: List[Dict[str, Decimal]] = []

    # вырожденный диапазон — все бины одинаковые
    if max_val <= min_val:
        for i in range(bins_count):
            name = f"bin_{i}"
            bins.append(
                {
                    "name": name,
                    "min": min_val,
                    "max": max_val,
                }
            )
        return bins

    total_range = max_val - min_val
    step = total_range / Decimal(bins_count)

    # первые bins_count-1 бинов [min, max)
    for i in range(bins_count - 1):
        lo = min_val + step * Decimal(i)
        hi = min_val + step * Decimal(i + 1)
        name = f"bin_{i}"
        bins.append(
            {
                "name": name,
                "min": lo,
                "max": hi,
            }
        )

    # последний бин [min_last, max_val] включительно
    lo_last = min_val + step * Decimal(bins_count - 1)
    bins.append(
        {
            "name": f"bin_{bins_count - 1}",
            "min": lo_last,
            "max": max_val,
        }
    )

    return bins


# 🔸 Определение имени бина для значения DMI-gap
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