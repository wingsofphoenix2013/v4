# bt_analysis_bb_band_bin.py — анализатор распределения позиций по полосам Bollinger Bands

import logging
import json
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_BB_BAND_BIN")


# 🔸 Публичная точка входа анализатора BB/band_bin
async def run_bb_band_bin_analysis(
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
    tf = _get_str_param(params, "tf", default="m5")             # TF из raw_stat["tf"][tf]
    bb_prefix = _get_str_param(params, "param_name", "bb20_2_0")  # базовое имя BB: bb20_2_0

    log.debug(
        "BT_ANALYSIS_BB_BAND_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, bb_prefix=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        bb_prefix,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (есть raw_stat)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_BB_BAND_BIN: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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
        entry_price = p["entry_price"]

        upper, lower = _extract_bb_from_raw_stat(raw_stat, tf, bb_prefix)
        if upper is None or lower is None:
            positions_skipped += 1
            continue

        # ширина канала
        H = upper - lower
        if H <= Decimal("0"):
            positions_skipped += 1
            continue

        # цена входа должна быть валидной
        if entry_price is None:
            positions_skipped += 1
            continue

        price = entry_price

        # бин 0: выше верхней границы
        if price > upper:
            bin_name = "bin_0"
        # бин 9: ниже нижней границы
        elif price < lower:
            bin_name = "bin_9"
        else:
            # внутри канала [lower, upper]
            rel = (upper - price) / H  # 0 → верх, 1 → низ

            if rel < Decimal("0"):
                rel = Decimal("0")
            if rel > Decimal("1"):
                rel = Decimal("1")

            # 8 полос внутри: rel ∈ [0,1] → idx ∈ [0,7]
            idx = int((rel * Decimal("8")).quantize(Decimal("0"), rounding=ROUND_DOWN))
            if idx >= 8:
                idx = 7

            bin_idx = 1 + idx  # bin_1..bin_8
            bin_name = f"bin_{bin_idx}"

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": price,   # можно хранить цену входа, при желании
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_BB_BAND_BIN: анализатор id=%s (family=%s, key=%s, name=%s), "
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
                timeframe,
                direction,
                pnl_abs,
                raw_stat,
                entry_price
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
                "entry_price": _safe_decimal(r["entry_price"]),
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_BB_BAND_BIN: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение верхней и нижней полос BB из raw_stat по TF и префиксу (например, 'bb20_2_0')
def _extract_bb_from_raw_stat(
    raw_stat: Any,
    tf: str,
    bb_prefix: str,
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    # если raw_stat пришёл строкой из jsonb — парсим
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None, None

    if not isinstance(raw_stat, dict):
        return None, None

    tf_block = (raw_stat.get("tf") or {}).get(tf)
    if not isinstance(tf_block, dict):
        return None, None

    indicators = tf_block.get("indicators") or {}
    bb_family = indicators.get("bb") or {}
    if not isinstance(bb_family, dict):
        return None, None

    upper_key = f"{bb_prefix}_upper"
    lower_key = f"{bb_prefix}_lower"

    upper_val = bb_family.get(upper_key)
    lower_val = bb_family.get(lower_key)

    if upper_val is None or lower_val is None:
        return None, None

    upper = _safe_decimal(upper_val)
    lower = _safe_decimal(lower_val)

    return upper, lower


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