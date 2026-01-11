# bt_analysis_bb_band_bin.py — анализатор распределения позиций по полосам Bollinger Bands (названия бинов берутся из bt_analysis_bins_dict)

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
    tf = _get_str_param(params, "tf", default="m5")                 # TF из raw_stat["tf"][tf]
    bb_prefix = _get_str_param(params, "param_name", "bb20_2_0")    # базовое имя BB: bb20_2_0

    # загружаем словарь биннов (имена) из bt_analysis_bins_dict
    if analysis_id is None:
        log.debug(
            "BT_ANALYSIS_BB_BAND_BIN: analysis_id отсутствует (family=%s, key=%s, name=%s), "
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

    bin_names_by_dir = await _load_bin_names_dict_for_analysis(pg, int(analysis_id), tf)
    if not bin_names_by_dir:
        log.debug(
            "BT_ANALYSIS_BB_BAND_BIN: нет биннов в bt_analysis_bins_dict для analysis_id=%s, tf=%s "
            "(family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — анализ пропущен",
            analysis_id,
            tf,
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
        "BT_ANALYSIS_BB_BAND_BIN: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, bb_prefix=%s, bins_loaded=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        bb_prefix,
        {d: len(m) for d, m in bin_names_by_dir.items()},
    )

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id, window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_BB_BAND_BIN: нет позиций для анализа id=%s (family=%s, key=%s, name=%s), "
            "scenario_id=%s, signal_id=%s",
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
        entry_price = p["entry_price"]

        # имена биннов зависят от направления
        names_map = bin_names_by_dir.get(direction) or {}
        if not names_map:
            positions_skipped += 1
            continue

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

        # сохраняем прежнюю логику биннинга 1-в-1 (только имя берём из словаря)
        # bin_order:
        #   0 -> выше upper
        #   9 -> ниже lower
        #   1..8 -> 8 полос внутри канала
        if price > upper:
            bin_order = 0
        elif price < lower:
            bin_order = 9
        else:
            # внутри канала [lower, upper]
            rel = (upper - price) / H  # 0 → верх, 1 → низ

            # защита от численных артефактов
            if rel < Decimal("0"):
                rel = Decimal("0")
            if rel > Decimal("1"):
                rel = Decimal("1")

            # 8 полос внутри: rel ∈ [0,1] → idx ∈ [0,7]
            idx = int((rel * Decimal("8")).quantize(Decimal("0"), rounding=ROUND_DOWN))
            if idx >= 8:
                idx = 7

            bin_order = 1 + idx  # 1..8

        bin_name = names_map.get(bin_order)
        if not bin_name:
            positions_skipped += 1
            continue

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": price,   # сохраняем прежний смысл: цена входа
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.debug(
        "BT_ANALYSIS_BB_BAND_BIN: анализатор id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "tf=%s, bb_prefix=%s, позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        tf,
        bb_prefix,
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


# 🔸 Загрузка имен биннов из bt_analysis_bins_dict для analysis_id + tf (map direction -> bin_order -> bin_name)
async def _load_bin_names_dict_for_analysis(
    pg,
    analysis_id: int,
    timeframe: str,
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
              AND timeframe   = $2
              AND bin_type    = 'bins'
            ORDER BY direction, bin_order
            """,
            analysis_id,
            timeframe,
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
    window_from: Optional[Any],
    window_to: Optional[Any],
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        # условия достаточности
        if window_from is None or window_to is None:
            return []

        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                timeframe,
                direction,
                pnl_abs,
                raw_stat,
                entry_price
            FROM bt_scenario_positions_v2
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc_v2 = true
              AND entry_time BETWEEN $3 AND $4
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
            window_from,
            window_to,
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