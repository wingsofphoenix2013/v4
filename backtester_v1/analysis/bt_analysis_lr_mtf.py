# bt_analysis_lr_mtf.py — анализатор распределения позиций по MTF-корзинкам LR (H1/M15 бины + квантиль m5)

import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_LR_MTF")

# 🔸 Дефолтные параметры анализатора
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_LENGTH = 50
LR_MTF_QUANTILES = 5


# 🔸 Публичная точка входа анализатора LR MTF (h1 + m15 + m5)
async def run_lr_mtf_analysis(
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
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)
    length = _get_int_param(params, "length", DEFAULT_LENGTH)

    log.debug(
        "BT_ANALYSIS_LR_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, min_share=%s, length=%s, params=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        min_share,
        length,
        params,
    )

    # загружаем позиции данного сценария/сигнала, прошедшие постпроцессинг (postproc=true)
    positions = await _load_positions_for_analysis(pg, scenario_id, signal_id)
    if not positions:
        log.debug(
            "BT_ANALYSIS_LR_MTF: нет позиций для анализа id=%s, scenario_id=%s, signal_id=%s",
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

    positions_total = 0
    positions_skipped = 0

    # 🔸 Первый проход: собираем H1/M15-бин и непрерывный rel_m5 для каждой позиции
    # H1/M15: bin_0..bin_5, m5: rel_m5 ∈ (-∞, +∞)
    base_list: List[Dict[str, Any]] = []

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = p["direction"]
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]
        entry_price = p["entry_price"]

        price = _safe_float(entry_price)
        if price is None:
            positions_skipped += 1
            continue

        # границы LR на h1 и m15
        h1_upper, h1_lower = _extract_lr_bounds(raw_stat, "h1", length)
        m15_upper, m15_lower = _extract_lr_bounds(raw_stat, "m15", length)
        m5_upper, m5_lower = _extract_lr_bounds(raw_stat, "m5", length)

        if (
            h1_upper is None or h1_lower is None
            or m15_upper is None or m15_lower is None
            or m5_upper is None or m5_lower is None
        ):
            positions_skipped += 1
            continue

        # бины для H1/M15
        h1_bin = _lr_position_to_bin(price, h1_upper, h1_lower)
        m15_bin = _lr_position_to_bin(price, m15_upper, m15_lower)

        if h1_bin is None or m15_bin is None:
            positions_skipped += 1
            continue

        # непрерывная позиция внутри канала на m5: rel = (price - lower) / (upper - lower)
        rel_m5 = _lr_relative_position(price, m5_upper, m5_lower)
        if rel_m5 is None:
            positions_skipped += 1
            continue

        base_list.append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "h1_bin": h1_bin,       # "bin_0".."bin_5"
                "m15_bin": m15_bin,     # "bin_0".."bin_5"
                "rel_m5": rel_m5,       # float, может быть <0 или >1
            }
        )

    positions_used = len(base_list)

    if positions_used == 0:
        log.debug(
            "BT_ANALYSIS_LR_MTF: после фильтрации нет позиций для анализа scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        summary = {
            "positions_total": positions_total,
            "positions_used": 0,
            "positions_skipped": positions_skipped,
        }
        return {
            "rows": [],
            "summary": summary,
        }

    total_for_share = Decimal(positions_used)

    # 🔸 Группировка по H1-бинам
    by_h1: Dict[str, List[Dict[str, Any]]] = {}
    for rec in base_list:
        h1_bin = rec["h1_bin"]
        by_h1.setdefault(h1_bin, []).append(rec)

    rows: List[Dict[str, Any]] = []

    # 🔸 Последовательное применение min_share: H1 → M15 → M5 (квантили)
    for h1_bin, group_h in by_h1.items():
        group_n_h = len(group_h)
        share_h = Decimal(group_n_h) / total_for_share

        # H1-бин не прошёл порог → всё в H1_bin_X|M15_bin0|M5_Q0
        if share_h < min_share:
            for rec in group_h:
                bin_name = f"H1_{h1_bin}|M15_bin0|M5_Q0"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": rec["rel_m5"],
                        "pnl_abs": rec["pnl_abs"],
                    }
                )
            continue

        # 🔸 Внутри H1-бина — группировка по M15
        by_m15: Dict[str, List[Dict[str, Any]]] = {}
        for rec in group_h:
            m15_bin = rec["m15_bin"]
            by_m15.setdefault(m15_bin, []).append(rec)

        for m15_bin, group_m15 in by_m15.items():
            group_n_m15 = len(group_m15)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # (H1, M15)-зона не прошла min_share → H1_bin_X|M15_bin_Y|M5_Q0
            if share_m15 < min_share:
                for rec in group_m15:
                    bin_name = f"H1_{h1_bin}|M15_{m15_bin}|M5_Q0"
                    rows.append(
                        {
                            "position_uid": rec["position_uid"],
                            "timeframe": "mtf",
                            "direction": rec["direction"],
                            "bin_name": bin_name,
                            "value": rec["rel_m5"],
                            "pnl_abs": rec["pnl_abs"],
                        }
                    )
                continue

            # 🔸 Полноценный квантильный разрез по rel_m5: H1_bin_X|M15_bin_Y|M5_QZ
            sortable: List[Tuple[float, Dict[str, Any]]] = []
            for rec in group_m15:
                rel = rec["rel_m5"]
                try:
                    rel_f = float(rel)
                except (TypeError, ValueError):
                    continue

                direction = str(rec["direction"] or "").lower()

                # для коротких инвертируем, чтобы "более выгодная" сторона шорта шла в те же квантильные группы
                if direction == "short":
                    sort_key = -rel_f
                else:
                    sort_key = rel_f

                sortable.append((sort_key, rec))

            if not sortable:
                continue

            sortable.sort(key=lambda x: x[0])
            n = len(sortable)

            for idx, (_, rec) in enumerate(sortable):
                # NTILE(Q): q_idx = floor(i * Q / n) + 1
                q_idx = (idx * LR_MTF_QUANTILES) // n + 1
                if q_idx < 1:
                    q_idx = 1
                if q_idx > LR_MTF_QUANTILES:
                    q_idx = LR_MTF_QUANTILES

                bin_name = f"H1_{h1_bin}|M15_{m15_bin}|M5_Q{q_idx}"
                rows.append(
                    {
                        "position_uid": rec["position_uid"],
                        "timeframe": "mtf",
                        "direction": rec["direction"],
                        "bin_name": bin_name,
                        "value": rec["rel_m5"],
                        "pnl_abs": rec["pnl_abs"],
                    }
                )

    log.debug(
        "BT_ANALYSIS_LR_MTF: анализатор id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s, length=%s, min_share=%s — "
        "позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s, H1-бинов=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        length,
        min_share,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        len(by_h1),
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
                entry_price,
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
                "entry_price": r["entry_price"],
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_LR_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение верхней и нижней границ LR-канала из raw_stat по TF и длине
def _extract_lr_bounds(
    raw_stat: Any,
    tf: str,
    length: int,
) -> Tuple[Optional[float], Optional[float]]:
    if raw_stat is None:
        return None, None

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
    if not isinstance(indicators, dict):
        return None, None

    lr_family = indicators.get("lr") or {}
    if not isinstance(lr_family, dict):
        return None, None

    prefix = f"lr{length}"
    upper_val = lr_family.get(f"{prefix}_upper")
    lower_val = lr_family.get(f"{prefix}_lower")

    upper = _safe_float(upper_val)
    lower = _safe_float(lower_val)

    return upper, lower


# 🔸 Маппинг цены относительно LR-канала в дискретный бин bin_0..bin_5
def _lr_position_to_bin(
    price: float,
    upper: float,
    lower: float,
) -> Optional[str]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except (TypeError, ValueError):
        return None

    H = u - l
    if H <= 0:
        return None

    # выше верхней границы
    if p > u:
        return "bin_0"

    # ниже нижней границы
    if p < l:
        return "bin_5"

    # внутри канала: делим на 4 зоны сверху вниз
    # rel = 0 → на верхней границе, rel = 1 → на нижней
    rel = (u - p) / H

    if rel < 0:
        rel = 0.0
    if rel > 1:
        rel = 1.0

    idx = int(rel * 4)  # 0..3
    if idx < 0:
        idx = 0
    if idx > 3:
        idx = 3

    # idx=0 → верхняя четверть, idx=3 → нижняя четверть
    return f"bin_{1 + idx}"


# 🔸 Непрерывная позиция внутри/вокруг LR-канала на m5: rel = (price - lower) / (upper - lower)
def _lr_relative_position(
    price: float,
    upper: float,
    lower: float,
) -> Optional[float]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except (TypeError, ValueError):
        return None

    H = u - l
    if H <= 0:
        return None

    # без обрезки: rel < 0 → ниже канала, rel > 1 → выше канала
    return (p - l) / H


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: безопасное приведение к float
def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError, InvalidOperation):
        return None


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