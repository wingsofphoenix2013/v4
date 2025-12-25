# bt_analysis_bb_mtf.py — анализатор распределения позиций по MTF-корзинкам BB (H1/M15 словарь + адаптивные квантили m5 в bt_analysis_bin_dict_adaptive)

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, InvalidOperation, ROUND_DOWN

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_BB_MTF")

# 🔸 Дефолтные параметры анализатора
DEFAULT_MIN_SHARE = Decimal("0.01")
DEFAULT_BB_KEY = "bb20_2_0"
BB_MTF_QUANTILES = 5

# 🔸 Единая точность для адаптивных квантилей rel_m5 (источник истины)
Q6 = Decimal("0.000001")


# 🔸 Публичная точка входа анализатора BB MTF (h1 + m15 + m5, квантили через bt_analysis_bin_dict_adaptive)
async def run_bb_mtf_analysis(
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

    # run-aware поля
    run_id = analysis_ctx.get("run_id")
    run_finished_at = analysis_ctx.get("run_finished_at")

    # параметры анализатора
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)

    # bb_key — префикс в raw_stat["indicators"]["bb"], например "bb20_2_0"
    bb_key_cfg = params.get("bb_key")
    if bb_key_cfg is not None:
        bb_key_raw = bb_key_cfg.get("value")
        bb_key = str(bb_key_raw).strip() if bb_key_raw is not None else DEFAULT_BB_KEY
    else:
        bb_key = DEFAULT_BB_KEY

    if analysis_id is None or scenario_id is None or signal_id is None or run_id is None:
        log.debug(
            "BT_ANALYSIS_BB_MTF: анализ пропущен (нет обязательных id) analysis_id=%s, scenario_id=%s, signal_id=%s",
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
                "skipped_reason": "missing_ids",
            },
        }

    log.debug(
        "BT_ANALYSIS_BB_MTF: старт анализа id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, min_share=%s, bb_key=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        min_share,
        bb_key,
    )

    # загружаем компонентные бины H1/M15 из статического словаря (индексные 0..5)
    bins_h1_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "h1")
    bins_m15_by_dir = await _load_bins_dict_for_analysis(pg, int(analysis_id), "m15")

    # условия достаточности словаря
    if not bins_h1_by_dir or not bins_m15_by_dir:
        log.debug(
            "BT_ANALYSIS_BB_MTF: анализ пропущен (нет биннов H1/M15 в bt_analysis_bins_dict) "
            "analysis_id=%s, scenario_id=%s, signal_id=%s, bins_h1=%s, bins_m15=%s",
            analysis_id,
            scenario_id,
            signal_id,
            bool(bins_h1_by_dir),
            bool(bins_m15_by_dir),
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

    # загружаем позиции окна run (status=closed + postproc=true) — строго в границах window_from/window_to
    window_from = analysis_ctx.get("window_from")
    window_to = analysis_ctx.get("window_to")

    positions = await _load_positions_for_analysis(pg, int(scenario_id), int(signal_id), window_from, window_to)
    if not positions:
        log.debug(
            "BT_ANALYSIS_BB_MTF: нет позиций для анализа analysis_id=%s, scenario_id=%s, signal_id=%s",
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

    # первый проход: собираем H1/M15 бин-индексы и rel_m5 для каждой позиции
    base_list: List[Dict[str, Any]] = []

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        direction = str(p["direction"] or "").strip().lower()
        pnl_abs = p["pnl_abs"]
        raw_stat = p["raw_stat"]
        entry_price = p["entry_price"]

        price = _safe_float(entry_price)
        if price is None:
            positions_skipped += 1
            continue

        # границы BB на h1, m15, m5
        h1_upper, h1_lower = _extract_bb_bounds(raw_stat, "h1", bb_key)
        m15_upper, m15_lower = _extract_bb_bounds(raw_stat, "m15", bb_key)
        m5_upper, m5_lower = _extract_bb_bounds(raw_stat, "m5", bb_key)

        if (
            h1_upper is None or h1_lower is None
            or m15_upper is None or m15_lower is None
            or m5_upper is None or m5_lower is None
        ):
            positions_skipped += 1
            continue

        # индексы бинов H1/M15 по правилам BB-канала
        h1_idx = _bb_position_to_bin_idx(price, h1_upper, h1_lower)
        m15_idx = _bb_position_to_bin_idx(price, m15_upper, m15_lower)

        if h1_idx is None or m15_idx is None:
            positions_skipped += 1
            continue

        # непрерывная позиция внутри/вокруг BB-канала на m5: rel = (price - lower) / (upper - lower)
        rel_m5 = _bb_relative_position(price, m5_upper, m5_lower)
        if rel_m5 is None:
            positions_skipped += 1
            continue

        rel_q = _q6(rel_m5)

        # нормализуем имена H1/M15 через статический словарь
        bins_h1 = bins_h1_by_dir.get(direction)
        bins_m15 = bins_m15_by_dir.get(direction)
        if not bins_h1 or not bins_m15:
            positions_skipped += 1
            continue

        h1_bin_name = _assign_bin(bins_h1, Decimal(h1_idx))
        m15_bin_name = _assign_bin(bins_m15, Decimal(m15_idx))
        if h1_bin_name is None or m15_bin_name is None:
            positions_skipped += 1
            continue

        base_list.append(
            {
                "position_uid": position_uid,
                "direction": direction,
                "pnl_abs": pnl_abs,
                "h1_idx": int(h1_idx),
                "m15_idx": int(m15_idx),
                "h1_bin": h1_bin_name,      # "H1_bin_0".."H1_bin_5"
                "m15_bin": m15_bin_name,    # "M15_bin_0".."M15_bin_5"
                "rel_m5": rel_q,            # Decimal q6
            }
        )

    positions_used = len(base_list)

    if positions_used == 0:
        log.debug(
            "BT_ANALYSIS_BB_MTF: после фильтрации нет позиций для анализа analysis_id=%s, scenario_id=%s, signal_id=%s "
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

    # группировка по H1-бинам (по нормализованному имени)
    by_h1: Dict[str, List[Dict[str, Any]]] = {}
    for rec in base_list:
        h1_bin = rec["h1_bin"]
        by_h1.setdefault(h1_bin, []).append(rec)

    rows: List[Dict[str, Any]] = []

    # source_finished_at — единый по run: bt_signal_backfill_runs.finished_at
    source_finished_at = run_finished_at if isinstance(run_finished_at, datetime) else datetime.utcnow()

    # собираем адаптивные квантили для записи в bt_analysis_bin_dict_adaptive
    adaptive_to_store: List[Tuple[int, str, int, int, int, Decimal, Decimal, bool]] = []
    # формат: (analysis_id, direction, h1_idx, m15_idx, q_idx, val_from, val_to, to_inclusive)

    # последовательное применение min_share: H1 → M15 → m5 (квантили)
    for h1_bin, group_h in by_h1.items():
        group_n_h = len(group_h)
        share_h = Decimal(group_n_h) / total_for_share

        # H1-группа не прошла порог → H1_bin_X|M15_0|M5_0
        if share_h < min_share:
            for rec in group_h:
                bin_name = f"{h1_bin}|M15_0|M5_0"
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

        # внутри H1-группы — группировка по M15
        by_m15: Dict[str, List[Dict[str, Any]]] = {}
        for rec in group_h:
            m15_bin = rec["m15_bin"]
            by_m15.setdefault(m15_bin, []).append(rec)

        for m15_bin, group_m15 in by_m15.items():
            group_n_m15 = len(group_m15)
            share_m15 = Decimal(group_n_m15) / total_for_share

            # (H1, M15)-группа не прошла min_share → H1_bin_X|M15_bin_Y|M5_0
            if share_m15 < min_share:
                for rec in group_m15:
                    bin_name = f"{h1_bin}|{m15_bin}|M5_0"
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

            # полноценный квантильный разрез по rel_m5: строим границы и записываем в adaptive dict
            if not group_m15:
                continue

            # извлекаем индексы группы для bin_order-кодирования
            h1_idx = int(group_m15[0].get("h1_idx", 0))
            m15_idx = int(group_m15[0].get("m15_idx", 0))

            # собираем список (sort_key, rec)
            sortable: List[Tuple[Decimal, Dict[str, Any]]] = []
            for rec in group_m15:
                rel = rec.get("rel_m5")
                if rel is None:
                    continue

                rel_q = _q6(rel)
                direction = str(rec.get("direction") or "").strip().lower()

                # для шорта инвертируем ключ сортировки
                if direction == "short":
                    sort_key = _q6(-rel_q)
                else:
                    sort_key = rel_q

                sortable.append((sort_key, rec))

            if not sortable:
                continue

            sortable.sort(key=lambda x: x[0])
            n = len(sortable)

            # считаем q-группы NTILE(Q) и одновременно собираем границы для каждой q_idx
            q_groups: Dict[int, List[Decimal]] = {q: [] for q in range(1, BB_MTF_QUANTILES + 1)}
            for idx, (sort_key, _) in enumerate(sortable):
                q_idx = (idx * BB_MTF_QUANTILES) // n + 1
                if q_idx < 1:
                    q_idx = 1
                if q_idx > BB_MTF_QUANTILES:
                    q_idx = BB_MTF_QUANTILES
                q_groups[q_idx].append(_q6(sort_key))

            # строим границы [min..max] по каждому квантилю (в q6)
            for q_idx in range(1, BB_MTF_QUANTILES + 1):
                vals = q_groups.get(q_idx) or []
                if not vals:
                    continue

                v_min = min(vals)
                v_max = max(vals)

                # last quantile inclusive
                to_inclusive = (q_idx == BB_MTF_QUANTILES)

                adaptive_to_store.append(
                    (
                        int(analysis_id),
                        str(group_m15[0].get("direction") or "").strip().lower(),
                        h1_idx,
                        m15_idx,
                        q_idx,
                        _q6(v_min),
                        _q6(v_max),
                        to_inclusive,
                    )
                )

            # назначаем квантиль каждой позиции и формируем bin_name
            for idx, (_, rec) in enumerate(sortable):
                q_idx = (idx * BB_MTF_QUANTILES) // n + 1
                if q_idx < 1:
                    q_idx = 1
                if q_idx > BB_MTF_QUANTILES:
                    q_idx = BB_MTF_QUANTILES

                bin_name = f"{h1_bin}|{m15_bin}|M5_Q{q_idx}"
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

    # записываем адаптивные квантильные границы в bt_analysis_bin_dict_adaptive
    adaptive_inserted = 0
    try:
        adaptive_inserted = await _store_adaptive_quantiles(
            pg=pg,
            run_id=int(run_id),
            analysis_id=int(analysis_id),
            scenario_id=int(scenario_id),
            signal_id=int(signal_id),
            records=adaptive_to_store,
            source_finished_at=source_finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_BB_MTF: ошибка записи bt_analysis_bin_dict_adaptive для analysis_id=%s, scenario_id=%s, signal_id=%s: %s",
            analysis_id,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    log.debug(
        "BT_ANALYSIS_BB_MTF: завершено analysis_id=%s (family=%s, key=%s, name=%s), scenario_id=%s, signal_id=%s — "
        "min_share=%s, bb_key=%s, pos_total=%s, pos_used=%s, pos_skipped=%s, rows=%s, H1_groups=%s, adaptive_quantiles=%s",
        analysis_id,
        family_key,
        analysis_key,
        name,
        scenario_id,
        signal_id,
        min_share,
        bb_key,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
        len(by_h1),
        adaptive_inserted,
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
            "adaptive_quantiles_rows": adaptive_inserted,
            "source_finished_at": source_finished_at.isoformat(),
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


# 🔸 Запись адаптивных квантилей в bt_analysis_bin_dict_adaptive (timeframe='mtf', bin_type='quantiles')
async def _store_adaptive_quantiles(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    records: List[Tuple[int, str, int, int, int, Decimal, Decimal, bool]],
    source_finished_at: datetime,
) -> int:
    if not records:
        return 0

    to_insert: List[Tuple[Any, ...]] = []

    for _, direction, h1_idx, m15_idx, q_idx, v_from, v_to, to_inclusive in records:
        # уникальный bin_order по группе: (h1_idx, m15_idx, q_idx)
        bin_order = int(h1_idx) * 100 + int(m15_idx) * 10 + int(q_idx)

        h1_bin_name = f"H1_bin_{int(h1_idx)}"
        m15_bin_name = f"M15_bin_{int(m15_idx)}"
        bin_name = f"{h1_bin_name}|{m15_bin_name}|M5_Q{int(q_idx)}"

        to_insert.append(
            (
                run_id,
                analysis_id,
                scenario_id,
                signal_id,
                str(direction),
                "mtf",
                "quantiles",
                bin_order,
                bin_name,
                _q6(v_from),
                _q6(v_to),
                bool(to_inclusive),
                source_finished_at,
            )
        )

    async with pg.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO bt_analysis_bin_dict_adaptive (
                run_id,
                analysis_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                bin_type,
                bin_order,
                bin_name,
                val_from,
                val_to,
                to_inclusive,
                source_finished_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10, $11,
                $12, $13,
                now()
            )
            ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, direction, timeframe, bin_type, bin_order)
            DO UPDATE SET
                bin_name           = EXCLUDED.bin_name,
                val_from           = EXCLUDED.val_from,
                val_to             = EXCLUDED.val_to,
                to_inclusive       = EXCLUDED.to_inclusive,
                source_finished_at = EXCLUDED.source_finished_at,
                updated_at         = now()
            """,
            to_insert,
        )

    return len(to_insert)


# 🔸 Определение имени бина для значения (по границам из bt_analysis_bins_dict)
def _assign_bin(
    bins: List[Dict[str, Any]],
    value: Decimal,
) -> Optional[str]:
    if not bins:
        return None

    last_index = len(bins) - 1
    v = _safe_decimal(value)

    for idx, b in enumerate(bins):
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        lo_d = _safe_decimal(lo)
        hi_d = _safe_decimal(hi)

        # обычный бин: [min, max)
        # inclusive бин: [min, max]
        if to_inclusive or idx == last_index:
            if lo_d <= v <= hi_d:
                return str(name)
        else:
            if lo_d <= v < hi_d:
                return str(name)

    return None


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
                direction,
                pnl_abs,
                entry_price,
                raw_stat
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc    = true
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
                "direction": r["direction"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "entry_price": r["entry_price"],
                "raw_stat": raw,
            }
        )

    log.debug(
        "BT_ANALYSIS_BB_MTF: загружено позиций для анализа scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Извлечение верхней и нижней границ BB-канала из raw_stat по TF и ключу
def _extract_bb_bounds(
    raw_stat: Any,
    tf: str,
    bb_key: str,
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

    bb_family = indicators.get("bb") or {}
    if not isinstance(bb_family, dict):
        return None, None

    upper_val = bb_family.get(f"{bb_key}_upper")
    lower_val = bb_family.get(f"{bb_key}_lower")

    upper = _safe_float(upper_val)
    lower = _safe_float(lower_val)

    return upper, lower


# 🔸 Маппинг цены относительно BB-канала в индекс бина 0..5
def _bb_position_to_bin_idx(
    price: float,
    upper: float,
    lower: float,
) -> Optional[int]:
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
        return 0

    # ниже нижней границы
    if p < l:
        return 5

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

    return 1 + idx


# 🔸 Непрерывная позиция внутри/вокруг BB-канала на m5: rel = (price - lower) / (upper - lower)
def _bb_relative_position(
    price: float,
    upper: float,
    lower: float,
) -> Optional[Decimal]:
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
    return Decimal(str((p - l) / H))


# 🔸 q6 квантизация (ROUND_DOWN) — источник истины
def _q6(value: Any) -> Decimal:
    try:
        if isinstance(value, Decimal):
            d = value
        else:
            d = Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


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