# bb_mtf.py — плагин биннинга для анализатора BB MTF (bb_mtf): расчёт bin_name (H1/M15 bins + adaptive quantiles M5) для кандидата сигнала

import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_PLUGIN_BB_MTF")

# 🔸 Единая точность (q6) — как в bt_analysis_bb_mtf.py
Q6 = Decimal("0.000001")

# 🔸 Таблицы
BT_ANALYSIS_PARAMS_TABLE = "bt_analysis_parameters"
BT_BINS_DICT_TABLE = "bt_analysis_bins_dict"
BT_ADAPTIVE_DICT_TABLE = "bt_analysis_bin_dict_adaptive"
BT_INDICATOR_VALUES_TABLE = "indicator_values_v4"

# 🔸 BB instance_id overrides (договорённость системы)
BB_INSTANCE_OVERRIDES: Dict[str, Dict[str, Optional[int]]] = {
    "bb20_2_0": {"m5": 17, "m15": 34, "h1": 51},
}

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Запас влево для загрузки серий (чтобы не терять первые кандидаты окна)
SERIES_LOOKBACK_MARGIN_MINUTES = 120

# 🔸 Кеш параметров анализаторов: analysis_id -> {param_name: param_value}
_analysis_params_cache: Dict[int, Dict[str, str]] = {}

# 🔸 Кеш bins_dict: (analysis_id, direction, timeframe) -> bins_list
_bins_dict_cache: Dict[Tuple[int, str, str], List[Dict[str, Any]]] = {}


# 🔸 q6 квантизация (ROUND_DOWN)
def _q6(value: Any) -> Decimal:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Безопасный float
def _f(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


# 🔸 Округление времени вниз до границы TF (минуты)
def _floor_to_tf(dt: datetime, tf_minutes: int) -> datetime:
    dt0 = dt.replace(second=0, microsecond=0)
    remainder = dt0.minute % int(tf_minutes)
    return dt0 - timedelta(minutes=remainder)


# 🔸 Тайм-алиасинг: open_time TF, который мог быть известен на decision_time
def resolve_open_time_tf(decision_time: datetime, tf: str) -> Optional[datetime]:
    tf_l = str(tf or "").strip().lower()
    step = TF_STEP_MINUTES.get(tf_l)
    if not step:
        return None

    # open_time + Δ(tf) <= decision_time  <=> open_time <= decision_time - Δ(tf)
    upper_bound = decision_time - timedelta(minutes=int(step))
    return _floor_to_tf(upper_bound, int(step))


# 🔸 Определение BB instance_id по (bb_key, timeframe) через overrides
def resolve_bb_instance_id(bb_key: str, timeframe: str) -> Optional[int]:
    key = str(bb_key or "").strip().lower()
    tf = str(timeframe or "").strip().lower()

    iid = (BB_INSTANCE_OVERRIDES.get(key) or {}).get(tf)
    try:
        iid_i = int(iid) if iid is not None else 0
    except Exception:
        iid_i = 0

    return iid_i if iid_i > 0 else None


# 🔸 Загрузка параметров анализатора (min_share / bb_key / param_name)
async def load_analysis_params(pg, analysis_id: int) -> Dict[str, str]:
    aid = int(analysis_id)
    cached = _analysis_params_cache.get(aid)
    if cached is not None:
        return cached

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT param_name, param_value
            FROM {BT_ANALYSIS_PARAMS_TABLE}
            WHERE analysis_id = $1
            """,
            aid,
        )

    out: Dict[str, str] = {}
    for r in rows:
        pn = str(r["param_name"] or "").strip()
        pv = str(r["param_value"] or "").strip()
        if pn:
            out[pn] = pv

    _analysis_params_cache[aid] = out
    return out


# 🔸 Загрузка bins_dict для analysis_id + direction + timeframe (h1/m15)
async def load_bins_dict(pg, analysis_id: int, direction: str, timeframe: str) -> List[Dict[str, Any]]:
    key = (int(analysis_id), str(direction).strip().lower(), str(timeframe).strip().lower())
    cached = _bins_dict_cache.get(key)
    if cached is not None:
        return cached

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT bin_order, bin_name, val_from, val_to, to_inclusive
            FROM {BT_BINS_DICT_TABLE}
            WHERE analysis_id = $1
              AND direction   = $2
              AND timeframe   = $3
              AND bin_type    = 'bins'
            ORDER BY bin_order
            """,
            int(analysis_id),
            str(direction).strip().lower(),
            str(timeframe).strip().lower(),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "name": str(r["bin_name"]),
                "min": _d(r["val_from"]),
                "max": _d(r["val_to"]),
                "to_inclusive": bool(r["to_inclusive"]),
            }
        )

    _bins_dict_cache[key] = out
    return out


# 🔸 Загрузка adaptive quantiles для run/analysis/scenario/signal/direction (все пары H1|M15 сразу)
async def load_adaptive_quantiles(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
) -> Dict[str, Any]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT bin_name, val_from, val_to, to_inclusive, bin_order
            FROM {BT_ADAPTIVE_DICT_TABLE}
            WHERE run_id      = $1
              AND analysis_id = $2
              AND scenario_id = $3
              AND signal_id   = $4
              AND direction   = $5
              AND timeframe   = 'mtf'
              AND bin_type    = 'quantiles'
            ORDER BY bin_order
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(parent_signal_id),
            str(direction).strip().lower(),
        )

    # quantiles_by_pair[(h1_bin, m15_bin)] = [ {bin_name, lo, hi, to_inclusive}, ... ]
    quantiles_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    h1_bins_with_any_quantiles: Set[str] = set()

    for r in rows:
        bn = str(r["bin_name"] or "").strip()
        if not bn:
            continue

        # формат: H1_bin_X|M15_bin_Y|M5_Qk
        parts = bn.split("|")
        if len(parts) < 3:
            continue

        h1_bin = parts[0]
        m15_bin = parts[1]

        lo = _q6(_d(r["val_from"]))
        hi = _q6(_d(r["val_to"]))
        to_inclusive = bool(r["to_inclusive"])

        quantiles_by_pair.setdefault((h1_bin, m15_bin), []).append(
            {
                "bin_name": bn,
                "val_from": lo,
                "val_to": hi,
                "to_inclusive": to_inclusive,
                "bin_order": int(r["bin_order"] or 0),
            }
        )

        h1_bins_with_any_quantiles.add(h1_bin)

    # сортируем внутри каждой пары по bin_order
    for k in list(quantiles_by_pair.keys()):
        quantiles_by_pair[k] = sorted(quantiles_by_pair[k], key=lambda x: int(x.get("bin_order") or 0))

    return {
        "quantiles_by_pair": quantiles_by_pair,
        "h1_bins_with_any_quantiles": h1_bins_with_any_quantiles,
    }


# 🔸 Загрузка серии BB bounds (upper/lower) для одного instance_id и symbol в окне
async def load_bb_bounds_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
    bb_key: str,
) -> Dict[datetime, Tuple[float, float]]:
    # условия достаточности
    if instance_id <= 0:
        return {}

    # небольшой запас влево для алиасинга m15/h1
    from_ext = from_time - timedelta(minutes=SERIES_LOOKBACK_MARGIN_MINUTES)

    upper_name = f"{str(bb_key).strip().lower()}_upper"
    lower_name = f"{str(bb_key).strip().lower()}_lower"

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, param_name, value
            FROM {BT_INDICATOR_VALUES_TABLE}
            WHERE instance_id = $1
              AND symbol      = $2
              AND open_time  BETWEEN $3 AND $4
              AND param_name  = ANY($5::text[])
            ORDER BY open_time
            """,
            int(instance_id),
            str(symbol),
            from_ext,
            to_time,
            [upper_name, lower_name],
        )

    tmp: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        ts = r["open_time"]
        pn = str(r["param_name"] or "").strip().lower()
        fv = _f(r["value"])
        if fv is None:
            continue
        d = tmp.setdefault(ts, {})
        d[pn] = float(fv)

    out: Dict[datetime, Tuple[float, float]] = {}
    for ts, kv in tmp.items():
        u = kv.get(upper_name)
        l = kv.get(lower_name)
        if u is None or l is None:
            continue
        out[ts] = (float(u), float(l))

    return out


# 🔸 Маппинг цены относительно BB-канала в индекс бина 0..5 (как в bt_analysis_bb_mtf.py)
def _bb_position_to_bin_idx(price: float, upper: float, lower: float) -> Optional[int]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except Exception:
        return None

    H = u - l
    if H <= 0:
        return None

    if p > u:
        return 0
    if p < l:
        return 5

    # rel = 0 → верхняя граница, rel = 1 → нижняя граница
    rel = (u - p) / H

    if rel < 0:
        rel = 0.0
    if rel > 1:
        rel = 1.0

    idx = int(rel * 4)
    if idx < 0:
        idx = 0
    if idx > 3:
        idx = 3

    return 1 + idx


# 🔸 Непрерывная позиция внутри/вокруг BB-канала: rel = (price - lower) / (upper - lower)
def _bb_relative_position(price: float, upper: float, lower: float) -> Optional[Decimal]:
    try:
        p = float(price)
        u = float(upper)
        l = float(lower)
    except Exception:
        return None

    H = u - l
    if H <= 0:
        return None

    return Decimal(str((p - l) / H))


# 🔸 Определение имени бина по границам bins_dict
def _assign_bin(bins: List[Dict[str, Any]], value: Decimal) -> Optional[str]:
    if not bins:
        return None

    last_index = len(bins) - 1
    v = _d(value)

    for idx, b in enumerate(bins):
        name = b.get("name")
        lo = b.get("min")
        hi = b.get("max")
        to_inclusive = bool(b.get("to_inclusive"))

        if lo is None or hi is None or name is None:
            continue

        lo_d = _d(lo)
        hi_d = _d(hi)

        # inclusive для последнего бина и для to_inclusive=true
        if to_inclusive or idx == last_index:
            if lo_d <= v <= hi_d:
                return str(name)
        else:
            if lo_d <= v < hi_d:
                return str(name)

    return None


# 🔸 Выбор квантильного бина по sort_key
def _pick_quantile_bin(q_rows: List[Dict[str, Any]], sort_key: Decimal) -> Optional[str]:
    if not q_rows:
        return None

    v = _q6(sort_key)

    for rec in q_rows:
        lo = _q6(rec.get("val_from"))
        hi = _q6(rec.get("val_to"))
        to_inclusive = bool(rec.get("to_inclusive"))

        if to_inclusive:
            if lo <= v <= hi:
                return str(rec.get("bin_name") or "")
        else:
            if lo <= v < hi:
                return str(rec.get("bin_name") or "")

    return None


# 🔸 Инициализация контекста плагина под конкретный winner (analysis_id) и пару (run/scenario/parent_signal/direction)
async def init_bb_mtf_plugin_context(
    pg,
    run_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    analysis_id: int,
) -> Dict[str, Any]:
    params = await load_analysis_params(pg, int(analysis_id))

    bb_key = str(params.get("bb_key") or "").strip().lower()
    if not bb_key:
        # условия достаточности
        raise RuntimeError(f"BB_MTF plugin: analysis_id={analysis_id} has no valid bb_key param")

    # bins_dict
    bins_h1 = await load_bins_dict(pg, int(analysis_id), str(direction), "h1")
    bins_m15 = await load_bins_dict(pg, int(analysis_id), str(direction), "m15")

    # adaptive quantiles (run-aware)
    adaptive = await load_adaptive_quantiles(
        pg=pg,
        run_id=int(run_id),
        analysis_id=int(analysis_id),
        scenario_id=int(scenario_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction).strip().lower(),
    )

    ctx = {
        "analysis_id": int(analysis_id),
        "direction": str(direction).strip().lower(),
        "bb_key": str(bb_key),
        "param_name": str(params.get("param_name") or "").strip(),
        "min_share": str(params.get("min_share") or "").strip(),
        "bins_h1": bins_h1,
        "bins_m15": bins_m15,
        "quantiles_by_pair": adaptive.get("quantiles_by_pair") or {},
        "h1_bins_with_any_quantiles": set(adaptive.get("h1_bins_with_any_quantiles") or set()),
    }

    log.info(
        "BT_SIG_PLUGIN_BB_MTF: init ctx — analysis_id=%s dir=%s bb_key=%s bins_h1=%s bins_m15=%s quant_pairs=%s h1_bins_with_q=%s",
        ctx["analysis_id"],
        ctx["direction"],
        ctx["bb_key"],
        len(bins_h1),
        len(bins_m15),
        len(ctx["quantiles_by_pair"]),
        len(ctx["h1_bins_with_any_quantiles"]),
    )

    return ctx


# 🔸 Подготовка серий для одного символа (BB bounds на m5/m15/h1)
async def prepare_symbol_series(
    pg,
    plugin_ctx: Dict[str, Any],
    symbol: str,
    window_from: datetime,
    window_to: datetime,
) -> Dict[str, Any]:
    bb_key = str(plugin_ctx.get("bb_key") or "").strip().lower()
    if not bb_key:
        return {}

    m5_iid = resolve_bb_instance_id(bb_key, "m5") or 0
    m15_iid = resolve_bb_instance_id(bb_key, "m15") or 0
    h1_iid = resolve_bb_instance_id(bb_key, "h1") or 0

    # условия достаточности
    if m5_iid <= 0 or m15_iid <= 0 or h1_iid <= 0:
        return {}

    m5_bounds = await load_bb_bounds_series(pg, m5_iid, symbol, window_from, window_to, bb_key)
    m15_bounds = await load_bb_bounds_series(pg, m15_iid, symbol, window_from, window_to, bb_key)
    h1_bounds = await load_bb_bounds_series(pg, h1_iid, symbol, window_from, window_to, bb_key)

    return {
        "m5": m5_bounds,     # open_time(m5) -> (upper, lower)
        "m15": m15_bounds,   # open_time(m15) -> (upper, lower)
        "h1": h1_bounds,     # open_time(h1) -> (upper, lower)
    }


# 🔸 Основная функция плагина: вычислить bin_name для bounce-кандидата
def compute_bb_mtf_bin_name(
    plugin_ctx: Dict[str, Any],
    symbol_series: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Optional[str]:
    # candidate ожидается: {"open_time": dt, "decision_time": dt, "direction": "long/short", "price": float, ...}
    direction = str(candidate.get("direction") or "").strip().lower()
    if not direction:
        return None

    if direction != str(plugin_ctx.get("direction") or "").strip().lower():
        return None

    ts_m5 = candidate.get("open_time")
    decision_time = candidate.get("decision_time")
    price = candidate.get("price")

    if not isinstance(ts_m5, datetime) or not isinstance(decision_time, datetime):
        return None

    price_f = _f(price)
    if price_f is None:
        return None

    # resolve tf open_time, который мог быть известен к decision_time
    open_time_m15 = resolve_open_time_tf(decision_time, "m15")
    open_time_h1 = resolve_open_time_tf(decision_time, "h1")
    if open_time_m15 is None or open_time_h1 is None:
        return None

    m5_bounds: Dict[datetime, Tuple[float, float]] = (symbol_series.get("m5") or {})
    m15_bounds: Dict[datetime, Tuple[float, float]] = (symbol_series.get("m15") or {})
    h1_bounds: Dict[datetime, Tuple[float, float]] = (symbol_series.get("h1") or {})

    # условия достаточности bounds
    b_m5 = m5_bounds.get(ts_m5)
    b_m15 = m15_bounds.get(open_time_m15)
    b_h1 = h1_bounds.get(open_time_h1)
    if not b_m5 or not b_m15 or not b_h1:
        return None

    m5_upper, m5_lower = b_m5
    m15_upper, m15_lower = b_m15
    h1_upper, h1_lower = b_h1

    # индексы H1/M15 по правилам BB-канала
    h1_idx = _bb_position_to_bin_idx(price_f, h1_upper, h1_lower)
    m15_idx = _bb_position_to_bin_idx(price_f, m15_upper, m15_lower)
    if h1_idx is None or m15_idx is None:
        return None

    # rel_m5 для квантилей
    rel_m5 = _bb_relative_position(price_f, m5_upper, m5_lower)
    if rel_m5 is None:
        return None

    rel_q6 = _q6(rel_m5)

    # нормализуем H1/M15 имена через bins_dict
    bins_h1: List[Dict[str, Any]] = plugin_ctx.get("bins_h1") or []
    bins_m15: List[Dict[str, Any]] = plugin_ctx.get("bins_m15") or []
    h1_bin = _assign_bin(bins_h1, Decimal(int(h1_idx)))
    m15_bin = _assign_bin(bins_m15, Decimal(int(m15_idx)))
    if not h1_bin or not m15_bin:
        return None

    h1_bin_s = str(h1_bin)
    m15_bin_s = str(m15_bin)

    quantiles_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = plugin_ctx.get("quantiles_by_pair") or {}
    h1_bins_with_any_quantiles: Set[str] = set(plugin_ctx.get("h1_bins_with_any_quantiles") or set())

    # short: инверсия sort_key (как в bt_analysis_bb_mtf.py)
    sort_key = _q6(-rel_q6) if direction == "short" else rel_q6

    # H1 хвост: если для H1_bin нет квантилей ни для одной M15 группы
    if h1_bin_s not in h1_bins_with_any_quantiles:
        return f"{h1_bin_s}|M15_0|M5_0"

    # M15 хвост: если конкретная пара (H1,M15) не имеет квантилей
    q_rows = quantiles_by_pair.get((h1_bin_s, m15_bin_s)) or []
    if not q_rows:
        return f"{h1_bin_s}|{m15_bin_s}|M5_0"

    q_bin = _pick_quantile_bin(q_rows, sort_key)
    if not q_bin:
        return None

    return str(q_bin)