# lr_angle_mtf.py — плагин биннинга для анализатора LR ANGLE MTF (lr50_angle_mtf / lr100_angle_mtf): расчёт bin_name (H1/M15 зоны + adaptive quantiles M5) для кандидата сигнала

import logging
import bisect
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_PLUGIN_LR_ANGLE_MTF")

# 🔸 Единая точность (q6) — как в bt_analysis_lr_angle_mtf.py
Q6 = Decimal("0.000001")

# 🔸 Таблицы
BT_ANALYSIS_PARAMS_TABLE = "bt_analysis_parameters"
BT_BINS_DICT_TABLE = "bt_analysis_bins_dict"
BT_ADAPTIVE_DICT_TABLE = "bt_analysis_bin_dict_adaptive"
BT_INDICATOR_VALUES_TABLE = "indicator_values_v4"

# 🔸 OHLCV таблицы для алиасинга (таблицы аналогичны, меняется только имя)
OHLCV_TABLE_BY_TF: Dict[str, str] = {
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}

# 🔸 LR instance_id overrides (договорённость системы)
LR_INSTANCE_OVERRIDES: Dict[str, Dict[str, Optional[int]]] = {
    "lr50": {"m5": 7, "m15": 24, "h1": 41},
    "lr100": {"m5": 8, "m15": 25, "h1": 42},
}

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m15": 15,
    "h1": 60,
}

# 🔸 Зоны угла (как в bt_analysis_lr_angle_mtf.py)
ZONE_TO_BIN_IDX = {
    "SD": 0,
    "MD": 1,
    "FLAT": 2,
    "MU": 3,
    "SU": 4,
}

# 🔸 Запас влево для загрузки серий (чтобы не терять алиасинг на границе окна)
SERIES_LOOKBACK_MARGIN_MINUTES = 180

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


# 🔸 Преобразование угла в зону (как в bt_analysis_lr_angle_mtf.py)
def _angle_to_zone(angle: Decimal) -> Optional[str]:
    try:
        val = float(angle)
    except Exception:
        return None

    if val <= -0.10:
        return "SD"
    if -0.10 < val < -0.02:
        return "MD"
    if -0.02 <= val <= 0.02:
        return "FLAT"
    if 0.02 < val < 0.10:
        return "MU"
    if val >= 0.10:
        return "SU"
    return None


# 🔸 Определение имени бина по границам bins_dict (как в анализаторах)
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


# 🔸 Определение LR instance_id по (lr_prefix, timeframe) через overrides
def resolve_lr_instance_id(lr_prefix: str, timeframe: str) -> Optional[int]:
    prefix = str(lr_prefix or "").strip().lower()
    tf = str(timeframe or "").strip().lower()

    iid = (LR_INSTANCE_OVERRIDES.get(prefix) or {}).get(tf)
    try:
        iid_i = int(iid) if iid is not None else 0
    except Exception:
        iid_i = 0
    return iid_i if iid_i > 0 else None


# 🔸 Загрузка параметров анализатора (lr_prefix / param_name / min_share)
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
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
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

    quantiles_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for r in rows:
        bn = str(r["bin_name"] or "").strip()
        if not bn:
            continue

        parts = bn.split("|")
        if len(parts) < 3:
            continue

        h1_bin = parts[0]
        m15_bin = parts[1]

        quantiles_by_pair.setdefault((h1_bin, m15_bin), []).append(
            {
                "bin_name": bn,
                "val_from": _q6(_d(r["val_from"])),
                "val_to": _q6(_d(r["val_to"])),
                "to_inclusive": bool(r["to_inclusive"]),
                "bin_order": int(r["bin_order"] or 0),
            }
        )

    for k in list(quantiles_by_pair.keys()):
        quantiles_by_pair[k] = sorted(quantiles_by_pair[k], key=lambda x: int(x.get("bin_order") or 0))

    return quantiles_by_pair


# 🔸 Загрузка серии углов LR (lr_prefix_angle) для одного instance_id/symbol в окне
async def load_lr_angle_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
    lr_prefix: str,
) -> Dict[datetime, Decimal]:
    if instance_id <= 0:
        return {}

    from_ext = from_time - timedelta(minutes=SERIES_LOOKBACK_MARGIN_MINUTES)
    param_name = f"{str(lr_prefix).strip().lower()}_angle"

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, value
            FROM {BT_INDICATOR_VALUES_TABLE}
            WHERE instance_id = $1
              AND symbol      = $2
              AND open_time  BETWEEN $3 AND $4
              AND param_name  = $5
            ORDER BY open_time
            """,
            int(instance_id),
            str(symbol),
            from_ext,
            to_time,
            str(param_name),
        )

    out: Dict[datetime, Decimal] = {}
    for r in rows:
        ts = r["open_time"]
        out[ts] = _q6(_d(r["value"]))
    return out


# 🔸 Загрузка open_time массива из OHLCV для алиасинга (m15/h1)
async def load_ohlcv_open_times(
    pg,
    symbol: str,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> List[datetime]:
    tf = str(timeframe or "").strip().lower()
    table = OHLCV_TABLE_BY_TF.get(tf)
    if not table:
        return []

    from_ext = from_time - timedelta(minutes=SERIES_LOOKBACK_MARGIN_MINUTES)

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time
            FROM {table}
            WHERE symbol = $1
              AND open_time BETWEEN $2 AND $3
            ORDER BY open_time
            """,
            str(symbol),
            from_ext,
            to_time,
        )

    return [r["open_time"] for r in rows if r and r["open_time"] is not None]


# 🔸 Алиасинг open_time по списку свечей: last open_time <= decision_time - tf_minutes
def resolve_open_time_from_series(
    open_times: List[datetime],
    decision_time: datetime,
    tf_minutes: int,
) -> Optional[datetime]:
    if not open_times:
        return None

    upper_bound = decision_time - timedelta(minutes=int(tf_minutes))
    idx = bisect.bisect_right(open_times, upper_bound) - 1
    if idx < 0:
        return None
    return open_times[idx]


# 🔸 Инициализация контекста плагина под конкретный winner и пару (run/scenario/parent_signal/direction)
async def init_lr_angle_mtf_plugin_context(
    pg,
    run_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    analysis_id: int,
) -> Dict[str, Any]:
    params = await load_analysis_params(pg, int(analysis_id))

    # lr_prefix: из параметра lr_prefix, либо из param_name, либо default lr50
    lr_prefix = str(params.get("lr_prefix") or "").strip().lower()
    param_name = str(params.get("param_name") or "").strip()

    if not lr_prefix:
        pn = param_name.lower()
        if "lr100" in pn:
            lr_prefix = "lr100"
        elif "lr50" in pn:
            lr_prefix = "lr50"
        else:
            lr_prefix = "lr50"

    bins_h1 = await load_bins_dict(pg, int(analysis_id), str(direction), "h1")
    bins_m15 = await load_bins_dict(pg, int(analysis_id), str(direction), "m15")

    quantiles_by_pair = await load_adaptive_quantiles(
        pg=pg,
        run_id=int(run_id),
        analysis_id=int(analysis_id),
        scenario_id=int(scenario_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction).strip().lower(),
    )

    return {
        "analysis_id": int(analysis_id),
        "direction": str(direction).strip().lower(),
        "lr_prefix": str(lr_prefix),
        "param_name": param_name,
        "min_share": str(params.get("min_share") or "").strip(),
        "bins_h1": bins_h1,
        "bins_m15": bins_m15,
        "quantiles_by_pair": quantiles_by_pair,
    }


# 🔸 Подготовка серий для одного символа (angles на m5/m15/h1 + open_times m15/h1)
async def prepare_symbol_series(
    pg,
    plugin_ctx: Dict[str, Any],
    symbol: str,
    window_from: datetime,
    window_to: datetime,
) -> Dict[str, Any]:
    lr_prefix = str(plugin_ctx.get("lr_prefix") or "").strip().lower()
    if not lr_prefix:
        return {}

    m5_iid = resolve_lr_instance_id(lr_prefix, "m5") or 0
    m15_iid = resolve_lr_instance_id(lr_prefix, "m15") or 0
    h1_iid = resolve_lr_instance_id(lr_prefix, "h1") or 0

    if m5_iid <= 0 or m15_iid <= 0 or h1_iid <= 0:
        return {}

    # series angles
    m5_angle = await load_lr_angle_series(pg, m5_iid, symbol, window_from, window_to, lr_prefix)
    m15_angle = await load_lr_angle_series(pg, m15_iid, symbol, window_from, window_to, lr_prefix)
    h1_angle = await load_lr_angle_series(pg, h1_iid, symbol, window_from, window_to, lr_prefix)

    # ohlcv open_times for aliasing (robust to gaps)
    m15_times = await load_ohlcv_open_times(pg, symbol, "m15", window_from, window_to)
    h1_times = await load_ohlcv_open_times(pg, symbol, "h1", window_from, window_to)

    return {
        "m5_angle": m5_angle,
        "m15_angle": m15_angle,
        "h1_angle": h1_angle,
        "m15_times": m15_times,
        "h1_times": h1_times,
    }


# 🔸 Основная функция плагина: вычислить bin_name для кандидата
def compute_lr_angle_mtf_bin_name(
    plugin_ctx: Dict[str, Any],
    symbol_series: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Optional[str]:
    direction = str(candidate.get("direction") or "").strip().lower()
    if not direction:
        return None

    if direction != str(plugin_ctx.get("direction") or "").strip().lower():
        return None

    ts_m5 = candidate.get("open_time")
    decision_time = candidate.get("decision_time")
    if not isinstance(ts_m5, datetime) or not isinstance(decision_time, datetime):
        return None

    m5_angle: Dict[datetime, Decimal] = symbol_series.get("m5_angle") or {}
    m15_angle: Dict[datetime, Decimal] = symbol_series.get("m15_angle") or {}
    h1_angle: Dict[datetime, Decimal] = symbol_series.get("h1_angle") or {}
    m15_times: List[datetime] = symbol_series.get("m15_times") or []
    h1_times: List[datetime] = symbol_series.get("h1_times") or []

    a_m5 = m5_angle.get(ts_m5)
    if a_m5 is None:
        return None

    open_time_m15 = resolve_open_time_from_series(m15_times, decision_time, TF_STEP_MINUTES["m15"])
    open_time_h1 = resolve_open_time_from_series(h1_times, decision_time, TF_STEP_MINUTES["h1"])
    if open_time_m15 is None or open_time_h1 is None:
        return None

    a_m15 = m15_angle.get(open_time_m15)
    a_h1 = h1_angle.get(open_time_h1)
    if a_m15 is None or a_h1 is None:
        return None

    z_h1 = _angle_to_zone(a_h1)
    z_m15 = _angle_to_zone(a_m15)
    if z_h1 is None or z_m15 is None:
        return None

    h1_idx = ZONE_TO_BIN_IDX.get(z_h1)
    m15_idx = ZONE_TO_BIN_IDX.get(z_m15)
    if h1_idx is None or m15_idx is None:
        return None

    bins_h1: List[Dict[str, Any]] = plugin_ctx.get("bins_h1") or []
    bins_m15: List[Dict[str, Any]] = plugin_ctx.get("bins_m15") or []

    h1_bin = _assign_bin(bins_h1, Decimal(int(h1_idx)))
    m15_bin = _assign_bin(bins_m15, Decimal(int(m15_idx)))
    if not h1_bin or not m15_bin:
        return None

    h1_bin_s = str(h1_bin)
    m15_bin_s = str(m15_bin)

    quantiles_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = plugin_ctx.get("quantiles_by_pair") or {}
    q_rows = quantiles_by_pair.get((h1_bin_s, m15_bin_s)) or []

    # если квантилей нет — схлопывание M5_0
    if not q_rows:
        return f"{h1_bin_s}|{m15_bin_s}|M5_0"

    # sort_key как в анализаторе: для short инверсия
    a_m5_q6 = _q6(a_m5)
    sort_key = _q6(-a_m5_q6) if direction == "short" else a_m5_q6

    q_bin = _pick_quantile_bin(q_rows, sort_key)
    if not q_bin:
        return None

    return str(q_bin)