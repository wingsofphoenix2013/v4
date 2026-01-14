# emastate.py — плагин биннинга для анализатора EMASTATE MTF: расчёт bin_name (H1/M15/M5 состояния EMA vs close + tail min_share) для кандидатов сигнала

import logging
import bisect
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_PLUGIN_EMASTATE_MTF")

# 🔸 Дефолтные параметры (как в bt_analysis_emastate_mtf.py)
DEFAULT_N_BARS = 12
DEFAULT_EPS_K = Decimal("0.0001")          # 0.01% от цены
DEFAULT_MIN_SHARE = Decimal("0.01")

# 🔸 Таблицы
BT_ANALYSIS_PARAMS_TABLE = "bt_analysis_parameters"
BT_RUNS_TABLE = "bt_signal_backfill_runs"
BT_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_EVENTS_TABLE = "bt_signals_values"
BT_INDICATOR_VALUES_TABLE = "indicator_values_v4"

# 🔸 OHLCV таблицы по TF (таблицы аналогичны, меняется только имя)
OHLCV_TABLES_BY_TF: Dict[str, str] = {
    "m5": "ohlcv_bb_m5",
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}

# 🔸 Шаг времени TF
TF_STEP: Dict[str, timedelta] = {
    "m5": timedelta(minutes=5),
    "m15": timedelta(minutes=15),
    "h1": timedelta(hours=1),
}

# 🔸 Каталог соответствий состояния -> bin_idx (0..6)
STATE_TO_BIN_IDX: Dict[str, int] = {
    "ABOVE_AWAY": 0,
    "ABOVE_STAG": 1,
    "ABOVE_APPROACH": 2,
    "NEAR": 3,
    "BELOW_APPROACH": 4,
    "BELOW_STAG": 5,
    "BELOW_AWAY": 6,
}

# 🔸 Кеш параметров анализаторов: analysis_id -> {param_name: param_value}
_analysis_params_cache: Dict[int, Dict[str, str]] = {}

# 🔸 Кеш окна run: run_id -> {"from_time":..., "to_time":...}
_run_window_cache: Dict[int, Dict[str, Any]] = {}


# 🔸 Безопасный Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Абсолютное значение Decimal
def _abs(x: Decimal) -> Decimal:
    return x if x >= 0 else -x


# 🔸 Наклон линейной регрессии y(t) по t=0..n-1 (в единицах y на бар)
def _linreg_slope(values: List[Decimal]) -> Decimal:
    n = len(values)
    if n <= 1:
        return Decimal("0")

    xs: List[Decimal] = [Decimal(i) for i in range(n)]
    mean_x = sum(xs) / Decimal(n)
    mean_y = sum(values) / Decimal(n)

    cov = Decimal("0")
    var = Decimal("0")

    for i in range(n):
        dx = xs[i] - mean_x
        dy = values[i] - mean_y
        cov += dx * dy
        var += dx * dx

    if var == 0:
        return Decimal("0")

    return cov / var


# 🔸 Алиасинг open_time по списку свечей: last open_time <= decision_time - tf_delta
def resolve_open_time_from_series(
    open_times: List[datetime],
    decision_time: datetime,
    tf_delta: timedelta,
) -> Optional[datetime]:
    if not open_times:
        return None

    upper_bound = decision_time - tf_delta
    idx = bisect.bisect_right(open_times, upper_bound) - 1
    if idx < 0:
        return None
    return open_times[idx]


# 🔸 Загрузка параметров анализатора (instance_id_* / param_name / n_bars / eps_k / min_share)
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


# 🔸 Загрузка окна parent-run (from_time/to_time)
async def load_run_window(pg, run_id: int) -> Optional[Dict[str, Any]]:
    rid = int(run_id)
    cached = _run_window_cache.get(rid)
    if cached is not None:
        return cached

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT from_time, to_time
            FROM {BT_RUNS_TABLE}
            WHERE id = $1
            """,
            rid,
        )

    if not row:
        return None

    out = {"from_time": row["from_time"], "to_time": row["to_time"]}
    _run_window_cache[rid] = out
    return out


# 🔸 Загрузка кандидатов parent membership -> events (для заданного run_id/parent_signal_id/direction)
async def load_parent_candidates(
    pg,
    run_id: int,
    parent_signal_id: int,
    direction: str,
    window_from: datetime,
    window_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                e.id            AS signal_value_id,
                e.symbol        AS symbol,
                e.open_time     AS open_time,
                e.decision_time AS decision_time,
                e.direction     AS direction
            FROM {BT_MEMBERSHIP_TABLE} m
            JOIN {BT_EVENTS_TABLE} e
              ON e.id = m.signal_value_id
            WHERE m.run_id = $1
              AND m.signal_id = $2
              AND e.timeframe = 'm5'
              AND e.direction = $3
              AND e.open_time BETWEEN $4 AND $5
            ORDER BY e.symbol, e.open_time
            """,
            int(run_id),
            int(parent_signal_id),
            str(direction).strip().lower(),
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        # условия достаточности
        if r["decision_time"] is None or r["open_time"] is None:
            continue
        out.append(
            {
                "signal_value_id": int(r["signal_value_id"]),
                "symbol": str(r["symbol"] or "").strip(),
                "open_time": r["open_time"],
                "decision_time": r["decision_time"],
                "direction": str(r["direction"] or "").strip().lower(),
            }
        )
    return out


# 🔸 Загрузка open_time массива из OHLCV для алиасинга (m15/h1)
async def load_ohlcv_open_times(
    pg,
    symbol: str,
    tf: str,
    from_time: datetime,
    to_time: datetime,
) -> List[datetime]:
    table = OHLCV_TABLES_BY_TF.get(str(tf).strip().lower())
    if not table:
        return []

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
            from_time,
            to_time,
        )

    return [r["open_time"] for r in rows if r and r["open_time"] is not None]


# 🔸 Prefetch: загрузка close для symbol+tf в диапазоне времени
async def prefetch_close_map(
    pg,
    tf: str,
    symbol: str,
    time_from: datetime,
    time_to: datetime,
) -> Dict[datetime, Decimal]:
    table = OHLCV_TABLES_BY_TF.get(str(tf).strip().lower())
    if not table:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, "close"
            FROM {table}
            WHERE symbol = $1
              AND open_time BETWEEN $2 AND $3
            ORDER BY open_time
            """,
            str(symbol),
            time_from,
            time_to,
        )

    out: Dict[datetime, Decimal] = {}
    for r in rows:
        out[r["open_time"]] = _safe_decimal(r["close"])
    return out


# 🔸 Prefetch: загрузка EMA для instance_id+symbol+param_name в диапазоне времени
async def prefetch_ema_map(
    pg,
    instance_id: int,
    symbol: str,
    param_name: str,
    time_from: datetime,
    time_to: datetime,
) -> Dict[datetime, Decimal]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, value
            FROM {BT_INDICATOR_VALUES_TABLE}
            WHERE instance_id = $1
              AND symbol      = $2
              AND param_name  = $3
              AND open_time BETWEEN $4 AND $5
            ORDER BY open_time
            """,
            int(instance_id),
            str(symbol),
            str(param_name),
            time_from,
            time_to,
        )

    out: Dict[datetime, Decimal] = {}
    for r in rows:
        out[r["open_time"]] = _safe_decimal(r["value"])
    return out


# 🔸 Расчёт состояния на одном TF по заранее загруженным series map'ам (EMA/close)
def calc_tf_state_from_maps(
    tf: str,
    anchor_open_time: datetime,
    ema_map: Dict[datetime, Decimal],
    close_map: Dict[datetime, Decimal],
    n_bars: int,
    eps_k: Decimal,
) -> Optional[str]:
    step = TF_STEP.get(str(tf).strip().lower())
    if step is None:
        return None

    # собираем список времен последних N баров включая anchor
    times: List[datetime] = []
    for i in range(int(n_bars) - 1, -1, -1):
        times.append(anchor_open_time - (step * i))

    # условия достаточности: все времена должны присутствовать и в EMA, и в close
    ema_series: List[Decimal] = []
    close_series: List[Decimal] = []

    for t in times:
        e = ema_map.get(t)
        c = close_map.get(t)
        if e is None or c is None:
            return None
        ema_series.append(e)
        close_series.append(c)

    ema_last = ema_series[-1]
    close_last = close_series[-1]

    d_last = ema_last - close_last
    a_last = _abs(d_last)

    eps = _safe_decimal(close_last) * _safe_decimal(eps_k)

    # near имеет приоритет над всем
    if a_last <= eps:
        return "NEAR"

    # ряд расстояний |ema-close| по окну
    a_series: List[Decimal] = []
    for i in range(len(ema_series)):
        a_series.append(_abs(ema_series[i] - close_series[i]))

    slope = _linreg_slope(a_series)

    # порог значимости движения (как в анализаторе): thr = eps / N
    thr = eps / Decimal(max(int(n_bars), 1))

    # классификация тренда расстояния
    if slope <= -thr:
        motion = "APPROACH"
    elif slope >= thr:
        motion = "AWAY"
    else:
        motion = "STAG"

    # над/под ценой определяется по последней точке окна
    if d_last > 0:
        return f"ABOVE_{motion}"
    else:
        return f"BELOW_{motion}"


# 🔸 Вспомогательная функция: безопасное чтение int из dict[str,str]
def _get_int_param(params: Dict[str, str], name: str, default: int) -> int:
    raw = params.get(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


# 🔸 Вспомогательная функция: безопасное чтение Decimal из dict[str,str]
def _get_decimal_param(params: Dict[str, str], name: str, default: Decimal) -> Decimal:
    raw = params.get(name)
    if raw is None:
        return default
    try:
        return Decimal(str(raw).strip())
    except Exception:
        return default


# 🔸 Вспомогательная функция: безопасное чтение str из dict[str,str]
def _get_str_param(params: Dict[str, str], name: str, default: str) -> str:
    raw = params.get(name)
    if raw is None:
        return default
    return str(raw).strip()


# 🔸 Инициализация контекста плагина под конкретный winner и пару (run/scenario/parent_signal/direction)
async def init_emastate_mtf_plugin_context(
    pg,
    run_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    analysis_id: int,
) -> Dict[str, Any]:
    params = await load_analysis_params(pg, int(analysis_id))

    instance_id_m5 = _get_int_param(params, "instance_id_m5", 0)
    instance_id_m15 = _get_int_param(params, "instance_id_m15", 0)
    instance_id_h1 = _get_int_param(params, "instance_id_h1", 0)
    param_name = _get_str_param(params, "param_name", "")

    n_bars = _get_int_param(params, "n_bars", DEFAULT_N_BARS)
    eps_k = _get_decimal_param(params, "eps_k", DEFAULT_EPS_K)
    min_share = _get_decimal_param(params, "min_share", DEFAULT_MIN_SHARE)

    # условия достаточности
    if not param_name or instance_id_m5 <= 0 or instance_id_m15 <= 0 or instance_id_h1 <= 0:
        log.info(
            "BT_SIG_PLUGIN_EMASTATE_MTF: init skip (missing params) analysis_id=%s run_id=%s scenario_id=%s parent_signal_id=%s dir=%s "
            "param_name='%s' iid_m5=%s iid_m15=%s iid_h1=%s",
            int(analysis_id),
            int(run_id),
            int(scenario_id),
            int(parent_signal_id),
            str(direction).strip().lower(),
            str(param_name),
            int(instance_id_m5),
            int(instance_id_m15),
            int(instance_id_h1),
        )
        return {}

    # окно parent-run (источник истины по границам)
    run_window = await load_run_window(pg, int(run_id))
    if not run_window:
        log.info(
            "BT_SIG_PLUGIN_EMASTATE_MTF: init skip (run window not found) analysis_id=%s run_id=%s",
            int(analysis_id),
            int(run_id),
        )
        return {}

    window_from: datetime = run_window["from_time"]
    window_to: datetime = run_window["to_time"]

    # кандидаты parent membership
    candidates = await load_parent_candidates(
        pg=pg,
        run_id=int(run_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction).strip().lower(),
        window_from=window_from,
        window_to=window_to,
    )

    # условий достаточности
    if not candidates:
        log.info(
            "BT_SIG_PLUGIN_EMASTATE_MTF: init no candidates analysis_id=%s run_id=%s parent_signal_id=%s scenario_id=%s dir=%s window=[%s..%s]",
            int(analysis_id),
            int(run_id),
            int(parent_signal_id),
            int(scenario_id),
            str(direction).strip().lower(),
            window_from,
            window_to,
        )
        return {
            "analysis_id": int(analysis_id),
            "direction": str(direction).strip().lower(),
            "param_name": str(param_name),
            "instance_id_m5": int(instance_id_m5),
            "instance_id_m15": int(instance_id_m15),
            "instance_id_h1": int(instance_id_h1),
            "n_bars": int(n_bars),
            "eps_k": eps_k,
            "min_share": min_share,
            "run_id": int(run_id),
            "scenario_id": int(scenario_id),
            "parent_signal_id": int(parent_signal_id),
            "window_from": window_from,
            "window_to": window_to,
            "bin_by_value_id": {},
        }

    # группируем по symbol
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for c in candidates:
        sym = str(c.get("symbol") or "").strip()
        if not sym:
            continue
        by_symbol.setdefault(sym, []).append(c)

    # lookback для prefetch: (N-1)*step по каждому TF + запас под алиасинг
    lookback_h1 = TF_STEP["h1"] * max(int(n_bars) - 1, 0)
    lookback_m15 = TF_STEP["m15"] * max(int(n_bars) - 1, 0)
    lookback_m5 = TF_STEP["m5"] * max(int(n_bars) - 1, 0)
    margin = max(lookback_h1, lookback_m15, lookback_m5) + TF_STEP["h1"]

    total_candidates = len(candidates)
    used_candidates = 0
    skipped_candidates = 0
    symbols_total = len(by_symbol)
    symbols_processed = 0
    symbols_skipped = 0

    # промежуточные результаты по кандидатам
    triple_by_value_id: Dict[int, Tuple[int, int, int]] = {}

    # основной проход: для каждого symbol считаем индексы состояний на H1/M15/M5
    for symbol, plist in by_symbol.items():
        if not plist:
            continue

        # условия достаточности
        if symbol == "":
            symbols_skipped += 1
            continue

        symbols_processed += 1

        # окно для загрузки open_times m15/h1 под алиасинг
        from_ext = window_from - margin
        to_ext = window_to

        try:
            m15_open_times = await load_ohlcv_open_times(pg, symbol, "m15", from_ext, to_ext)
            h1_open_times = await load_ohlcv_open_times(pg, symbol, "h1", from_ext, to_ext)
        except Exception:
            symbols_skipped += 1
            continue

        # собираем якоря для m15/h1 на основе decision_time (без заглядывания вперёд)
        anchors_m15: List[datetime] = []
        anchors_h1: List[datetime] = []
        anchors_m5: List[datetime] = []

        for c in plist:
            ts_m5 = c.get("open_time")
            dt = c.get("decision_time")
            if not isinstance(ts_m5, datetime) or not isinstance(dt, datetime):
                continue

            anchor_m15 = resolve_open_time_from_series(m15_open_times, dt, TF_STEP["m15"])
            anchor_h1 = resolve_open_time_from_series(h1_open_times, dt, TF_STEP["h1"])

            if anchor_m15 is None or anchor_h1 is None:
                continue

            anchors_m5.append(ts_m5)
            anchors_m15.append(anchor_m15)
            anchors_h1.append(anchor_h1)

        # условий достаточности
        if not anchors_m5 or not anchors_m15 or not anchors_h1:
            symbols_skipped += 1
            continue

        # диапазоны prefetch по каждому TF: min_anchor - lookback .. max_anchor
        t_from_m5 = min(anchors_m5) - lookback_m5
        t_to_m5 = max(anchors_m5)

        t_from_m15 = min(anchors_m15) - lookback_m15
        t_to_m15 = max(anchors_m15)

        t_from_h1 = min(anchors_h1) - lookback_h1
        t_to_h1 = max(anchors_h1)

        try:
            closes_m5 = await prefetch_close_map(pg, "m5", symbol, t_from_m5, t_to_m5)
            closes_m15 = await prefetch_close_map(pg, "m15", symbol, t_from_m15, t_to_m15)
            closes_h1 = await prefetch_close_map(pg, "h1", symbol, t_from_h1, t_to_h1)

            emas_m5 = await prefetch_ema_map(pg, int(instance_id_m5), symbol, param_name, t_from_m5, t_to_m5)
            emas_m15 = await prefetch_ema_map(pg, int(instance_id_m15), symbol, param_name, t_from_m15, t_to_m15)
            emas_h1 = await prefetch_ema_map(pg, int(instance_id_h1), symbol, param_name, t_from_h1, t_to_h1)
        except Exception as e:
            symbols_skipped += 1
            log.debug(
                "BT_SIG_PLUGIN_EMASTATE_MTF: prefetch failed symbol=%s analysis_id=%s run_id=%s err=%s",
                symbol,
                int(analysis_id),
                int(run_id),
                e,
            )
            continue

        # вычисляем triple для каждого кандидата данного symbol
        for c in plist:
            value_id = int(c.get("signal_value_id") or 0)
            ts_m5 = c.get("open_time")
            dt = c.get("decision_time")
            dir_c = str(c.get("direction") or "").strip().lower()

            # условия достаточности
            if value_id <= 0 or dir_c != str(direction).strip().lower():
                skipped_candidates += 1
                continue
            if not isinstance(ts_m5, datetime) or not isinstance(dt, datetime):
                skipped_candidates += 1
                continue

            anchor_m15 = resolve_open_time_from_series(m15_open_times, dt, TF_STEP["m15"])
            anchor_h1 = resolve_open_time_from_series(h1_open_times, dt, TF_STEP["h1"])
            anchor_m5 = ts_m5

            if anchor_m15 is None or anchor_h1 is None:
                skipped_candidates += 1
                continue

            h1_state = calc_tf_state_from_maps(
                tf="h1",
                anchor_open_time=anchor_h1,
                ema_map=emas_h1,
                close_map=closes_h1,
                n_bars=int(n_bars),
                eps_k=eps_k,
            )
            m15_state = calc_tf_state_from_maps(
                tf="m15",
                anchor_open_time=anchor_m15,
                ema_map=emas_m15,
                close_map=closes_m15,
                n_bars=int(n_bars),
                eps_k=eps_k,
            )
            m5_state = calc_tf_state_from_maps(
                tf="m5",
                anchor_open_time=anchor_m5,
                ema_map=emas_m5,
                close_map=closes_m5,
                n_bars=int(n_bars),
                eps_k=eps_k,
            )

            if h1_state is None or m15_state is None or m5_state is None:
                skipped_candidates += 1
                continue

            h1_idx = STATE_TO_BIN_IDX.get(h1_state)
            m15_idx = STATE_TO_BIN_IDX.get(m15_state)
            m5_idx = STATE_TO_BIN_IDX.get(m5_state)

            if h1_idx is None or m15_idx is None or m5_idx is None:
                skipped_candidates += 1
                continue

            triple_by_value_id[value_id] = (int(h1_idx), int(m15_idx), int(m5_idx))
            used_candidates += 1

    # если ничего не смогли посчитать — возвращаем пустой контекст
    if not triple_by_value_id:
        log.info(
            "BT_SIG_PLUGIN_EMASTATE_MTF: init computed none analysis_id=%s run_id=%s parent_signal_id=%s scenario_id=%s dir=%s "
            "candidates_total=%s used=%s skipped=%s symbols_total=%s processed=%s skipped_symbols=%s",
            int(analysis_id),
            int(run_id),
            int(parent_signal_id),
            int(scenario_id),
            str(direction).strip().lower(),
            int(total_candidates),
            int(used_candidates),
            int(skipped_candidates),
            int(symbols_total),
            int(symbols_processed),
            int(symbols_skipped),
        )
        return {}

    # считаем доли (tail) как в анализаторе: доля от total_used (по текущему direction)
    total_used = Decimal(len(triple_by_value_id))
    by_h1_count: Dict[int, int] = {}
    by_pair_count: Dict[Tuple[int, int], int] = {}

    for _, (h1_idx, m15_idx, _m5_idx) in triple_by_value_id.items():
        by_h1_count[h1_idx] = by_h1_count.get(h1_idx, 0) + 1
        by_pair_count[(h1_idx, m15_idx)] = by_pair_count.get((h1_idx, m15_idx), 0) + 1

    # предварительно отмечаем tail группы
    tail_h1: Set[int] = set()
    tail_pair: Set[Tuple[int, int]] = set()

    for h1_idx, cnt in by_h1_count.items():
        share_h1 = (Decimal(cnt) / total_used) if total_used > 0 else Decimal(0)
        if share_h1 < min_share:
            tail_h1.add(int(h1_idx))

    for (h1_idx, m15_idx), cnt in by_pair_count.items():
        share_pair = (Decimal(cnt) / total_used) if total_used > 0 else Decimal(0)
        if share_pair < min_share:
            tail_pair.add((int(h1_idx), int(m15_idx)))

    # финальная таблица биннинга: signal_value_id -> bin_name
    bin_by_value_id: Dict[int, str] = {}

    tail_h1_rows = 0
    tail_m15_rows = 0
    full_rows = 0

    for value_id, (h1_idx, m15_idx, m5_idx) in triple_by_value_id.items():
        if h1_idx in tail_h1:
            bin_name = f"H1_bin_{h1_idx}|M15_0|M5_0"
            tail_h1_rows += 1
        elif (h1_idx, m15_idx) in tail_pair:
            bin_name = f"H1_bin_{h1_idx}|M15_bin_{m15_idx}|M5_0"
            tail_m15_rows += 1
        else:
            bin_name = f"H1_bin_{h1_idx}|M15_bin_{m15_idx}|M5_bin_{m5_idx}"
            full_rows += 1

        bin_by_value_id[int(value_id)] = str(bin_name)

    log.info(
        "BT_SIG_PLUGIN_EMASTATE_MTF: init done analysis_id=%s run_id=%s scenario_id=%s parent_signal_id=%s dir=%s "
        "param_name=%s n_bars=%s eps_k=%s min_share=%s candidates_total=%s used=%s skipped=%s "
        "symbols_total=%s processed=%s skipped_symbols=%s h1_groups=%s pair_groups=%s rows=%s (tail_h1=%s tail_m15=%s full=%s)",
        int(analysis_id),
        int(run_id),
        int(scenario_id),
        int(parent_signal_id),
        str(direction).strip().lower(),
        str(param_name),
        int(n_bars),
        str(eps_k),
        str(min_share),
        int(total_candidates),
        int(len(triple_by_value_id)),
        int(skipped_candidates),
        int(symbols_total),
        int(symbols_processed),
        int(symbols_skipped),
        int(len(by_h1_count)),
        int(len(by_pair_count)),
        int(len(bin_by_value_id)),
        int(tail_h1_rows),
        int(tail_m15_rows),
        int(full_rows),
    )

    return {
        "analysis_id": int(analysis_id),
        "direction": str(direction).strip().lower(),
        "param_name": str(param_name),
        "instance_id_m5": int(instance_id_m5),
        "instance_id_m15": int(instance_id_m15),
        "instance_id_h1": int(instance_id_h1),
        "n_bars": int(n_bars),
        "eps_k": eps_k,
        "min_share": min_share,
        "run_id": int(run_id),
        "scenario_id": int(scenario_id),
        "parent_signal_id": int(parent_signal_id),
        "window_from": window_from,
        "window_to": window_to,
        "bin_by_value_id": bin_by_value_id,
    }


# 🔸 Подготовка серий для одного символа (для совместимости с интерфейсом level2; биннинг уже рассчитан в init)
async def prepare_symbol_series(
    pg,
    plugin_ctx: Dict[str, Any],
    symbol: str,
    window_from: datetime,
    window_to: datetime,
) -> Dict[str, Any]:
    # условий достаточности
    if not plugin_ctx or not plugin_ctx.get("bin_by_value_id"):
        return {}
    return {"symbol": str(symbol)}


# 🔸 Основная функция плагина: вычислить bin_name для кандидата
def compute_emastate_mtf_bin_name(
    plugin_ctx: Dict[str, Any],
    symbol_series: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Optional[str]:
    # условия достаточности
    if not plugin_ctx:
        return None

    direction = str(candidate.get("direction") or "").strip().lower()
    if not direction:
        return None

    if direction != str(plugin_ctx.get("direction") or "").strip().lower():
        return None

    value_id = candidate.get("signal_value_id")
    try:
        vid = int(value_id)
    except Exception:
        vid = 0

    if vid <= 0:
        return None

    bin_map: Dict[int, str] = plugin_ctx.get("bin_by_value_id") or {}
    bn = bin_map.get(int(vid))
    if not bn:
        return None

    return str(bn)