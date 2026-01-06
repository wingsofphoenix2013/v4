# bt_signals_lr_universal_level2.py — stream-backfill воркер уровня 2: после postproc_ready_v2 применяет winner bins (v2) к сигналам (generate/refilter) с помощью плагинов (lr_mtf / lr_angle_mtf)

import asyncio
import logging
import json
import uuid
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Set, Tuple, List, Callable

# 🔸 Path bootstrap для signals_plugins (директория в корне проекта)
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# 🔸 Плагины биннинга (универсальные по инстансам анализаторов)
from signals_plugins.lr_mtf import (
    init_lr_mtf_plugin_context,
    prepare_symbol_series as prepare_lr_mtf_symbol_series,
    compute_lr_mtf_bin_name,
)
from signals_plugins.lr_angle_mtf import (
    init_lr_angle_mtf_plugin_context,
    prepare_symbol_series as prepare_lr_angle_mtf_symbol_series,
    compute_lr_angle_mtf_bin_name,
)
from signals_plugins.bb_mtf import (
    init_bb_mtf_plugin_context,
    prepare_symbol_series as prepare_bb_mtf_symbol_series,
    compute_bb_mtf_bin_name,
)

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_all_ticker_symbols,
    get_ticker_info,
    get_signal_instance,
    get_analysis_instance,
)

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI_L2")

# 🔸 Стримы
BT_POSTPROC_READY_STREAM_V2 = "bt:analysis:postproc_ready_v2"
BT_SIGNALS_READY_STREAM = "bt:signals:ready"

# 🔸 Таблицы
BT_LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"
BT_SIGNALS_VALUES_TABLE = "bt_signals_values"
BT_BACKFILL_RUNS_TABLE = "bt_signal_backfill_runs"
BT_INDICATOR_VALUES_TABLE = "indicator_values_v4"
BT_OHLCV_M5_TABLE = "ohlcv_bb_m5"

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
}

# 🔸 Ограничение параллелизма по тикерам
SYMBOL_MAX_CONCURRENCY = 5


# 🔸 Парсер сообщения bt:analysis:postproc_ready_v2
def _parse_postproc_ready_v2(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id = int(str(fields.get("scenario_id") or "").strip())
        signal_id = int(str(fields.get("signal_id") or "").strip())
        run_id = int(str(fields.get("run_id") or "").strip())

        winner_analysis_id = int(str(fields.get("winner_analysis_id") or "0").strip() or 0)
        winner_param = str(fields.get("winner_param") or "").strip()
        score_version = str(fields.get("score_version") or "v1").strip()

        finished_at_raw = str(fields.get("finished_at") or "").strip()
        finished_at = datetime.fromisoformat(finished_at_raw) if finished_at_raw else None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "winner_analysis_id": winner_analysis_id,
            "winner_param": winner_param,
            "score_version": score_version,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка окна run из bt_signal_backfill_runs
async def _load_run_info(pg, run_id: int) -> Optional[Dict[str, Any]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT id, signal_id, from_time, to_time, finished_at, status
            FROM {BT_BACKFILL_RUNS_TABLE}
            WHERE id = $1
            """,
            int(run_id),
        )
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "signal_id": int(row["signal_id"]),
        "from_time": row["from_time"],
        "to_time": row["to_time"],
        "finished_at": row["finished_at"],
        "status": row["status"],
    }


# 🔸 Загрузка whitelist good bins из bt_analysis_bins_labels_v2
async def _load_good_bins_v2(
    pg,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    score_version: str,
    analysis_id: int,
) -> Tuple[Set[str], Set[str]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT bin_name, timeframe
            FROM {BT_LABELS_V2_TABLE}
            WHERE scenario_id   = $1
              AND signal_id     = $2
              AND direction     = $3
              AND score_version = $4
              AND analysis_id   = $5
              AND state         = 'good'
            """,
            int(scenario_id),
            int(parent_signal_id),
            str(direction),
            str(score_version),
            int(analysis_id),
        )

    bins: Set[str] = set()
    tfs: Set[str] = set()

    for r in rows:
        bn = r["bin_name"]
        tf = r["timeframe"]
        if bn is not None:
            bins.add(str(bn))
        if tf is not None:
            tfs.add(str(tf))

    return bins, tfs


# 🔸 Загрузка уже существующих событий целевого сигнала в окне (идемпотентность)
async def _load_existing_events(
    pg,
    signal_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
    direction: str,
) -> Set[Tuple[str, datetime, str]]:
    existing: Set[Tuple[str, datetime, str]] = set()
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT symbol, open_time, direction
            FROM {BT_SIGNALS_VALUES_TABLE}
            WHERE signal_id = $1
              AND timeframe = $2
              AND direction = $3
              AND open_time BETWEEN $4 AND $5
            """,
            int(signal_id),
            str(timeframe),
            str(direction),
            from_time,
            to_time,
        )
    for r in rows:
        existing.add((str(r["symbol"]), r["open_time"], str(r["direction"])))
    return existing


# 🔸 Загрузка parent событий из bt_signals_values в окне (для режима refilter)
async def _load_parent_events_for_symbol(
    pg,
    parent_signal_id: int,
    symbol: str,
    timeframe: str,
    direction: str,
    window_from: datetime,
    window_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, decision_time, raw_message
            FROM {BT_SIGNALS_VALUES_TABLE}
            WHERE signal_id = $1
              AND symbol = $2
              AND timeframe = $3
              AND direction = $4
              AND open_time BETWEEN $5 AND $6
            ORDER BY open_time
            """,
            int(parent_signal_id),
            str(symbol),
            str(timeframe),
            str(direction),
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = r["raw_message"]
        # jsonb обычно приходит dict, но защищаемся
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None

        if not isinstance(raw, dict):
            raw = {}

        # цена для кандидата — та же, что использовали в предыдущем слое
        price = raw.get("price")
        if price is None:
            # условия достаточности
            continue

        out.append(
            {
                "symbol": str(symbol),
                "open_time": r["open_time"],
                "decision_time": r["decision_time"] or None,
                "direction": str(direction),
                "price": price,
                "raw_message": raw,
            }
        )

    return out


# 🔸 Загрузка LR-серии (angle/upper/lower/center) для bounce-инстанса (только generate)
async def _load_lr_series_for_bounce(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Dict[str, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, param_name, value
            FROM {BT_INDICATOR_VALUES_TABLE}
            WHERE instance_id = $1
              AND symbol      = $2
              AND open_time  BETWEEN $3 AND $4
            ORDER BY open_time
            """,
            int(instance_id),
            str(symbol),
            from_time,
            to_time,
        )

    series: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        ts = r["open_time"]
        pname = str(r["param_name"] or "")
        val = r["value"]

        entry = series.setdefault(ts, {})

        pname_l = pname.lower()
        try:
            fval = float(val)
        except Exception:
            continue

        if pname_l.endswith("_angle"):
            entry["angle"] = fval
        elif pname_l.endswith("_upper"):
            entry["upper"] = fval
        elif pname_l.endswith("_lower"):
            entry["lower"] = fval
        elif pname_l.endswith("_center"):
            entry["center"] = fval

    return series


# 🔸 Загрузка OHLCV m5 в окне (только generate)
async def _load_ohlcv_m5(
    pg,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Tuple[float, float, float, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, open, high, low, close
            FROM {BT_OHLCV_M5_TABLE}
            WHERE symbol = $1
              AND open_time BETWEEN $2 AND $3
            ORDER BY open_time
            """,
            str(symbol),
            from_time,
            to_time,
        )

    series: Dict[datetime, Tuple[float, float, float, float]] = {}
    for r in rows:
        try:
            series[r["open_time"]] = (
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
            )
        except Exception:
            continue
    return series


# 🔸 Поиск bounce-кандидатов (generate): логика как в bt_signals_lr_universal.py
def _find_lr_bounce_candidates(
    symbol: str,
    direction: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    precision_price: int,
    lr_series: Dict[datetime, Dict[str, float]],
    ohlcv: Dict[datetime, Tuple[float, float, float, float]],
) -> List[Dict[str, Any]]:
    # условия достаточности
    if not lr_series or not ohlcv:
        return []

    times = sorted(set(lr_series.keys()) & set(ohlcv.keys()))
    if len(times) < 2:
        return []

    tf_delta = timedelta(minutes=TF_STEP_MINUTES["m5"])
    out: List[Dict[str, Any]] = []

    for i in range(1, len(times)):
        prev_ts = times[i - 1]
        ts = times[i]

        lr_prev = lr_series.get(prev_ts)
        lr_curr = lr_series.get(ts)
        if not lr_prev or not lr_curr:
            continue

        ohlcv_prev = ohlcv.get(prev_ts)
        ohlcv_curr = ohlcv.get(ts)
        if not ohlcv_prev or not ohlcv_curr:
            continue

        close_prev = ohlcv_prev[3]
        close_curr = ohlcv_curr[3]
        if close_curr is None or close_curr == 0:
            continue

        angle_m5 = lr_curr.get("angle")
        upper_curr = lr_curr.get("upper")
        lower_curr = lr_curr.get("lower")
        upper_prev = lr_prev.get("upper")
        lower_prev = lr_prev.get("lower")
        center_curr = lr_curr.get("center")

        if (
            angle_m5 is None
            or upper_curr is None
            or lower_curr is None
            or upper_prev is None
            or lower_prev is None
        ):
            continue

        # если keep_half включён, но нет center_curr — пропускаем
        if keep_half and center_curr is None:
            continue

        try:
            angle_f = float(angle_m5)
            upper_prev_f = float(upper_prev)
            lower_prev_f = float(lower_prev)
            close_prev_f = float(close_prev)
            close_curr_f = float(close_curr)
            center_curr_f = float(center_curr) if center_curr is not None else 0.0
        except Exception:
            continue

        H = upper_prev_f - lower_prev_f
        if H <= 0:
            continue

        # условия по тренду
        if trend_type == "trend":
            dir_ok = (direction == "long" and angle_f > 0.0) or (direction == "short" and angle_f < 0.0)
        elif trend_type == "counter":
            dir_ok = (direction == "long" and angle_f < 0.0) or (direction == "short" and angle_f > 0.0)
        else:
            dir_ok = True

        if not dir_ok:
            continue

        matched = False

        if direction == "long":
            if zone_k == 0.0:
                in_zone_prev = close_prev_f <= lower_prev_f
            else:
                threshold = lower_prev_f + (float(zone_k) * H)
                in_zone_prev = close_prev_f <= threshold

            if in_zone_prev and close_curr_f > lower_prev_f:
                if keep_half and not (close_curr_f <= center_curr_f):
                    continue
                matched = True
        else:
            if zone_k == 0.0:
                in_zone_prev = close_prev_f >= upper_prev_f
            else:
                threshold = upper_prev_f - (float(zone_k) * H)
                in_zone_prev = close_prev_f >= threshold

            if in_zone_prev and close_curr_f < upper_prev_f:
                if keep_half and not (close_curr_f >= center_curr_f):
                    continue
                matched = True

        if not matched:
            continue

        # цена как в lr_universal: close_curr
        try:
            price_rounded = float(f"{close_curr_f:.{precision_price}f}")
        except Exception:
            price_rounded = close_curr_f

        decision_time = ts + tf_delta

        out.append(
            {
                "symbol": symbol,
                "open_time": ts,
                "decision_time": decision_time,
                "direction": direction,
                "price": price_rounded,
                # для удобства отладки
                "angle_m5": angle_f,
                "upper_prev": upper_prev_f,
                "lower_prev": lower_prev_f,
            }
        )

    return out


# 🔸 Публичная точка входа: stream-backfill сигнал уровня 2
async def run_lr_universal_level2_stream_backfill(
    signal: Dict[str, Any],
    msg_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    signal_id = int(signal.get("id") or 0)
    signal_key = str(signal.get("key") or "").strip()
    name = signal.get("name")
    timeframe = str(signal.get("timeframe") or "").strip().lower()
    params = signal.get("params") or {}

    # условия достаточности
    if signal_id <= 0 or timeframe != "m5":
        return

    # сообщение должно приходить из bt:analysis:postproc_ready_v2
    stream_key = str((msg_ctx or {}).get("stream_key") or "")
    fields = (msg_ctx or {}).get("fields") or {}
    if stream_key != BT_POSTPROC_READY_STREAM_V2:
        return

    evt = _parse_postproc_ready_v2(fields)
    if not evt:
        return

    msg_scenario_id = evt["scenario_id"]
    msg_parent_signal_id = evt["signal_id"]
    run_id = evt["run_id"]
    winner_analysis_id = evt["winner_analysis_id"]
    winner_param = evt.get("winner_param") or ""
    score_version = evt.get("score_version") or "v1"

    # parent_signal_id / parent_scenario_id из параметров инстанса
    parent_sig_cfg = params.get("parent_signal_id")
    parent_sc_cfg = params.get("parent_scenario_id")
    dir_cfg = params.get("direction_mask")

    # режим пайплайна: generate|refilter
    pipeline_mode_cfg = params.get("pipeline_mode")
    pipeline_mode = str((pipeline_mode_cfg or {}).get("value") or "generate").strip().lower()

    try:
        configured_parent_signal_id = int((parent_sig_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_signal_id = 0

    try:
        configured_parent_scenario_id = int((parent_sc_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_scenario_id = 0

    direction = str((dir_cfg or {}).get("value") or "").strip().lower()

    # условия достаточности
    if configured_parent_signal_id <= 0 or configured_parent_scenario_id <= 0:
        return
    if direction not in ("long", "short"):
        return

    if pipeline_mode not in ("generate", "refilter"):
        # если настройка кривая — считаем generate
        pipeline_mode = "generate"

    # сообщение должно относиться к нашей связке
    if msg_parent_signal_id != configured_parent_signal_id or msg_scenario_id != configured_parent_scenario_id:
        return

    parent_signal_id = configured_parent_signal_id
    scenario_id = configured_parent_scenario_id

    # окно run — источник истины
    run_info = await _load_run_info(pg, run_id)
    if not run_info:
        log.warning(
            "BT_SIG_LR_UNI_L2: run not found — level2_signal_id=%s parent_signal_id=%s parent_scenario_id=%s run_id=%s",
            signal_id,
            parent_signal_id,
            scenario_id,
            run_id,
        )
        return

    # sanity: run должен принадлежать parent_signal_id только для generate
    if pipeline_mode == "generate":
        if int(run_info.get("signal_id") or 0) != int(parent_signal_id):
            log.warning(
                "BT_SIG_LR_UNI_L2: run belongs to another signal — run_id=%s run.signal_id=%s expected parent_signal_id=%s",
                run_id,
                run_info.get("signal_id"),
                parent_signal_id,
            )
            return
    else:
        # refilter: run_id может принадлежать исходному timer-сигналу (7/8), это нормально
        if int(run_info.get("signal_id") or 0) != int(parent_signal_id):
            log.debug(
                "BT_SIG_LR_UNI_L2: refilter uses parent run_id — run_id=%s run.signal_id=%s parent_signal_id=%s",
                run_id,
                run_info.get("signal_id"),
                parent_signal_id,
            )

    window_from: datetime = run_info["from_time"]
    window_to: datetime = run_info["to_time"]

    # настройки bounce нужны только для generate (берём их из parent_signal)
    lr_bounce_m5_instance_id: Optional[int] = None
    parent_trend_type = "agnostic"
    parent_zone_k = 0.0
    parent_keep_half = False

    if pipeline_mode == "generate":
        parent_signal = get_signal_instance(parent_signal_id)
        if not parent_signal:
            log.warning(
                "BT_SIG_LR_UNI_L2: parent signal not found in cache — parent_signal_id=%s (level2_signal_id=%s, run_id=%s)",
                parent_signal_id,
                signal_id,
                run_id,
            )
            return

        parent_params = parent_signal.get("params") or {}

        # обязательный параметр родителя: indicator (LR m5 instance для bounce)
        try:
            lr_cfg = parent_params["indicator"]
            lr_bounce_m5_instance_id = int(lr_cfg["value"])
        except Exception:
            log.warning(
                "BT_SIG_LR_UNI_L2: parent signal has no valid 'indicator' param — parent_signal_id=%s",
                parent_signal_id,
            )
            return

        parent_direction_mask = str((parent_params.get("direction_mask") or {}).get("value") or "both").strip().lower()
        parent_trend_type = str((parent_params.get("trend_type") or {}).get("value") or "agnostic").strip().lower()
        parent_keep_half = str((parent_params.get("keep_half") or {}).get("value") or "false").strip().lower() == "true"
        try:
            parent_zone_k = float(str((parent_params.get("zone_k") or {}).get("value") or "0"))
        except Exception:
            parent_zone_k = 0.0

        # моно-направленность
        if parent_direction_mask != direction:
            log.warning(
                "BT_SIG_LR_UNI_L2: mismatch direction with parent — level2_signal_id=%s dir=%s parent_direction_mask=%s parent_signal_id=%s",
                signal_id,
                direction,
                parent_direction_mask,
                parent_signal_id,
            )
            return

    # winner analysis cfg (для выбора плагина)
    analysis_cfg = get_analysis_instance(int(winner_analysis_id))
    if not analysis_cfg:
        log.info(
            "BT_SIG_LR_UNI_L2: winner analysis not in cache — skip (winner_analysis_id=%s)",
            winner_analysis_id,
        )
        return

    family_key = str(analysis_cfg.get("family_key") or "").strip().lower()
    analysis_key = str(analysis_cfg.get("key") or "").strip().lower()

    # выбор плагина
    plugin_name: str = ""
    init_plugin: Optional[Callable[..., Any]] = None
    prepare_series_fn: Optional[Callable[..., Any]] = None
    compute_bin_fn: Optional[Callable[..., Any]] = None

    if family_key == "lr" and analysis_key == "lr_mtf":
        plugin_name = "lr_mtf"
        init_plugin = init_lr_mtf_plugin_context
        prepare_series_fn = prepare_lr_mtf_symbol_series
        compute_bin_fn = compute_lr_mtf_bin_name

    elif family_key == "lr" and analysis_key == "lr_angle_mtf":
        plugin_name = "lr_angle_mtf"
        init_plugin = init_lr_angle_mtf_plugin_context
        prepare_series_fn = prepare_lr_angle_mtf_symbol_series
        compute_bin_fn = compute_lr_angle_mtf_bin_name

    elif family_key == "bb" and analysis_key == "bb_mtf":
        plugin_name = "bb_mtf"
        init_plugin = init_bb_mtf_plugin_context
        prepare_series_fn = prepare_bb_mtf_symbol_series
        compute_bin_fn = compute_bb_mtf_bin_name

    else:
        log.info(
            "BT_SIG_LR_UNI_L2: winner plugin not supported yet — winner_analysis_id=%s family=%s key=%s winner_param='%s'",
            winner_analysis_id,
            family_key,
            analysis_key,
            str(winner_param),
        )
        return

    # загружаем свежие good bins победителя (v2) — для пары (scenario,parent_signal,dir)
    good_bins, timeframes = await _load_good_bins_v2(
        pg=pg,
        scenario_id=scenario_id,
        parent_signal_id=parent_signal_id,
        direction=direction,
        score_version=score_version,
        analysis_id=winner_analysis_id,
    )

    # условий достаточности
    if not good_bins:
        log.info(
            "BT_SIG_LR_UNI_L2: no good bins — skip generation (level2_signal_id=%s parent_scenario_id=%s parent_signal_id=%s run_id=%s winner=%s dir=%s)",
            signal_id,
            scenario_id,
            parent_signal_id,
            run_id,
            winner_analysis_id,
            direction,
        )
        return

    # init plugin context (run-aware)
    plugin_ctx = await init_plugin(
        pg=pg,
        run_id=int(run_id),
        scenario_id=int(scenario_id),
        parent_signal_id=int(parent_signal_id),
        direction=str(direction),
        analysis_id=int(winner_analysis_id),
    )

    # список тикеров
    symbols = get_all_ticker_symbols()
    if not symbols:
        return

    # existing events для идемпотентности (для текущего signal_id)
    existing_events = await _load_existing_events(
        pg=pg,
        signal_id=int(signal_id),
        timeframe="m5",
        from_time=window_from,
        to_time=window_to,
        direction=direction,
    )

    log.info(
        "BT_SIG_LR_UNI_L2: start — level2_signal_id=%s name='%s' pipeline_mode=%s parent_scenario_id=%s parent_signal_id=%s run_id=%s "
        "winner_analysis_id=%s winner_param='%s' score_version=%s dir=%s plugin=%s window=[%s..%s] tickers=%s bins=%s timeframes=%s existing=%s",
        signal_id,
        name,
        pipeline_mode,
        scenario_id,
        parent_signal_id,
        run_id,
        winner_analysis_id,
        str(winner_param),
        score_version,
        direction,
        plugin_name,
        window_from,
        window_to,
        len(symbols),
        len(good_bins),
        sorted(timeframes),
        len(existing_events),
    )

    sema = asyncio.Semaphore(SYMBOL_MAX_CONCURRENCY)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                _process_symbol(
                    pg=pg,
                    sema=sema,
                    pipeline_mode=pipeline_mode,
                    symbol=symbol,
                    signal_id=signal_id,
                    signal_key=signal_key,
                    timeframe="m5",
                    run_id=run_id,
                    parent_signal_id=parent_signal_id,
                    scenario_id=scenario_id,
                    direction=direction,
                    # bounce settings (generate only)
                    lr_bounce_m5_instance_id=lr_bounce_m5_instance_id,
                    trend_type=parent_trend_type,
                    zone_k=parent_zone_k,
                    keep_half=parent_keep_half,
                    # window
                    window_from=window_from,
                    window_to=window_to,
                    # plugin
                    plugin_name=plugin_name,
                    plugin_ctx=plugin_ctx,
                    prepare_series_fn=prepare_series_fn,
                    compute_bin_fn=compute_bin_fn,
                    good_bins=good_bins,
                    # idempotency
                    existing_events=existing_events,
                ),
                name=f"BT_SIG_LR_UNI_L2_{pipeline_mode}_{signal_id}_{symbol}",
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    candidates_total = 0
    candidates_with_bin = 0
    candidates_good = 0
    inserted_attempted = 0
    skipped_existing = 0
    skipped_no_data = 0
    skipped_no_bin = 0
    skipped_not_good_bin = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        (
            c_total,
            c_with_bin,
            c_good,
            ins_attempted,
            s_existing,
            s_no_data,
            s_no_bin,
            s_not_good,
        ) = res

        candidates_total += c_total
        candidates_with_bin += c_with_bin
        candidates_good += c_good
        inserted_attempted += ins_attempted
        skipped_existing += s_existing
        skipped_no_data += s_no_data
        skipped_no_bin += s_no_bin
        skipped_not_good_bin += s_not_good

    log.info(
        "BT_SIG_LR_UNI_L2: summary — level2_signal_id=%s pipeline_mode=%s parent_scenario_id=%s parent_signal_id=%s run_id=%s winner=%s dir=%s "
        "plugin=%s bins=%s candidates=%s with_bin=%s good=%s insert_attempted=%s skipped_existing=%s skipped_no_data=%s skipped_no_bin=%s skipped_not_good=%s",
        signal_id,
        pipeline_mode,
        scenario_id,
        parent_signal_id,
        run_id,
        winner_analysis_id,
        direction,
        plugin_name,
        len(good_bins),
        candidates_total,
        candidates_with_bin,
        candidates_good,
        inserted_attempted,
        skipped_existing,
        skipped_no_data,
        skipped_no_bin,
        skipped_not_good_bin,
    )

    # публикуем готовность сигналов (run-aware, используем родительский run_id)
    finished_at = datetime.utcnow()
    try:
        await redis.xadd(
            BT_SIGNALS_READY_STREAM,
            {
                "signal_id": str(signal_id),
                "run_id": str(int(run_id)),
                "from_time": window_from.isoformat(),
                "to_time": window_to.isoformat(),
                "finished_at": finished_at.isoformat(),
            },
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI_L2: failed to publish bt:signals:ready — signal_id=%s run_id=%s err=%s",
            signal_id,
            run_id,
            e,
            exc_info=True,
        )


# 🔸 Обработка по символу: generate/refilter -> bin_name -> good_bins -> insert bt_signals_values
async def _process_symbol(
    pg,
    sema: asyncio.Semaphore,
    pipeline_mode: str,
    symbol: str,
    signal_id: int,
    signal_key: str,
    timeframe: str,
    run_id: int,
    parent_signal_id: int,
    scenario_id: int,
    direction: str,
    # bounce settings (generate only)
    lr_bounce_m5_instance_id: Optional[int],
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    # window
    window_from: datetime,
    window_to: datetime,
    # plugin
    plugin_name: str,
    plugin_ctx: Dict[str, Any],
    prepare_series_fn: Callable[..., Any],
    compute_bin_fn: Callable[..., Any],
    good_bins: Set[str],
    # idempotency
    existing_events: Set[Tuple[str, datetime, str]],
) -> Tuple[int, int, int, int, int, int, int, int]:
    async with sema:
        # подготовка серий плагина (на основе symbol + окна)
        symbol_series = await prepare_series_fn(
            pg=pg,
            plugin_ctx=plugin_ctx,
            symbol=str(symbol),
            window_from=window_from,
            window_to=window_to,
        )

        # условий достаточности
        if not symbol_series:
            return 0, 0, 0, 0, 0, 1, 0, 0

        candidates: List[Dict[str, Any]] = []

        # generate: ищем bounce по LR+OHLCV
        if pipeline_mode == "generate":
            if not lr_bounce_m5_instance_id:
                return 0, 0, 0, 0, 0, 1, 0, 0

            lr_series = await _load_lr_series_for_bounce(pg, int(lr_bounce_m5_instance_id), symbol, window_from, window_to)
            ohlcv = await _load_ohlcv_m5(pg, symbol, window_from, window_to)

            if not lr_series or not ohlcv:
                return 0, 0, 0, 0, 0, 1, 0, 0

            # precision цены
            ticker_info = get_ticker_info(symbol) or {}
            try:
                precision_price = int(ticker_info.get("precision_price") or 8)
            except Exception:
                precision_price = 8

            candidates = _find_lr_bounce_candidates(
                symbol=symbol,
                direction=direction,
                trend_type=trend_type,
                zone_k=float(zone_k),
                keep_half=bool(keep_half),
                precision_price=precision_price,
                lr_series=lr_series,
                ohlcv=ohlcv,
            )

        # refilter: берём события parent_signal_id и фильтруем дальше
        else:
            parent_events = await _load_parent_events_for_symbol(
                pg=pg,
                parent_signal_id=int(parent_signal_id),
                symbol=str(symbol),
                timeframe=str(timeframe),
                direction=str(direction),
                window_from=window_from,
                window_to=window_to,
            )

            # условия достаточности
            if not parent_events:
                return 0, 0, 0, 0, 0, 0, 0, 0

            # decision_time обязателен для алиасинга; если его нет — пропускаем кандидат
            for ev in parent_events:
                if not isinstance(ev.get("decision_time"), datetime):
                    continue
                candidates.append(
                    {
                        "symbol": str(symbol),
                        "open_time": ev["open_time"],
                        "decision_time": ev["decision_time"],
                        "direction": str(direction),
                        "price": ev.get("price"),
                        "raw_message": ev.get("raw_message") or {},
                    }
                )

        if not candidates:
            return 0, 0, 0, 0, 0, 0, 0, 0

        to_insert: List[Tuple[Any, ...]] = []

        with_bin = 0
        good = 0
        skipped_existing = 0
        skipped_no_bin = 0
        skipped_not_good = 0

        for cand in candidates:
            ts: datetime = cand["open_time"]
            key_event = (symbol, ts, direction)

            if key_event in existing_events:
                skipped_existing += 1
                continue

            bin_name = compute_bin_fn(
                plugin_ctx=plugin_ctx,
                symbol_series=symbol_series,
                candidate=cand,
            )

            if not bin_name:
                skipped_no_bin += 1
                continue

            with_bin += 1

            if bin_name not in good_bins:
                skipped_not_good += 1
                continue

            good += 1

            # message
            message = "LR_UNI_L2_BOUNCE_LONG" if direction == "long" else "LR_UNI_L2_BOUNCE_SHORT"

            # raw_message
            if pipeline_mode == "refilter" and isinstance(cand.get("raw_message"), dict):
                raw_message = dict(cand["raw_message"])
            else:
                raw_message = {}

            # перезаписываем/добавляем уровень и бин
            raw_message.update(
                {
                    "signal_key": signal_key,
                    "signal_id": int(signal_id),
                    "symbol": str(symbol),
                    "timeframe": "m5",
                    "open_time": ts.isoformat(),
                    "decision_time": cand["decision_time"].isoformat(),
                    "direction": direction,
                    "price": cand.get("price"),
                    "pattern": raw_message.get("pattern") or "bounce",
                    "bin_name": str(bin_name),
                    "winner_analysis_id": int(plugin_ctx.get("analysis_id") or 0),
                    "plugin": str(plugin_name),
                    "plugin_param_name": str(plugin_ctx.get("param_name") or ""),
                    "lr_prefix": str(plugin_ctx.get("lr_prefix") or ""),
                    "length": int(plugin_ctx.get("length") or 0),
                    "source": "lr_universal_level2",
                    "pipeline_mode": str(pipeline_mode),
                    "parent_signal_id": int(parent_signal_id),
                    "parent_scenario_id": int(scenario_id),
                    "parent_run_id": int(run_id),
                }
            )

            to_insert.append(
                (
                    str(uuid.uuid4()),
                    int(signal_id),
                    str(symbol),
                    "m5",
                    ts,
                    cand["decision_time"],
                    str(direction),
                    str(message),
                    json.dumps(raw_message),
                    int(run_id),  # first_backfill_run_id = parent run
                )
            )

        if not to_insert:
            return len(candidates), with_bin, good, 0, skipped_existing, 0, skipped_no_bin, skipped_not_good

        async with pg.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {BT_SIGNALS_VALUES_TABLE}
                    (signal_uuid, signal_id, symbol, timeframe, open_time, decision_time, direction, message, raw_message, first_backfill_run_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
                ON CONFLICT (signal_id, symbol, timeframe, open_time, direction) DO NOTHING
                """,
                to_insert,
            )

        return len(candidates), with_bin, good, len(to_insert), skipped_existing, 0, skipped_no_bin, skipped_not_good