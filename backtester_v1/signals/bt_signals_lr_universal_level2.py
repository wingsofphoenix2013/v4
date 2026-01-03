# bt_signals_lr_universal_level2.py — stream-backfill воркер уровня 2: после postproc_ready_v2 загружает winner bins, считает bounce-кандидатов (как lr_universal) и вызывает заглушку плагина (без записи в БД)

import asyncio
import logging
import json
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional, Set, Tuple, List

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_all_ticker_symbols,
    get_ticker_info,
    get_signal_instance,
)

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI_L2")

# 🔸 Стрим-триггер v2
BT_POSTPROC_READY_STREAM_V2 = "bt:analysis:postproc_ready_v2"

# 🔸 Таблица winner bins v2
BT_LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
}

# 🔸 Единая точность (как в анализаторах)
Q6 = Decimal("0.000001")

# 🔸 Ограничение параллелизма по тикерам
SYMBOL_MAX_CONCURRENCY = 5

# 🔸 Кеш: чтобы не спамить логами об отсутствии плагина
_warned_missing_plugin: Set[Tuple[int, str]] = set()


# 🔸 q6 квантизация (ROUND_DOWN)
def _q6(value: Any) -> Decimal:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное чтение bool-параметра
def _get_bool_param(params: Dict[str, Any], name: str, default: bool) -> bool:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw).strip().lower() == "true"


# 🔸 Вспомогательная функция: безопасное чтение float-параметра
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default


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
            """
            SELECT id, signal_id, from_time, to_time, finished_at, status
            FROM bt_signal_backfill_runs
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


# 🔸 Загрузка LR-серии (angle/upper/lower/center) для bounce-инстанса
async def _load_lr_series(
    pg,
    instance_id: int,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Dict[str, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, param_name, value
            FROM indicator_values_v4
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


# 🔸 Загрузка OHLCV m5 в окне
async def _load_ohlcv_m5(
    pg,
    symbol: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Tuple[float, float, float, float]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, open, high, low, close
            FROM ohlcv_bb_m5
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


# 🔸 Поиск bounce-кандидатов LR (логика как в bt_signals_lr_universal.py), только подсчёт
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

    H0 = 0.0  # для типа

    out: List[Dict[str, Any]] = []
    tf_delta = timedelta(minutes=TF_STEP_MINUTES["m5"])

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

        # высота канала
        H = upper_prev_f - lower_prev_f
        if H <= 0:
            continue

        # условия по тренду (как в lr_universal)
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

        # округляем цену для raw_message (как в lr_universal)
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
                "angle_m5": angle_f,
            }
        )

    return out


# 🔸 Заглушка плагина: вычисление bin_name (пока не реализовано)
def _plugin_stub_compute_bin_name(
    winner_analysis_id: int,
    winner_param: str,
    candidate: Dict[str, Any],
) -> Optional[str]:
    # пока всегда None — "плагин не дал бин"
    return None


# 🔸 Публичная точка входа: stream-backfill сигнал (handler для STREAM_BACKFILL_HANDLERS)
async def run_lr_universal_level2_stream_backfill(
    signal: Dict[str, Any],
    msg_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> None:
    signal_id = int(signal.get("id") or 0)
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

    # parent_signal_id / parent_scenario_id из параметров инстанса level2-сигнала
    parent_sig_cfg = params.get("parent_signal_id")
    parent_sc_cfg = params.get("parent_scenario_id")
    dir_cfg = params.get("direction_mask")

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

    # sanity: run должен принадлежать parent_signal_id
    if int(run_info.get("signal_id") or 0) != int(parent_signal_id):
        log.warning(
            "BT_SIG_LR_UNI_L2: run belongs to another signal — run_id=%s run.signal_id=%s expected parent_signal_id=%s",
            run_id,
            run_info.get("signal_id"),
            parent_signal_id,
        )
        return

    window_from: datetime = run_info["from_time"]
    window_to: datetime = run_info["to_time"]

    # грузим инстанс родительского сигнала (настройки bounce)
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

    parent_direction_mask = _get_str_param(parent_params, "direction_mask", "both").strip().lower()
    parent_trend_type = _get_str_param(parent_params, "trend_type", "agnostic").strip().lower()
    parent_keep_half = _get_bool_param(parent_params, "keep_half", False)
    parent_zone_k = _get_float_param(parent_params, "zone_k", 0.0)

    # моно-направленность (как договорённость в системе)
    if parent_direction_mask != direction:
        log.warning(
            "BT_SIG_LR_UNI_L2: mismatch direction with parent — level2_signal_id=%s dir=%s parent_direction_mask=%s parent_signal_id=%s",
            signal_id,
            direction,
            parent_direction_mask,
            parent_signal_id,
        )
        return

    # загружаем свежие good bins победителя
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
            "BT_SIG_LR_UNI_L2: no good bins — skip bounce scan (level2_signal_id=%s parent_scenario_id=%s parent_signal_id=%s run_id=%s winner=%s dir=%s score_version=%s)",
            signal_id,
            scenario_id,
            parent_signal_id,
            run_id,
            winner_analysis_id,
            direction,
            score_version,
        )
        return

    # список тикеров
    symbols = get_all_ticker_symbols()
    if not symbols:
        return

    # предупреждение один раз: плагина ещё нет
    warn_key = (int(winner_analysis_id), str(score_version))
    if warn_key not in _warned_missing_plugin:
        _warned_missing_plugin.add(warn_key)
        log.info(
            "BT_SIG_LR_UNI_L2: plugin stub active — winner_analysis_id=%s winner_param='%s' (no real binning yet)",
            winner_analysis_id,
            str(winner_param),
        )

    log.debug(
        "BT_SIG_LR_UNI_L2: start bounce scan — level2_signal_id=%s name='%s' parent_scenario_id=%s parent_signal_id=%s run_id=%s "
        "winner_analysis_id=%s winner_param='%s' score_version=%s dir=%s window=[%s..%s] tickers=%s bins=%s timeframes=%s "
        "bounce_lr_instance_id=%s trend_type=%s zone_k=%.3f keep_half=%s",
        signal_id,
        name,
        scenario_id,
        parent_signal_id,
        run_id,
        winner_analysis_id,
        str(winner_param),
        score_version,
        direction,
        window_from,
        window_to,
        len(symbols),
        len(good_bins),
        sorted(timeframes),
        lr_bounce_m5_instance_id,
        parent_trend_type,
        float(parent_zone_k),
        bool(parent_keep_half),
    )

    sema = asyncio.Semaphore(SYMBOL_MAX_CONCURRENCY)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                _process_symbol_scan(
                    pg=pg,
                    sema=sema,
                    symbol=symbol,
                    direction=direction,
                    trend_type=parent_trend_type,
                    zone_k=parent_zone_k,
                    keep_half=parent_keep_half,
                    lr_bounce_m5_instance_id=lr_bounce_m5_instance_id,
                    window_from=window_from,
                    window_to=window_to,
                    winner_analysis_id=winner_analysis_id,
                    winner_param=str(winner_param),
                    good_bins=good_bins,
                ),
                name=f"BT_SIG_LR_UNI_L2_SCAN_{signal_id}_{symbol}",
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    candidates_total = 0
    candidates_with_bin = 0
    candidates_good = 0
    skipped_no_data = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        c_total, c_with_bin, c_good, c_no_data = res
        candidates_total += c_total
        candidates_with_bin += c_with_bin
        candidates_good += c_good
        skipped_no_data += c_no_data

    log.info(
        "BT_SIG_LR_UNI_L2: bounce scan done — level2_signal_id=%s parent_scenario_id=%s parent_signal_id=%s run_id=%s winner=%s dir=%s "
        "bins=%s candidates=%s plugin_bin=%s would_pass_good=%s skipped_no_data=%s",
        signal_id,
        scenario_id,
        parent_signal_id,
        run_id,
        winner_analysis_id,
        direction,
        len(good_bins),
        candidates_total,
        candidates_with_bin,
        candidates_good,
        skipped_no_data,
    )


# 🔸 Обработка одного символа: поиск bounce-кандидатов + заглушка плагина
async def _process_symbol_scan(
    pg,
    sema: asyncio.Semaphore,
    symbol: str,
    direction: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    lr_bounce_m5_instance_id: int,
    window_from: datetime,
    window_to: datetime,
    winner_analysis_id: int,
    winner_param: str,
    good_bins: Set[str],
) -> Tuple[int, int, int, int]:
    async with sema:
        # загружаем данные
        lr_series = await _load_lr_series(pg, lr_bounce_m5_instance_id, symbol, window_from, window_to)
        ohlcv = await _load_ohlcv_m5(pg, symbol, window_from, window_to)

        # если данных нет — считаем как skipped_no_data
        if not lr_series or not ohlcv:
            return 0, 0, 0, 1

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

        if not candidates:
            return 0, 0, 0, 0

        with_bin = 0
        would_pass_good = 0

        for cand in candidates:
            # заглушка плагина: пока не вычисляет bin_name
            bin_name = _plugin_stub_compute_bin_name(
                winner_analysis_id=winner_analysis_id,
                winner_param=winner_param,
                candidate=cand,
            )
            if bin_name is None:
                continue

            with_bin += 1
            if bin_name in good_bins:
                would_pass_good += 1

        return len(candidates), with_bin, would_pass_good, 0