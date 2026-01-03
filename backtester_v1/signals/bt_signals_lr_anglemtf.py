# bt_signals_lr_anglemtf.py — stream-backfill воркер: LR UNI bounce m5 с фильтрацией по winner bins (lr_angle_mtf)

import asyncio
import logging
import uuid
import json
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_all_ticker_symbols,
    get_ticker_info,
    get_signal_instance,
    get_all_indicator_instances,
)

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_ANGLEMTF")

# 🔸 Стримы
BT_SIGNALS_READY_STREAM = "bt:signals:ready"          # дальше конвейер: сценарии
BT_PREPROC_READY_STREAM = "bt:analysis:preproc_ready" # источник триггера (для ясности)

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Пороговые зоны угла (как в bt_analysis_lr_angle_mtf.py)
ZONE_TO_BIN_IDX = {
    "SD": 0,
    "MD": 1,
    "FLAT": 2,
    "MU": 3,
    "SU": 4,
}

# 🔸 Единая точность для углов/границ квантилей (как в bt_analysis_lr_angle_mtf.py)
Q6 = Decimal("0.000001")

# 🔸 Разрешённые winner-анализаторы (договорённость)
ALLOWED_WINNER_ANALYSIS_IDS: Set[int] = {83, 85}

# 🔸 Маппинг winner analysis_id -> lr_prefix / indicator_param (договорённость)
ANALYSIS_META: Dict[int, Dict[str, str]] = {
    83: {"lr_prefix": "lr50", "indicator_param": "lr50_angle_mtf"},
    85: {"lr_prefix": "lr100", "indicator_param": "lr100_angle_mtf"},
}

# 🔸 Фиксированные instance_id LR по TF для lr50/lr100
LR_INSTANCE_OVERRIDES: Dict[str, Dict[str, Optional[int]]] = {
    "lr50":  {"m5": 7, "m15": 24, "h1": 41},
    "lr100": {"m5": 8, "m15": 25, "h1": 42},
}


# 🔸 Публичная точка входа: stream-backfill сигнал (handler для STREAM_BACKFILL_HANDLERS)
async def run_lr_anglemtf_stream_backfill(
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

    # входное сообщение должно приходить из bt:analysis:preproc_ready
    stream_key = str((msg_ctx or {}).get("stream_key") or "")
    fields = (msg_ctx or {}).get("fields") or {}
    if stream_key != BT_PREPROC_READY_STREAM:
        return

    # парсим сообщение bt:analysis:preproc_ready
    preproc = _parse_preproc_ready(fields)
    if not preproc:
        return

    msg_scenario_id = preproc["scenario_id"]
    msg_parent_signal_id = preproc["signal_id"]
    run_id = preproc["run_id"]
    winner_analysis_id = preproc["winner_analysis_id"]

    # parent_signal_id / parent_scenario_id из параметров инстанса stream-сигнала
    parent_sig_cfg = params.get("parent_signal_id")
    parent_sc_cfg = params.get("parent_scenario_id")

    try:
        configured_parent_signal_id = int((parent_sig_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_signal_id = 0

    try:
        configured_parent_scenario_id = int((parent_sc_cfg or {}).get("value") or 0)
    except Exception:
        configured_parent_scenario_id = 0

    # проверяем, что сообщение относится к нашей связке (сценарий + сигнал)
    if (
        configured_parent_signal_id <= 0
        or configured_parent_scenario_id <= 0
        or msg_parent_signal_id != configured_parent_signal_id
        or msg_scenario_id != configured_parent_scenario_id
    ):
        return

    parent_signal_id = configured_parent_signal_id
    scenario_id = configured_parent_scenario_id

    # проверяем winner
    if winner_analysis_id not in ALLOWED_WINNER_ANALYSIS_IDS:
        log.debug(
            "BT_SIG_LR_ANGLEMTF: skip — winner_analysis_id=%s not allowed (signal_id=%s, parent_signal_id=%s, parent_scenario_id=%s, run_id=%s)",
            winner_analysis_id,
            signal_id,
            parent_signal_id,
            scenario_id,
            run_id,
        )
        return

    # грузим инстанс родительского сигнала (копия настроек)
    parent_signal = get_signal_instance(parent_signal_id)
    if not parent_signal:
        log.warning(
            "BT_SIG_LR_ANGLEMTF: parent signal not found in cache — parent_signal_id=%s (signal_id=%s, parent_scenario_id=%s, run_id=%s)",
            parent_signal_id,
            signal_id,
            scenario_id,
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
            "BT_SIG_LR_ANGLEMTF: parent signal has no valid 'indicator' param — parent_signal_id=%s",
            parent_signal_id,
        )
        return

    # параметры bounce берём как у родителя
    parent_direction_mask = _get_str_param(parent_params, "direction_mask", "both").lower()
    parent_trend_type = _get_str_param(parent_params, "trend_type", "agnostic").lower()
    parent_keep_half = _get_bool_param(parent_params, "keep_half", False)
    parent_zone_k = _get_float_param(parent_params, "zone_k", 0.0)

    # моно-направленность (по договорённости)
    if parent_direction_mask not in ("long", "short"):
        log.warning(
            "BT_SIG_LR_ANGLEMTF: parent signal is not mono-directional (direction_mask=%s), skip (parent_signal_id=%s)",
            parent_direction_mask,
            parent_signal_id,
        )
        return

    direction = parent_direction_mask

    # окно run — источник истины
    run_info = await _load_run_info(pg, run_id)
    if not run_info:
        log.warning(
            "BT_SIG_LR_ANGLEMTF: run not found — run_id=%s (signal_id=%s, parent_signal_id=%s, parent_scenario_id=%s)",
            run_id,
            signal_id,
            parent_signal_id,
            scenario_id,
        )
        return

    # sanity: run должен принадлежать родительскому сигналу
    if int(run_info.get("signal_id") or 0) != int(parent_signal_id):
        log.warning(
            "BT_SIG_LR_ANGLEMTF: run belongs to another signal — run_id=%s run.signal_id=%s, expected parent_signal_id=%s",
            run_id,
            run_info.get("signal_id"),
            parent_signal_id,
        )
        return

    window_from: datetime = run_info["from_time"]
    window_to: datetime = run_info["to_time"]

    # meta по winner анализатору
    meta = ANALYSIS_META.get(winner_analysis_id) or {}
    lr_prefix = str(meta.get("lr_prefix") or "").strip()
    indicator_param = str(meta.get("indicator_param") or "").strip()

    if not lr_prefix or not indicator_param:
        log.warning(
            "BT_SIG_LR_ANGLEMTF: meta missing for winner_analysis_id=%s",
            winner_analysis_id,
        )
        return

    # решаем instance_id LR (для lr_prefix) по TF
    lr_m5_instance_id = _resolve_lr_instance_id(lr_prefix, "m5")
    lr_m15_instance_id = _resolve_lr_instance_id(lr_prefix, "m15")
    lr_h1_instance_id = _resolve_lr_instance_id(lr_prefix, "h1")

    if not (lr_m5_instance_id and lr_m15_instance_id and lr_h1_instance_id):
        log.warning(
            "BT_SIG_LR_ANGLEMTF: LR instances not resolved for lr_prefix=%s (m5=%s, m15=%s, h1=%s)",
            lr_prefix,
            lr_m5_instance_id,
            lr_m15_instance_id,
            lr_h1_instance_id,
        )
        return

    # загружаем whitelist good bins для (parent_scenario, parent_signal, direction, winner)
    good_bins = await _load_good_bins(
        pg=pg,
        scenario_id=scenario_id,
        parent_signal_id=parent_signal_id,
        direction=direction,
        analysis_id=winner_analysis_id,
        indicator_param=indicator_param,
    )

    if not good_bins:
        log.debug(
            "BT_SIG_LR_ANGLEMTF: no good bins — skip generation (parent_scenario_id=%s, parent_signal_id=%s, run_id=%s, winner=%s, dir=%s)",
            scenario_id,
            parent_signal_id,
            run_id,
            winner_analysis_id,
            direction,
        )
        return

    # загружаем бин-словари H1/M15 (для нормализации индексов зон -> H1_bin_* / M15_bin_*)
    bins_h1 = await _load_bins_dict(pg, winner_analysis_id, direction, "h1")
    bins_m15 = await _load_bins_dict(pg, winner_analysis_id, direction, "m15")

    if not bins_h1 or not bins_m15:
        log.debug(
            "BT_SIG_LR_ANGLEMTF: bins_dict missing (h1=%s, m15=%s) — skip generation (analysis_id=%s, dir=%s)",
            bool(bins_h1),
            bool(bins_m15),
            winner_analysis_id,
            direction,
        )
        return

    # список тикеров
    symbols = get_all_ticker_symbols()
    if not symbols:
        return

    # загружаем уже существующие события нового сигнала в окне (идемпотентность)
    existing_events = await _load_existing_events(pg, signal_id, timeframe, window_from, window_to)

    log.debug(
        "BT_SIG_LR_ANGLEMTF: start stream-backfill signal_id=%s ('%s', key=%s) parent_signal_id=%s, parent_scenario_id=%s, run_id=%s, winner=%s, lr_prefix=%s, dir=%s, window=[%s..%s], tickers=%s",
        signal_id,
        name,
        signal_key,
        parent_signal_id,
        scenario_id,
        run_id,
        winner_analysis_id,
        lr_prefix,
        direction,
        window_from,
        window_to,
        len(symbols),
    )

    sema = asyncio.Semaphore(5)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                _process_symbol(
                    pg=pg,
                    sema=sema,
                    signal_id=signal_id,
                    signal_key=signal_key,
                    timeframe=timeframe,
                    symbol=symbol,
                    run_id=run_id,
                    scenario_id=scenario_id,
                    parent_signal_id=parent_signal_id,
                    direction=direction,
                    # bounce settings
                    lr_bounce_m5_instance_id=lr_bounce_m5_instance_id,
                    trend_type=parent_trend_type,
                    zone_k=parent_zone_k,
                    keep_half=parent_keep_half,
                    # binning settings
                    winner_analysis_id=winner_analysis_id,
                    lr_prefix=lr_prefix,
                    lr_m5_instance_id=int(lr_m5_instance_id),
                    lr_m15_instance_id=int(lr_m15_instance_id),
                    lr_h1_instance_id=int(lr_h1_instance_id),
                    bins_h1=bins_h1,
                    bins_m15=bins_m15,
                    good_bins=good_bins,
                    # window
                    window_from=window_from,
                    window_to=window_to,
                    existing_events=existing_events,
                ),
                name=f"BT_SIG_LR_ANGLEMTF_{signal_id}_{symbol}",
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_inserted = 0
    total_candidates = 0
    total_bin_skipped = 0
    total_data_skipped = 0
    total_existing = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        inserted, candidates, bin_skipped, data_skipped, skipped_existing = res
        total_inserted += inserted
        total_candidates += candidates
        total_bin_skipped += bin_skipped
        total_data_skipped += data_skipped
        total_existing += skipped_existing

    log.debug(
        "BT_SIG_LR_ANGLEMTF: summary signal_id=%s parent_signal_id=%s parent_scenario_id=%s run_id=%s winner=%s dir=%s window=[%s..%s] — candidates=%s, inserted=%s, skipped_existing=%s, skipped_no_data=%s, skipped_not_good_bin=%s",
        signal_id,
        parent_signal_id,
        scenario_id,
        run_id,
        winner_analysis_id,
        direction,
        window_from,
        window_to,
        total_candidates,
        total_inserted,
        total_existing,
        total_data_skipped,
        total_bin_skipped,
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
            "BT_SIG_LR_ANGLEMTF: failed to publish bt:signals:ready — signal_id=%s run_id=%s err=%s",
            signal_id,
            run_id,
            e,
            exc_info=True,
        )


# 🔸 Обработка одного символа: генерация bounce-кандидатов + фильтр по good bins
async def _process_symbol(
    pg,
    sema: asyncio.Semaphore,
    signal_id: int,
    signal_key: str,
    timeframe: str,
    symbol: str,
    run_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    # bounce settings
    lr_bounce_m5_instance_id: int,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    # binning settings
    winner_analysis_id: int,
    lr_prefix: str,
    lr_m5_instance_id: int,
    lr_m15_instance_id: int,
    lr_h1_instance_id: int,
    bins_h1: List[Dict[str, Any]],
    bins_m15: List[Dict[str, Any]],
    good_bins: Set[str],
    # window
    window_from: datetime,
    window_to: datetime,
    existing_events: Set[Tuple[str, datetime, str]],
) -> Tuple[int, int, int, int, int]:
    async with sema:
        # bounce candidates
        candidates = await _find_lr_bounce_candidates(
            pg=pg,
            symbol=symbol,
            timeframe=timeframe,
            lr_bounce_m5_instance_id=lr_bounce_m5_instance_id,
            window_from=window_from,
            window_to=window_to,
            direction=direction,
            trend_type=trend_type,
            zone_k=zone_k,
            keep_half=keep_half,
            signal_key=signal_key,
            signal_id=signal_id,
        )

        if not candidates:
            return 0, 0, 0, 0, 0

        inserted = 0
        skipped_not_good_bin = 0
        skipped_no_data = 0
        skipped_existing = 0

        to_insert: List[Tuple[Any, ...]] = []

        # кеш квантилей по (H1_bin, M15_bin)
        quantiles_cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

        for cand in candidates:
            ts: datetime = cand["open_time"]
            decision_time: datetime = cand["decision_time"]

            key_event = (symbol, ts, direction)
            if key_event in existing_events:
                skipped_existing += 1
                continue

            # определяем open_time m15/h1 "что известно к моменту decision_time"
            open_time_m15 = await _resolve_open_time_tf(pg, symbol, "m15", decision_time)
            open_time_h1 = await _resolve_open_time_tf(pg, symbol, "h1", decision_time)

            if open_time_m15 is None or open_time_h1 is None:
                skipped_no_data += 1
                continue

            # читаем углы lr_prefix на m5/m15/h1 из indicator_values_v4
            angle_m5 = await _load_lr_angle(pg, lr_m5_instance_id, symbol, ts, lr_prefix)
            angle_m15 = await _load_lr_angle(pg, lr_m15_instance_id, symbol, open_time_m15, lr_prefix)
            angle_h1 = await _load_lr_angle(pg, lr_h1_instance_id, symbol, open_time_h1, lr_prefix)

            if angle_m5 is None or angle_m15 is None or angle_h1 is None:
                skipped_no_data += 1
                continue

            # зоны по h1/m15
            z_h1 = _angle_to_zone(angle_h1)
            z_m15 = _angle_to_zone(angle_m15)
            if z_h1 is None or z_m15 is None:
                skipped_no_data += 1
                continue

            h1_idx = ZONE_TO_BIN_IDX.get(z_h1)
            m15_idx = ZONE_TO_BIN_IDX.get(z_m15)
            if h1_idx is None or m15_idx is None:
                skipped_no_data += 1
                continue

            # имена H1/M15 бинов (через bins_dict)
            h1_bin = _assign_bin(bins_h1, Decimal(h1_idx))
            m15_bin = _assign_bin(bins_m15, Decimal(m15_idx))
            if not h1_bin or not m15_bin:
                skipped_no_data += 1
                continue

            # определяем M5_Qk или M5_0 через adaptive dict (если квантилей нет — схлопывание)
            base_key = (str(h1_bin), str(m15_bin))
            q_rows = quantiles_cache.get(base_key)
            if q_rows is None:
                q_rows = await _load_quantiles_for_pair(
                    pg=pg,
                    run_id=run_id,
                    analysis_id=winner_analysis_id,
                    scenario_id=scenario_id,
                    parent_signal_id=parent_signal_id,
                    direction=direction,
                    h1_bin=str(h1_bin),
                    m15_bin=str(m15_bin),
                )
                quantiles_cache[base_key] = q_rows

            # sort_key как в анализаторе: для short инверсия
            angle_m5_q6 = _q6(angle_m5)
            sort_key = _q6(-angle_m5_q6) if direction == "short" else angle_m5_q6

            if not q_rows:
                bin_name = f"{h1_bin}|{m15_bin}|M5_0"
            else:
                q_bin = _pick_quantile_bin(q_rows, sort_key)
                if q_bin is None:
                    skipped_no_data += 1
                    continue
                bin_name = str(q_bin)

            # whitelist
            if bin_name not in good_bins:
                skipped_not_good_bin += 1
                continue

            # формируем raw_message как у LR_UNI + bin_name
            raw_message = dict(cand.get("raw_message") or {})
            raw_message["bin_name"] = bin_name

            to_insert.append(
                (
                    str(uuid.uuid4()),
                    int(signal_id),
                    str(symbol),
                    str(timeframe),
                    ts,
                    decision_time,
                    str(direction),
                    str(cand["message"]),
                    json.dumps(raw_message),
                    int(run_id),  # first_backfill_run_id = родительский run
                )
            )

        if not to_insert:
            return 0, len(candidates), skipped_not_good_bin, skipped_no_data, skipped_existing

        # вставка пачкой
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_signals_values
                    (signal_uuid, signal_id, symbol, timeframe, open_time, decision_time, direction, message, raw_message, first_backfill_run_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
                ON CONFLICT (signal_id, symbol, timeframe, open_time, direction) DO NOTHING
                """,
                to_insert,
            )

        inserted = len(to_insert)
        return inserted, len(candidates), skipped_not_good_bin, skipped_no_data, skipped_existing


# 🔸 Поиск bounce-кандидатов LR (как в bt_signals_lr_universal.py), но без записи в БД
async def _find_lr_bounce_candidates(
    pg,
    symbol: str,
    timeframe: str,
    lr_bounce_m5_instance_id: int,
    window_from: datetime,
    window_to: datetime,
    direction: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    signal_key: str,
    signal_id: int,
) -> List[Dict[str, Any]]:
    if timeframe != "m5":
        return []

    # грузим LR-канал на m5 (bounce-инстанс)
    lr_series = await _load_lr_series(pg, lr_bounce_m5_instance_id, symbol, window_from, window_to)
    if not lr_series or len(lr_series) < 2:
        return []

    # грузим OHLCV m5
    ohlcv = await _load_ohlcv_m5(pg, symbol, window_from, window_to)
    if not ohlcv:
        return []

    times = sorted(set(lr_series.keys()) & set(ohlcv.keys()))
    if len(times) < 2:
        return []

    # precision цены
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    tf_delta = timedelta(minutes=TF_STEP_MINUTES["m5"])

    out: List[Dict[str, Any]] = []

    # перебор пар (prev_ts, ts)
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

        if angle_m5 is None or upper_curr is None or lower_curr is None or upper_prev is None or lower_prev is None:
            continue

        if keep_half and center_curr is None:
            continue

        try:
            angle_f = float(angle_m5)
            upper_curr_f = float(upper_curr)
            lower_curr_f = float(lower_curr)
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

        # bounce условие по направлению
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

        # округляем цену для raw_message
        try:
            price_rounded = float(f"{close_curr_f:.{precision_price}f}")
        except Exception:
            price_rounded = close_curr_f

        decision_time = ts + tf_delta

        message = "LR_ANGLEMTF_BOUNCE_LONG" if direction == "long" else "LR_ANGLEMTF_BOUNCE_SHORT"

        raw_message = {
            "signal_key": signal_key,
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": ts.isoformat(),
            "decision_time": decision_time.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "pattern": "bounce",
            "angle_m5": angle_f,
            "upper_prev": upper_prev_f,
            "lower_prev": lower_prev_f,
            "upper_curr": upper_curr_f,
            "lower_curr": lower_curr_f,
            "center_curr": center_curr_f,
            "zone_k": float(zone_k),
            "trend_type": trend_type,
            "keep_half": bool(keep_half),
            "lr_m5_instance_id": int(lr_bounce_m5_instance_id),
        }

        out.append(
            {
                "open_time": ts,
                "decision_time": decision_time,
                "message": message,
                "raw_message": raw_message,
            }
        )

    return out


# 🔸 Загрузка уже существующих событий сигнала в окне (идемпотентность)
async def _load_existing_events(
    pg,
    signal_id: int,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> Set[Tuple[str, datetime, str]]:
    existing: Set[Tuple[str, datetime, str]] = set()
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, open_time, direction
            FROM bt_signals_values
            WHERE signal_id = $1
              AND timeframe = $2
              AND open_time BETWEEN $3 AND $4
            """,
            int(signal_id),
            str(timeframe),
            from_time,
            to_time,
        )
    for r in rows:
        existing.add((r["symbol"], r["open_time"], r["direction"]))
    return existing


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


# 🔸 Парсер сообщения bt:analysis:preproc_ready
def _parse_preproc_ready(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id = int(str(fields.get("scenario_id") or "").strip())
        signal_id = int(str(fields.get("signal_id") or "").strip())
        run_id = int(str(fields.get("run_id") or "").strip())
        winner_analysis_id = int(str(fields.get("winner_analysis_id") or "0").strip() or 0)
        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "winner_analysis_id": winner_analysis_id,
        }
    except Exception:
        return None


# 🔸 Загрузка whitelist good bins из bt_analysis_bins_labels (по родительскому signal_id)
async def _load_good_bins(
    pg,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
) -> Set[str]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT bin_name
            FROM bt_analysis_bins_labels
            WHERE scenario_id    = $1
              AND signal_id      = $2
              AND direction      = $3
              AND analysis_id    = $4
              AND timeframe      = 'mtf'
              AND state          = 'good'
              AND (indicator_param = $5 OR $5 = '')
            """,
            int(scenario_id),
            int(parent_signal_id),
            str(direction),
            int(analysis_id),
            str(indicator_param or ""),
        )
    return {str(r["bin_name"]) for r in rows if r and r["bin_name"] is not None}


# 🔸 Загрузка bins_dict (H1/M15) для нормализации bin_name
async def _load_bins_dict(
    pg,
    analysis_id: int,
    direction: str,
    timeframe: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT bin_name, val_from, val_to, to_inclusive
            FROM bt_analysis_bins_dict
            WHERE analysis_id = $1
              AND direction   = $2
              AND timeframe   = $3
              AND bin_type    = 'bins'
            ORDER BY bin_order
            """,
            int(analysis_id),
            str(direction),
            str(timeframe),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "name": str(r["bin_name"]),
                "min": _safe_decimal(r["val_from"]),
                "max": _safe_decimal(r["val_to"]),
                "to_inclusive": bool(r["to_inclusive"]),
            }
        )
    return out


# 🔸 Загрузка квантилей (adaptive dict) для пары H1_bin|M15_bin в конкретном run
async def _load_quantiles_for_pair(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    parent_signal_id: int,
    direction: str,
    h1_bin: str,
    m15_bin: str,
) -> List[Dict[str, Any]]:
    prefix = f"{h1_bin}|{m15_bin}|M5_Q"
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT bin_name, val_from, val_to, to_inclusive, bin_order
            FROM bt_analysis_bin_dict_adaptive
            WHERE run_id      = $1
              AND analysis_id = $2
              AND scenario_id = $3
              AND signal_id   = $4
              AND direction   = $5
              AND timeframe   = 'mtf'
              AND bin_type    = 'quantiles'
              AND bin_name LIKE $6
            ORDER BY bin_order
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(parent_signal_id),
            str(direction),
            prefix + "%",
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "bin_name": str(r["bin_name"]),
                "val_from": _safe_decimal(r["val_from"]),
                "val_to": _safe_decimal(r["val_to"]),
                "to_inclusive": bool(r["to_inclusive"]),
                "bin_order": int(r["bin_order"] or 0),
            }
        )
    return out


# 🔸 Выбор квантильного бина по sort_key
def _pick_quantile_bin(q_rows: List[Dict[str, Any]], sort_key: Decimal) -> Optional[str]:
    if not q_rows:
        return None

    v = _q6(sort_key)

    for rec in q_rows:
        lo = _safe_decimal(rec.get("val_from"))
        hi = _safe_decimal(rec.get("val_to"))
        to_inclusive = bool(rec.get("to_inclusive"))

        # условия достаточности
        if lo is None or hi is None:
            continue

        if to_inclusive:
            if lo <= v <= hi:
                return str(rec.get("bin_name") or "")
        else:
            if lo <= v < hi:
                return str(rec.get("bin_name") or "")

    return None


# 🔸 Определение open_time TF (m15/h1) "что известно к моменту decision_time"
async def _resolve_open_time_tf(
    pg,
    symbol: str,
    timeframe: str,
    decision_time: datetime,
) -> Optional[datetime]:
    tf = str(timeframe or "").strip().lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return None

    # условие open_time + Δ(tf) <= decision_time  <=> open_time <= decision_time - Δ(tf)
    upper_bound = decision_time - timedelta(minutes=int(step_min))

    table_name = _ohlcv_table_for_timeframe(tf)
    if not table_name:
        return None

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT max(open_time) AS open_time
            FROM {table_name}
            WHERE symbol = $1
              AND open_time <= $2
            """,
            str(symbol),
            upper_bound,
        )
    if not row or row["open_time"] is None:
        return None
    return row["open_time"]


# 🔸 Загрузка угла LR (lr_prefix_angle) на конкретном open_time из indicator_values_v4
async def _load_lr_angle(
    pg,
    instance_id: int,
    symbol: str,
    open_time: datetime,
    lr_prefix: str,
) -> Optional[Decimal]:
    param_name = f"{lr_prefix}_angle"
    async with pg.acquire() as conn:
        val = await conn.fetchval(
            """
            SELECT value
            FROM indicator_values_v4
            WHERE instance_id = $1
              AND symbol      = $2
              AND open_time   = $3
              AND param_name  = $4
            """,
            int(instance_id),
            str(symbol),
            open_time,
            str(param_name),
        )
    if val is None:
        return None
    return _safe_decimal(val)


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


# 🔸 Определение имени бина по границам bins_dict (как в bt_analysis_lr_angle_mtf.py)
def _assign_bin(bins: List[Dict[str, Any]], value: Decimal) -> Optional[str]:
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


# 🔸 q6 квантизация (ROUND_DOWN)
def _q6(value: Any) -> Decimal:
    try:
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Q6, rounding=ROUND_DOWN)
    except Exception:
        return Decimal("0").quantize(Q6, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _safe_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None


# 🔸 Разбор параметров сигнала (как в остальных файлах системы)
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw).strip()


def _get_bool_param(params: Dict[str, Any], name: str, default: bool) -> bool:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw).strip().lower() == "true"


def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default


# 🔸 Поиск LR instance_id (lr50/lr100) по TF: override → динамический поиск в кеше indicator_instances_v4
def _resolve_lr_instance_id(lr_prefix: str, timeframe: str) -> Optional[int]:
    tf = str(timeframe or "").strip().lower()
    prefix = str(lr_prefix or "").strip().lower()

    # override в коде
    ov = (LR_INSTANCE_OVERRIDES.get(prefix) or {}).get(tf)
    if ov is not None:
        try:
            ov_i = int(ov)
            if ov_i > 0:
                return ov_i
        except Exception:
            pass

    # динамический поиск: indicator='lr', timeframe=tf, params.length == N
    try:
        length_str = prefix.replace("lr", "").strip()
        length_i = int(length_str)
    except Exception:
        return None

    all_instances = get_all_indicator_instances()  # instance_id -> {indicator, timeframe, params, ...}
    for iid, inst in (all_instances or {}).items():
        if str(inst.get("indicator") or "").strip().lower() != "lr":
            continue
        if str(inst.get("timeframe") or "").strip().lower() != tf:
            continue

        p = inst.get("params") or {}
        raw_len = p.get("length")
        try:
            inst_len = int(str(raw_len).strip())
        except Exception:
            continue

        if inst_len == length_i:
            try:
                return int(iid)
            except Exception:
                return None

    return None