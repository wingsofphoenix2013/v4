# bt_signals_lr_universal.py — timer-backfill воркер RAW (LR bounce) для m5: пишет events + membership + ready_v2

import asyncio
import logging
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set


# 🔸 Кеши backtester_v1
from backtester_config import get_all_ticker_symbols, get_ticker_info

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI")

# 🔸 Стрим готовности датасета сигналов (v2)
BT_SIGNALS_READY_STREAM_V2 = "bt:signals:ready_v2"

# 🔸 Таблицы
BT_SIGNAL_EVENTS_TABLE = "bt_signals_values"
BT_SIGNAL_MEMBERSHIP_TABLE = "bt_signals_membership"
BT_RUNS_TABLE = "bt_signal_backfill_runs"

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
}

# 🔸 Ограничение параллелизма по тикерам
SYMBOL_MAX_CONCURRENCY = 5


# 🔸 Длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").strip().lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Публичная точка входа: backfill по окну run для одного инстанса LR-сигнала
async def run_lr_universal_backfill(
    signal: Dict[str, Any],
    pg,
    redis,
    run_id: Optional[int] = None,
    window_from_time: Optional[datetime] = None,
    window_to_time: Optional[datetime] = None,
) -> None:
    signal_id = int(signal.get("id") or 0)
    signal_key = str(signal.get("key") or "").strip()
    name = signal.get("name")
    timeframe = str(signal.get("timeframe") or "").strip().lower()
    params = signal.get("params") or {}

    # условия достаточности
    if signal_id <= 0 or timeframe != "m5":
        return
    if run_id is None or window_from_time is None or window_to_time is None:
        log.warning(
            "BT_SIG_LR_UNI: пропуск backfill — signal_id=%s ('%s'), run_id/window отсутствуют (run_id=%s, from=%s, to=%s)",
            signal_id,
            name,
            run_id,
            window_from_time,
            window_to_time,
        )
        return

    # читаем LR instance (обязательный)
    try:
        lr_cfg = params["indicator"]
        lr_m5_instance_id = int(lr_cfg["value"])
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') — некорректные параметры indicator (LR instance_id m5): %s",
            signal_id,
            name,
            e,
        )
        return

    # маска направлений: 'long' / 'short' / 'both' (по умолчанию both)
    dir_mask_cfg = params.get("direction_mask")
    if dir_mask_cfg:
        mask_val_raw = dir_mask_cfg.get("value") or ""
        mask_val = str(mask_val_raw).strip().lower()
    else:
        mask_val = "both"

    if mask_val == "long":
        allowed_directions: Set[str] = {"long"}
    elif mask_val == "short":
        allowed_directions = {"short"}
    else:
        allowed_directions = {"long", "short"}

    # режим работы по тренду: trend / counter / agnostic
    trend_cfg = params.get("trend_type")
    if trend_cfg:
        trend_raw = trend_cfg.get("value") or ""
        trend_type = str(trend_raw).strip().lower()
    else:
        trend_type = "agnostic"

    if trend_type not in ("trend", "counter", "agnostic"):
        log.warning(
            "BT_SIG_LR_UNI: сигнал id=%s ('%s') имеет неизвестный trend_type=%s, используется 'agnostic'",
            signal_id,
            name,
            trend_type,
        )
        trend_type = "agnostic"

    # флаг: нужно ли следить, чтобы после отскока цена оставалась в своей половине канала
    keep_half_cfg = params.get("keep_half")
    if keep_half_cfg:
        keep_half_raw = keep_half_cfg.get("value") or ""
        keep_half = str(keep_half_raw).strip().lower() == "true"
    else:
        keep_half = False

    # параметр зоны у границы канала: доля высоты канала (0.0 .. 0.5)
    zone_k = _get_float_param(params, "zone_k", 0.0)
    if zone_k < 0.0:
        zone_k = 0.0
    if zone_k > 0.5:
        zone_k = 0.5

    # паттерн — только bounce
    pattern = "bounce"

    from_time = window_from_time
    to_time = window_to_time

    # идентификатор типа события (общий event-layer, без signal_id)
    event_key = f"lr_universal_bounce_{timeframe}"

    # подпись параметров детектора (стабильная часть идентичности событий)
    event_params_hash = _make_event_params_hash(
        lr_instance_id=lr_m5_instance_id,
        timeframe=timeframe,
        trend_type=trend_type,
        zone_k=zone_k,
        keep_half=keep_half,
    )

    # RAW membership не требует scenario/winner
    sys_scenario_id = None
    sys_analysis_id = None

    # список активных тикеров из кеша
    symbols = get_all_ticker_symbols()
    if not symbols:
        log.debug("BT_SIG_LR_UNI: нет активных тикеров для обработки, signal_id=%s ('%s')", signal_id, name)
        return

    log.debug(
        "BT_SIG_LR_UNI: старт backfill — signal_id=%s ('%s', key=%s), run_id=%s, TF=%s, окно=[%s..%s], тикеров=%s, "
        "direction_mask=%s, lr_m5_instance_id=%s, pattern=%s, trend_type=%s, zone_k=%.3f, keep_half=%s, event_key=%s, hash=%s",
        signal_id,
        name,
        signal_key,
        int(run_id),
        timeframe,
        from_time,
        to_time,
        len(symbols),
        mask_val,
        lr_m5_instance_id,
        pattern,
        trend_type,
        zone_k,
        keep_half,
        event_key,
        event_params_hash,
    )

    sema = asyncio.Semaphore(SYMBOL_MAX_CONCURRENCY)
    tasks: List[asyncio.Task] = []

    for symbol in symbols:
        tasks.append(
            asyncio.create_task(
                _process_symbol(
                    pg=pg,
                    sema=sema,
                    run_id=int(run_id),
                    signal_id=signal_id,
                    scenario_id=sys_scenario_id,
                    winner_analysis_id=sys_analysis_id,
                    parent_run_id=int(run_id),
                    parent_signal_id=signal_id,
                    symbol=str(symbol),
                    timeframe=timeframe,
                    from_time=from_time,
                    to_time=to_time,
                    lr_m5_instance_id=lr_m5_instance_id,
                    allowed_directions=allowed_directions,
                    trend_type=trend_type,
                    zone_k=zone_k,
                    keep_half=keep_half,
                    pattern=pattern,
                    event_key=event_key,
                    event_params_hash=event_params_hash,
                ),
                name=f"BT_SIG_LR_UNI_{signal_id}_{symbol}",
            )
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_candidates = 0
    total_events_inserted = 0
    total_membership_inserted = 0
    total_long = 0
    total_short = 0
    total_no_data = 0

    for res in results:
        if isinstance(res, Exception):
            continue
        cands, ev_ins, mem_ins, longs, shorts, no_data = res
        total_candidates += cands
        total_events_inserted += ev_ins
        total_membership_inserted += mem_ins
        total_long += longs
        total_short += shorts
        total_no_data += no_data

    finished_at = datetime.utcnow()

    log.debug(
        "BT_SIG_LR_UNI: backfill готов — signal_id=%s run_id=%s TF=%s window=[%s..%s] tickers=%s "
        "candidates=%s (long=%s short=%s) events_inserted=%s membership_inserted=%s no_data=%s",
        signal_id,
        int(run_id),
        timeframe,
        from_time,
        to_time,
        len(symbols),
        total_candidates,
        total_long,
        total_short,
        total_events_inserted,
        total_membership_inserted,
        total_no_data,
    )

    # публикуем ready_v2: downstream читает датасет через membership(run_id, signal_id)
    try:
        await redis.xadd(
            BT_SIGNALS_READY_STREAM_V2,
            {
                "signal_id": str(signal_id),
                "run_id": str(int(run_id)),
                "from_time": from_time.isoformat(),
                "to_time": to_time.isoformat(),
                "finished_at": finished_at.isoformat(),
                "dataset_kind": "membership",
                "parent_run_id": str(int(run_id)),
                "parent_signal_id": str(int(signal_id)),
            },
        )
    except Exception as e:
        log.error(
            "BT_SIG_LR_UNI: не удалось опубликовать ready_v2 (signal_id=%s run_id=%s): %s",
            signal_id,
            int(run_id),
            e,
            exc_info=True,
        )


# 🔸 Обработка одного символа: поиск bounce-кандидатов -> upsert events -> insert membership
async def _process_symbol(
    pg,
    sema: asyncio.Semaphore,
    run_id: int,
    signal_id: int,
    scenario_id: int,
    winner_analysis_id: int,
    parent_run_id: int,
    parent_signal_id: int,
    symbol: str,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
    lr_m5_instance_id: int,
    allowed_directions: Set[str],
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    pattern: str,
    event_key: str,
    event_params_hash: str,
) -> Tuple[int, int, int, int, int, int]:
    async with sema:
        # грузим LR-канал на m5
        lr_series = await _load_lr_series(pg, lr_m5_instance_id, symbol, from_time, to_time)
        if not lr_series or len(lr_series) < 2:
            return 0, 0, 0, 0, 0, 1

        # грузим OHLCV для m5 (для цен)
        ohlcv = await _load_ohlcv_series(pg, symbol, timeframe, from_time, to_time)
        if not ohlcv:
            return 0, 0, 0, 0, 0, 1

        # precision цены
        ticker_info = get_ticker_info(symbol) or {}
        try:
            precision_price = int(ticker_info.get("precision_price") or 8)
        except Exception:
            precision_price = 8

        tf_delta = _get_timeframe_timedelta(timeframe)
        if tf_delta <= timedelta(0):
            return 0, 0, 0, 0, 0, 1

        # генерируем bounce-кандидаты (для разрешённых направлений)
        candidates, long_count, short_count = _find_bounce_candidates(
            symbol=symbol,
            allowed_directions=allowed_directions,
            trend_type=trend_type,
            zone_k=zone_k,
            keep_half=keep_half,
            precision_price=precision_price,
            lr_series=lr_series,
            ohlcv=ohlcv,
            tf_delta=tf_delta,
            pattern=pattern,
            lr_m5_instance_id=lr_m5_instance_id,
        )

        if not candidates:
            return 0, 0, 0, 0, 0, 0

        # вставляем events (идемпотентно)
        events_inserted = await _upsert_events(
            pg=pg,
            symbol=symbol,
            timeframe=timeframe,
            event_key=event_key,
            event_params_hash=event_params_hash,
            candidates=candidates,
        )

        # получаем id событий для membership (по open_time+direction)
        event_ids_by_key = await _load_event_ids_for_candidates(
            pg=pg,
            symbol=symbol,
            timeframe=timeframe,
            event_key=event_key,
            event_params_hash=event_params_hash,
            candidates=candidates,
        )

        # формируем membership rows
        to_membership: List[Tuple[Any, ...]] = []
        for cand in candidates:
            key = (cand["open_time"], cand["direction"])
            ev_id = event_ids_by_key.get(key)
            if not ev_id:
                continue

            to_membership.append(
                (
                    int(run_id),
                    int(signal_id),
                    int(ev_id),
                    None,          # scenario_id
                    int(parent_run_id),
                    int(parent_signal_id),
                    None,          # winner_analysis_id
                    "v1",          # score_version
                    None,          # winner_param
                    "raw",         # bin_name
                    None,          # plugin
                    None,          # plugin_param_name
                    None,          # lr_prefix
                    None,          # length
                    "generate",    # pipeline_mode
                )
            )

        membership_inserted = 0
        if to_membership:
            membership_inserted = await _insert_membership(pg, to_membership)

        return len(candidates), int(events_inserted), int(membership_inserted), int(long_count), int(short_count), 0


# 🔸 Поиск bounce-кандидатов (логика как раньше, но результат — список dict)
def _find_bounce_candidates(
    symbol: str,
    allowed_directions: Set[str],
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    precision_price: int,
    lr_series: Dict[datetime, Dict[str, float]],
    ohlcv: Dict[datetime, Tuple[float, float, float, float]],
    tf_delta: timedelta,
    pattern: str,
    lr_m5_instance_id: int,
) -> Tuple[List[Dict[str, Any]], int, int]:
    # условия достаточности
    if not lr_series or not ohlcv:
        return [], 0, 0

    times = sorted(set(lr_series.keys()) & set(ohlcv.keys()))
    if len(times) < 2:
        return [], 0, 0

    out: List[Dict[str, Any]] = []
    long_count = 0
    short_count = 0

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

        angle = lr_curr.get("angle")
        upper_curr = lr_curr.get("upper")
        lower_curr = lr_curr.get("lower")
        upper_prev = lr_prev.get("upper")
        lower_prev = lr_prev.get("lower")
        center_curr = lr_curr.get("center")

        if angle is None or upper_curr is None or lower_curr is None or upper_prev is None or lower_prev is None:
            continue

        # если keep_half включён, но нет center_curr — пропускаем
        if keep_half and center_curr is None:
            continue

        try:
            angle_f = float(angle)
            upper_prev_f = float(upper_prev)
            lower_prev_f = float(lower_prev)
            upper_curr_f = float(upper_curr)
            lower_curr_f = float(lower_curr)
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
            long_trend_ok = angle_f > 0.0
            short_trend_ok = angle_f < 0.0
        elif trend_type == "counter":
            long_trend_ok = angle_f < 0.0
            short_trend_ok = angle_f > 0.0
        else:
            long_trend_ok = True
            short_trend_ok = True

        direction: Optional[str] = None

        # LONG bounce
        if "long" in allowed_directions and long_trend_ok:
            if zone_k == 0.0:
                in_zone_prev = close_prev_f <= lower_prev_f
            else:
                threshold = lower_prev_f + (float(zone_k) * H)
                in_zone_prev = close_prev_f <= threshold

            if in_zone_prev and close_curr_f > lower_prev_f:
                if keep_half and not (close_curr_f <= center_curr_f):
                    continue
                direction = "long"

        # SHORT bounce
        if direction is None and "short" in allowed_directions and short_trend_ok:
            if zone_k == 0.0:
                in_zone_prev = close_prev_f >= upper_prev_f
            else:
                threshold = upper_prev_f - (float(zone_k) * H)
                in_zone_prev = close_prev_f >= threshold

            if in_zone_prev and close_curr_f < upper_prev_f:
                if keep_half and not (close_curr_f >= center_curr_f):
                    continue
                direction = "short"

        if direction is None:
            continue

        # округляем цену (важно: не конвертируем в float, чтобы не получать длинные хвосты в numeric)
        try:
            price_rounded = f"{close_curr_f:.{precision_price}f}"
        except Exception:
            price_rounded = str(close_curr_f)

        decision_time = ts + tf_delta

        # стабильный payload для отладки (не зависит от run/winner/bins)
        payload_stable = {
            "pattern": pattern,
            "symbol": symbol,
            "timeframe": "m5",
            "open_time": ts.isoformat(),
            "decision_time": decision_time.isoformat(),
            "direction": direction,
            "price": price_rounded,
            "angle_m5": angle_f,
            "upper_prev": upper_prev_f,
            "lower_prev": lower_prev_f,
            "upper_curr": upper_curr_f,
            "lower_curr": lower_curr_f,
            "center_curr": center_curr_f,
            "zone_k": float(zone_k),
            "trend_type": str(trend_type),
            "keep_half": bool(keep_half),
            "lr_m5_instance_id": int(lr_m5_instance_id),
        }

        out.append(
            {
                "symbol": symbol,
                "timeframe": "m5",
                "open_time": ts,
                "decision_time": decision_time,
                "direction": direction,
                "price": price_rounded,
                "pattern": pattern,
                "payload_stable": payload_stable,
            }
        )

        if direction == "long":
            long_count += 1
        else:
            short_count += 1

    return out, long_count, short_count


# 🔸 Upsert events в bt_signals_values (общий event-layer)
async def _upsert_events(
    pg,
    symbol: str,
    timeframe: str,
    event_key: str,
    event_params_hash: str,
    candidates: List[Dict[str, Any]],
) -> int:
    # условия достаточности
    if not candidates:
        return 0

    uuids: List[str] = []
    symbols: List[str] = []
    tfs: List[str] = []
    open_times: List[datetime] = []
    decision_times: List[datetime] = []
    directions: List[str] = []
    prices: List[str] = []
    patterns: List[str] = []
    payloads: List[str] = []
    event_keys: List[str] = []
    hashes: List[str] = []

    for c in candidates:
        uuids.append(str(uuid.uuid4()))
        symbols.append(str(symbol))
        tfs.append(str(timeframe))
        open_times.append(c["open_time"])
        decision_times.append(c["decision_time"])
        directions.append(str(c["direction"]))
        prices.append(str(c.get("price") or ""))
        patterns.append(str(c.get("pattern") or ""))
        payloads.append(json.dumps(c.get("payload_stable") or {}))
        event_keys.append(str(event_key))
        hashes.append(str(event_params_hash))

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            INSERT INTO {BT_SIGNAL_EVENTS_TABLE}
                (signal_uuid, symbol, timeframe, open_time, decision_time, direction, price, pattern, payload_stable, event_key, event_params_hash)
            SELECT
                u.signal_uuid,
                u.symbol,
                u.timeframe,
                u.open_time,
                u.decision_time,
                u.direction,
                NULLIF(u.price,'')::numeric,
                NULLIF(u.pattern,''),
                u.payload_stable::jsonb,
                u.event_key,
                u.event_params_hash
            FROM unnest(
                $1::uuid[],
                $2::text[],
                $3::text[],
                $4::timestamp[],
                $5::timestamp[],
                $6::text[],
                $7::text[],
                $8::text[],
                $9::text[],
                $10::text[],
                $11::text[]
            ) AS u(
                signal_uuid,
                symbol,
                timeframe,
                open_time,
                decision_time,
                direction,
                price,
                pattern,
                payload_stable,
                event_key,
                event_params_hash
            )
            ON CONFLICT (event_key, event_params_hash, symbol, timeframe, open_time, direction)
            DO NOTHING
            RETURNING id
            """,
            uuids,
            symbols,
            tfs,
            open_times,
            decision_times,
            directions,
            prices,
            patterns,
            payloads,
            event_keys,
            hashes,
        )

    return len(rows)

# 🔸 Загрузка id событий для кандидатов (для membership)
async def _load_event_ids_for_candidates(
    pg,
    symbol: str,
    timeframe: str,
    event_key: str,
    event_params_hash: str,
    candidates: List[Dict[str, Any]],
) -> Dict[Tuple[datetime, str], int]:
    # условия достаточности
    if not candidates:
        return {}

    open_times: List[datetime] = [c["open_time"] for c in candidates]
    directions: List[str] = [str(c["direction"]) for c in candidates]

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, open_time, direction
            FROM {BT_SIGNAL_EVENTS_TABLE}
            WHERE event_key = $1
              AND event_params_hash = $2
              AND symbol = $3
              AND timeframe = $4
              AND (open_time, direction) IN (
                    SELECT * FROM unnest($5::timestamp[], $6::text[])
              )
            """,
            str(event_key),
            str(event_params_hash),
            str(symbol),
            str(timeframe),
            open_times,
            directions,
        )

    out: Dict[Tuple[datetime, str], int] = {}
    for r in rows:
        out[(r["open_time"], str(r["direction"]))] = int(r["id"])
    return out


# 🔸 Вставка membership (идемпотентно)
async def _insert_membership(pg, rows: List[Tuple[Any, ...]]) -> int:
    # условия достаточности
    if not rows:
        return 0

    run_ids: List[int] = []
    signal_ids: List[int] = []
    value_ids: List[int] = []
    scenario_ids: List[Optional[int]] = []
    parent_run_ids: List[int] = []
    parent_signal_ids: List[int] = []
    winner_ids: List[Optional[int]] = []
    score_versions: List[str] = []
    winner_params: List[Optional[str]] = []
    bin_names: List[str] = []
    plugins: List[Optional[str]] = []
    plugin_params: List[Optional[str]] = []
    lr_prefixes: List[Optional[str]] = []
    lengths: List[Optional[int]] = []
    pipeline_modes: List[str] = []

    for r in rows:
        run_ids.append(int(r[0]))
        signal_ids.append(int(r[1]))
        value_ids.append(int(r[2]))
        scenario_ids.append(r[3] if r[3] is None else int(r[3]))
        parent_run_ids.append(int(r[4]))
        parent_signal_ids.append(int(r[5]))
        winner_ids.append(r[6] if r[6] is None else int(r[6]))
        score_versions.append(str(r[7]))
        winner_params.append(None if r[8] is None else str(r[8]))
        bin_names.append(str(r[9]))
        plugins.append(None if r[10] is None else str(r[10]))
        plugin_params.append(None if r[11] is None else str(r[11]))
        lr_prefixes.append(None if r[12] is None else str(r[12]))
        lengths.append(None if r[13] is None else int(r[13]))
        pipeline_modes.append(str(r[14]))

    async with pg.acquire() as conn:
        inserted_rows = await conn.fetch(
            f"""
            INSERT INTO {BT_SIGNAL_MEMBERSHIP_TABLE}
                (run_id, signal_id, signal_value_id, scenario_id, parent_run_id, parent_signal_id,
                 winner_analysis_id, score_version, winner_param, bin_name,
                 plugin, plugin_param_name, lr_prefix, length, pipeline_mode)
            SELECT
                u.run_id,
                u.signal_id,
                u.signal_value_id,
                u.scenario_id,
                u.parent_run_id,
                u.parent_signal_id,
                u.winner_analysis_id,
                u.score_version,
                u.winner_param,
                u.bin_name,
                u.plugin,
                u.plugin_param_name,
                u.lr_prefix,
                u.length,
                u.pipeline_mode
            FROM unnest(
                $1::bigint[],
                $2::int[],
                $3::int[],
                $4::int[],
                $5::bigint[],
                $6::int[],
                $7::int[],
                $8::text[],
                $9::text[],
                $10::text[],
                $11::text[],
                $12::text[],
                $13::text[],
                $14::int[],
                $15::text[]
            ) AS u(
                run_id,
                signal_id,
                signal_value_id,
                scenario_id,
                parent_run_id,
                parent_signal_id,
                winner_analysis_id,
                score_version,
                winner_param,
                bin_name,
                plugin,
                plugin_param_name,
                lr_prefix,
                length,
                pipeline_mode
            )
            ON CONFLICT (run_id, signal_id, signal_value_id) DO NOTHING
            RETURNING id
            """,
            run_ids,
            signal_ids,
            value_ids,
            scenario_ids,
            parent_run_ids,
            parent_signal_ids,
            winner_ids,
            score_versions,
            winner_params,
            bin_names,
            plugins,
            plugin_params,
            lr_prefixes,
            lengths,
            pipeline_modes,
        )

    return len(inserted_rows)
# 🔸 Загрузка LR-серии (angle/upper/lower/center) для одного инстанса / символа / окна
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


# 🔸 Загрузка OHLCV-серии для одного символа / TF / окна
async def _load_ohlcv_series(
    pg,
    symbol: str,
    timeframe: str,
    from_time: datetime,
    to_time: datetime,
) -> Dict[datetime, Tuple[float, float, float, float]]:
    # условия достаточности
    if timeframe != "m5":
        return {}

    table_name = "ohlcv_bb_m5"

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time, open, high, low, close
            FROM {table_name}
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


# 🔸 Вспомогательная функция: безопасное чтение float-параметров сигнала
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default


# 🔸 Формирование стабильного hash набора параметров детектора (для event_params_hash)
def _make_event_params_hash(
    lr_instance_id: int,
    timeframe: str,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
) -> str:
    s = f"lr={int(lr_instance_id)}|tf={str(timeframe)}|trend={str(trend_type)}|zone_k={float(zone_k)}|keep_half={bool(keep_half)}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]
