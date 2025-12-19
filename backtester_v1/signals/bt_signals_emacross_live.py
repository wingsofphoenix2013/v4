# bt_signals_emacross_live.py — live-воркер EMA-cross: raw + filtered(mirror) с неблокирующей фильтрацией (queue+workers) для backtester_v1

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# 🔸 Кеши backtester_v1
from backtester_config import get_indicator_instance, get_ticker_info

# 🔸 Кеш bad-bins для mirror-фильтрации
from bt_signals_cache_config import get_mirror_bad_cache, load_initial_mirror_caches

log = logging.getLogger("BT_SIG_EMA_CROSS_LIVE")

# 🔸 RedisTimeSeries ключи (индикаторы)
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Redis ключи ind_pack
IND_PACK_STATIC_KEY = "ind_pack:{analysis_id}:{direction}:{symbol}:{timeframe}"
IND_PACK_PAIR_KEY = "ind_pack:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"

# 🔸 Настройки ожидания ключей для filtered (общий дедлайн на комплект)
FILTER_WAIT_TOTAL_SEC = 90
FILTER_WAIT_STEP_SEC = 5

# 🔸 Защита от “догоняющих” сигналов (не принимаем решения по кандидату позднее этого окна)
STALE_MAX_SEC = 90

# 🔸 Ограничение ресурсов фильтрации
FILTER_MAX_CONCURRENCY = 50
FILTER_QUEUE_MAXSIZE = 500
FILTER_WORKERS = 20

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Типы состояний EMA
EMA_STATE_ABOVE = "above"
EMA_STATE_BELOW = "below"
EMA_STATE_NEUTRAL = "neutral"


# 🔸 Инициализация live-контекста (один ctx на key=emacross)
async def init_emacross_live(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    if not signals:
        raise RuntimeError("init_emacross_live: empty signals list")

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    fast_instance_id = None
    slow_instance_id = None

    raw_cnt = 0
    filt_cnt = 0

    # разбираем инстансы сигналов
    for s in signals:
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # EMA fast/slow instance_id
        try:
            fast_cfg = params["ema_fast_instance_id"]
            slow_cfg = params["ema_slow_instance_id"]
            f_id = int(fast_cfg.get("value"))
            s_id = int(slow_cfg.get("value"))
        except Exception:
            raise RuntimeError(f"init_emacross_live: signal_id={sid} missing/invalid ema_*_instance_id params")

        # direction_mask (у directional инстанса должен быть long/short)
        try:
            dm_cfg = params["direction_mask"]
            direction = str(dm_cfg.get("value") or "").strip().lower()
        except Exception:
            raise RuntimeError(f"init_emacross_live: signal_id={sid} missing/invalid param 'direction_mask'")

        if direction not in ("long", "short"):
            raise RuntimeError(f"init_emacross_live: signal_id={sid} invalid direction_mask={direction}")

        # message
        try:
            msg_cfg = params["message"]
            message = str(msg_cfg.get("value") or "").strip()
        except Exception:
            raise RuntimeError(f"init_emacross_live: signal_id={sid} missing/invalid param 'message'")

        # mirror params (optional)
        mirror_scenario_id = None
        mirror_signal_id = None

        ms_cfg = params.get("mirror_scenario_id")
        mi_cfg = params.get("mirror_signal_id")
        if ms_cfg and mi_cfg:
            try:
                mirror_scenario_id = int(ms_cfg.get("value"))
                mirror_signal_id = int(mi_cfg.get("value"))
            except Exception:
                mirror_scenario_id = None
                mirror_signal_id = None

        is_filtered = mirror_scenario_id is not None and mirror_signal_id is not None
        if is_filtered:
            filt_cnt += 1
        else:
            raw_cnt += 1

        cfgs.append(
            {
                "signal_id": sid,
                "timeframe": tf,
                "direction": direction,
                "message": message,
                "fast_instance_id": f_id,
                "slow_instance_id": s_id,
                "mirror_scenario_id": mirror_scenario_id,
                "mirror_signal_id": mirror_signal_id,
                "is_filtered": is_filtered,
            }
        )

        if timeframe is None:
            timeframe = tf
        if fast_instance_id is None:
            fast_instance_id = f_id
        if slow_instance_id is None:
            slow_instance_id = s_id

    # условия достаточности
    if timeframe is None:
        raise RuntimeError("init_emacross_live: timeframe missing")
    if timeframe not in TF_STEP_MINUTES:
        raise RuntimeError(f"init_emacross_live: unsupported timeframe={timeframe}")

    for c in cfgs:
        if c["timeframe"] != timeframe:
            raise RuntimeError("init_emacross_live: mixed timeframes in emacross live signals")
        if c["fast_instance_id"] != fast_instance_id or c["slow_instance_id"] != slow_instance_id:
            raise RuntimeError("init_emacross_live: mixed EMA instance ids in emacross live signals")

    # indicator naming: base = ema{length}
    fast_base = _get_indicator_base_name(int(fast_instance_id))
    slow_base = _get_indicator_base_name(int(slow_instance_id))
    if not fast_base or not slow_base:
        raise RuntimeError("init_emacross_live: failed to resolve EMA base names for instances")

    # триггеримся по ready slow EMA (чтобы не удваивать поток)
    trigger_indicator = slow_base

    # per-symbol state machine
    symbol_locks: Dict[str, asyncio.Lock] = {}
    prev_state_by_symbol: Dict[str, Optional[str]] = {}

    # фильтрация: очередь + воркеры
    filter_queue: asyncio.Queue = asyncio.Queue(maxsize=FILTER_QUEUE_MAXSIZE)
    filter_sema = asyncio.Semaphore(FILTER_MAX_CONCURRENCY)

    filter_workers: List[asyncio.Task] = []
    for i in range(FILTER_WORKERS):
        task = asyncio.create_task(
            _filter_worker_loop(pg, redis, filter_queue, filter_sema),
            name=f"BT_EMA_CROSS_FILTER_WORKER_{i}",
        )
        filter_workers.append(task)

    log.info(
        "BT_SIG_EMA_CROSS_LIVE: init ok — signals=%s (raw=%s, filtered=%s), tf=%s, fast=%s(id=%s), slow=%s(id=%s), trigger=%s, "
        "filter_workers=%s, filter_queue_max=%s, filter_max_concurrency=%s",
        len(cfgs),
        raw_cnt,
        filt_cnt,
        timeframe,
        fast_base,
        fast_instance_id,
        slow_base,
        slow_instance_id,
        trigger_indicator,
        FILTER_WORKERS,
        FILTER_QUEUE_MAXSIZE,
        FILTER_MAX_CONCURRENCY,
    )

    return {
        "timeframe": timeframe,
        "step_delta": timedelta(minutes=int(TF_STEP_MINUTES[timeframe])),
        "fast_base": fast_base,
        "slow_base": slow_base,
        "trigger_indicator": trigger_indicator,
        "prev_state_by_symbol": prev_state_by_symbol,
        "symbol_locks": symbol_locks,
        "signals": cfgs,
        "filter_queue": filter_queue,
        "filter_workers": filter_workers,
        "counters": {
            "messages_total": 0,
            "raw_sent_total": 0,
            "filtered_sent_total": 0,
            "blocked_bad": 0,
            "blocked_timeout": 0,
            "dropped_stale": 0,
            "dropped_overload": 0,
            "ignored_total": 0,
            "errors_total": 0,
        },
    }


# 🔸 Обработка одного ready-сообщения из indicator_stream (быстро, без ожиданий)
async def handle_emacross_indicator_ready(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    live_signals: List[Dict[str, Any]] = []

    symbol = (fields.get("symbol") or "").strip()
    indicator_base = (fields.get("indicator") or "").strip()
    timeframe = (fields.get("timeframe") or "").strip().lower()
    open_time_iso = (fields.get("open_time") or "").strip()
    status = (fields.get("status") or "").strip().lower()

    ctx["counters"]["messages_total"] = int(ctx["counters"].get("messages_total", 0)) + 1

    # условия достаточности
    if status != "ready" or not symbol or not open_time_iso or not timeframe:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    # фильтр на нужный tf и trigger indicator
    if timeframe != str(ctx.get("timeframe")) or indicator_base != str(ctx.get("trigger_indicator")):
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    # не принимаем решение по слишком старым событиям: stale считаем от close_time бара
    now_utc = datetime.utcnow().replace(tzinfo=None)
    decision_time = open_time + ctx["step_delta"]
    age_sec = (now_utc - decision_time).total_seconds()

    if age_sec > STALE_MAX_SEC:
        details = {
            "event": {"symbol": symbol, "indicator": indicator_base, "timeframe": timeframe, "open_time": open_time_iso},
            "rule": "dropped_stale_backlog",
            "age_sec": float(age_sec),
            "stale_max_sec": STALE_MAX_SEC,
            "decision_time": decision_time.isoformat(),
        }
        for scfg in ctx.get("signals") or []:
            await _upsert_live_log(
                pg,
                int(scfg["signal_id"]),
                symbol,
                timeframe,
                open_time,
                "dropped_stale_backlog",
                details,
            )
        ctx["counters"]["dropped_stale"] = int(ctx["counters"].get("dropped_stale", 0)) + 1
        return []

    # per-symbol lock (state machine)
    lock = ctx["symbol_locks"].setdefault(symbol, asyncio.Lock())
    async with lock:
        ts_ms = _to_ms_utc(open_time)

        fast_key = IND_TS_KEY.format(symbol=symbol, tf=timeframe, param_name=str(ctx["fast_base"]))
        slow_key = IND_TS_KEY.format(symbol=symbol, tf=timeframe, param_name=str(ctx["slow_base"]))

        pipe = redis.pipeline()
        pipe.execute_command("TS.RANGE", fast_key, ts_ms, ts_ms)
        pipe.execute_command("TS.RANGE", slow_key, ts_ms, ts_ms)

        try:
            res = await pipe.execute()
        except Exception as e:
            ctx["counters"]["errors_total"] = int(ctx["counters"].get("errors_total", 0)) + 1
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(
                    pg=pg,
                    signal_id=int(scfg["signal_id"]),
                    symbol=symbol,
                    timeframe=timeframe,
                    open_time=open_time,
                    status="error",
                    details={"reason": "redis_pipeline_error", "error": str(e), "event": fields},
                )
            return []

        fast_val = _extract_ts_value(res[0])
        slow_val = _extract_ts_value(res[1])

        if fast_val is None or slow_val is None:
            details = {
                "event": {"symbol": symbol, "indicator": indicator_base, "timeframe": timeframe, "open_time": open_time_iso},
                "reason": "data_missing",
                "missing": {"fast": fast_val is None, "slow": slow_val is None},
                "fast_key": fast_key,
                "slow_key": slow_key,
            }
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(pg, int(scfg["signal_id"]), symbol, timeframe, open_time, "data_missing", details)
            return []

        # epsilon = ticksize (как в backfill)
        ticker_info = get_ticker_info(symbol) or {}
        ticksize = ticker_info.get("ticksize")
        try:
            epsilon = float(ticksize) if ticksize is not None else 0.0
        except Exception:
            epsilon = 0.0

        diff = float(fast_val) - float(slow_val)
        state = _classify_state(diff, epsilon)

        prev_state_by_symbol: Dict[str, Optional[str]] = ctx["prev_state_by_symbol"]
        prev_state = prev_state_by_symbol.get(symbol)

        details_base = {
            "event": {"symbol": symbol, "indicator": indicator_base, "timeframe": timeframe, "open_time": open_time_iso},
            "ema": {
                "fast": float(fast_val),
                "slow": float(slow_val),
                "diff": float(diff),
                "epsilon": float(epsilon),
                "state": state,
                "prev_state": prev_state,
                "fast_param": str(ctx["fast_base"]),
                "slow_param": str(ctx["slow_base"]),
            },
        }

        # нейтральная зона — состояние не меняем
        if state == EMA_STATE_NEUTRAL:
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(
                    pg, int(scfg["signal_id"]), symbol, timeframe, open_time,
                    "no_cross_neutral_zone",
                    {**details_base, "result": {"passed": False, "status": "no_cross_neutral_zone"}},
                )
            return []

        # первый раз — фиксируем состояние и не генерим кросс
        if prev_state is None:
            prev_state_by_symbol[symbol] = state
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(
                    pg, int(scfg["signal_id"]), symbol, timeframe, open_time,
                    "no_cross_state_init",
                    {**details_base, "result": {"passed": False, "status": "no_cross_state_init"}},
                )
            return []

        # состояние не изменилось — кросса нет
        if state == prev_state:
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(
                    pg, int(scfg["signal_id"]), symbol, timeframe, open_time,
                    "no_cross_state_unchanged",
                    {**details_base, "result": {"passed": False, "status": "no_cross_state_unchanged"}},
                )
            return []

        # есть смена состояния
        cross_dir: Optional[str] = None
        if prev_state == EMA_STATE_BELOW and state == EMA_STATE_ABOVE:
            cross_dir = "long"
        elif prev_state == EMA_STATE_ABOVE and state == EMA_STATE_BELOW:
            cross_dir = "short"

        # всегда обновляем prev_state при смене non-neutral
        prev_state_by_symbol[symbol] = state

        if cross_dir is None:
            for scfg in ctx.get("signals") or []:
                await _upsert_live_log(
                    pg, int(scfg["signal_id"]), symbol, timeframe, open_time,
                    "no_cross_state_changed",
                    {**details_base, "result": {"passed": False, "status": "no_cross_state_changed"}},
                )
            return []

        # раскладываем по directional инстансам
        for scfg in ctx.get("signals") or []:
            signal_id = int(scfg["signal_id"])
            inst_dir = str(scfg["direction"])
            message = str(scfg["message"])
            is_filtered = bool(scfg.get("is_filtered"))

            mirror_scenario_id = scfg.get("mirror_scenario_id")
            mirror_signal_id = scfg.get("mirror_signal_id")

            # кросс не в нужную сторону
            if inst_dir != cross_dir:
                await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "cross_rejected_direction_mask",
                    {
                        **details_base,
                        "signal": {"signal_id": signal_id, "direction": inst_dir, "message": message, "filtered": is_filtered},
                        "result": {"passed": False, "status": "cross_rejected_direction_mask", "cross_dir": cross_dir},
                    },
                )
                continue

            # raw — отдаём наружу (bt_signals_main опубликует и залогирует в bt_signals_values)
            if not is_filtered:
                should_send = await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "signal_sent",
                    {
                        **details_base,
                        "signal": {"signal_id": signal_id, "direction": inst_dir, "message": message, "filtered": False},
                        "result": {"passed": True, "status": "signal_sent"},
                    },
                )
                if should_send:
                    live_signals.append(
                        {
                            "signal_id": signal_id,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "direction": inst_dir,
                            "open_time": open_time,
                            "message": message,
                            "raw_message": {
                                "signal_id": signal_id,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "open_time": open_time.isoformat(),
                                "direction": inst_dir,
                                "message": message,
                                "source": "backtester_v1",
                                "mode": "live_raw",
                                "ema": {"fast": float(fast_val), "slow": float(slow_val), "diff": float(diff), "epsilon": float(epsilon)},
                            },
                        }
                    )
                    ctx["counters"]["raw_sent_total"] = int(ctx["counters"].get("raw_sent_total", 0)) + 1
                continue

            # filtered — enqueue на фильтрацию
            if mirror_scenario_id is None or mirror_signal_id is None:
                await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "blocked_missing_keys_timeout",
                    {
                        **details_base,
                        "signal": {"signal_id": signal_id, "direction": inst_dir, "message": message, "filtered": True},
                        "filter": {"reason": "missing_mirror_params"},
                    },
                )
                ctx["counters"]["blocked_timeout"] = int(ctx["counters"].get("blocked_timeout", 0)) + 1
                continue

            required_pairs, bad_bins_map = get_mirror_bad_cache(int(mirror_scenario_id), int(mirror_signal_id), inst_dir)
            if required_pairs is None or bad_bins_map is None:
                await load_initial_mirror_caches(pg)
                required_pairs, bad_bins_map = get_mirror_bad_cache(int(mirror_scenario_id), int(mirror_signal_id), inst_dir)

            if not required_pairs or not bad_bins_map:
                await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "blocked_missing_keys_timeout",
                    {
                        **details_base,
                        "signal": {"signal_id": signal_id, "direction": inst_dir, "message": message, "filtered": True},
                        "filter": {"reason": "mirror_cache_empty"},
                    },
                )
                ctx["counters"]["blocked_timeout"] = int(ctx["counters"].get("blocked_timeout", 0)) + 1
                continue

            # waiting
            await _upsert_live_log(
                pg,
                signal_id,
                symbol,
                timeframe,
                open_time,
                "filter_waiting",
                {
                    **details_base,
                    "signal": {
                        "signal_id": signal_id,
                        "direction": inst_dir,
                        "message": message,
                        "filtered": True,
                        "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                    },
                    "filter": {"rule": "waiting_keys", "required_total": len(required_pairs), "deadline_sec": FILTER_WAIT_TOTAL_SEC},
                },
            )

            candidate = {
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "open_time": open_time,
                "direction": inst_dir,
                "message": message,
                "mirror_scenario_id": int(mirror_scenario_id),
                "mirror_signal_id": int(mirror_signal_id),
                "required_pairs": set(required_pairs),
                "bad_bins_map": {k: set(v) for k, v in bad_bins_map.items()},
                "detected_at": now_utc,
                "details_base": details_base,
            }

            q: asyncio.Queue = ctx["filter_queue"]
            try:
                q.put_nowait(candidate)
            except asyncio.QueueFull:
                await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "dropped_overload",
                    {
                        **details_base,
                        "signal": {
                            "signal_id": signal_id,
                            "direction": inst_dir,
                            "message": message,
                            "filtered": True,
                            "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                        },
                        "filter": {"rule": "dropped_overload", "queue_maxsize": FILTER_QUEUE_MAXSIZE},
                    },
                )
                ctx["counters"]["dropped_overload"] = int(ctx["counters"].get("dropped_overload", 0)) + 1

    return live_signals


# 🔸 Фильтр-воркер: отдельные задачи, дедлайн 60с (общий на комплект)
async def _filter_worker_loop(pg, redis, queue: asyncio.Queue, sema: asyncio.Semaphore) -> None:
    while True:
        candidate = await queue.get()
        try:
            async with sema:
                await _process_filter_candidate(pg, redis, candidate)
        except Exception as e:
            log.error("BT_SIG_EMA_CROSS_LIVE: filter worker error: %s", e, exc_info=True)
        finally:
            queue.task_done()


# 🔸 Обработка одного кандидата фильтра (1 сигнал) с дедлайном
async def _process_filter_candidate(pg, redis, c: Dict[str, Any]) -> None:
    signal_id = int(c["signal_id"])
    symbol = str(c["symbol"])
    timeframe = str(c["timeframe"])
    open_time: datetime = c["open_time"]
    direction = str(c["direction"])
    message = str(c["message"])

    mirror_scenario_id = int(c["mirror_scenario_id"])
    mirror_signal_id = int(c["mirror_signal_id"])

    required_pairs: Set[Tuple[int, str]] = set(c.get("required_pairs") or set())
    bad_bins_map: Dict[Tuple[int, str], Set[str]] = c.get("bad_bins_map") or {}
    detected_at: datetime = c.get("detected_at") or datetime.utcnow().replace(tzinfo=None)
    details_base = c.get("details_base") or {}

    started_at = datetime.utcnow().replace(tzinfo=None)
    queue_delay_sec = (started_at - detected_at).total_seconds()

    # stale по задержке в очереди
    if queue_delay_sec > STALE_MAX_SEC:
        await _upsert_live_log(
            pg,
            signal_id,
            symbol,
            timeframe,
            open_time,
            "dropped_stale_backlog",
            {
                **details_base,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
                },
                "filter": {"rule": "dropped_stale_backlog", "queue_delay_sec": float(queue_delay_sec), "stale_max_sec": STALE_MAX_SEC},
            },
        )
        return

    deadline = detected_at + timedelta(seconds=FILTER_WAIT_TOTAL_SEC)

    found_bins: Dict[Tuple[int, str], str] = {}
    missing_pairs: Set[Tuple[int, str]] = set(required_pairs)

    required_total = len(required_pairs)
    attempt = 0

    while True:
        now = datetime.utcnow().replace(tzinfo=None)
        if now > deadline:
            break

        # читаем missing пачкой
        if missing_pairs:
            newly_found = await _read_ind_pack_bins(
                redis=redis,
                symbol=symbol,
                direction=direction,
                mirror_scenario_id=mirror_scenario_id,
                mirror_signal_id=mirror_signal_id,
                pairs=list(missing_pairs),
            )

            for pair, bn in newly_found.items():
                if bn is None:
                    continue
                found_bins[pair] = bn
                if pair in missing_pairs:
                    missing_pairs.remove(pair)

        # bad-hit — сразу стоп
        for (aid, tf), bn in found_bins.items():
            bad_set = bad_bins_map.get((aid, tf)) or set()
            if bn in bad_set:
                await _upsert_live_log(
                    pg,
                    signal_id,
                    symbol,
                    timeframe,
                    open_time,
                    "blocked_bad_bin",
                    {
                        **details_base,
                        "signal": {
                            "signal_id": signal_id,
                            "direction": direction,
                            "message": message,
                            "filtered": True,
                            "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
                        },
                        "filter": {
                            "rule": "bad_hit",
                            "attempt": attempt,
                            "required_total": required_total,
                            "found_total": len(found_bins),
                            "missing_total": len(missing_pairs),
                            "hit": {"analysis_id": int(aid), "timeframe": tf, "bin_name": bn},
                        },
                    },
                )
                return

        # полный комплект и bad нет — пропускаем
        if not missing_pairs:
            should_send = await _upsert_live_log(
                pg,
                signal_id,
                symbol,
                timeframe,
                open_time,
                "signal_sent",
                {
                    **details_base,
                    "signal": {
                        "signal_id": signal_id,
                        "direction": direction,
                        "message": message,
                        "filtered": True,
                        "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
                    },
                    "filter": {"rule": "no_bad_and_full_set", "attempt": attempt, "required_total": required_total},
                    "result": {"passed": True, "status": "signal_sent"},
                },
            )
            if should_send:
                await _publish_to_signals_stream(redis, symbol, open_time, message)
            return

        attempt += 1
        await asyncio.sleep(FILTER_WAIT_STEP_SEC)

    # timeout — блок
    missing_sorted = sorted(list(missing_pairs), key=lambda x: (x[1], x[0]))
    await _upsert_live_log(
        pg,
        signal_id,
        symbol,
        timeframe,
        open_time,
        "blocked_missing_keys_timeout",
        {
            **details_base,
            "signal": {
                "signal_id": signal_id,
                "direction": direction,
                "message": message,
                "filtered": True,
                "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
            },
            "filter": {
                "rule": "timeout_missing_keys",
                "attempt": attempt,
                "required_total": required_total,
                "found_total": len(found_bins),
                "missing_total": len(missing_pairs),
                "missing_pairs": [{"analysis_id": int(a), "timeframe": tf} for a, tf in missing_sorted],
            },
        },
    )


# 🔸 Публикация filtered-live сигнала в signals_stream
async def _publish_to_signals_stream(redis, symbol: str, open_time: datetime, message: str) -> None:
    now_iso = datetime.utcnow().isoformat()
    bar_time_iso = open_time.isoformat()

    try:
        await redis.xadd(
            "signals_stream",
            {
                "message": message,
                "symbol": symbol,
                "bar_time": bar_time_iso,
                "sent_at": now_iso,
                "received_at": now_iso,
                "source": "backtester_v1",
            },
        )
    except Exception as e:
        log.error(
            "BT_SIG_EMA_CROSS_LIVE: failed to publish to signals_stream: %s (symbol=%s, time=%s, msg=%s)",
            e,
            symbol,
            bar_time_iso,
            message,
            exc_info=True,
        )


# 🔸 Чтение ind_pack ключей: сначала pair-key, потом static-key (батч MGET)
async def _read_ind_pack_bins(
    redis,
    symbol: str,
    direction: str,
    mirror_scenario_id: int,
    mirror_signal_id: int,
    pairs: List[Tuple[int, str]],
) -> Dict[Tuple[int, str], Optional[str]]:
    # pair keys
    pair_keys: List[str] = []
    for aid, tf in pairs:
        pair_keys.append(
            IND_PACK_PAIR_KEY.format(
                analysis_id=int(aid),
                scenario_id=int(mirror_scenario_id),
                signal_id=int(mirror_signal_id),
                direction=str(direction),
                symbol=symbol,
                timeframe=str(tf),
            )
        )

    res_pair = await redis.mget(pair_keys)

    out: Dict[Tuple[int, str], Optional[str]] = {}
    missing_static: List[Tuple[int, str]] = []

    for (aid, tf), val in zip(pairs, res_pair):
        if val is not None:
            out[(aid, tf)] = str(val)
        else:
            out[(aid, tf)] = None
            missing_static.append((aid, tf))

    # static keys for missing
    if missing_static:
        static_keys: List[str] = []
        for aid, tf in missing_static:
            static_keys.append(
                IND_PACK_STATIC_KEY.format(
                    analysis_id=int(aid),
                    direction=str(direction),
                    symbol=symbol,
                    timeframe=str(tf),
                )
            )

        res_static = await redis.mget(static_keys)

        for (aid, tf), val in zip(missing_static, res_static):
            if val is not None:
                out[(aid, tf)] = str(val)

    return out


# 🔸 Классификация состояния fast vs slow по diff и epsilon
def _classify_state(diff: float, epsilon: float) -> str:
    if epsilon <= 0:
        if diff > 0:
            return EMA_STATE_ABOVE
        if diff < 0:
            return EMA_STATE_BELOW
        return EMA_STATE_NEUTRAL

    if diff > epsilon:
        return EMA_STATE_ABOVE
    if diff < -epsilon:
        return EMA_STATE_BELOW
    return EMA_STATE_NEUTRAL


# 🔸 Вытянуть “base param_name” индикатора для TS/KV (как в indicators_v4)
def _get_indicator_base_name(instance_id: int) -> str:
    inst = get_indicator_instance(int(instance_id))
    if not inst:
        return ""

    indicator = str(inst.get("indicator") or "").strip().lower()
    params = inst.get("params") or {}

    if indicator == "macd":
        fast = str(params.get("fast") or "").strip()
        return f"{indicator}{fast}" if fast else indicator

    if "length" in params:
        try:
            length = int(str(params.get("length")))
            return f"{indicator}{length}"
        except Exception:
            return indicator

    return indicator


# 🔸 Upsert в bt_signals_live (не затираем status='signal_sent')
async def _upsert_live_log(
    pg,
    signal_id: int,
    symbol: str,
    timeframe: str,
    open_time: datetime,
    status: str,
    details: Dict[str, Any],
) -> bool:
    payload = json.dumps(details, ensure_ascii=False)

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            INSERT INTO bt_signals_live (signal_id, symbol, timeframe, open_time, status, details)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            ON CONFLICT (signal_id, symbol, timeframe, open_time)
            DO UPDATE
               SET status = EXCLUDED.status,
                   details = EXCLUDED.details
            WHERE bt_signals_live.status <> 'signal_sent'
            RETURNING status
            """,
            int(signal_id),
            symbol,
            timeframe,
            open_time,
            status,
            payload,
        )

    if not rows:
        return False

    st = str(rows[0]["status"] or "")
    return st == "signal_sent"


# 🔸 Парсинг open_time ISO (UTC-naive)
def _parse_open_time_iso(open_time_iso: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(open_time_iso)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


# 🔸 Naive UTC datetime → epoch ms
def _to_ms_utc(dt_naive_utc: datetime) -> int:
    return int(dt_naive_utc.replace(tzinfo=timezone.utc).timestamp() * 1000)


# 🔸 Извлечение одного значения из TS.RANGE ответа
def _extract_ts_value(ts_range_result: Any) -> Optional[float]:
    try:
        if not ts_range_result:
            return None
        point = ts_range_result[0]
        if not point or len(point) < 2:
            return None
        return float(point[1])
    except Exception:
        return None