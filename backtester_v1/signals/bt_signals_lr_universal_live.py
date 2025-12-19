# bt_signals_lr_universal_live.py — live-воркер LR universal bounce (raw) + mirror-фильтрация (filtered) без блокировки потока indicator_stream

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# 🔸 Кеши backtester_v1
from backtester_config import get_indicator_instance, get_ticker_info

# 🔸 Кеш bad-bins для mirror-фильтрации
from bt_signals_cache_config import get_mirror_bad_cache, load_initial_mirror_caches

log = logging.getLogger("BT_SIG_LR_UNI_LIVE")

# 🔸 RedisTimeSeries ключи
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Redis ключи ind_pack
IND_PACK_STATIC_KEY = "ind_pack:{analysis_id}:{direction}:{symbol}:{timeframe}"
IND_PACK_PAIR_KEY = "ind_pack:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"

# 🔸 Настройки ожидания ключей для filtered (общий дедлайн на комплект)
FILTER_WAIT_TOTAL_SEC = 90
FILTER_WAIT_STEP_SEC = 5

# 🔸 Защита от “догоняющих” сигналов (не обрабатываем фильтрацию, если событие уже слишком старое)
FILTER_STALE_MAX_SEC = 90

# 🔸 Ограничение ресурсов фильтрации
FILTER_MAX_CONCURRENCY = 50
FILTER_QUEUE_MAXSIZE = 500
FILTER_WORKERS = 20

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {"m5": 5}


# 🔸 Инициализация live-контекста (один ctx на key=lr_universal)
async def init_lr_universal_live(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    if not signals:
        raise RuntimeError("init_lr_universal_live: empty signals list")

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    lr_instance_id = None
    raw_cnt = 0
    filt_cnt = 0

    for s in signals:
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # indicator (LR instance_id)
        try:
            lr_cfg = params["indicator"]
            lr_id = int(lr_cfg.get("value"))
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'indicator'")

        # direction
        try:
            dm_cfg = params["direction_mask"]
            direction = str(dm_cfg.get("value") or "").strip().lower()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'direction_mask'")

        if direction not in ("long", "short"):
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} invalid direction_mask={direction}")

        # message
        try:
            msg_cfg = params["message"]
            message = str(msg_cfg.get("value") or "").strip()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'message'")

        # bounce params
        trend_cfg = params.get("trend_type")
        trend_type = str((trend_cfg or {}).get("value") or "agnostic").strip().lower()
        if trend_type not in ("trend", "counter", "agnostic"):
            trend_type = "agnostic"

        zone_cfg = params.get("zone_k")
        try:
            zone_k = float(str((zone_cfg or {}).get("value") or "0"))
        except Exception:
            zone_k = 0.0
        if zone_k < 0.0:
            zone_k = 0.0
        if zone_k > 0.5:
            zone_k = 0.5

        keep_cfg = params.get("keep_half")
        keep_half_raw = str((keep_cfg or {}).get("value") or "").strip().lower()
        keep_half = keep_half_raw == "true"

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
                "trend_type": trend_type,
                "zone_k": zone_k,
                "keep_half": keep_half,
                "lr_instance_id": lr_id,
                "mirror_scenario_id": mirror_scenario_id,
                "mirror_signal_id": mirror_signal_id,
                "is_filtered": is_filtered,
            }
        )

        if timeframe is None:
            timeframe = tf
        if lr_instance_id is None:
            lr_instance_id = lr_id

    # условия достаточности
    if timeframe is None or timeframe != "m5":
        raise RuntimeError(f"init_lr_universal_live: unsupported timeframe={timeframe} (expected m5)")

    for c in cfgs:
        if c["timeframe"] != timeframe:
            raise RuntimeError("init_lr_universal_live: mixed timeframes in signals")
        if c["lr_instance_id"] != lr_instance_id:
            raise RuntimeError("init_lr_universal_live: mixed lr_instance_id in signals")

    # indicator naming: base = f"{indicator}{length}" (как в indicators_v4)
    ind_inst = get_indicator_instance(int(lr_instance_id))
    if not ind_inst:
        raise RuntimeError(f"init_lr_universal_live: indicator instance_id={lr_instance_id} not found in cache")

    indicator = str(ind_inst.get("indicator") or "").strip().lower()
    ind_params = ind_inst.get("params") or {}

    if indicator == "macd":
        fast = str(ind_params.get("fast") or "").strip()
        indicator_base = f"{indicator}{fast}" if fast else indicator
    elif "length" in ind_params:
        try:
            length_i = int(str(ind_params.get("length")))
            indicator_base = f"{indicator}{length_i}"
        except Exception:
            indicator_base = indicator
    else:
        indicator_base = indicator

    param_angle = f"{indicator_base}_angle"
    param_upper = f"{indicator_base}_upper"
    param_lower = f"{indicator_base}_lower"
    param_center = f"{indicator_base}_center"

    step_min = TF_STEP_MINUTES.get(timeframe, 0)
    if step_min <= 0:
        raise RuntimeError(f"init_lr_universal_live: unknown timeframe step for tf={timeframe}")

    # фильтрация: очередь + воркеры
    filter_queue: asyncio.Queue = asyncio.Queue(maxsize=FILTER_QUEUE_MAXSIZE)
    filter_sema = asyncio.Semaphore(FILTER_MAX_CONCURRENCY)

    filter_workers: List[asyncio.Task] = []
    for i in range(FILTER_WORKERS):
        task = asyncio.create_task(
            _filter_worker_loop(pg, redis, filter_queue, filter_sema),
            name=f"BT_LR_UNI_FILTER_WORKER_{i}",
        )
        filter_workers.append(task)

    log.info(
        "BT_SIG_LR_UNI_LIVE: init ok — signals=%s (raw=%s, filtered=%s), tf=%s, lr_instance_id=%s, indicator_base=%s, "
        "filter_workers=%s, filter_queue_max=%s, filter_max_concurrency=%s",
        len(cfgs),
        raw_cnt,
        filt_cnt,
        timeframe,
        lr_instance_id,
        indicator_base,
        FILTER_WORKERS,
        FILTER_QUEUE_MAXSIZE,
        FILTER_MAX_CONCURRENCY,
    )

    return {
        "timeframe": timeframe,
        "step_delta": timedelta(minutes=step_min),
        "lr_instance_id": int(lr_instance_id),
        "indicator_base": indicator_base,
        "param_angle": param_angle,
        "param_upper": param_upper,
        "param_lower": param_lower,
        "param_center": param_center,
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
async def handle_lr_universal_indicator_ready(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    live_signals: List[Dict[str, Any]] = []

    tf_expected = str(ctx.get("timeframe") or "m5")
    base_expected = str(ctx.get("indicator_base") or "")

    symbol = (fields.get("symbol") or "").strip()
    indicator_base = (fields.get("indicator") or "").strip()
    timeframe = (fields.get("timeframe") or "").strip().lower()
    open_time_iso = (fields.get("open_time") or "").strip()
    status = (fields.get("status") or "").strip().lower()

    ctx["counters"]["messages_total"] = int(ctx["counters"].get("messages_total", 0)) + 1

    # условия достаточности
    if not symbol or not open_time_iso or not timeframe:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    # жёсткая фильтрация входного события (в журнал не пишем, чтобы не раздувать)
    if status != "ready" or timeframe != tf_expected or indicator_base != base_expected:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta

    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)

    # 🔸 Читаем OHLCV close и LR-канал из Redis TS по prev/curr
    close_key = BB_TS_CLOSE_KEY.format(symbol=symbol, tf=tf_expected)

    angle_name = str(ctx["param_angle"])
    upper_name = str(ctx["param_upper"])
    lower_name = str(ctx["param_lower"])
    center_name = str(ctx["param_center"])

    angle_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=angle_name)
    upper_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=upper_name)
    lower_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=lower_name)
    center_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=center_name)

    pipe = redis.pipeline()
    pipe.execute_command("TS.RANGE", close_key, prev_ms, prev_ms)   # close_prev
    pipe.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)       # close_curr
    pipe.execute_command("TS.RANGE", upper_key, prev_ms, prev_ms)   # upper_prev
    pipe.execute_command("TS.RANGE", lower_key, prev_ms, prev_ms)   # lower_prev
    pipe.execute_command("TS.RANGE", angle_key, ts_ms, ts_ms)       # angle_curr
    pipe.execute_command("TS.RANGE", center_key, ts_ms, ts_ms)      # center_curr
    pipe.execute_command("TS.RANGE", upper_key, ts_ms, ts_ms)       # upper_curr (details)
    pipe.execute_command("TS.RANGE", lower_key, ts_ms, ts_ms)       # lower_curr (details)

    try:
        res = await pipe.execute()
    except Exception as e:
        ctx["counters"]["errors_total"] = int(ctx["counters"].get("errors_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE: redis pipeline error — symbol=%s, open_time=%s, err=%s",
            symbol,
            open_time_iso,
            e,
            exc_info=True,
        )
        return []

    close_prev = _extract_ts_value(res[0])
    close_curr = _extract_ts_value(res[1])
    upper_prev = _extract_ts_value(res[2])
    lower_prev = _extract_ts_value(res[3])
    angle_curr = _extract_ts_value(res[4])
    center_curr = _extract_ts_value(res[5])
    upper_curr = _extract_ts_value(res[6])
    lower_curr = _extract_ts_value(res[7])

    missing = []
    if close_prev is None:
        missing.append("close_prev")
    if close_curr is None:
        missing.append("close_curr")
    if upper_prev is None:
        missing.append("upper_prev")
    if lower_prev is None:
        missing.append("lower_prev")
    if angle_curr is None:
        missing.append("angle_curr")
    if center_curr is None:
        missing.append("center_curr")

    if missing:
        # пишем в журнал по всем инстансам только 1 раз (на случай диагностики TS дыр)
        details = {
            "event": {"status": status, "symbol": symbol, "indicator": indicator_base, "open_time": open_time_iso, "timeframe": timeframe},
            "reason": "data_missing",
            "missing": missing,
        }
        for scfg in ctx.get("signals") or []:
            await _upsert_live_log(
                pg=pg,
                signal_id=int(scfg["signal_id"]),
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="data_missing",
                details=details,
            )
        return []

    # precision цены для деталей
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    H = float(upper_prev) - float(lower_prev)

    base_details = {
        "event": {"status": status, "symbol": symbol, "indicator": indicator_base, "open_time": open_time_iso, "timeframe": timeframe},
        "ts": {"ts_ms": ts_ms, "prev_ms": prev_ms, "open_time": open_time.isoformat(), "prev_time": prev_time.isoformat()},
        "ohlcv": {
            "close_prev": _round_price(float(close_prev), precision_price),
            "close_curr": _round_price(float(close_curr), precision_price),
        },
        "lr": {
            "H": float(H),
            "angle_curr": float(angle_curr),
            "upper_prev": float(upper_prev),
            "lower_prev": float(lower_prev),
            "center_curr": float(center_curr),
            "upper_curr": float(upper_curr) if upper_curr is not None else None,
            "lower_curr": float(lower_curr) if lower_curr is not None else None,
        },
    }

    # 🔸 Один read → обработка всех инстансов (raw + filtered)
    for scfg in ctx.get("signals") or []:
        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        trend_type = str(scfg["trend_type"])
        zone_k = float(scfg["zone_k"])
        keep_half = bool(scfg["keep_half"])

        is_filtered = bool(scfg.get("is_filtered"))
        mirror_scenario_id = scfg.get("mirror_scenario_id")
        mirror_signal_id = scfg.get("mirror_signal_id")

        # bounce
        if H <= 0:
            passed = False
            eval_status = "data_missing"
            eval_extra = {"reason": "invalid_channel_height", "H": float(H)}
        else:
            passed, eval_status, eval_extra = _evaluate_lr_bounce(
                direction=direction,
                close_prev=float(close_prev),
                close_curr=float(close_curr),
                angle_curr=float(angle_curr),
                upper_prev=float(upper_prev),
                lower_prev=float(lower_prev),
                center_curr=float(center_curr),
                trend_type=trend_type,
                zone_k=zone_k,
                keep_half=keep_half,
                H=float(H),
            )

        # bounce не прошёл — просто журнал и дальше
        if not passed:
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "trend_type": trend_type,
                    "zone_k": zone_k,
                    "keep_half": keep_half,
                    "filtered": is_filtered,
                    "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id} if is_filtered else None,
                },
                "result": {"passed": False, "status": eval_status, "extra": eval_extra},
            }
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status=eval_status,
                details=details,
            )
            continue

        # bounce прошёл
        if not is_filtered:
            # raw — публикуем через bt_signals_main (возвращаем payload), журнал как раньше
            details = {
                **base_details,
                "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filtered": False},
                "result": {"passed": True, "status": "signal_sent"},
            }
            should_send = await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="signal_sent",
                details=details,
            )
            if should_send:
                live_signals.append(
                    {
                        "signal_id": signal_id,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "direction": direction,
                        "open_time": open_time,
                        "message": message,
                        "raw_message": {
                            "signal_id": signal_id,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "open_time": open_time.isoformat(),
                            "direction": direction,
                            "message": message,
                            "source": "backtester_v1",
                            "mode": "live_raw",
                        },
                    }
                )
                ctx["counters"]["raw_sent_total"] = int(ctx["counters"].get("raw_sent_total", 0)) + 1
            continue

        # filtered — создаём кандидат и отдаём в очередь (не блокируем поток)
        if mirror_scenario_id is None or mirror_signal_id is None:
            details = {
                **base_details,
                "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filtered": True},
                "filter": {"reason": "missing_mirror_params"},
            }
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="blocked_missing_keys_timeout",
                details=details,
            )
            ctx["counters"]["blocked_timeout"] = int(ctx["counters"].get("blocked_timeout", 0)) + 1
            continue

        # “не догоняем”: stale считаем от close_time бара (open_time + step), а не от open_time
        now_utc = datetime.utcnow().replace(tzinfo=None)
        decision_time = open_time + step_delta
        age_sec = (now_utc - decision_time).total_seconds()

        if age_sec > FILTER_STALE_MAX_SEC:
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                },
                "filter": {
                    "rule": "dropped_stale_backlog",
                    "age_sec": float(age_sec),
                    "stale_max_sec": FILTER_STALE_MAX_SEC,
                    "decision_time": decision_time.isoformat(),
                },
            }
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="dropped_stale_backlog",
                details=details,
            )
            ctx["counters"]["dropped_stale"] = int(ctx["counters"].get("dropped_stale", 0)) + 1
            continue

        # кеш зеркала
        required_pairs, bad_bins_map = get_mirror_bad_cache(int(mirror_scenario_id), int(mirror_signal_id), direction)
        if required_pairs is None or bad_bins_map is None:
            await load_initial_mirror_caches(pg)
            required_pairs, bad_bins_map = get_mirror_bad_cache(int(mirror_scenario_id), int(mirror_signal_id), direction)

        if not required_pairs or not bad_bins_map:
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                },
                "filter": {"reason": "mirror_cache_empty"},
            }
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="blocked_missing_keys_timeout",
                details=details,
            )
            ctx["counters"]["blocked_timeout"] = int(ctx["counters"].get("blocked_timeout", 0)) + 1
            continue

        candidate = {
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": open_time,
            "direction": direction,
            "message": message,
            "mirror_scenario_id": int(mirror_scenario_id),
            "mirror_signal_id": int(mirror_signal_id),
            "required_pairs": set(required_pairs),
            "bad_bins_map": {k: set(v) for k, v in bad_bins_map.items()},
            "detected_at": now_utc,
            "base_details": base_details,
        }

        # записываем, что начали ждать (это НЕ множит строки, т.к. unique key)
        await _upsert_live_log(
            pg=pg,
            signal_id=signal_id,
            symbol=symbol,
            timeframe=timeframe,
            open_time=open_time,
            status="filter_waiting",
            details={
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                },
                "filter": {
                    "rule": "waiting_keys",
                    "required_total": len(required_pairs),
                    "deadline_sec": FILTER_WAIT_TOTAL_SEC,
                    "step_sec": FILTER_WAIT_STEP_SEC,
                },
            },
        )

        # кладём в очередь (bounded)
        q: asyncio.Queue = ctx["filter_queue"]
        try:
            q.put_nowait(candidate)
        except asyncio.QueueFull:
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": int(mirror_scenario_id), "signal_id": int(mirror_signal_id)},
                },
                "filter": {
                    "rule": "dropped_overload",
                    "queue_maxsize": FILTER_QUEUE_MAXSIZE,
                },
            }
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="dropped_overload",
                details=details,
            )
            ctx["counters"]["dropped_overload"] = int(ctx["counters"].get("dropped_overload", 0)) + 1

    # суммирующий лог раз в 200 событий
    total = int(ctx["counters"].get("messages_total", 0))
    if total % 200 == 0:
        log.info(
            "BT_SIG_LR_UNI_LIVE: summary — messages=%s, raw_sent=%s, filt_sent=%s, blocked_bad=%s, blocked_timeout=%s, dropped_stale=%s, dropped_overload=%s, errors=%s",
            total,
            int(ctx["counters"].get("raw_sent_total", 0)),
            int(ctx["counters"].get("filtered_sent_total", 0)),
            int(ctx["counters"].get("blocked_bad", 0)),
            int(ctx["counters"].get("blocked_timeout", 0)),
            int(ctx["counters"].get("dropped_stale", 0)),
            int(ctx["counters"].get("dropped_overload", 0)),
            int(ctx["counters"].get("errors_total", 0)),
        )

    return live_signals


# 🔸 Воркер фильтрации: отдельные задачи, дедлайн 60с (общий на комплект), без блокировки indicator_stream
async def _filter_worker_loop(pg, redis, queue: asyncio.Queue, sema: asyncio.Semaphore) -> None:
    while True:
        candidate = await queue.get()
        try:
            async with sema:
                await _process_filter_candidate(pg, redis, candidate)
        except Exception as e:
            log.error("BT_SIG_LR_UNI_LIVE: filter worker error: %s", e, exc_info=True)
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

    base_details = c.get("base_details") or {}

    started_at = datetime.utcnow().replace(tzinfo=None)
    queue_delay_sec = (started_at - detected_at).total_seconds()

    # если кандидат слишком “залежался” в очереди — не обрабатываем
    if queue_delay_sec > FILTER_STALE_MAX_SEC:
        details = {
            **base_details,
            "signal": {
                "signal_id": signal_id,
                "direction": direction,
                "message": message,
                "filtered": True,
                "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
            },
            "filter": {
                "rule": "dropped_stale_backlog",
                "queue_delay_sec": float(queue_delay_sec),
                "stale_max_sec": FILTER_STALE_MAX_SEC,
            },
        }
        await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, "dropped_stale_backlog", details)
        return

    deadline = detected_at + timedelta(seconds=FILTER_WAIT_TOTAL_SEC)

    found_bins: Dict[Tuple[int, str], str] = {}
    missing_pairs: Set[Tuple[int, str]] = set(required_pairs)

    required_list = sorted(list(required_pairs), key=lambda x: (x[1], x[0]))
    required_total = len(required_list)

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
                details = {
                    **base_details,
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
                }
                await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, "blocked_bad_bin", details)
                return

        # полный комплект и bad нет — пропускаем
        if not missing_pairs:
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filtered": True,
                    "mirror": {"scenario_id": mirror_scenario_id, "signal_id": mirror_signal_id},
                },
                "filter": {
                    "rule": "no_bad_and_full_set",
                    "attempt": attempt,
                    "required_total": required_total,
                    "found_total": len(found_bins),
                    "missing_total": 0,
                },
                "result": {"passed": True, "status": "signal_sent"},
            }

            should_send = await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, "signal_sent", details)
            if should_send:
                await _publish_to_signals_stream(redis, symbol, open_time, message)
            return

        # ждём до следующего шага (если ещё есть время)
        attempt += 1
        await asyncio.sleep(FILTER_WAIT_STEP_SEC)

    # timeout — блок
    missing_sorted = sorted(list(missing_pairs), key=lambda x: (x[1], x[0]))
    details = {
        **base_details,
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
    }
    await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, "blocked_missing_keys_timeout", details)


# 🔸 Публикация filtered-live сигнала в signals_stream (без записи в bt_signals_values)
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
            "BT_SIG_LR_UNI_LIVE: failed to publish to signals_stream: %s (symbol=%s, time=%s, msg=%s)",
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
    # сначала pair-specific keys
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

    # затем static keys только для missing
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


# 🔸 Оценка bounce (1-в-1 с backfill)
def _evaluate_lr_bounce(
    direction: str,
    close_prev: float,
    close_curr: float,
    angle_curr: float,
    upper_prev: float,
    lower_prev: float,
    center_curr: float,
    trend_type: str,
    zone_k: float,
    keep_half: bool,
    H: float,
) -> Tuple[bool, str, Dict[str, Any]]:
    if trend_type == "trend":
        long_trend_ok = angle_curr > 0.0
        short_trend_ok = angle_curr < 0.0
    elif trend_type == "counter":
        long_trend_ok = angle_curr < 0.0
        short_trend_ok = angle_curr > 0.0
    else:
        long_trend_ok = True
        short_trend_ok = True

    if direction == "long":
        if not long_trend_ok:
            return False, "rejected_trend", {"angle_curr": angle_curr, "trend_type": trend_type}

        if zone_k == 0.0:
            in_zone_prev = close_prev <= lower_prev
            threshold = lower_prev
        else:
            threshold = lower_prev + zone_k * H
            in_zone_prev = close_prev <= threshold

        if not in_zone_prev:
            return False, "rejected_zone", {"threshold": threshold, "close_prev": close_prev, "lower_prev": lower_prev}

        if not (close_curr > lower_prev):
            return False, "no_bounce", {"close_curr": close_curr, "lower_prev": lower_prev}

        if keep_half and not (close_curr <= center_curr):
            return False, "rejected_keep_half", {"close_curr": close_curr, "center_curr": center_curr}

        return True, "signal_sent", {"threshold": threshold}

    if direction == "short":
        if not short_trend_ok:
            return False, "rejected_trend", {"angle_curr": angle_curr, "trend_type": trend_type}

        if zone_k == 0.0:
            in_zone_prev = close_prev >= upper_prev
            threshold = upper_prev
        else:
            threshold = upper_prev - zone_k * H
            in_zone_prev = close_prev >= threshold

        if not in_zone_prev:
            return False, "rejected_zone", {"threshold": threshold, "close_prev": close_prev, "upper_prev": upper_prev}

        if not (close_curr < upper_prev):
            return False, "no_bounce", {"close_curr": close_curr, "upper_prev": upper_prev}

        if keep_half and not (close_curr >= center_curr):
            return False, "rejected_keep_half", {"close_curr": close_curr, "center_curr": center_curr}

        return True, "signal_sent", {"threshold": threshold}

    return False, "error", {"reason": "unknown_direction", "direction": direction}


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


# 🔸 Округление цены для логов/details
def _round_price(value: float, precision_price: int) -> float:
    try:
        return float(f"{value:.{int(precision_price)}f}")
    except Exception:
        return value