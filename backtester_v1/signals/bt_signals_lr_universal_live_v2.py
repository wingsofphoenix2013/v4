# bt_signals_lr_universal_live_v2.py — live-воркер LR universal bounce (raw) + v2-фильтрация (1/2 слоя AND) через ind_pack KV и bt_analysis_bins_labels_v2 кеш

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# 🔸 Кеши backtester_v1
from backtester_config import get_indicator_instance, get_ticker_info

# 🔸 Кеш v2 good-bins по mirror1/mirror2 (+ applied run_id)
from bt_signals_cache_config_v2 import (
    get_mirror_label_cache_v2,
    get_mirror_run_id_v2,
    load_initial_mirror_caches_v2,
)

log = logging.getLogger("BT_SIG_LR_UNI_LIVE_V2")

# 🔸 RedisTimeSeries ключи
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Redis KV ключи ind_pack (winner-driven pair key)
IND_PACK_PAIR_KEY = "ind_pack:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {"m5": 5}

# 🔸 Ожидание ind_pack (общий дедлайн на слой)
FILTER_WAIT_TOTAL_SEC = 60
FILTER_WAIT_STEP_SEC = 3

# 🔸 Защита от “догоняющих” событий
FILTER_STALE_MAX_SEC = 90

# 🔸 Таймаут обработки одного кандидата фильтра (защита от зависаний воркера)
FILTER_CANDIDATE_TIMEOUT_SEC = 90

# 🔸 Ограничение ресурсов фильтрации
FILTER_MAX_CONCURRENCY = 50
FILTER_QUEUE_MAXSIZE = 800
FILTER_WORKERS = 20

# 🔸 Таблицы
BT_SIGNALS_VALUES_TABLE = "bt_signals_values"
BT_SIGNALS_LIVE_TABLE = "bt_signals_live"

# 🔸 Контракт ind_pack v1 (success/fail)
#    {"ok": true, "bin_name": "..."}
#    {"ok": false, "reason": "...", "details": {...}}
def _parse_ind_pack_value(raw: Optional[str]) -> Dict[str, Any]:
    if raw is None:
        return {"state": "absent", "bin_name": None, "reason": None, "details": None}

    s = str(raw)
    try:
        obj = json.loads(s)
    except Exception:
        return {
            "state": "fail",
            "bin_name": None,
            "reason": "invalid_input_value",
            "details": {"kind": "invalid_json", "raw": s[:500]},
        }

    ok = obj.get("ok")
    if ok is True:
        bn = obj.get("bin_name")
        if bn is None:
            return {"state": "fail", "bin_name": None, "reason": "internal_error", "details": {"kind": "missing_bin_name"}}
        return {"state": "ok", "bin_name": str(bn), "reason": None, "details": None}

    if ok is False:
        reason = obj.get("reason")
        details = obj.get("details")
        return {
            "state": "fail",
            "bin_name": None,
            "reason": str(reason) if reason is not None else "internal_error",
            "details": details if isinstance(details, dict) else {},
        }

    return {"state": "fail", "bin_name": None, "reason": "internal_error", "details": {"kind": "invalid_payload_shape"}}


# 🔸 Определение “permanent” причины (v2: каталог размечен; делаем эвристику)
def _is_permanent_reason(reason: Optional[str], details: Optional[Dict[str, Any]]) -> bool:
    r = str(reason or "").strip().lower()
    d = details if isinstance(details, dict) else {}

    # флаг из внешнего модуля (если есть)
    if d.get("permanent") is True:
        return True

    # эвристики по имени причины
    if r.startswith("permanent_") or r.startswith("perm_"):
        return True
    if "permanent" in r:
        return True

    # не распознали -> считаем transient (ждём)
    return False


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

        return True, "bounce_ok", {"threshold": threshold}

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

        return True, "bounce_ok", {"threshold": threshold}

    return False, "invalid_direction", {"direction": direction}


# 🔸 Upsert в bt_signals_live (не затираем status='signal_sent')
async def _upsert_live_log(
    pg,
    signal_id: int,
    symbol: str,
    timeframe: str,
    open_time: datetime,
    decision_time: datetime,
    status: str,
    details: Dict[str, Any],
    processed: bool = True,
) -> bool:
    payload = json.dumps(details, ensure_ascii=False)

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            INSERT INTO {BT_SIGNALS_LIVE_TABLE} (signal_id, symbol, timeframe, open_time, decision_time, status, details, processed)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            ON CONFLICT (signal_id, symbol, timeframe, open_time)
            DO UPDATE
               SET decision_time = EXCLUDED.decision_time,
                   status = EXCLUDED.status,
                   details = EXCLUDED.details,
                   processed = EXCLUDED.processed
            WHERE {BT_SIGNALS_LIVE_TABLE}.status <> 'signal_sent'
            RETURNING status
            """,
            int(signal_id),
            str(symbol),
            str(timeframe),
            open_time,
            decision_time,
            str(status),
            payload,
            bool(processed),
        )

    if not rows:
        return False

    st = str(rows[0]["status"] or "")
    return st == "signal_sent"


# 🔸 Вставка live-сигнала в bt_signals_values (idempotent), публикация в signals_stream только при новом insert
async def _persist_live_signal(
    pg,
    redis,
    signal_id: int,
    symbol: str,
    timeframe: str,
    direction: str,
    open_time: datetime,
    decision_time: datetime,
    message: str,
    raw_message: Dict[str, Any],
) -> bool:
    signal_uuid = str(uuid.uuid4())
    raw_json = json.dumps(raw_message, ensure_ascii=False)

    inserted = False
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {BT_SIGNALS_VALUES_TABLE}
                (signal_uuid, signal_id, symbol, timeframe, open_time, decision_time, direction, message, raw_message, first_backfill_run_id)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NULL)
            ON CONFLICT (signal_id, symbol, timeframe, open_time, direction) DO NOTHING
            RETURNING id
            """,
            signal_uuid,
            int(signal_id),
            str(symbol),
            str(timeframe),
            open_time,
            decision_time,
            str(direction),
            str(message),
            raw_json,
        )
        inserted = row is not None

    if inserted:
        now_iso = datetime.utcnow().isoformat()
        try:
            await redis.xadd(
                "signals_stream",
                {
                    "message": str(message),
                    "symbol": str(symbol),
                    "bar_time": open_time.isoformat(),
                    "sent_at": now_iso,
                    "received_at": now_iso,
                    "source": "backtester_v1",
                },
            )
        except Exception as e:
            log.error(
                "BT_SIG_LR_UNI_LIVE_V2: failed to publish to signals_stream: %s (signal_id=%s, symbol=%s, time=%s)",
                e,
                signal_id,
                symbol,
                open_time.isoformat(),
                exc_info=True,
            )

    return inserted


# 🔸 Сборка ind_pack ключа
def _ind_pack_key(analysis_id: int, scenario_id: int, signal_id: int, direction: str, symbol: str, timeframe: str) -> str:
    return IND_PACK_PAIR_KEY.format(
        analysis_id=int(analysis_id),
        scenario_id=int(scenario_id),
        signal_id=int(signal_id),
        direction=str(direction),
        symbol=str(symbol),
        timeframe=str(timeframe),
    )


# 🔸 Структура слоя фильтра
def _build_filter_layer_from_cache(
    mirror_scenario_id: int,
    mirror_signal_id: int,
    direction: str,
) -> Optional[Dict[str, Any]]:
    req, good_map = get_mirror_label_cache_v2(mirror_scenario_id, mirror_signal_id, direction)
    run_id = get_mirror_run_id_v2(mirror_scenario_id, mirror_signal_id, direction)

    # условия достаточности
    if not req or not good_map:
        return None

    # в winner-driven режиме обычно одна пара (analysis_id, timeframe)
    return {
        "mirror_scenario_id": int(mirror_scenario_id),
        "mirror_signal_id": int(mirror_signal_id),
        "direction": str(direction),
        "applied_run_id": int(run_id) if run_id is not None else None,
        "required_pairs": set(req),
        "good_bins_map": {k: set(v) for k, v in good_map.items()},
    }


# 🔸 Инициализация live-контекста (один ctx на key=lr_universal_v2)
async def init_lr_universal_live_v2(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    if not signals:
        raise RuntimeError("init_lr_universal_live_v2: empty signals list")

    # стартовая загрузка кешей (вдруг watcher ещё не успел)
    await load_initial_mirror_caches_v2(pg)

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    lr_instance_id = None

    raw_cnt = 0
    filt1_cnt = 0
    filt2_cnt = 0

    for s in signals:
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # indicator (LR instance_id)
        try:
            lr_cfg = params["indicator"]
            lr_id = int(lr_cfg.get("value"))
        except Exception:
            raise RuntimeError(f"init_lr_universal_live_v2: signal_id={sid} missing/invalid param 'indicator'")

        # direction
        try:
            dm_cfg = params["direction_mask"]
            direction = str(dm_cfg.get("value") or "").strip().lower()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live_v2: signal_id={sid} missing/invalid param 'direction_mask'")

        if direction not in ("long", "short"):
            raise RuntimeError(f"init_lr_universal_live_v2: signal_id={sid} invalid direction_mask={direction}")

        # message
        try:
            msg_cfg = params["message"]
            message = str(msg_cfg.get("value") or "").strip()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live_v2: signal_id={sid} missing/invalid param 'message'")

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

        # mirror layer 1 (optional)
        m1_sc = None
        m1_si = None
        m1s_cfg = params.get("mirror1_scenario_id")
        m1i_cfg = params.get("mirror1_signal_id")
        if m1s_cfg and m1i_cfg:
            try:
                m1_sc = int(m1s_cfg.get("value"))
                m1_si = int(m1i_cfg.get("value"))
            except Exception:
                m1_sc = None
                m1_si = None

        # mirror layer 2 (optional)
        m2_sc = None
        m2_si = None
        m2s_cfg = params.get("mirror2_scenario_id")
        m2i_cfg = params.get("mirror2_signal_id")
        if m2s_cfg and m2i_cfg:
            try:
                m2_sc = int(m2s_cfg.get("value"))
                m2_si = int(m2i_cfg.get("value"))
            except Exception:
                m2_sc = None
                m2_si = None

        has_layer1 = m1_sc is not None and m1_si is not None and int(m1_sc) > 0 and int(m1_si) > 0
        has_layer2 = m2_sc is not None and m2_si is not None and int(m2_sc) > 0 and int(m2_si) > 0

        filter_mode = "raw"
        if has_layer1 and has_layer2:
            filter_mode = "filter2"
            filt2_cnt += 1
        elif has_layer1:
            filter_mode = "filter1"
            filt1_cnt += 1
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
                "filter_mode": filter_mode,
                "mirror1": {"scenario_id": m1_sc, "signal_id": m1_si} if has_layer1 else None,
                "mirror2": {"scenario_id": m2_sc, "signal_id": m2_si} if has_layer2 else None,
            }
        )

        if timeframe is None:
            timeframe = tf
        if lr_instance_id is None:
            lr_instance_id = lr_id

    # условия достаточности
    if timeframe is None or timeframe != "m5":
        raise RuntimeError(f"init_lr_universal_live_v2: unsupported timeframe={timeframe} (expected m5)")

    for c in cfgs:
        if c["timeframe"] != timeframe:
            raise RuntimeError("init_lr_universal_live_v2: mixed timeframes in signals")
        if c["lr_instance_id"] != lr_instance_id:
            raise RuntimeError("init_lr_universal_live_v2: mixed lr_instance_id in signals")

    # indicator naming: base = f"{indicator}{length}" (как в indicators_v4)
    ind_inst = get_indicator_instance(int(lr_instance_id))
    if not ind_inst:
        raise RuntimeError(f"init_lr_universal_live_v2: indicator instance_id={lr_instance_id} not found in cache")

    indicator = str(ind_inst.get("indicator") or "").strip().lower()
    ind_params = ind_inst.get("params") or {}

    if "length" in ind_params:
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
        raise RuntimeError(f"init_lr_universal_live_v2: unknown timeframe step for tf={timeframe}")

    # фильтрация: очередь + воркеры
    filter_queue: asyncio.Queue = asyncio.Queue(maxsize=FILTER_QUEUE_MAXSIZE)
    filter_sema = asyncio.Semaphore(FILTER_MAX_CONCURRENCY)

    filter_workers: List[asyncio.Task] = []
    for i in range(FILTER_WORKERS):
        task = asyncio.create_task(
            _filter_worker_loop(pg, redis, filter_queue, filter_sema),
            name=f"BT_LR_UNI_V2_FILTER_WORKER_{i}",
        )
        filter_workers.append(task)

    log.info(
        "BT_SIG_LR_UNI_LIVE_V2: init ok — signals=%s (raw=%s, filter1=%s, filter2=%s), tf=%s, lr_instance_id=%s, indicator_base=%s, "
        "filter_workers=%s, filter_queue_max=%s, filter_max_concurrency=%s",
        len(cfgs),
        raw_cnt,
        filt1_cnt,
        filt2_cnt,
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
            "blocked_no_cache": 0,
            "blocked_timeout": 0,
            "blocked_no_good": 0,
            "blocked_not_good": 0,
            "dropped_stale": 0,
            "dropped_overload": 0,
            "ignored_total": 0,
            "errors_total": 0,
        },
    }


# 🔸 Обработка одного ready-сообщения из indicator_stream (быстро, без ожиданий)
async def handle_lr_universal_indicator_ready_v2(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    # live dispatcher ожидает список, но мы пишем всё сами (в БД и live-лог), поэтому возвращаем []
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

    # жёсткая фильтрация входного события
    if status != "ready" or timeframe != tf_expected or indicator_base != base_expected:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta
    decision_time = open_time + step_delta

    # stale: если событие слишком старое относительно decision_time
    now_utc = datetime.utcnow().replace(tzinfo=None)
    if (now_utc - decision_time).total_seconds() > FILTER_STALE_MAX_SEC:
        ctx["counters"]["dropped_stale"] = int(ctx["counters"].get("dropped_stale", 0)) + 1
        return []

    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)
    decision_ms = _to_ms_utc(decision_time)

    # читаем OHLCV close и LR-канал из Redis TS по prev/curr
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
            "BT_SIG_LR_UNI_LIVE_V2: redis pipeline error — symbol=%s, open_time=%s, err=%s",
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

    # precision цены для logs/details
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    base_details = {
        "event": {"status": status, "symbol": symbol, "indicator": indicator_base, "open_time": open_time_iso, "timeframe": timeframe},
        "ts": {
            "ts_ms": ts_ms,
            "prev_ms": prev_ms,
            "decision_ms": decision_ms,
            "open_time": open_time.isoformat(),
            "prev_time": prev_time.isoformat(),
            "decision_time": decision_time.isoformat(),
        },
        "ohlcv": {
            "close_prev": _round_price(float(close_prev), precision_price) if close_prev is not None else None,
            "close_curr": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
        },
        "lr": {
            "angle_curr": float(angle_curr) if angle_curr is not None else None,
            "upper_prev": float(upper_prev) if upper_prev is not None else None,
            "lower_prev": float(lower_prev) if lower_prev is not None else None,
            "center_curr": float(center_curr) if center_curr is not None else None,
            "upper_curr": float(upper_curr) if upper_curr is not None else None,
            "lower_curr": float(lower_curr) if lower_curr is not None else None,
        },
    }

    if missing:
        details = {**base_details, "result": {"passed": False, "status": "data_missing", "missing": missing}}
        for scfg in ctx.get("signals") or []:
            await _upsert_live_log(pg, int(scfg["signal_id"]), symbol, timeframe, open_time, decision_time, "data_missing", {**details, "signal": scfg})
        return []

    H = float(upper_prev) - float(lower_prev)
    if H <= 0:
        details = {**base_details, "result": {"passed": False, "status": "invalid_channel_height", "H": float(H)}}
        for scfg in ctx.get("signals") or []:
            await _upsert_live_log(pg, int(scfg["signal_id"]), symbol, timeframe, open_time, decision_time, "invalid_channel_height", {**details, "signal": scfg})
        return []

    # Один read → обработка всех инстансов (raw + filter1 + filter2)
    for scfg in ctx.get("signals") or []:
        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        trend_type = str(scfg["trend_type"])
        zone_k = float(scfg["zone_k"])
        keep_half = bool(scfg["keep_half"])
        filter_mode = str(scfg.get("filter_mode") or "raw")

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

        if not passed:
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                decision_time=decision_time,
                status=eval_status,
                details={
                    **base_details,
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "trend_type": trend_type, "zone_k": zone_k, "keep_half": keep_half, "filter_mode": filter_mode},
                    "result": {"passed": False, "status": eval_status, "extra": eval_extra},
                },
            )
            continue

        # bounce прошёл
        if filter_mode == "raw":
            details = {
                **base_details,
                "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": "raw"},
                "result": {"passed": True, "status": "signal_sent"},
            }
            should_send = await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, decision_time, "signal_sent", details)
            if should_send:
                raw_message = {
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open_time": open_time.isoformat(),
                    "decision_time": decision_time.isoformat(),
                    "direction": direction,
                    "message": message,
                    "source": "backtester_v1",
                    "mode": "live_raw_v2",
                }
                await _persist_live_signal(pg, redis, signal_id, symbol, timeframe, direction, open_time, decision_time, message, raw_message)
                ctx["counters"]["raw_sent_total"] = int(ctx["counters"].get("raw_sent_total", 0)) + 1
                log.info(
                    "BT_SIG_LR_UNI_LIVE_V2: signal_sent RAW — signal_id=%s %s %s %s",
                    signal_id,
                    symbol,
                    direction,
                    open_time.isoformat(),
                )
            continue

        # filter1 / filter2 — enqueue на фильтрацию (не блокируем indicator_stream)
        q: asyncio.Queue = ctx["filter_queue"]
        try:
            q.put_nowait(
                {
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "direction": direction,
                    "open_time": open_time,
                    "decision_time": decision_time,
                    "message": message,
                    "filter_mode": filter_mode,
                    "mirror1": scfg.get("mirror1"),
                    "mirror2": scfg.get("mirror2"),
                    "detected_at": now_utc,
                    "base_details": base_details,
                }
            )
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                decision_time=decision_time,
                status="filter_waiting",
                details={
                    **base_details,
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode, "mirror1": scfg.get("mirror1"), "mirror2": scfg.get("mirror2")},
                    "filter": {"rule": "enqueued", "deadline_sec": FILTER_WAIT_TOTAL_SEC, "step_sec": FILTER_WAIT_STEP_SEC},
                },
            )
        except asyncio.QueueFull:
            ctx["counters"]["dropped_overload"] = int(ctx["counters"].get("dropped_overload", 0)) + 1
            await _upsert_live_log(
                pg=pg,
                signal_id=signal_id,
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                decision_time=decision_time,
                status="dropped_overload",
                details={
                    **base_details,
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                    "filter": {"rule": "queue_full", "queue_maxsize": FILTER_QUEUE_MAXSIZE},
                },
            )

    return []


# 🔸 Воркер фильтрации: отдельные задачи, дедлайн (общий на слой), без блокировки indicator_stream
async def _filter_worker_loop(pg, redis, queue: asyncio.Queue, sema: asyncio.Semaphore) -> None:
    while True:
        candidate = await queue.get()

        # базовые поля кандидата (нужны для финального статуса при ошибке/таймауте)
        signal_id = int((candidate or {}).get("signal_id") or 0)
        symbol = str((candidate or {}).get("symbol") or "")
        timeframe = str((candidate or {}).get("timeframe") or "")
        direction = str((candidate or {}).get("direction") or "")
        open_time = (candidate or {}).get("open_time")
        decision_time = (candidate or {}).get("decision_time") or open_time
        message = str((candidate or {}).get("message") or "")
        filter_mode = str((candidate or {}).get("filter_mode") or "")
        mirror1 = (candidate or {}).get("mirror1")
        mirror2 = (candidate or {}).get("mirror2")
        detected_at = (candidate or {}).get("detected_at")
        base_details = (candidate or {}).get("base_details") or {}

        try:
            async with sema:
                # таймаут нужен, чтобы зависшие await (Redis/PG) не оставляли filter_waiting навсегда
                await asyncio.wait_for(
                    _process_filter_candidate(pg, redis, candidate),
                    timeout=FILTER_CANDIDATE_TIMEOUT_SEC,
                )

        except asyncio.TimeoutError:
            # если обработка кандидата зависла — фиксируем финальный статус в bt_signals_live
            try:
                await _upsert_live_log(
                    pg=pg,
                    signal_id=signal_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    open_time=open_time,
                    decision_time=decision_time,
                    status="filter_timeout",
                    details={
                        **base_details,
                        "signal": {
                            "signal_id": signal_id,
                            "direction": direction,
                            "message": message,
                            "filter_mode": filter_mode,
                            "mirror1": mirror1,
                            "mirror2": mirror2,
                        },
                        "filter": {
                            "rule": "worker_timeout",
                            "timeout_sec": int(FILTER_CANDIDATE_TIMEOUT_SEC),
                            "detected_at": (detected_at.isoformat() if isinstance(detected_at, datetime) else None),
                        },
                    },
                    processed=True,
                )
                log.info(
                    "BT_SIG_LR_UNI_LIVE_V2: filter candidate timeout — signal_id=%s %s %s %s mode=%s",
                    signal_id,
                    symbol,
                    direction,
                    (open_time.isoformat() if isinstance(open_time, datetime) else str(open_time)),
                    filter_mode,
                )
            except Exception as e:
                log.error("BT_SIG_LR_UNI_LIVE_V2: failed to persist filter_timeout status: %s", e, exc_info=True)

        except Exception as e:
            # любая ошибка воркера должна приводить к финальному статусу, иначе filter_waiting остаётся навсегда
            log.error("BT_SIG_LR_UNI_LIVE_V2: filter worker error: %s", e, exc_info=True)
            try:
                await _upsert_live_log(
                    pg=pg,
                    signal_id=signal_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    open_time=open_time,
                    decision_time=decision_time,
                    status="filter_error",
                    details={
                        **base_details,
                        "signal": {
                            "signal_id": signal_id,
                            "direction": direction,
                            "message": message,
                            "filter_mode": filter_mode,
                            "mirror1": mirror1,
                            "mirror2": mirror2,
                        },
                        "filter": {
                            "rule": "worker_exception",
                            "error": str(e),
                            "detected_at": (detected_at.isoformat() if isinstance(detected_at, datetime) else None),
                        },
                    },
                    processed=True,
                )
                log.info(
                    "BT_SIG_LR_UNI_LIVE_V2: filter candidate error marked — signal_id=%s %s %s %s mode=%s",
                    signal_id,
                    symbol,
                    direction,
                    (open_time.isoformat() if isinstance(open_time, datetime) else str(open_time)),
                    filter_mode,
                )
            except Exception as ee:
                log.error("BT_SIG_LR_UNI_LIVE_V2: failed to persist filter_error status: %s", ee, exc_info=True)

        finally:
            queue.task_done()


# 🔸 Обработка одного кандидата фильтра (1 сигнал) с дедлайном
async def _process_filter_candidate(pg, redis, c: Dict[str, Any]) -> None:
    signal_id = int(c["signal_id"])
    symbol = str(c["symbol"])
    timeframe = str(c["timeframe"])
    direction = str(c["direction"])
    open_time: datetime = c["open_time"]
    decision_time: datetime = c.get("decision_time") or open_time
    message = str(c.get("message") or "")
    filter_mode = str(c.get("filter_mode") or "filter1")

    mirror1 = c.get("mirror1") or {}
    mirror2 = c.get("mirror2") or {}

    detected_at: datetime = c.get("detected_at") or datetime.utcnow().replace(tzinfo=None)
    base_details = c.get("base_details") or {}

    started_at = datetime.utcnow().replace(tzinfo=None)
    queue_delay_sec = (started_at - detected_at).total_seconds()

    if queue_delay_sec > FILTER_STALE_MAX_SEC:
        await _upsert_live_log(
            pg,
            signal_id,
            symbol,
            timeframe,
            open_time,
            decision_time,
            "dropped_stale_backlog",
            {
                **base_details,
                "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                "filter": {"rule": "dropped_stale_backlog", "queue_delay_sec": float(queue_delay_sec), "stale_max_sec": FILTER_STALE_MAX_SEC},
            },
        )
        return

    # layers list (1 or 2)
    layers: List[Dict[str, Any]] = []
    if filter_mode in ("filter1", "filter2"):
        if mirror1.get("scenario_id") and mirror1.get("signal_id"):
            layers.append({"layer": 1, "scenario_id": int(mirror1["scenario_id"]), "signal_id": int(mirror1["signal_id"])})
        else:
            await _upsert_live_log(
                pg, signal_id, symbol, timeframe, open_time, decision_time, "blocked_no_cache",
                {**base_details, "signal": {"signal_id": signal_id, "filter_mode": filter_mode}, "filter": {"reason": "missing_mirror1"}},
            )
            return

    if filter_mode == "filter2":
        if mirror2.get("scenario_id") and mirror2.get("signal_id"):
            layers.append({"layer": 2, "scenario_id": int(mirror2["scenario_id"]), "signal_id": int(mirror2["signal_id"])})
        else:
            await _upsert_live_log(
                pg, signal_id, symbol, timeframe, open_time, decision_time, "blocked_no_cache",
                {**base_details, "signal": {"signal_id": signal_id, "filter_mode": filter_mode}, "filter": {"reason": "missing_mirror2"}},
            )
            return

    # по каждому слою: ждём ind_pack и проверяем good
    layer_results: List[Dict[str, Any]] = []

    for layer in layers:
        layer_no = int(layer["layer"])
        ms = int(layer["scenario_id"])
        si = int(layer["signal_id"])

        cache_layer = _build_filter_layer_from_cache(ms, si, direction)

        if cache_layer is None:
            # пробуем освежить кеши (watcher может ещё не прогрузил)
            await load_initial_mirror_caches_v2(pg)
            cache_layer = _build_filter_layer_from_cache(ms, si, direction)

        if cache_layer is None:
            await _upsert_live_log(
                pg,
                signal_id,
                symbol,
                timeframe,
                open_time,
                decision_time,
                "blocked_no_cache",
                {
                    **base_details,
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                    "filter": {"layer": layer_no, "reason": "mirror_cache_missing", "mirror": {"scenario_id": ms, "signal_id": si}},
                },
            )
            return

        ok, info = await _wait_and_check_layer(redis, cache_layer, symbol, direction, decision_time)
        layer_results.append({"layer": layer_no, **info})

        if not ok:
            # записываем отказ и выходим
            await _upsert_live_log(
                pg,
                signal_id,
                symbol,
                timeframe,
                open_time,
                decision_time,
                str(info.get("status") or "blocked_timeout"),
                {
                    **base_details,
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                    "filter": {"layer": layer_no, "mirror": {"scenario_id": ms, "signal_id": si}, **info},
                },
            )
            return

    # все слои прошли
    details = {
        **base_details,
        "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode, "mirror1": mirror1, "mirror2": mirror2 if filter_mode == "filter2" else None},
        "filter": {"rule": "passed_all_layers", "layers": layer_results},
        "result": {"passed": True, "status": "signal_sent"},
    }

    should_send = await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, decision_time, "signal_sent", details)
    if not should_send:
        return

    # формируем raw_message, пишем в bt_signals_values
    raw_message = {
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "open_time": open_time.isoformat(),
        "decision_time": decision_time.isoformat(),
        "direction": direction,
        "message": message,
        "source": "backtester_v1",
        "mode": "live_filtered_v2",
        "filter_mode": filter_mode,
        "layers": layer_results,
    }

    await _persist_live_signal(pg, redis, signal_id, symbol, timeframe, direction, open_time, decision_time, message, raw_message)

    log.info(
        "BT_SIG_LR_UNI_LIVE_V2: signal_sent FILTERED — signal_id=%s %s %s %s mode=%s",
        signal_id,
        symbol,
        direction,
        open_time.isoformat(),
        filter_mode,
    )


# 🔸 Ожидание ind_pack по слою и проверка good bins
async def _wait_and_check_layer(
    redis,
    layer_cache: Dict[str, Any],
    symbol: str,
    direction: str,
    decision_time: datetime,
) -> Tuple[bool, Dict[str, Any]]:
    ms = int(layer_cache["mirror_scenario_id"])
    si = int(layer_cache["mirror_signal_id"])
    applied_run_id = layer_cache.get("applied_run_id")
    required_pairs: Set[Tuple[int, str]] = set(layer_cache.get("required_pairs") or set())
    good_bins_map: Dict[Tuple[int, str], Set[str]] = layer_cache.get("good_bins_map") or {}

    deadline = datetime.utcnow().replace(tzinfo=None) + timedelta(seconds=FILTER_WAIT_TOTAL_SEC)

    # ждём все required_pairs; pass = есть хотя бы один good-hit
    found_bins: Dict[Tuple[int, str], str] = {}
    good_hit = False

    attempt = 0
    while True:
        now = datetime.utcnow().replace(tzinfo=None)
        if now > deadline:
            return False, {
                "status": "blocked_missing_keys_timeout",
                "rule": "timeout_missing_keys",
                "attempt": attempt,
                "required_total": len(required_pairs),
                "found_total": len(found_bins),
                "applied_run_id": applied_run_id,
            }

        # читаем пачкой все missing keys
        missing = [p for p in required_pairs if p not in found_bins]
        if missing:
            keys = []
            for aid, tf in missing:
                keys.append(_ind_pack_key(int(aid), ms, si, direction, symbol, str(tf)))

            values = await redis.mget(keys)

            for (aid, tf), raw in zip(missing, values):
                parsed = _parse_ind_pack_value(raw)
                st = str(parsed.get("state") or "absent")

                if st == "ok":
                    bn = parsed.get("bin_name")
                    if bn is not None:
                        found_bins[(aid, tf)] = str(bn)
                    continue

                if st == "fail":
                    reason = parsed.get("reason")
                    details = parsed.get("details") if isinstance(parsed.get("details"), dict) else {}
                    if _is_permanent_reason(reason, details):
                        return False, {
                            "status": "blocked_permanent_pack_fail",
                            "rule": "permanent_pack_fail",
                            "attempt": attempt,
                            "permanent_fail": {"analysis_id": int(aid), "timeframe": str(tf), "reason": str(reason), "details": details},
                            "applied_run_id": applied_run_id,
                        }
                    # transient fail -> ждём дальше
                    continue

                # absent -> ждём

        # если все пришли — проверяем good_hit
        if len(found_bins) == len(required_pairs) and required_pairs:
            for (aid, tf), bn in found_bins.items():
                good_set = good_bins_map.get((aid, tf)) or set()
                if bn in good_set:
                    good_hit = True

            if not good_hit:
                return False, {
                    "status": "blocked_no_labels_match",
                    "rule": "no_good_match",
                    "attempt": attempt,
                    "required_total": len(required_pairs),
                    "found_total": len(found_bins),
                    "applied_run_id": applied_run_id,
                }

            return True, {
                "status": "ok",
                "rule": "all_keys_ready_and_good_hit",
                "attempt": attempt,
                "applied_run_id": applied_run_id,
                "found_bins": [{"analysis_id": int(a), "timeframe": str(tf), "bin_name": str(bn)} for (a, tf), bn in found_bins.items()],
            }

        attempt += 1
        await asyncio.sleep(FILTER_WAIT_STEP_SEC)