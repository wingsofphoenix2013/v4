# bt_signals_macdcross_live_v2.py — live-воркер MACD cross: RAW от indicator_stream + FILTER(1/2) от ind_pack_stream_ready (results_json). Принципы: 1-в-1 как bt_signals_lr_universal_live_v2.py

# 🔸 Imports
import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# 🔸 Project imports
from backtester_config import get_indicator_instance, get_ticker_info

from bt_signals_cache_config_v2 import (
    get_mirror_label_cache_v2,
    get_mirror_run_id_v2,
    load_initial_mirror_caches_v2,
)

# 🔸 Logger
log = logging.getLogger("BT_SIG_MACD_CROSS_LIVE_V2")

# 🔸 Stream keys (в live_stream_key инстанса)
INDICATOR_STREAM_KEY = "indicator_stream"
IND_PACK_READY_STREAM_KEY = "ind_pack_stream_ready"

# 🔸 RedisTimeSeries ключи
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Таймшаги TF (в минутах) — пока как в backfill (m5)
TF_STEP_MINUTES = {"m5": 5}

# 🔸 Защита от “догоняющих” событий
FILTER_STALE_MAX_SEC = 120

# 🔸 Таймаут для одиночных Redis-вызовов
REDIS_OP_TIMEOUT_SEC = 5

# 🔸 Таблицы
BT_SIGNALS_VALUES_TABLE = "bt_signals_values"
BT_SIGNALS_LIVE_TABLE = "bt_signals_live"


# 🔸 Helpers: time parsing / conversion
def _parse_open_time_iso(open_time_iso: str) -> Optional[datetime]:
    # parse ISO time, return naive UTC
    try:
        dt = datetime.fromisoformat(open_time_iso)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def _from_ms_utc(ms: int) -> Optional[datetime]:
    # ms -> naive UTC datetime
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _to_ms_utc(dt_naive_utc: datetime) -> int:
    # naive UTC datetime -> ms
    return int(dt_naive_utc.replace(tzinfo=timezone.utc).timestamp() * 1000)


# 🔸 Helpers: RedisTimeSeries extraction / formatting
def _extract_ts_value(ts_range_result: Any) -> Optional[float]:
    # TS.RANGE -> [[ts, value]]; take first point
    try:
        if not ts_range_result:
            return None
        point = ts_range_result[0]
        if not point or len(point) < 2:
            return None
        return float(point[1])
    except Exception:
        return None


def _round_price(value: float, precision_price: int) -> float:
    # round to ticker precision
    try:
        return float(f"{value:.{int(precision_price)}f}")
    except Exception:
        return value


# 🔸 MACD CROSS (1-в-1 с backfill)
def _evaluate_macd_cross(
    direction: str,
    hist_prev: float,
    hist_curr: float,
    macd_curr: float,
    min_abs_hist: float,
    require_macd_sign: bool,
) -> Tuple[bool, str, Dict[str, Any]]:
    # сила
    if float(min_abs_hist) > 0.0 and abs(float(hist_curr)) < float(min_abs_hist):
        return False, "rejected_strength", {"min_abs_hist": float(min_abs_hist), "abs_hist_curr": abs(float(hist_curr))}

    if direction == "long":
        # правило strict cross
        if not (float(hist_prev) < 0.0 and float(hist_curr) > 0.0):
            return False, "no_cross", {"rule": "hist_strict_cross", "hist_prev": float(hist_prev), "hist_curr": float(hist_curr)}
        # доп. ограничение по знаку MACD
        if require_macd_sign and not (float(macd_curr) > 0.0):
            return False, "rejected_macd_sign", {"require_macd_sign": True, "macd_curr": float(macd_curr), "expected": ">0"}
        return True, "cross_ok", {"rule": "hist_strict_cross"}

    if direction == "short":
        # правило strict cross
        if not (float(hist_prev) > 0.0 and float(hist_curr) < 0.0):
            return False, "no_cross", {"rule": "hist_strict_cross", "hist_prev": float(hist_prev), "hist_curr": float(hist_curr)}
        # доп. ограничение по знаку MACD
        if require_macd_sign and not (float(macd_curr) < 0.0):
            return False, "rejected_macd_sign", {"require_macd_sign": True, "macd_curr": float(macd_curr), "expected": "<0"}
        return True, "cross_ok", {"rule": "hist_strict_cross"}

    return False, "invalid_direction", {"direction": direction}


# 🔸 DB: upsert live log (bt_signals_live)
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


# 🔸 DB+Redis: persist signal (bt_signals_values + signals_stream)
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

    # stable key composition
    mode = str((raw_message or {}).get("mode") or "live_v2").strip().lower()
    event_key = f"macd_cross_live_{mode}_{timeframe}"

    macd_instance_id = str((raw_message or {}).get("macd_instance_id") or "")
    indicator_base = str((raw_message or {}).get("indicator_base") or "")

    min_abs_hist = str((raw_message or {}).get("min_abs_hist") or "")
    require_macd_sign = str((raw_message or {}).get("require_macd_sign") or "")
    rule = str((raw_message or {}).get("rule") or "hist_strict_cross")

    # зеркала (если filtered): краткая подпись для различения конфигов
    filter_mode = str((raw_message or {}).get("filter_mode") or "").strip().lower()
    layers = (raw_message or {}).get("layers") or []
    mirrors_sig = ""
    try:
        if isinstance(layers, list) and layers:
            ms = str(((layers[0] or {}).get("mirror") or {}).get("scenario_id") or "")
            si = str(((layers[0] or {}).get("mirror") or {}).get("signal_id") or "")
            mirrors_sig = f"{si}:{ms}"
    except Exception:
        mirrors_sig = ""

    import hashlib

    ep = (
        f"macd_instance_id={macd_instance_id}|base={indicator_base}|tf={timeframe}|"
        f"min_abs_hist={min_abs_hist}|require_macd_sign={require_macd_sign}|rule={rule}|"
        f"filter={filter_mode}|mirror={mirrors_sig}"
    )
    event_params_hash = hashlib.sha1(ep.encode("utf-8")).hexdigest()[:16]

    payload_stable_json = json.dumps(raw_message or {}, ensure_ascii=False)

    # insert into bt_signals_values with dedupe
    inserted = False
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {BT_SIGNALS_VALUES_TABLE}
                (signal_uuid, symbol, timeframe, open_time, decision_time, direction,
                 payload_stable, event_key, event_params_hash, pattern, price)
            VALUES
                ($1, $2, $3, $4, $5, $6,
                 $7::jsonb, $8, $9, $10, $11::numeric)
            ON CONFLICT (event_key, event_params_hash, symbol, timeframe, open_time, direction) DO NOTHING
            RETURNING id
            """,
            signal_uuid,
            str(symbol),
            str(timeframe),
            open_time,
            decision_time,
            str(direction),
            payload_stable_json,
            str(event_key),
            str(event_params_hash),
            "cross",
            (raw_message or {}).get("price"),
        )
        inserted = row is not None

    # publish to signals_stream only if inserted
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
                "BT_SIG_MACD_CROSS_LIVE_V2: failed to publish to signals_stream: %s (signal_id=%s, symbol=%s, time=%s)",
                e,
                signal_id,
                symbol,
                open_time.isoformat(),
                exc_info=True,
            )

    return inserted


# 🔸 FILTER: build layer cache (mirror labels)
def _build_filter_layer_from_cache(
    mirror_scenario_id: int,
    mirror_signal_id: int,
    direction: str,
) -> Optional[Dict[str, Any]]:
    req, good_map = get_mirror_label_cache_v2(mirror_scenario_id, mirror_signal_id, direction)
    run_id = get_mirror_run_id_v2(mirror_scenario_id, mirror_signal_id, direction)

    if not req or not good_map:
        return None

    return {
        "mirror_scenario_id": int(mirror_scenario_id),
        "mirror_signal_id": int(mirror_signal_id),
        "direction": str(direction).strip().lower(),
        "applied_run_id": int(run_id) if run_id is not None else None,
        "required_pairs": set(req),
        "good_bins_map": {k: set(v) for k, v in good_map.items()},
    }


# 🔸 FILTER: parse results_json (pack ready)
def _parse_results_json(raw: Optional[str]) -> Dict[str, Dict[str, Any]]:
    if raw is None:
        return {}
    s = str(raw).strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in obj.items():
        if isinstance(k, str) and isinstance(v, dict):
            out[k.strip()] = v
    return out


# 🔸 FILTER: check layer using pack results_json
def _check_layer_by_pack_results(
    layer_cache: Dict[str, Any],
    results_by_pair: Dict[str, Dict[str, Any]],
    pack_timeframe: str,
) -> Tuple[bool, Dict[str, Any]]:
    ms = int(layer_cache["mirror_scenario_id"])
    si = int(layer_cache["mirror_signal_id"])
    direction = str(layer_cache["direction"])
    applied_run_id = layer_cache.get("applied_run_id")

    pair_key = f"{si}:{ms}"
    rec = results_by_pair.get(pair_key)

    if not rec:
        return False, {
            "status": "blocked_pack_pair_missing",
            "rule": "pack_pair_missing",
            "pair_key": pair_key,
            "mirror": {"scenario_id": ms, "signal_id": si},
            "applied_run_id": applied_run_id,
        }

    ok = rec.get("ok")
    analysis_id = rec.get("analysis_id")
    run_id = rec.get("run_id")

    if ok is not True:
        reason = rec.get("reason")
        return False, {
            "status": "blocked_pack_fail",
            "rule": "pack_result_fail",
            "pair_key": pair_key,
            "mirror": {"scenario_id": ms, "signal_id": si},
            "pack_result": {
                "ok": bool(ok) if isinstance(ok, bool) else None,
                "reason": str(reason) if reason is not None else None,
                "analysis_id": analysis_id,
                "run_id": run_id,
            },
            "applied_run_id": applied_run_id,
        }

    bin_name = rec.get("bin_name")
    if bin_name is None or str(bin_name).strip() == "":
        return False, {
            "status": "blocked_pack_fail",
            "rule": "pack_missing_bin_name",
            "pair_key": pair_key,
            "mirror": {"scenario_id": ms, "signal_id": si},
            "pack_result": {"ok": True, "analysis_id": analysis_id, "run_id": run_id},
            "applied_run_id": applied_run_id,
        }

    bn = str(bin_name).strip()

    required_pairs: Set[Tuple[int, str]] = set(layer_cache.get("required_pairs") or set())
    good_bins_map: Dict[Tuple[int, str], Set[str]] = layer_cache.get("good_bins_map") or {}

    try:
        aid_i = int(analysis_id) if analysis_id is not None else None
    except Exception:
        aid_i = None

    # select required pairs matching pack timeframe (+ analysis_id if present)
    candidate_pairs: List[Tuple[int, str]] = []
    for (aid, tf) in required_pairs:
        if str(tf).strip().lower() != str(pack_timeframe).strip().lower():
            continue
        if aid_i is None or int(aid) == int(aid_i):
            candidate_pairs.append((int(aid), str(tf).strip().lower()))

    if not candidate_pairs:
        return False, {
            "status": "blocked_no_labels_match",
            "rule": "no_required_pairs_match",
            "pair_key": pair_key,
            "mirror": {"scenario_id": ms, "signal_id": si},
            "pack_result": {"ok": True, "bin_name": bn, "analysis_id": analysis_id, "run_id": run_id},
            "applied_run_id": applied_run_id,
            "required_pairs_total": len(required_pairs),
        }

    # check bin_name in good bins for any candidate pair
    good_hit = False
    checked_pairs: List[Dict[str, Any]] = []

    for (aid, tf) in candidate_pairs:
        good_set = good_bins_map.get((aid, tf)) or set()
        hit = bn in good_set
        checked_pairs.append({"analysis_id": int(aid), "timeframe": str(tf), "hit": bool(hit), "good_bins_count": len(good_set)})
        if hit:
            good_hit = True

    if not good_hit:
        return False, {
            "status": "blocked_no_labels_match",
            "rule": "no_good_match",
            "pair_key": pair_key,
            "mirror": {"scenario_id": ms, "signal_id": si},
            "pack_result": {"ok": True, "bin_name": bn, "analysis_id": analysis_id, "run_id": run_id},
            "applied_run_id": applied_run_id,
            "checked_pairs": checked_pairs,
        }

    return True, {
        "status": "ok",
        "rule": "good_match",
        "pair_key": pair_key,
        "mirror": {"scenario_id": ms, "signal_id": si},
        "applied_run_id": applied_run_id,
        "pack_result": {"ok": True, "bin_name": bn, "analysis_id": analysis_id, "run_id": run_id},
        "checked_pairs": checked_pairs,
    }


# 🔸 indicator_base для MACD (instance_id -> base)
_MACD_INSTANCE_BASE_MAP = {
    15: "macd12", 32: "macd12", 49: "macd12",
    16: "macd5",  33: "macd5",  50: "macd5",
}


# 🔸 MACD: resolve indicator_base from instance_id
def _macd_indicator_base_from_instance(instance_id: int) -> str:
    # в твоей системе это фиксировано (по твоему сообщению)
    base = _MACD_INSTANCE_BASE_MAP.get(int(instance_id))
    return str(base or "").strip()


# 🔸 Init: build ctx for live worker
async def init_macdcross_live_v2(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    if not signals:
        raise RuntimeError("init_macdcross_live_v2: empty signals list")

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    stream_key = None

    raw_cnt = 0
    filt1_cnt = 0
    filt2_cnt = 0

    # допускаем несколько macd_instance_id в одной группе, но stream_key/timeframe должны быть одинаковыми
    bases: Set[str] = set()
    instances: Set[int] = set()

    for s in signals:
        # базовые поля
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # live stream key
        lsk_cfg = params.get("live_stream_key") or params.get("stream_key")
        lsk = str((lsk_cfg or {}).get("value") or "").strip()
        if stream_key is None:
            stream_key = lsk

        # instance_id MACD
        try:
            macd_cfg = params["indicator"]
            macd_instance_id = int(macd_cfg.get("value"))
        except Exception:
            raise RuntimeError(f"init_macdcross_live_v2: signal_id={sid} missing/invalid param 'indicator'")

        indicator_base = _macd_indicator_base_from_instance(macd_instance_id)
        if not indicator_base:
            raise RuntimeError(f"init_macdcross_live_v2: unknown MACD indicator_base for instance_id={macd_instance_id}")

        # direction
        try:
            dm_cfg = params["direction_mask"]
            direction = str(dm_cfg.get("value") or "").strip().lower()
        except Exception:
            raise RuntimeError(f"init_macdcross_live_v2: signal_id={sid} missing/invalid param 'direction_mask'")

        if direction not in ("long", "short"):
            raise RuntimeError(f"init_macdcross_live_v2: signal_id={sid} invalid direction_mask={direction}")

        # message
        try:
            msg_cfg = params["message"]
            message = str(msg_cfg.get("value") or "").strip()
        except Exception:
            raise RuntimeError(f"init_macdcross_live_v2: signal_id={sid} missing/invalid param 'message'")

        # MACD params (как в backfill)
        try:
            mah_cfg = params.get("min_abs_hist")
            min_abs_hist = float(str((mah_cfg or {}).get("value") or "0"))
        except Exception:
            min_abs_hist = 0.0
        if min_abs_hist < 0.0:
            min_abs_hist = 0.0

        rms_cfg = params.get("require_macd_sign")
        require_macd_sign = str((rms_cfg or {}).get("value") or "").strip().lower() == "true"

        # mirror layer 1/2 (optional)
        m1_sc = m1_si = None
        m2_sc = m2_si = None

        m1s_cfg = params.get("mirror1_scenario_id")
        m1i_cfg = params.get("mirror1_signal_id")
        if m1s_cfg and m1i_cfg:
            try:
                m1_sc = int(m1s_cfg.get("value"))
                m1_si = int(m1i_cfg.get("value"))
            except Exception:
                m1_sc = m1_si = None

        m2s_cfg = params.get("mirror2_scenario_id")
        m2i_cfg = params.get("mirror2_signal_id")
        if m2s_cfg and m2i_cfg:
            try:
                m2_sc = int(m2s_cfg.get("value"))
                m2_si = int(m2i_cfg.get("value"))
            except Exception:
                m2_sc = m2_si = None

        has_layer1 = m1_sc is not None and m1_si is not None and int(m1_sc) > 0 and int(m1_si) > 0
        has_layer2 = m2_sc is not None and m2_si is not None and int(m2_sc) > 0 and int(m2_si) > 0

        # filter mode
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
                "macd_instance_id": int(macd_instance_id),
                "indicator_base": indicator_base,
                "min_abs_hist": float(min_abs_hist),
                "require_macd_sign": bool(require_macd_sign),
                "filter_mode": filter_mode,
                "mirror1": {"scenario_id": m1_sc, "signal_id": m1_si} if has_layer1 else None,
                "mirror2": {"scenario_id": m2_sc, "signal_id": m2_si} if has_layer2 else None,
            }
        )

        bases.add(indicator_base)
        instances.add(int(macd_instance_id))

        if timeframe is None:
            timeframe = tf

    if timeframe is None or timeframe not in TF_STEP_MINUTES:
        raise RuntimeError(f"init_macdcross_live_v2: unsupported timeframe={timeframe}")

    for c in cfgs:
        if c["timeframe"] != timeframe:
            raise RuntimeError("init_macdcross_live_v2: mixed timeframes in signals")

    stream_key = str(stream_key or "").strip()
    if stream_key not in (INDICATOR_STREAM_KEY, IND_PACK_READY_STREAM_KEY):
        raise RuntimeError(f"init_macdcross_live_v2: unsupported live_stream_key='{stream_key}'")

    # initial load caches только если поток фильтров
    if stream_key == IND_PACK_READY_STREAM_KEY:
        await load_initial_mirror_caches_v2(pg)

    step_min = TF_STEP_MINUTES.get(timeframe, 0)
    if step_min <= 0:
        raise RuntimeError(f"init_macdcross_live_v2: unknown timeframe step for tf={timeframe}")

    log.info(
        "BT_SIG_MACD_CROSS_LIVE_V2: init ok — stream=%s signals=%s (raw=%s, filter1=%s, filter2=%s), tf=%s, bases=%s, instances=%s",
        stream_key,
        len(cfgs),
        raw_cnt,
        filt1_cnt,
        filt2_cnt,
        timeframe,
        sorted(list(bases)),
        sorted(list(instances)),
    )

    return {
        "stream_key": stream_key,
        "timeframe": timeframe,
        "step_delta": timedelta(minutes=step_min),
        "signals": cfgs,
        "counters": {
            "messages_total": 0,
            "ignored_total": 0,
            "dropped_stale": 0,
            "errors_total": 0,
            "raw_sent_total": 0,
            "filtered_sent_total": 0,
        },
    }


# 🔸 Router: handle incoming live message
async def handle_macdcross_indicator_ready_v2(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    ctx["counters"]["messages_total"] = int(ctx["counters"].get("messages_total", 0)) + 1

    stream_key = str(ctx.get("stream_key") or "").strip()

    if stream_key == INDICATOR_STREAM_KEY:
        await _handle_indicator_ready_message(ctx, fields, pg, redis)
        return []

    if stream_key == IND_PACK_READY_STREAM_KEY:
        await _handle_pack_ready_message(ctx, fields, pg, redis)
        return []

    ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
    return []


# 🔸 RAW: indicator_stream handler
async def _handle_indicator_ready_message(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> None:
    tf_expected = str(ctx.get("timeframe") or "m5")

    # parse event fields
    symbol = (fields.get("symbol") or "").strip()
    indicator_base = (fields.get("indicator") or "").strip()
    timeframe = (fields.get("timeframe") or "").strip().lower()
    open_time_iso = (fields.get("open_time") or "").strip()
    status = (fields.get("status") or "").strip().lower()

    # basic validation
    if not symbol or not open_time_iso or not timeframe:
        ctx["counters"]["ignored_total"] += 1
        return

    open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None:
        ctx["counters"]["ignored_total"] += 1
        return

    # жёсткая фильтрация входного события
    if status != "ready" or timeframe != tf_expected:
        ctx["counters"]["ignored_total"] += 1
        return

    # indicator_base должен совпасть с одним из cfg (иначе игнор)
    matching_cfgs = [s for s in (ctx.get("signals") or []) if str(s.get("indicator_base") or "") == str(indicator_base)]
    if not matching_cfgs:
        ctx["counters"]["ignored_total"] += 1
        return

    # compute times
    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta
    decision_time = open_time + step_delta

    # stale protection
    now_utc = datetime.utcnow().replace(tzinfo=None)
    if (now_utc - decision_time).total_seconds() > FILTER_STALE_MAX_SEC:
        ctx["counters"]["dropped_stale"] += 1
        return

    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)

    close_key = BB_TS_CLOSE_KEY.format(symbol=symbol, tf=tf_expected)

    # читаем TS (на базе indicator_base)
    macd_name = f"{indicator_base}_macd"
    sig_name = f"{indicator_base}_macd_signal"
    hist_name = f"{indicator_base}_macd_hist"

    macd_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=macd_name)
    sig_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=sig_name)
    hist_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=hist_name)

    # redis pipeline read
    pipe = redis.pipeline()
    pipe.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)          # close_curr
    pipe.execute_command("TS.RANGE", close_key, prev_ms, prev_ms)      # close_prev (details)
    pipe.execute_command("TS.RANGE", hist_key, prev_ms, prev_ms)       # hist_prev
    pipe.execute_command("TS.RANGE", hist_key, ts_ms, ts_ms)           # hist_curr
    pipe.execute_command("TS.RANGE", macd_key, ts_ms, ts_ms)           # macd_curr
    pipe.execute_command("TS.RANGE", sig_key, ts_ms, ts_ms)            # signal_curr

    try:
        res = await asyncio.wait_for(pipe.execute(), timeout=REDIS_OP_TIMEOUT_SEC)
    except Exception as e:
        ctx["counters"]["errors_total"] += 1
        log.error(
            "BT_SIG_MACD_CROSS_LIVE_V2: redis pipeline error (indicator_stream) — symbol=%s, open_time=%s, err=%s",
            symbol,
            open_time_iso,
            e,
            exc_info=True,
        )
        return

    close_curr = _extract_ts_value(res[0])
    close_prev = _extract_ts_value(res[1])
    hist_prev = _extract_ts_value(res[2])
    hist_curr = _extract_ts_value(res[3])
    macd_curr = _extract_ts_value(res[4])
    sig_curr = _extract_ts_value(res[5])

    # missing data detection
    missing = []
    if close_curr is None:
        missing.append("close_curr")
    if hist_prev is None:
        missing.append("hist_prev")
    if hist_curr is None:
        missing.append("hist_curr")
    if macd_curr is None:
        missing.append("macd_curr")
    if sig_curr is None:
        missing.append("signal_curr")

    # ticker precision
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    base_details = {
        "source_stream": INDICATOR_STREAM_KEY,
        "event": {"status": status, "symbol": symbol, "indicator": indicator_base, "open_time": open_time_iso, "timeframe": timeframe},
        "ts": {
            "ts_ms": ts_ms,
            "prev_ms": prev_ms,
            "open_time": open_time.isoformat(),
            "prev_time": prev_time.isoformat(),
            "decision_time": decision_time.isoformat(),
        },
        "price": {
            "close_curr": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
            "close_prev": _round_price(float(close_prev), precision_price) if close_prev is not None else None,
        },
        "macd": {
            "hist_prev": float(hist_prev) if hist_prev is not None else None,
            "hist_curr": float(hist_curr) if hist_curr is not None else None,
            "macd_curr": float(macd_curr) if macd_curr is not None else None,
            "signal_curr": float(sig_curr) if sig_curr is not None else None,
        },
    }

    if missing:
        # data_missing: only RAW configs are logged here
        details = {**base_details, "result": {"passed": False, "status": "data_missing", "missing": missing}}
        for scfg in matching_cfgs:
            if str(scfg.get("filter_mode") or "") != "raw":
                continue
            await _upsert_live_log(
                pg,
                int(scfg["signal_id"]),
                symbol,
                timeframe,
                open_time,
                decision_time,
                "data_missing",
                {**details, "signal": scfg},
            )
        return

    # Один read → обработка всех RAW инстансов, относящихся к indicator_base
    sent_now = 0
    for scfg in matching_cfgs:
        # only RAW
        if str(scfg.get("filter_mode") or "raw") != "raw":
            continue

        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        min_abs_hist = float(scfg["min_abs_hist"])
        require_macd_sign = bool(scfg["require_macd_sign"])
        macd_instance_id = int(scfg["macd_instance_id"])

        # evaluate cross
        passed, eval_status, eval_extra = _evaluate_macd_cross(
            direction=direction,
            hist_prev=float(hist_prev),
            hist_curr=float(hist_curr),
            macd_curr=float(macd_curr),
            min_abs_hist=float(min_abs_hist),
            require_macd_sign=bool(require_macd_sign),
        )

        if not passed:
            # rejected -> log and continue
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
                    "signal": {
                        "signal_id": signal_id,
                        "direction": direction,
                        "message": message,
                        "filter_mode": "raw",
                        "macd_instance_id": macd_instance_id,
                        "min_abs_hist": min_abs_hist,
                        "require_macd_sign": require_macd_sign,
                        "rule": "hist_strict_cross",
                    },
                    "result": {"passed": False, "status": eval_status, "extra": eval_extra},
                },
            )
            continue

        # passed -> mark signal_sent in live log (with protection)
        details = {
            **base_details,
            "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": "raw"},
            "result": {"passed": True, "status": "signal_sent"},
        }

        should_send = await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, decision_time, "signal_sent", details)
        if not should_send:
            continue

        # build payload
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
            "pattern": "cross",
            "rule": "hist_strict_cross",
            "price": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
            "macd_instance_id": macd_instance_id,
            "indicator_base": str(indicator_base),
            "hist_prev": float(hist_prev),
            "hist_curr": float(hist_curr),
            "macd_curr": float(macd_curr),
            "signal_curr": float(sig_curr),
            "min_abs_hist": float(min_abs_hist),
            "require_macd_sign": bool(require_macd_sign),
        }

        await _persist_live_signal(pg, redis, signal_id, symbol, timeframe, direction, open_time, decision_time, message, raw_message)

        ctx["counters"]["raw_sent_total"] += 1
        sent_now += 1

        log.info(
            "BT_SIG_MACD_CROSS_LIVE_V2: signal_sent RAW — signal_id=%s %s %s %s base=%s",
            signal_id,
            symbol,
            direction,
            open_time.isoformat(),
            indicator_base,
        )

    # суммарный лог по событию (если что-то отправили)
    if sent_now > 0:
        log.info(
            "BT_SIG_MACD_CROSS_LIVE_V2: RAW batch result — symbol=%s open_time=%s base=%s sent_now=%s (raw_sent_total=%s)",
            symbol,
            open_time.isoformat(),
            indicator_base,
            sent_now,
            int(ctx["counters"].get("raw_sent_total", 0)),
        )


# 🔸 FILTER: ind_pack_stream_ready handler
async def _handle_pack_ready_message(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> None:
    tf_expected = str(ctx.get("timeframe") or "m5")

    # parse event fields
    symbol = (fields.get("symbol") or "").strip()
    pack_tf = (fields.get("timeframe") or "").strip().lower()          # ожидается "mtf"
    status = (fields.get("status") or "").strip().lower()              # ожидается "ok"
    open_time_iso = (fields.get("open_time") or "").strip()
    open_ts_ms_raw = (fields.get("open_ts_ms") or "").strip()
    results_json_raw = (fields.get("results_json") or "").strip()
    pairs_raw = (fields.get("pairs") or "").strip()
    expected_count = (fields.get("expected_count") or "").strip()
    received_count = (fields.get("received_count") or "").strip()

    # basic validation
    if not symbol or status != "ok":
        ctx["counters"]["ignored_total"] += 1
        return
    if pack_tf != "mtf":
        ctx["counters"]["ignored_total"] += 1
        return

    # open_time resolve: iso preferred, else ms
    open_time: Optional[datetime] = None
    if open_time_iso:
        open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None and open_ts_ms_raw:
        try:
            open_time = _from_ms_utc(int(open_ts_ms_raw))
        except Exception:
            open_time = None
    if open_time is None:
        ctx["counters"]["ignored_total"] += 1
        return

    # compute times
    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta
    decision_time = open_time + step_delta

    # stale protection
    now_utc = datetime.utcnow().replace(tzinfo=None)
    if (now_utc - decision_time).total_seconds() > FILTER_STALE_MAX_SEC:
        ctx["counters"]["dropped_stale"] += 1
        return

    # parse pack results
    results_by_pair = _parse_results_json(results_json_raw)

    base_details_pack = {
        "source_stream": IND_PACK_READY_STREAM_KEY,
        "pack": {
            "status": status,
            "symbol": symbol,
            "timeframe": pack_tf,
            "open_ts_ms": open_ts_ms_raw,
            "open_time": open_time_iso,
            "expected_count": expected_count,
            "received_count": received_count,
            "pairs": pairs_raw,
            "results_keys": sorted(list(results_by_pair.keys()))[:200],
        },
    }

    # Для MACD-cross фильтрация опирается на те же “свои” TS данные (hist prev/curr + macd sign), как и RAW.
    # Тут мы не знаем заранее, какой indicator_base нужен, поэтому читаем TS отдельно на каждый cfg (base),
    # но делаем это умно: сгруппируем по base.

    # ticker precision
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    # pick filter configs only
    all_cfgs = ctx.get("signals") or []
    filter_cfgs = [c for c in all_cfgs if str(c.get("filter_mode") or "") in ("filter1", "filter2")]
    if not filter_cfgs:
        return

    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)

    close_key = BB_TS_CLOSE_KEY.format(symbol=symbol, tf=tf_expected)

    # group by indicator_base
    by_base: Dict[str, List[Dict[str, Any]]] = {}
    for c in filter_cfgs:
        by_base.setdefault(str(c.get("indicator_base") or ""), []).append(c)

    # читаем close_curr один раз (общий)
    pipe0 = redis.pipeline()
    pipe0.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)
    try:
        res0 = await asyncio.wait_for(pipe0.execute(), timeout=REDIS_OP_TIMEOUT_SEC)
        close_curr = _extract_ts_value(res0[0])
    except Exception:
        close_curr = None

    # per-base TS reads + per-config filter checks
    sent_now = 0
    for indicator_base, cfgs_for_base in by_base.items():
        if not indicator_base:
            continue

        macd_name = f"{indicator_base}_macd"
        sig_name = f"{indicator_base}_macd_signal"
        hist_name = f"{indicator_base}_macd_hist"

        macd_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=macd_name)
        sig_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=sig_name)
        hist_key = IND_TS_KEY.format(symbol=symbol, tf=tf_expected, param_name=hist_name)

        # redis pipeline read (per base)
        pipe = redis.pipeline()
        pipe.execute_command("TS.RANGE", hist_key, prev_ms, prev_ms)   # hist_prev
        pipe.execute_command("TS.RANGE", hist_key, ts_ms, ts_ms)       # hist_curr
        pipe.execute_command("TS.RANGE", macd_key, ts_ms, ts_ms)       # macd_curr
        pipe.execute_command("TS.RANGE", sig_key, ts_ms, ts_ms)        # signal_curr
        try:
            res = await asyncio.wait_for(pipe.execute(), timeout=REDIS_OP_TIMEOUT_SEC)
        except Exception as e:
            ctx["counters"]["errors_total"] += 1
            log.error(
                "BT_SIG_MACD_CROSS_LIVE_V2: redis pipeline error (pack_ready) — symbol=%s, open_time=%s, base=%s err=%s",
                symbol,
                open_time.isoformat(),
                indicator_base,
                e,
                exc_info=True,
            )
            continue

        hist_prev = _extract_ts_value(res[0])
        hist_curr = _extract_ts_value(res[1])
        macd_curr = _extract_ts_value(res[2])
        sig_curr = _extract_ts_value(res[3])

        # missing data detection
        missing = []
        if hist_prev is None:
            missing.append("hist_prev")
        if hist_curr is None:
            missing.append("hist_curr")
        if macd_curr is None:
            missing.append("macd_curr")
        if sig_curr is None:
            missing.append("signal_curr")

        base_details = {
            **base_details_pack,
            "ts": {
                "ts_ms": ts_ms,
                "prev_ms": prev_ms,
                "open_time": open_time.isoformat(),
                "prev_time": prev_time.isoformat(),
                "decision_time": decision_time.isoformat(),
            },
            "price": {
                "close_curr": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
            },
            "macd": {
                "indicator_base": indicator_base,
                "hist_prev": float(hist_prev) if hist_prev is not None else None,
                "hist_curr": float(hist_curr) if hist_curr is not None else None,
                "macd_curr": float(macd_curr) if macd_curr is not None else None,
                "signal_curr": float(sig_curr) if sig_curr is not None else None,
            },
        }

        if missing:
            # data_missing: log for all configs of this base
            details = {**base_details, "result": {"passed": False, "status": "data_missing", "missing": missing}}
            for scfg in cfgs_for_base:
                await _upsert_live_log(
                    pg,
                    int(scfg["signal_id"]),
                    symbol,
                    tf_expected,
                    open_time,
                    decision_time,
                    "data_missing",
                    {**details, "signal": scfg},
                )
            continue

        # Один read → обработка всех FILTER инстансов для данного base
        for scfg in cfgs_for_base:
            filter_mode = str(scfg.get("filter_mode") or "filter1")
            signal_id = int(scfg["signal_id"])
            direction = str(scfg["direction"])
            message = str(scfg["message"])
            min_abs_hist = float(scfg["min_abs_hist"])
            require_macd_sign = bool(scfg["require_macd_sign"])
            mirror1 = scfg.get("mirror1")
            mirror2 = scfg.get("mirror2")
            macd_instance_id = int(scfg["macd_instance_id"])

            # evaluate cross
            passed, eval_status, eval_extra = _evaluate_macd_cross(
                direction=direction,
                hist_prev=float(hist_prev),
                hist_curr=float(hist_curr),
                macd_curr=float(macd_curr),
                min_abs_hist=float(min_abs_hist),
                require_macd_sign=bool(require_macd_sign),
            )

            if not passed:
                # rejected -> log and continue
                await _upsert_live_log(
                    pg=pg,
                    signal_id=signal_id,
                    symbol=symbol,
                    timeframe=tf_expected,
                    open_time=open_time,
                    decision_time=decision_time,
                    status=eval_status,
                    details={
                        **base_details,
                        "signal": {
                            "signal_id": signal_id,
                            "direction": direction,
                            "message": message,
                            "filter_mode": filter_mode,
                            "macd_instance_id": macd_instance_id,
                            "min_abs_hist": min_abs_hist,
                            "require_macd_sign": require_macd_sign,
                            "rule": "hist_strict_cross",
                            "mirror1": mirror1,
                            "mirror2": mirror2 if filter_mode == "filter2" else None,
                        },
                        "result": {"passed": False, "status": eval_status, "extra": eval_extra},
                    },
                )
                continue

            # cross прошёл -> проверяем слои по results_json
            layers_to_check: List[Dict[str, Any]] = []

            if filter_mode in ("filter1", "filter2"):
                # layer 1 mandatory
                if mirror1 and mirror1.get("scenario_id") and mirror1.get("signal_id"):
                    layers_to_check.append({"layer": 1, "scenario_id": int(mirror1["scenario_id"]), "signal_id": int(mirror1["signal_id"])})
                else:
                    await _upsert_live_log(
                        pg=pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_expected,
                        open_time=open_time,
                        decision_time=decision_time,
                        status="blocked_no_cache",
                        details={**base_details, "signal": {"signal_id": signal_id, "filter_mode": filter_mode}, "filter": {"reason": "missing_mirror1"}},
                    )
                    continue

            if filter_mode == "filter2":
                # layer 2 mandatory for filter2
                if mirror2 and mirror2.get("scenario_id") and mirror2.get("signal_id"):
                    layers_to_check.append({"layer": 2, "scenario_id": int(mirror2["scenario_id"]), "signal_id": int(mirror2["signal_id"])})
                else:
                    await _upsert_live_log(
                        pg=pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_expected,
                        open_time=open_time,
                        decision_time=decision_time,
                        status="blocked_no_cache",
                        details={**base_details, "signal": {"signal_id": signal_id, "filter_mode": filter_mode}, "filter": {"reason": "missing_mirror2"}},
                    )
                    continue

            layer_results: List[Dict[str, Any]] = []
            blocked = False

            for layer in layers_to_check:
                # build/check cache layer
                layer_no = int(layer["layer"])
                ms = int(layer["scenario_id"])
                si = int(layer["signal_id"])

                cache_layer = _build_filter_layer_from_cache(ms, si, direction)
                if cache_layer is None:
                    await load_initial_mirror_caches_v2(pg)
                    cache_layer = _build_filter_layer_from_cache(ms, si, direction)

                if cache_layer is None:
                    await _upsert_live_log(
                        pg=pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_expected,
                        open_time=open_time,
                        decision_time=decision_time,
                        status="blocked_no_cache",
                        details={
                            **base_details,
                            "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                            "filter": {"layer": layer_no, "reason": "mirror_cache_missing", "mirror": {"scenario_id": ms, "signal_id": si}},
                        },
                    )
                    blocked = True
                    break

                # check layer by pack results_json
                ok_layer, info = _check_layer_by_pack_results(cache_layer, results_by_pair, pack_timeframe=pack_tf)
                layer_results.append({"layer": layer_no, **info})

                if not ok_layer:
                    await _upsert_live_log(
                        pg=pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_expected,
                        open_time=open_time,
                        decision_time=decision_time,
                        status=str(info.get("status") or "blocked_no_labels_match"),
                        details={
                            **base_details,
                            "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": filter_mode},
                            "filter": {"layer": layer_no, "mirror": {"scenario_id": ms, "signal_id": si}, **info},
                        },
                    )
                    blocked = True
                    break

            if blocked:
                continue

            # all layers ok -> send
            details = {
                **base_details,
                "signal": {
                    "signal_id": signal_id,
                    "direction": direction,
                    "message": message,
                    "filter_mode": filter_mode,
                    "macd_instance_id": macd_instance_id,
                    "min_abs_hist": min_abs_hist,
                    "require_macd_sign": require_macd_sign,
                    "mirror1": mirror1,
                    "mirror2": mirror2 if filter_mode == "filter2" else None,
                },
                "filter": {"rule": "passed_all_layers", "layers": layer_results},
                "result": {"passed": True, "status": "signal_sent"},
            }

            should_send = await _upsert_live_log(pg, signal_id, symbol, tf_expected, open_time, decision_time, "signal_sent", details)
            if not should_send:
                continue

            raw_message = {
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": tf_expected,
                "open_time": open_time.isoformat(),
                "decision_time": decision_time.isoformat(),
                "direction": direction,
                "message": message,
                "source": "backtester_v1",
                "mode": "live_filtered_v2",
                "filter_mode": filter_mode,
                "pattern": "cross",
                "rule": "hist_strict_cross",
                "price": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
                "macd_instance_id": macd_instance_id,
                "indicator_base": str(indicator_base),
                "hist_prev": float(hist_prev),
                "hist_curr": float(hist_curr),
                "macd_curr": float(macd_curr),
                "signal_curr": float(sig_curr),
                "min_abs_hist": float(min_abs_hist),
                "require_macd_sign": bool(require_macd_sign),
                "pack": {"stream": IND_PACK_READY_STREAM_KEY, "open_ts_ms": open_ts_ms_raw, "pairs": pairs_raw},
                "layers": layer_results,
            }

            await _persist_live_signal(pg, redis, signal_id, symbol, tf_expected, direction, open_time, decision_time, message, raw_message)

            ctx["counters"]["filtered_sent_total"] += 1
            sent_now += 1

            log.info(
                "BT_SIG_MACD_CROSS_LIVE_V2: signal_sent FILTERED — signal_id=%s %s %s %s base=%s mode=%s",
                signal_id,
                symbol,
                direction,
                open_time.isoformat(),
                indicator_base,
                filter_mode,
            )

    # суммарный лог по pack-событию (если что-то отправили)
    if sent_now > 0:
        log.info(
            "BT_SIG_MACD_CROSS_LIVE_V2: FILTER batch result — symbol=%s open_time=%s sent_now=%s (filtered_sent_total=%s)",
            symbol,
            open_time.isoformat(),
            sent_now,
            int(ctx["counters"].get("filtered_sent_total", 0)),
        )