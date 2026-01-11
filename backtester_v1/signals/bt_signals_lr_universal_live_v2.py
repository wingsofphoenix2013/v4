# bt_signals_lr_universal_live_v2.py — live-воркер LR universal bounce: RAW от indicator_stream + FILTER(1/2) от ind_pack_stream_ready (results_json), без ожиданий ind_pack KV

# 🔸 Базовые импорты
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

# 🔸 Stream keys (в live_stream_key инстанса)
INDICATOR_STREAM_KEY = "indicator_stream"
IND_PACK_READY_STREAM_KEY = "ind_pack_stream_ready"

# 🔸 RedisTimeSeries ключи
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Таймшаги TF (в минутах)
TF_STEP_MINUTES = {"m5": 5}

# 🔸 Защита от “догоняющих” событий
FILTER_STALE_MAX_SEC = 120

# 🔸 Таймаут для одиночных Redis-вызовов (защита от зависаний await)
REDIS_OP_TIMEOUT_SEC = 5

# 🔸 Таблицы
BT_SIGNALS_VALUES_TABLE = "bt_signals_values"
BT_SIGNALS_LIVE_TABLE = "bt_signals_live"


# 🔸 Парсинг open_time ISO (UTC-naive)
def _parse_open_time_iso(open_time_iso: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat(open_time_iso)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


# 🔸 open_ts_ms -> naive UTC datetime
def _from_ms_utc(ms: int) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).replace(tzinfo=None)
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

# 🔸 Вставка live-сигнала в bt_signals_values (event-layer: event_key+event_params_hash), публикация в signals_stream только при новом insert
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

    # event-layer идентичность для live (чтобы не плодить дубликаты)
    # key отражает режим (raw/filter) + направление базового детектора
    mode = str((raw_message or {}).get("mode") or "live_v2").strip().lower()
    event_key = f"lr_universal_live_{mode}_{timeframe}"

    # hash параметров детектора (стабильная часть) — используем поля из raw_message, если есть
    # если чего-то нет, fallback на пустые значения (не упадём)
    trend_type = str((raw_message or {}).get("trend_type") or (raw_message or {}).get("trend") or "").strip().lower()
    zone_k = str((raw_message or {}).get("zone_k") or "")
    keep_half = str((raw_message or {}).get("keep_half") or "")

    # зеркала (если filtered): фиксируем, чтобы отличать разные конфиги фильтра в identity
    filter_mode = str((raw_message or {}).get("filter_mode") or "").strip().lower()
    mirror1 = (raw_message or {}).get("layers") or []
    mirrors_sig = ""
    try:
        # очень короткая подпись: layer_count + first mirror pair ids (для различения конфигов)
        if isinstance(mirror1, list) and mirror1:
            ms = str(((mirror1[0] or {}).get("mirror") or {}).get("scenario_id") or "")
            si = str(((mirror1[0] or {}).get("mirror") or {}).get("signal_id") or "")
            mirrors_sig = f"{si}:{ms}"
    except Exception:
        mirrors_sig = ""

    # event_params_hash — стабильная подпись конфигурации live-детектора/фильтра
    event_params_hash = f"trend={trend_type}|zone_k={zone_k}|keep_half={keep_half}|filter={filter_mode}|mirror={mirrors_sig}"
    # укоротим до 32 (как принято), но без hashlib тоже ок — используем uuid namespace? сделаем sha1 для стабильности
    import hashlib
    event_params_hash = hashlib.sha1(event_params_hash.encode("utf-8")).hexdigest()[:16]

    # payload_stable: всё важное кладём сюда (это и есть "raw_message")
    payload_stable_json = json.dumps(raw_message or {}, ensure_ascii=False)

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
            "bounce",
            (raw_message or {}).get("price"),
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

# 🔸 Получение слоя фильтра из кеша v2 (mirror -> required_pairs/good_bins_map/run_id)
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

    return {
        "mirror_scenario_id": int(mirror_scenario_id),
        "mirror_signal_id": int(mirror_signal_id),
        "direction": str(direction).strip().lower(),
        "applied_run_id": int(run_id) if run_id is not None else None,
        "required_pairs": set(req),
        "good_bins_map": {k: set(v) for k, v in good_map.items()},
    }


# 🔸 Парсер results_json из ind_pack_stream_ready (v4)
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
        if not isinstance(k, str):
            continue
        if not isinstance(v, dict):
            continue
        out[k.strip()] = v
    return out


# 🔸 Проверка слоя фильтра по готовому results_json (без ожиданий)
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

    # условий достаточности
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

    # ok=false -> блокируем сразу
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

    # good-hit: хотя бы одно совпадение по required_pairs
    required_pairs: Set[Tuple[int, str]] = set(layer_cache.get("required_pairs") or set())
    good_bins_map: Dict[Tuple[int, str], Set[str]] = layer_cache.get("good_bins_map") or {}

    try:
        aid_i = int(analysis_id) if analysis_id is not None else None
    except Exception:
        aid_i = None

    candidate_pairs: List[Tuple[int, str]] = []
    for (aid, tf) in required_pairs:
        if str(tf).strip().lower() != str(pack_timeframe).strip().lower():
            continue
        if aid_i is None or int(aid) == int(aid_i):
            candidate_pairs.append((int(aid), str(tf).strip().lower()))

    # если required_pairs пустой или не сошёлся по времени/analysis_id — считаем, что good-hit невозможен
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

    good_hit = False
    checked_pairs: List[Dict[str, Any]] = []

    for (aid, tf) in candidate_pairs:
        good_set = good_bins_map.get((aid, tf)) or set()
        hit = bn in good_set
        checked_pairs.append(
            {"analysis_id": int(aid), "timeframe": str(tf), "hit": bool(hit), "good_bins_count": len(good_set)}
        )
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


# 🔸 Инициализация live-контекста (ctx на key=lr_universal_v2 + stream group)
async def init_lr_universal_live_v2(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    if not signals:
        raise RuntimeError("init_lr_universal_live_v2: empty signals list")

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    lr_instance_id = None
    stream_key = None

    raw_cnt = 0
    filt1_cnt = 0
    filt2_cnt = 0

    for s in signals:
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # live_stream_key (одинаковый для группы)
        lsk_cfg = params.get("live_stream_key") or params.get("stream_key")
        lsk = str((lsk_cfg or {}).get("value") or "").strip()
        if stream_key is None:
            stream_key = lsk

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

    stream_key = str(stream_key or "").strip()
    if stream_key not in (INDICATOR_STREAM_KEY, IND_PACK_READY_STREAM_KEY):
        raise RuntimeError(f"init_lr_universal_live_v2: unsupported live_stream_key='{stream_key}'")

    # initial load caches только если поток фильтров
    if stream_key == IND_PACK_READY_STREAM_KEY:
        await load_initial_mirror_caches_v2(pg)

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

    log.debug(
        "BT_SIG_LR_UNI_LIVE_V2: init ok — stream=%s signals=%s (raw=%s, filter1=%s, filter2=%s), tf=%s, lr_instance_id=%s, indicator_base=%s",
        stream_key,
        len(cfgs),
        raw_cnt,
        filt1_cnt,
        filt2_cnt,
        timeframe,
        lr_instance_id,
        indicator_base,
    )

    return {
        "stream_key": stream_key,
        "timeframe": timeframe,
        "step_delta": timedelta(minutes=step_min),
        "lr_instance_id": int(lr_instance_id),
        "indicator_base": indicator_base,
        "param_angle": param_angle,
        "param_upper": param_upper,
        "param_lower": param_lower,
        "param_center": param_center,
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


# 🔸 Единый обработчик live-сообщений для key=lr_universal_v2 (ветвление по stream_key ctx)
async def handle_lr_universal_indicator_ready_v2(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    # live dispatcher ожидает список, но мы пишем всё сами (в БД и signals_stream), поэтому возвращаем []
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


# 🔸 Обработка indicator_stream (RAW): поиск bounce и немедленная отправка
async def _handle_indicator_ready_message(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> None:
    tf_expected = str(ctx.get("timeframe") or "m5")
    base_expected = str(ctx.get("indicator_base") or "")

    symbol = (fields.get("symbol") or "").strip()
    indicator_base = (fields.get("indicator") or "").strip()
    timeframe = (fields.get("timeframe") or "").strip().lower()
    open_time_iso = (fields.get("open_time") or "").strip()
    status = (fields.get("status") or "").strip().lower()

    # условия достаточности
    if not symbol or not open_time_iso or not timeframe:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return

    open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return

    # жёсткая фильтрация входного события
    if status != "ready" or timeframe != tf_expected or indicator_base != base_expected:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return

    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta
    decision_time = open_time + step_delta

    # stale: если событие слишком старое относительно decision_time
    now_utc = datetime.utcnow().replace(tzinfo=None)
    if (now_utc - decision_time).total_seconds() > FILTER_STALE_MAX_SEC:
        ctx["counters"]["dropped_stale"] = int(ctx["counters"].get("dropped_stale", 0)) + 1
        return

    # читаем OHLCV close и LR-канал из Redis TS по prev/curr
    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)
    decision_ms = _to_ms_utc(decision_time)

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
        res = await asyncio.wait_for(pipe.execute(), timeout=REDIS_OP_TIMEOUT_SEC)
    except Exception as e:
        ctx["counters"]["errors_total"] = int(ctx["counters"].get("errors_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE_V2: redis pipeline error (indicator_stream) — symbol=%s, open_time=%s, err=%s",
            symbol,
            open_time_iso,
            e,
            exc_info=True,
        )
        return

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
        "source_stream": INDICATOR_STREAM_KEY,
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
            # raw-only: если сюда попал filter инстанс, игнорируем
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

    H = float(upper_prev) - float(lower_prev)
    if H <= 0:
        details = {**base_details, "result": {"passed": False, "status": "invalid_channel_height", "H": float(H)}}
        for scfg in ctx.get("signals") or []:
            if str(scfg.get("filter_mode") or "") != "raw":
                continue
            await _upsert_live_log(
                pg,
                int(scfg["signal_id"]),
                symbol,
                timeframe,
                open_time,
                decision_time,
                "invalid_channel_height",
                {**details, "signal": scfg},
            )
        return

    # Один read → обработка всех RAW инстансов
    for scfg in ctx.get("signals") or []:
        filter_mode = str(scfg.get("filter_mode") or "raw")
        if filter_mode != "raw":
            continue

        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        trend_type = str(scfg["trend_type"])
        zone_k = float(scfg["zone_k"])
        keep_half = bool(scfg["keep_half"])

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
                    "signal": {"signal_id": signal_id, "direction": direction, "message": message, "trend_type": trend_type, "zone_k": zone_k, "keep_half": keep_half, "filter_mode": "raw"},
                    "result": {"passed": False, "status": eval_status, "extra": eval_extra},
                },
            )
            continue

        details = {
            **base_details,
            "signal": {"signal_id": signal_id, "direction": direction, "message": message, "filter_mode": "raw"},
            "result": {"passed": True, "status": "signal_sent"},
        }

        should_send = await _upsert_live_log(pg, signal_id, symbol, timeframe, open_time, decision_time, "signal_sent", details)
        if not should_send:
            continue

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
            "price": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
            "trend_type": trend_type,
            "zone_k": zone_k,
            "keep_half": keep_half,
        }

        await _persist_live_signal(pg, redis, signal_id, symbol, timeframe, direction, open_time, decision_time, message, raw_message)

        ctx["counters"]["raw_sent_total"] = int(ctx["counters"].get("raw_sent_total", 0)) + 1
        log.debug(
            "BT_SIG_LR_UNI_LIVE_V2: signal_sent RAW — signal_id=%s %s %s %s",
            signal_id,
            symbol,
            direction,
            open_time.isoformat(),
        )


# 🔸 Обработка ind_pack_stream_ready: bounce + фильтрация по results_json (без ожиданий)
async def _handle_pack_ready_message(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> None:
    tf_expected = str(ctx.get("timeframe") or "m5")

    symbol = (fields.get("symbol") or "").strip()
    pack_tf = (fields.get("timeframe") or "").strip().lower()          # ожидается "mtf"
    status = (fields.get("status") or "").strip().lower()              # ожидается "ok"
    open_time_iso = (fields.get("open_time") or "").strip()
    open_ts_ms_raw = (fields.get("open_ts_ms") or "").strip()
    results_json_raw = (fields.get("results_json") or "").strip()
    pairs_raw = (fields.get("pairs") or "").strip()
    expected_count = (fields.get("expected_count") or "").strip()
    received_count = (fields.get("received_count") or "").strip()

    # условия достаточности
    if not symbol or status != "ok":
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return
    if pack_tf != "mtf":
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return

    open_time: Optional[datetime] = None
    if open_time_iso:
        open_time = _parse_open_time_iso(open_time_iso)

    if open_time is None and open_ts_ms_raw:
        try:
            open_time = _from_ms_utc(int(open_ts_ms_raw))
        except Exception:
            open_time = None

    if open_time is None:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return

    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta
    decision_time = open_time + step_delta

    # stale: если событие слишком старое относительно decision_time
    now_utc = datetime.utcnow().replace(tzinfo=None)
    if (now_utc - decision_time).total_seconds() > FILTER_STALE_MAX_SEC:
        ctx["counters"]["dropped_stale"] = int(ctx["counters"].get("dropped_stale", 0)) + 1
        return

    # parse results_json
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

    # читаем OHLCV close и LR-канал из Redis TS по prev/curr
    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)
    decision_ms = _to_ms_utc(decision_time)

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
        res = await asyncio.wait_for(pipe.execute(), timeout=REDIS_OP_TIMEOUT_SEC)
    except Exception as e:
        ctx["counters"]["errors_total"] = int(ctx["counters"].get("errors_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE_V2: redis pipeline error (pack_ready) — symbol=%s, open_time=%s, err=%s",
            symbol,
            (open_time.isoformat() if isinstance(open_time, datetime) else str(open_time)),
            e,
            exc_info=True,
        )
        return

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
        **base_details_pack,
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
            # filter-only: если сюда попал raw инстанс, игнорируем
            if str(scfg.get("filter_mode") or "") == "raw":
                continue
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
        return

    H = float(upper_prev) - float(lower_prev)
    if H <= 0:
        details = {**base_details, "result": {"passed": False, "status": "invalid_channel_height", "H": float(H)}}
        for scfg in ctx.get("signals") or []:
            if str(scfg.get("filter_mode") or "") == "raw":
                continue
            await _upsert_live_log(
                pg,
                int(scfg["signal_id"]),
                symbol,
                tf_expected,
                open_time,
                decision_time,
                "invalid_channel_height",
                {**details, "signal": scfg},
            )
        return

    # Один read → обработка всех FILTER инстансов
    for scfg in ctx.get("signals") or []:
        filter_mode = str(scfg.get("filter_mode") or "raw")
        if filter_mode not in ("filter1", "filter2"):
            continue

        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        trend_type = str(scfg["trend_type"])
        zone_k = float(scfg["zone_k"])
        keep_half = bool(scfg["keep_half"])
        mirror1 = scfg.get("mirror1")
        mirror2 = scfg.get("mirror2")

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
                        "trend_type": trend_type,
                        "zone_k": zone_k,
                        "keep_half": keep_half,
                        "filter_mode": filter_mode,
                        "mirror1": mirror1,
                        "mirror2": mirror2 if filter_mode == "filter2" else None,
                    },
                    "result": {"passed": False, "status": eval_status, "extra": eval_extra},
                },
            )
            continue

        # bounce прошёл -> проверяем слои по results_json
        layers: List[Dict[str, Any]] = []
        if filter_mode in ("filter1", "filter2"):
            if mirror1 and mirror1.get("scenario_id") and mirror1.get("signal_id"):
                layers.append({"layer": 1, "scenario_id": int(mirror1["scenario_id"]), "signal_id": int(mirror1["signal_id"])})
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
            if mirror2 and mirror2.get("scenario_id") and mirror2.get("signal_id"):
                layers.append({"layer": 2, "scenario_id": int(mirror2["scenario_id"]), "signal_id": int(mirror2["signal_id"])})
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

        for layer in layers:
            layer_no = int(layer["layer"])
            ms = int(layer["scenario_id"])
            si = int(layer["signal_id"])

            cache_layer = _build_filter_layer_from_cache(ms, si, direction)
            if cache_layer is None:
                # пробуем освежить кеши (watcher может ещё не успел)
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

        details = {
            **base_details,
            "signal": {
                "signal_id": signal_id,
                "direction": direction,
                "message": message,
                "filter_mode": filter_mode,
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
            "price": _round_price(float(close_curr), precision_price) if close_curr is not None else None,
            "trend_type": trend_type,
            "zone_k": zone_k,
            "keep_half": keep_half,
            "pack": {
                "stream": IND_PACK_READY_STREAM_KEY,
                "open_ts_ms": open_ts_ms_raw,
                "pairs": pairs_raw,
            },
            "layers": layer_results,
        }

        await _persist_live_signal(pg, redis, signal_id, symbol, tf_expected, direction, open_time, decision_time, message, raw_message)

        ctx["counters"]["filtered_sent_total"] = int(ctx["counters"].get("filtered_sent_total", 0)) + 1
        log.debug(
            "BT_SIG_LR_UNI_LIVE_V2: signal_sent FILTERED — signal_id=%s %s %s %s mode=%s",
            signal_id,
            symbol,
            direction,
            open_time.isoformat(),
            filter_mode,
        )