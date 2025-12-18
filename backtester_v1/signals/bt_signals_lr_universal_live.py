# bt_signals_lr_universal_live.py — live-воркер LR universal bounce (trend/counter/agnostic) по indicator_stream на m5

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (инстансы индикаторов + тикеры/precision)
from backtester_config import get_indicator_instance, get_ticker_info

# 🔸 Логгер модуля
log = logging.getLogger("BT_SIG_LR_UNI_LIVE")

# 🔸 RedisTimeSeries ключи (источники)
BB_TS_CLOSE_KEY = "bb:ts:{symbol}:{tf}:c"
IND_TS_KEY = "ts_ind:{symbol}:{tf}:{param_name}"

# 🔸 Шаг таймфрейма (в минутах)
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Инициализация live-контекста (один ctx на key=lr_universal)
async def init_lr_universal_live(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    # подготовка контекста для live-обработчика lr_universal
    if not signals:
        raise RuntimeError("init_lr_universal_live: empty signals list")

    cfgs: List[Dict[str, Any]] = []

    timeframe = None
    lr_instance_id = None

    for s in signals:
        sid = int(s.get("id") or 0)
        tf = str(s.get("timeframe") or "").strip().lower()
        params = s.get("params") or {}

        # читаем обязательные параметры
        try:
            lr_cfg = params["indicator"]
            lr_id = int(lr_cfg.get("value"))
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'indicator'")

        try:
            dm_cfg = params["direction_mask"]
            direction_mask = str(dm_cfg.get("value") or "").strip().lower()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'direction_mask'")

        if direction_mask not in ("long", "short"):
            raise RuntimeError(
                f"init_lr_universal_live: signal_id={sid} invalid direction_mask={direction_mask} (expected long/short)"
            )

        try:
            msg_cfg = params["message"]
            message = str(msg_cfg.get("value") or "").strip()
        except Exception:
            raise RuntimeError(f"init_lr_universal_live: signal_id={sid} missing/invalid param 'message'")

        # параметры bounce (как в backfill)
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

        cfgs.append(
            {
                "signal_id": sid,
                "timeframe": tf,
                "direction": direction_mask,
                "message": message,
                "trend_type": trend_type,
                "zone_k": zone_k,
                "keep_half": keep_half,
                "lr_instance_id": lr_id,
            }
        )

        if timeframe is None:
            timeframe = tf
        if lr_instance_id is None:
            lr_instance_id = lr_id

    # базовая валидация консистентности пары long/short
    if timeframe is None or timeframe != "m5":
        raise RuntimeError(f"init_lr_universal_live: unsupported timeframe={timeframe} (expected m5)")

    for c in cfgs:
        if c["timeframe"] != timeframe:
            raise RuntimeError(
                f"init_lr_universal_live: mixed timeframes in signals (got {c['timeframe']} vs {timeframe})"
            )
        if c["lr_instance_id"] != lr_instance_id:
            raise RuntimeError(
                f"init_lr_universal_live: mixed lr_instance_id in signals (got {c['lr_instance_id']} vs {lr_instance_id})"
            )

    # правила naming: indicators_v4 compute_and_store -> base = f"{indicator}{length}" если length в params
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

    # имена параметров LR
    param_angle = f"{indicator_base}_angle"
    param_upper = f"{indicator_base}_upper"
    param_lower = f"{indicator_base}_lower"
    param_center = f"{indicator_base}_center"

    # шаг таймфрейма
    step_min = TF_STEP_MINUTES.get(timeframe, 0)
    if step_min <= 0:
        raise RuntimeError(f"init_lr_universal_live: unknown timeframe step for tf={timeframe}")
    step_delta = timedelta(minutes=step_min)

    log.info(
        "BT_SIG_LR_UNI_LIVE: init ok — signals=%s, tf=%s, lr_instance_id=%s, indicator_base=%s, params=[%s,%s,%s,%s]",
        len(cfgs),
        timeframe,
        lr_instance_id,
        indicator_base,
        param_angle,
        param_upper,
        param_lower,
        param_center,
    )

    return {
        "timeframe": timeframe,
        "step_delta": step_delta,
        "lr_instance_id": int(lr_instance_id),
        "indicator_base": indicator_base,
        "param_angle": param_angle,
        "param_upper": param_upper,
        "param_lower": param_lower,
        "param_center": param_center,
        "signals": cfgs,
        "counters": {
            "messages_total": 0,
            "sent_total": 0,
            "ignored_total": 0,
            "ignored_wrong_indicator": 0,
            "ignored_wrong_tf": 0,
            "ignored_not_ready": 0,
            "errors_total": 0,
        },
    }


# 🔸 Обработка одного ready-сообщения из indicator_stream
async def handle_lr_universal_indicator_ready(
    ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    # входной контракт indicator_stream: symbol, indicator(base), timeframe, open_time, status
    live_signals: List[Dict[str, Any]] = []

    tf_expected = str(ctx.get("timeframe") or "m5")
    base_expected = str(ctx.get("indicator_base") or "")

    symbol = (fields.get("symbol") or "").strip()
    indicator_base = (fields.get("indicator") or "").strip()
    timeframe = (fields.get("timeframe") or "").strip().lower()
    open_time_iso = (fields.get("open_time") or "").strip()
    status = (fields.get("status") or "").strip().lower()

    ctx["counters"]["messages_total"] = int(ctx["counters"].get("messages_total", 0)) + 1

    # минимальная валидация ключевых полей
    if not symbol or not open_time_iso or not timeframe:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE: ignored_missing_fields — fields=%s",
            fields,
        )
        return []

    open_time = _parse_open_time_iso(open_time_iso)
    if open_time is None:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE: ignored_invalid_open_time — symbol=%s, open_time=%s",
            symbol,
            open_time_iso,
        )
        return []

    # 🔸 Жёсткая фильтрация: "чужие" события не пишем в БД (чтобы не раздувать журнал)
    ignore_reason: Optional[str] = None
    if status != "ready":
        ignore_reason = "ignored_not_ready"
    elif timeframe != tf_expected:
        ignore_reason = "ignored_wrong_tf"
    elif indicator_base != base_expected:
        ignore_reason = "ignored_wrong_indicator"

    if ignore_reason:
        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1

        if ignore_reason == "ignored_wrong_indicator":
            ctx["counters"]["ignored_wrong_indicator"] = int(ctx["counters"].get("ignored_wrong_indicator", 0)) + 1
        elif ignore_reason == "ignored_wrong_tf":
            ctx["counters"]["ignored_wrong_tf"] = int(ctx["counters"].get("ignored_wrong_tf", 0)) + 1
        elif ignore_reason == "ignored_not_ready":
            ctx["counters"]["ignored_not_ready"] = int(ctx["counters"].get("ignored_not_ready", 0)) + 1

        return []

    # prev_time и ts_ms
    step_delta: timedelta = ctx["step_delta"]
    prev_time = open_time - step_delta

    ts_ms = _to_ms_utc(open_time)
    prev_ms = _to_ms_utc(prev_time)

    # 🔸 Читаем OHLCV (close) и LR-канал из Redis TS по prev/curr
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
    # close prev/curr
    pipe.execute_command("TS.RANGE", close_key, prev_ms, prev_ms)
    pipe.execute_command("TS.RANGE", close_key, ts_ms, ts_ms)
    # LR: upper/lower prev
    pipe.execute_command("TS.RANGE", upper_key, prev_ms, prev_ms)
    pipe.execute_command("TS.RANGE", lower_key, prev_ms, prev_ms)
    # LR: angle/center curr
    pipe.execute_command("TS.RANGE", angle_key, ts_ms, ts_ms)
    pipe.execute_command("TS.RANGE", center_key, ts_ms, ts_ms)
    # LR: upper/lower curr (для details)
    pipe.execute_command("TS.RANGE", upper_key, ts_ms, ts_ms)
    pipe.execute_command("TS.RANGE", lower_key, ts_ms, ts_ms)

    try:
        res = await pipe.execute()
    except Exception as e:
        ctx["counters"]["errors_total"] = int(ctx["counters"].get("errors_total", 0)) + 1
        log.error(
            "BT_SIG_LR_UNI_LIVE: error_redis_pipeline — symbol=%s, open_time=%s, err=%s",
            symbol,
            open_time_iso,
            e,
            exc_info=True,
        )

        details = {
            "reason": "error",
            "event": {
                "symbol": symbol,
                "indicator": indicator_base,
                "timeframe": timeframe,
                "open_time": open_time_iso,
                "status": status,
            },
            "error": str(e),
        }

        for scfg in ctx.get("signals") or []:
            await _upsert_live_log(
                pg=pg,
                signal_id=int(scfg["signal_id"]),
                symbol=symbol,
                timeframe=timeframe,
                open_time=open_time,
                status="error",
                details=details,
            )

        return []

    # распаковка
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
        details = {
            "reason": "data_missing",
            "missing": missing,
            "event": {
                "symbol": symbol,
                "indicator": indicator_base,
                "timeframe": timeframe,
                "open_time": open_time_iso,
                "status": status,
            },
            "ts": {
                "prev_time": prev_time.isoformat(),
                "open_time": open_time.isoformat(),
                "prev_ms": prev_ms,
                "ts_ms": ts_ms,
            },
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

        ctx["counters"]["ignored_total"] = int(ctx["counters"].get("ignored_total", 0)) + 1
        return []

    # precision цены
    ticker_info = get_ticker_info(symbol) or {}
    try:
        precision_price = int(ticker_info.get("precision_price") or 8)
    except Exception:
        precision_price = 8

    H = float(upper_prev) - float(lower_prev)

    base_details = {
        "event": {
            "symbol": symbol,
            "indicator": indicator_base,
            "timeframe": timeframe,
            "open_time": open_time_iso,
            "status": status,
        },
        "ts": {
            "prev_time": prev_time.isoformat(),
            "open_time": open_time.isoformat(),
            "prev_ms": prev_ms,
            "ts_ms": ts_ms,
        },
        "ohlcv": {
            "close_prev": _round_price(float(close_prev), precision_price),
            "close_curr": _round_price(float(close_curr), precision_price),
        },
        "lr": {
            "angle_curr": float(angle_curr),
            "upper_prev": float(upper_prev),
            "lower_prev": float(lower_prev),
            "center_curr": float(center_curr),
            "upper_curr": float(upper_curr) if upper_curr is not None else None,
            "lower_curr": float(lower_curr) if lower_curr is not None else None,
            "H": float(H),
        },
    }

    # 🔸 Один read → два решения (long и short)
    for scfg in ctx.get("signals") or []:
        signal_id = int(scfg["signal_id"])
        direction = str(scfg["direction"])
        message = str(scfg["message"])
        trend_type = str(scfg["trend_type"])
        zone_k = float(scfg["zone_k"])
        keep_half = bool(scfg["keep_half"])

        # условия достаточности
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

        details = {
            **base_details,
            "signal": {
                "signal_id": signal_id,
                "direction": direction,
                "message": message,
                "trend_type": trend_type,
                "zone_k": zone_k,
                "keep_half": keep_half,
            },
            "result": {
                "passed": bool(passed),
                "status": eval_status,
                "extra": eval_extra,
            },
        }

        should_send = await _upsert_live_log(
            pg=pg,
            signal_id=signal_id,
            symbol=symbol,
            timeframe=timeframe,
            open_time=open_time,
            status=("signal_sent" if passed else eval_status),
            details=details,
        )

        if passed and should_send:
            raw_message = {
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "open_time": open_time.isoformat(),
                "direction": direction,
                "message": message,
                "source": "backtester_v1",
                "indicator_base": indicator_base,
                "lr_instance_id": int(ctx.get("lr_instance_id") or 0),
                "trend_type": trend_type,
                "zone_k": float(zone_k),
                "keep_half": bool(keep_half),
                "lr": {
                    "angle_curr": float(angle_curr),
                    "upper_prev": float(upper_prev),
                    "lower_prev": float(lower_prev),
                    "center_curr": float(center_curr),
                    "H": float(H),
                },
                "ohlcv": {
                    "close_prev": _round_price(float(close_prev), precision_price),
                    "close_curr": _round_price(float(close_curr), precision_price),
                },
            }

            live_signals.append(
                {
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "direction": direction,
                    "open_time": open_time,
                    "message": message,
                    "raw_message": raw_message,
                }
            )

            ctx["counters"]["sent_total"] = int(ctx["counters"].get("sent_total", 0)) + 1
            log.info(
                "BT_SIG_LR_UNI_LIVE: signal_sent — signal_id=%s, symbol=%s, dir=%s, time=%s, msg=%s",
                signal_id,
                symbol,
                direction,
                open_time_iso,
                message,
            )

    # 🔸 Суммарный лог (без записи в БД по ignored_wrong_indicator)
    total = int(ctx["counters"].get("messages_total", 0))
    if total % 200 == 0:
        log.info(
            "BT_SIG_LR_UNI_LIVE: summary — messages=%s, sent=%s, ignored=%s (wrong_ind=%s, wrong_tf=%s, not_ready=%s), errors=%s",
            total,
            int(ctx["counters"].get("sent_total", 0)),
            int(ctx["counters"].get("ignored_total", 0)),
            int(ctx["counters"].get("ignored_wrong_indicator", 0)),
            int(ctx["counters"].get("ignored_wrong_tf", 0)),
            int(ctx["counters"].get("ignored_not_ready", 0)),
            int(ctx["counters"].get("errors_total", 0)),
        )

    return live_signals


# 🔸 Оценка bounce по правилам backfill (1-в-1)
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
    # условия по тренду
    if trend_type == "trend":
        long_trend_ok = angle_curr > 0.0
        short_trend_ok = angle_curr < 0.0
    elif trend_type == "counter":
        long_trend_ok = angle_curr < 0.0
        short_trend_ok = angle_curr > 0.0
    else:
        long_trend_ok = True
        short_trend_ok = True

    # LONG bounce
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
            return False, "rejected_zone", {"close_prev": close_prev, "lower_prev": lower_prev, "threshold": threshold}

        if not (close_curr > lower_prev):
            return False, "no_bounce", {"close_curr": close_curr, "lower_prev": lower_prev}

        if keep_half and not (close_curr <= center_curr):
            return False, "rejected_keep_half", {"close_curr": close_curr, "center_curr": center_curr}

        return True, "signal_sent", {"threshold": threshold}

    # SHORT bounce
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
            return False, "rejected_zone", {"close_prev": close_prev, "upper_prev": upper_prev, "threshold": threshold}

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

    try:
        st = str(rows[0]["status"] or "")
    except Exception:
        st = ""
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
        # формат: [[timestamp, "value"]] или []
        if not ts_range_result:
            return None
        point = ts_range_result[0]
        if not point or len(point) < 2:
            return None
        v = point[1]
        return float(v)
    except Exception:
        return None


# 🔸 Округление цены для логов/details
def _round_price(value: float, precision_price: int) -> float:
    try:
        return float(f"{value:.{int(precision_price)}f}")
    except Exception:
        return value