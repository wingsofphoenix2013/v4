# bt_signals_emacross_rsislope_online.py — live EMA-cross + RSI-slope сигналы

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_ticker_info,
    get_indicator_instance,
    get_analysis_instance,
)

# 🔸 Утилиты анализа фич (для feature_name)
from bt_analysis_utils import resolve_feature_name

log = logging.getLogger("BT_SIG_EMA_CROSS_RSISLOPE_LIVE")

# 🔸 Имя таблицы кандидатов good-бинов
BT_ANALYSIS_CANDIDATES_TABLE = "bt_analysis_candidates"


# 🔸 Публичная инициализация live-контекста для EMA-cross + RSI-slope
async def init_emacross_rsislope_live(
    signals: List[Dict[str, Any]],
    pg,
    redis,
) -> Dict[str, Any]:
    """
    Инициализация контекста для live-обработки EMA-cross + RSI-slope.
    На вход подаётся список bt_signals_instances для ema_cross_rsislope с mode ∈ {live, both}.
    """
    configs: List[Dict[str, Any]] = []

    # готовим конфиги для каждого live-сигнала
    for signal in signals:
        sid = signal.get("id")
        key = signal.get("key")
        name = signal.get("name")
        timeframe = signal.get("timeframe")
        params = signal.get("params") or {}

        # поддерживаем только m5 как TF базового сигнала
        if timeframe != "m5":
            log.warning(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') имеет неподдерживаемый timeframe=%s, "
                "ожидается 'm5', сигнал пропущен",
                sid,
                name,
                timeframe,
            )
            continue

        # читаем ema_fast/slow так же, как в обычном emacross
        try:
            fast_cfg = params["ema_fast_instance_id"]
            slow_cfg = params["ema_slow_instance_id"]
            fast_instance_id = int(fast_cfg["value"])
            slow_instance_id = int(slow_cfg["value"])
        except Exception as e:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') — некорректные параметры EMA-инстансов: %s",
                sid,
                name,
                e,
            )
            continue

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

        # для rsislope-сигнала ожидаем однозначное направление (только long или только short)
        if allowed_directions == {"long"}:
            signal_direction = "long"
        elif allowed_directions == {"short"}:
            signal_direction = "short"
        else:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') имеет неоднозначный direction_mask=%s "
                "(ожидается только 'long' или только 'short' для rsislope-сигнала)",
                sid,
                name,
                mask_val,
            )
            continue

        # читаем параметры привязки к анализу
        trigger_scenario_id = _get_int_param(params, "trigger_scenario_id", default=None)
        trigger_base_signal_id = _get_int_param(params, "trigger_base_signal_id", default=None)
        trigger_family_key = _get_str_param(params, "trigger_family_key", default=None)
        trigger_analysis_id = _get_int_param(params, "trigger_analysis_id", default=None)
        trigger_version = _get_str_param(params, "trigger_version", default=None) or "v2"

        if trigger_analysis_id is None:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') не имеет корректного trigger_analysis_id",
                sid,
                name,
            )
            continue

        # загружаем анализатор, чтобы взять timeframe/source_key/slope_k
        analysis_inst = get_analysis_instance(trigger_analysis_id)
        if not analysis_inst:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: для сигнала id=%s ('%s') не найден анализатор analysis_id=%s",
                sid,
                name,
                trigger_analysis_id,
            )
            continue

        analysis_family = (analysis_inst.get("family_key") or "").lower()
        analysis_params = analysis_inst.get("params") or {}

        # лёгкая проверка family_key
        if trigger_family_key and analysis_family and trigger_family_key.lower() != analysis_family:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') ожидает family_key=%s, "
                "но анализатор analysis_id=%s имеет family_key=%s",
                sid,
                name,
                trigger_family_key,
                trigger_analysis_id,
                analysis_family,
            )
            continue

        # читаем timeframe/source_key RSI из анализатора
        tf_cfg = analysis_params.get("timeframe")
        source_cfg = analysis_params.get("source_key")

        rsi_timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "h1"
        rsi_source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi21"

        # slope_k (шаг по RSI) читаем так же, как в анализаторе: из params или дефолт 3
        slope_k = _get_int_param_from_analysis(analysis_params, "slope_k", default=3)

        # имя фичи для кандидатов (строго так же, как в анализаторе)
        feature_name = resolve_feature_name(
            family_key="rsi",
            key="rsi_slope",
            timeframe=rsi_timeframe,
            source_key=rsi_source_key,
        )

        # читаем good-бинчики из bt_analysis_candidates для текущего направления
        candidate_ranges = await _load_rsi_slope_candidate_ranges(
            pg=pg,
            scenario_id=trigger_scenario_id,
            base_signal_id=trigger_base_signal_id,
            analysis_id=trigger_analysis_id,
            family_key="rsi",
            key="rsi_slope",
            direction=signal_direction,
            timeframe=rsi_timeframe,
            feature_name=feature_name,
            version=trigger_version,
        )

        if not candidate_ranges:
            log.info(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: для сигнала id=%s ('%s') нет кандидатов rsi_slope "
                "в %s, live-сигнал пропущен",
                sid,
                name,
                BT_ANALYSIS_CANDIDATES_TABLE,
            )
            continue

        # определяем имена индикаторов EMA (indicator_stream.indicator) через инстансы индикаторов
        fast_name = _resolve_ema_param_name(fast_instance_id)
        slow_name = _resolve_ema_param_name(slow_instance_id)
        if not fast_name or not slow_name:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: не удалось определить имена EMA-параметров "
                "для fast_instance_id=%s, slow_instance_id=%s (signal_id=%s)",
                fast_instance_id,
                slow_instance_id,
                sid,
            )
            continue

        # читаем сообщения для live-сигнала
        long_msg = _get_str_param(params, "long_message", default=None)
        short_msg = _get_str_param(params, "short_message", default=None)
        if not long_msg and not short_msg:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: сигнал id=%s ('%s') не имеет long_message/short_message, "
                "live-сигнал не сможет публиковаться в signals_stream",
                sid,
                name,
            )
            continue

        config: Dict[str, Any] = {
            "signal": signal,
            "signal_id": sid,
            "name": name,
            "key": key,
            "timeframe": timeframe,
            "allowed_directions": allowed_directions,
            "direction": signal_direction,
            "fast_indicator": fast_name,
            "slow_indicator": slow_name,
            "rsi_indicator": rsi_source_key,
            "rsi_timeframe": rsi_timeframe,
            "slope_k": slope_k,
            "candidate_ranges": candidate_ranges,
            "trigger_scenario_id": trigger_scenario_id,
            "trigger_base_signal_id": trigger_base_signal_id,
            "trigger_analysis_id": trigger_analysis_id,
            "trigger_version": trigger_version,
            "long_message": long_msg,
            "short_message": short_msg,
            # состояние EMA-кросса по символам
            "ema_prev_state": {},        # symbol -> "above"/"below"/None
            "ema_ready": {},             # (symbol, open_time_iso) -> {"fast": bool, "slow": bool}
            "processed_bars": set(),     # (symbol, open_time_iso) для защиты от дублей
            # ожидание RSI на границе часа: (symbol, anchor_h1_iso) -> set(open_time_m5_iso)
            "pending_rsi": {},
        }

        configs.append(config)

    if not configs:
        log.warning(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: ни один live-сигнал ema_cross_rsislope не инициализирован "
            "(из %s штук)",
            len(signals),
        )
    else:
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: инициализировано live-конфигов EMA-cross+RSI-slope: %s (из %s сигналов)",
            len(configs),
            len(signals),
        )

    return {
        "configs": configs,
    }


# 🔸 Обработка сообщения indicator_stream для EMA-cross + RSI-slope (live)
async def handle_emacross_rsislope_indicator_event(
    live_ctx: Dict[str, Any],
    fields: Dict[str, str],
    pg,
    redis,
) -> List[Dict[str, Any]]:
    """
    Обработка одного сообщения из indicator_stream.
    На выходе — список live-сигналов, готовых к публикации.
    Одновременно логируется bt_signals_live по каждому обработанному bar/symbol.
    """
    configs: List[Dict[str, Any]] = live_ctx.get("configs") or []
    if not configs:
        return []

    # минимальный набор полей indicator_stream:
    # symbol, indicator, timeframe, open_time, status
    status = fields.get("status")
    if status != "ready":
        return []

    symbol = fields.get("symbol")
    indicator = fields.get("indicator")
    timeframe = fields.get("timeframe")
    open_time_str = fields.get("open_time")

    if not (symbol and indicator and timeframe and open_time_str):
        return []

    try:
        open_time = datetime.fromisoformat(open_time_str)
    except Exception:
        # некорректный формат времени, пропускаем
        return []

    live_signals: List[Dict[str, Any]] = []

    # пробегаем по всем live-конфигам rsislope
    for cfg in configs:
        fast_name = cfg["fast_indicator"]
        slow_name = cfg["slow_indicator"]
        rsi_name = cfg["rsi_indicator"]
        rsi_tf = cfg["rsi_timeframe"]
        signal_id = cfg["signal_id"]
        tf_m5 = cfg["timeframe"]

        # ветка EMA m5
        if timeframe == tf_m5 and indicator in (fast_name, slow_name):
            # помечаем готовность EMA
            bar_key = (symbol, open_time_str)
            ema_ready = cfg["ema_ready"].setdefault(bar_key, {"fast": False, "slow": False})

            if indicator == fast_name:
                ema_ready["fast"] = True
            elif indicator == slow_name:
                ema_ready["slow"] = True

            # пока не готовы оба — дальше не идём
            if not (ema_ready["fast"] and ema_ready["slow"]):
                continue

            # EMA по этому бару m5 готова (оба значения)
            # решаем, считаем сразу или ждём RSI (граница часа)
            minute = open_time.minute

            # вычисляем "часовой контекст" для определения якорного часа
            # для m5 open_time используем open_time + 5 минут → floor_to_hour → anchor_h1 = hour - 1
            dt_for_hour = open_time + timedelta(minutes=5)
            floor_hour = dt_for_hour.replace(minute=0, second=0, microsecond=0)
            anchor_h1 = floor_hour - timedelta(hours=1)
            anchor_h1_iso = anchor_h1.isoformat()

            # защита от повторных вычислений
            processed_bars: Set[Tuple[str, str]] = cfg["processed_bars"]
            if bar_key in processed_bars:
                continue

            # bar не закрывает час → RSI прошлого часа уже должен быть,
            # считаем сразу
            if minute != 55:
                result_status, details, lsigs = await _compute_live_result_for_bar(
                    cfg,
                    symbol,
                    open_time,
                    anchor_h1,
                    pg,
                    redis,
                )
                # логируем результат, если был статус
                if result_status is not None:
                    await _log_live_result(
                        pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_m5,
                        open_time=open_time,
                        status=result_status,
                        details=details,
                    )
                if lsigs:
                    live_signals.extend(lsigs)
                # помечаем бар обработанным
                processed_bars.add(bar_key)
                # удаляем готовность по EMA для этого бара
                try:
                    del cfg["ema_ready"][bar_key]
                except KeyError:
                    pass
            else:
                # bar закрывает час (минуты == 55) → ждём RSI(h1) ready
                pending_rsi: Dict[Tuple[str, str], Set[str]] = cfg["pending_rsi"]
                pend_key = (symbol, anchor_h1_iso)
                bar_set = pending_rsi.setdefault(pend_key, set())
                bar_set.add(open_time_str)
                log.debug(
                    "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: m5-бар на границе часа, откладываем до готовности RSI "
                    "(symbol=%s, open_time_m5=%s, anchor_h1=%s, signal_id=%s)",
                    symbol,
                    open_time,
                    anchor_h1,
                    signal_id,
                )
                # здесь пока не считаем, просто ждём RSI
            continue

        # ветка RSI h1
        if timeframe == rsi_tf and indicator == rsi_name:
            # это сигнал о готовности RSI(h1) для некоторого часа
            anchor_h1 = open_time
            anchor_h1_iso = anchor_h1.isoformat()
            pending_rsi: Dict[Tuple[str, str], Set[str]] = cfg["pending_rsi"]
            pend_key = (symbol, anchor_h1_iso)

            if pend_key not in pending_rsi:
                # нет отложенных m5-баров, которым нужен этот RSI
                continue

            # есть список m5-баров, которые ждали этот RSI
            open_times_m5_str = list(pending_rsi[pend_key])
            del pending_rsi[pend_key]

            for ot_str in open_times_m5_str:
                try:
                    ot_m5 = datetime.fromisoformat(ot_str)
                except Exception:
                    continue

                bar_key = (symbol, ot_str)
                processed_bars: Set[Tuple[str, str]] = cfg["processed_bars"]
                if bar_key in processed_bars:
                    continue

                result_status, details, lsigs = await _compute_live_result_for_bar(
                    cfg,
                    symbol,
                    ot_m5,
                    anchor_h1,
                    pg,
                    redis,
                )
                if result_status is not None:
                    await _log_live_result(
                        pg,
                        signal_id=signal_id,
                        symbol=symbol,
                        timeframe=tf_m5,
                        open_time=ot_m5,
                        status=result_status,
                        details=details,
                    )
                if lsigs:
                    live_signals.extend(lsigs)

                processed_bars.add(bar_key)
                try:
                    del cfg["ema_ready"][bar_key]
                except KeyError:
                    pass

    return live_signals


# 🔸 Расчёт результата (статуса и, возможно, live-сигнала) по одному m5-бару
async def _compute_live_result_for_bar(
    cfg: Dict[str, Any],
    symbol: str,
    open_time_m5: datetime,
    anchor_h1: datetime,
    pg,
    redis,
) -> Tuple[Optional[str], Dict[str, Any], List[Dict[str, Any]]]:
    """
    Возвращает:
      - status (строка для bt_signals_live или None, если лог не нужен),
      - details (dict для JSON),
      - список live-сигналов (0 или 1 элемент).
    """
    signal_id = cfg["signal_id"]
    timeframe = cfg["timeframe"]  # ожидается 'm5'
    fast_name = cfg["fast_indicator"]
    slow_name = cfg["slow_indicator"]

    details: Dict[str, Any] = {
        "signal_id": signal_id,
        "signal_key": cfg.get("key"),
        "symbol": symbol,
        "timeframe": timeframe,
        "open_time_m5": open_time_m5.isoformat(),
        "anchor_h1": anchor_h1.isoformat(),
    }

    # читаем EMA fast/slow из Redis TS
    fast_val = await _get_indicator_value_ts(redis, symbol, timeframe, fast_name, open_time_m5)
    slow_val = await _get_indicator_value_ts(redis, symbol, timeframe, slow_name, open_time_m5)

    details["ema_fast"] = fast_val
    details["ema_slow"] = slow_val

    if fast_val is None or slow_val is None:
        status = "cross_rejected_rsi_not_ready"
        details["reason"] = "no_ema_values"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: нет EMA-значений для %s на баре %s (fast=%s, slow=%s), signal_id=%s",
            symbol,
            open_time_m5,
            fast_val,
            slow_val,
            signal_id,
        )
        return status, details, []

    # epsilon = 1 * ticksize (как в обычном emacross)
    ticker_info = get_ticker_info(symbol) or {}
    ticksize = ticker_info.get("ticksize")
    try:
        epsilon = 1.0 * float(ticksize) if ticksize is not None else 0.0
    except Exception:
        epsilon = 0.0

    details["ticksize"] = ticksize
    details["epsilon"] = epsilon

    ema_prev_state: Dict[str, Optional[str]] = cfg["ema_prev_state"]
    prev_state = ema_prev_state.get(symbol)

    diff = fast_val - slow_val
    state = _classify_state(diff, epsilon)

    details["ema_diff"] = diff
    details["ema_state_new"] = state
    details["ema_state_prev"] = prev_state

    # зона неопределённости — состояние не меняем, сигнала нет
    if state == "neutral":
        status = "no_cross_neutral_zone"
        details["reason"] = "neutral_zone"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: расчёт выполнен, но EMA-кросса нет (neutral), "
            "symbol=%s, time=%s, diff=%.8f, epsilon=%.8f, signal_id=%s",
            symbol,
            open_time_m5,
            diff,
            epsilon,
            signal_id,
        )
        ema_prev_state[symbol] = prev_state  # не меняем
        return status, details, []

    # первая инициализация состояния
    if prev_state is None:
        status = "no_cross_state_unchanged"
        details["reason"] = "initial_state"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: первая инициализация состояния EMA, "
            "symbol=%s, time=%s, state=%s, signal_id=%s",
            symbol,
            open_time_m5,
            state,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    # состояние не изменилось
    if state == prev_state:
        status = "no_cross_state_unchanged"
        details["reason"] = "state_not_changed"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросса нет — состояние не изменилось "
            "(symbol=%s, time=%s, state=%s, signal_id=%s)",
            symbol,
            open_time_m5,
            state,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    # фиксируем кросс с учётом смены состояния
    if prev_state == "below" and state == "above":
        direction = "long"
    elif prev_state == "above" and state == "below":
        direction = "short"
    else:
        status = "error"
        details["reason"] = "invalid_state_transition"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: изменение состояния EMA без валидного кросса "
            "(symbol=%s, time=%s, prev_state=%s, state=%s, signal_id=%s)",
            symbol,
            open_time_m5,
            prev_state,
            state,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    details["direction"] = direction
    log.info(
        "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: найден EMA-кросс, symbol=%s, time=%s, direction=%s, "
        "diff=%.8f, epsilon=%.8f, signal_id=%s",
        symbol,
        open_time_m5,
        direction,
        diff,
        epsilon,
        signal_id,
    )

    # фильтр по маске направлений
    allowed_directions: Set[str] = cfg["allowed_directions"]
    if direction not in allowed_directions:
        status = "cross_rejected_direction_mask"
        details["reason"] = "direction_mask"
        details["allowed_directions"] = sorted(allowed_directions)
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросс есть, но направление не разрешено маской "
            "(symbol=%s, time=%s, direction=%s, allowed=%s, signal_id=%s)",
            symbol,
            open_time_m5,
            direction,
            ",".join(sorted(allowed_directions)),
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    # расчитываем RSI-slope для этого бара
    rsi_pair = await _compute_rsi_slope_for_bar_ts(cfg, symbol, open_time_m5, anchor_h1, redis)
    if rsi_pair is None:
        status = "cross_rejected_rsi_not_ready"
        details["reason"] = "rsi_not_ready_or_insufficient_history"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросс есть, но RSI-slope ещё не готов или недостаточно истории "
            "(symbol=%s, time=%s, direction=%s, signal_id=%s)",
            symbol,
            open_time_m5,
            direction,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    rsi_t, rsi_prev, slope = rsi_pair
    details["rsi_t"] = rsi_t
    details["rsi_prev"] = rsi_prev
    details["rsi_slope"] = slope

    # проверяем попадание slope в кандидатные диапазоны
    candidate_ranges: List[Tuple[float, float]] = cfg["candidate_ranges"]
    details["candidate_ranges"] = [{"from": r[0], "to": r[1]} for r in candidate_ranges]

    if not _slope_in_ranges(slope, candidate_ranges):
        status = "cross_rejected_rsi_slope"
        details["reason"] = "slope_not_in_good_ranges"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросс есть, но не прошёл по RSI-slope "
            "(symbol=%s, time=%s, direction=%s, slope=%.5f, signal_id=%s)",
            symbol,
            open_time_m5,
            direction,
            slope,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    # достаём цену close на m5
    price = await _get_close_price_ts(redis, symbol, timeframe, open_time_m5)
    details["close_price"] = price

    if price is None:
        status = "cross_rejected_rsi_not_ready"
        details["reason"] = "no_close_price"
        log.info(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросс + RSI-slope прошли, но нет цены close "
            "(symbol=%s, time=%s, signal_id=%s)",
            symbol,
            open_time_m5,
            signal_id,
        )
        ema_prev_state[symbol] = state
        return status, details, []

    # формируем live-сигнал для публикации
    signal = cfg["signal"]
    long_message = cfg.get("long_message")
    short_message = cfg.get("short_message")

    if direction == "long":
        message = long_message or "EMA_CROSS_RSISLOPE_LONG"
    else:
        message = short_message or "EMA_CROSS_RSISLOPE_SHORT"

    raw_message = {
        "mode": "live",
        "signal_key": signal.get("key"),
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "open_time": open_time_m5.isoformat(),
        "direction": direction,
        "price": float(price),
        "rsi_timeframe": cfg["rsi_timeframe"],
        "rsi_source_key": cfg["rsi_indicator"],
        "slope_k": cfg["slope_k"],
        "rsi_t": float(rsi_t),
        "rsi_prev": float(rsi_prev),
        "slope": float(slope),
        "candidate_ranges": details["candidate_ranges"],
    }

    log.info(
        "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: EMA-кросс и RSI-slope прошли все фильтры, "
        "готовим live-сигнал symbol=%s, time=%s, direction=%s, price=%.8f, slope=%.5f, signal_id=%s",
        symbol,
        open_time_m5,
        direction,
        price,
        slope,
        signal_id,
    )

    details["reason"] = "signal_sent"

    live_signal = {
        "signal": signal,
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "direction": direction,
        "open_time": open_time_m5,
        "message": message,
        "raw_message": raw_message,
    }

    ema_prev_state[symbol] = state
    return "signal_sent", details, [live_signal]


# 🔸 Логирование результата в bt_signals_live
async def _log_live_result(
    pg,
    signal_id: int,
    symbol: str,
    timeframe: str,
    open_time: datetime,
    status: str,
    details: Dict[str, Any],
) -> None:
    log_db = logging.getLogger("BT_SIGNALS_LIVE_DB")
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bt_signals_live (signal_id, symbol, timeframe, open_time, status, details)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (signal_id, symbol, timeframe, open_time)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    details = EXCLUDED.details,
                    created_at = now()
                """,
                signal_id,
                symbol,
                timeframe,
                open_time,
                status,
                details,
            )
    except Exception as e:
        log_db.error(
            "BT_SIGNALS_LIVE_DB: ошибка при вставке bt_signals_live "
            "(signal_id=%s, symbol=%s, timeframe=%s, open_time=%s, status=%s): %s",
            signal_id,
            symbol,
            timeframe,
            open_time,
            status,
            e,
            exc_info=True,
        )


# 🔸 Расчёт RSI-slope по Redis TS с учётом якорного часа
async def _compute_rsi_slope_for_bar_ts(
    cfg: Dict[str, Any],
    symbol: str,
    open_time_m5: datetime,
    anchor_h1: datetime,
    redis,
) -> Optional[Tuple[float, float, float]]:
    rsi_tf = cfg["rsi_timeframe"]
    rsi_name = cfg["rsi_indicator"]
    slope_k = cfg["slope_k"]

    # ключ Redis TS для RSI
    ts_key = f"ts_ind:{symbol}:{rsi_tf}:{rsi_name}"
    anchor_ts = int(anchor_h1.timestamp() * 1000)

    # берём slope_k+1 последних баров ≤ anchor_h1
    rows = await _ts_revrange(redis, ts_key, 0, anchor_ts, slope_k + 1)
    if not rows or len(rows) < slope_k + 1:
        return None

    # rows отсортирован в обратном порядке по времени: [anchor_h1, предыдущий, ...]
    try:
        rsi_t = float(rows[0][1])
        rsi_prev = float(rows[slope_k][1])
    except Exception:
        return None

    slope = rsi_t - rsi_prev
    return rsi_t, rsi_prev, slope


# 🔸 Классификация состояния fast vs slow по diff и epsilon (та же логика, что в backfill)
def _classify_state(diff: float, epsilon: float) -> str:
    if epsilon <= 0:
        # без epsilon считаем только знак
        if diff > 0:
            return "above"
        elif diff < 0:
            return "below"
        else:
            return "neutral"

    if diff > epsilon:
        return "above"
    if diff < -epsilon:
        return "below"
    return "neutral"


# 🔸 Проверка попадания slope в один из кандидатных диапазонов
def _slope_in_ranges(slope: float, ranges: List[Tuple[float, float]]) -> bool:
    for b_from, b_to in ranges:
        # используем полуоткрытый интервал [from, to)
        if slope >= b_from and slope < b_to:
            return True
    return False


# 🔸 Загрузка good-диапазонов rsi_slope из bt_analysis_candidates
async def _load_rsi_slope_candidate_ranges(
    pg,
    scenario_id: Optional[int],
    base_signal_id: Optional[int],
    analysis_id: int,
    family_key: str,
    key: str,
    direction: str,
    timeframe: str,
    feature_name: str,
    version: Optional[str],
) -> List[Tuple[float, float]]:
    ranges: List[Tuple[float, float]] = []

    if scenario_id is None or base_signal_id is None:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: невозможно загрузить кандидатов rsi_slope — "
            "scenario_id или base_signal_id не заданы (scenario_id=%s, base_signal_id=%s)",
            scenario_id,
            base_signal_id,
        )
    else:
        async with pg.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT bin_from, bin_to, trades, winrate, bin_roi, coverage
                FROM {BT_ANALYSIS_CANDIDATES_TABLE}
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND analysis_id  = $3
                  AND family_key   = $4
                  AND "key"        = $5
                  AND direction    = $6
                  AND timeframe    = $7
                  AND feature_name = $8
                  AND version      = $9
                """,
                scenario_id,
                base_signal_id,
                analysis_id,
                family_key,
                key,
                direction,
                timeframe,
                feature_name,
                version or "v2",
            )

        for r in rows:
            b_from = r["bin_from"]
            b_to = r["bin_to"]
            try:
                f_from = float(b_from) if b_from is not None else float("-inf")
                f_to = float(b_to) if b_to is not None else float("inf")
                ranges.append((f_from, f_to))
            except Exception:
                continue

    log.info(
        "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: загружено кандидатов rsi_slope=%s для scenario_id=%s, base_signal_id=%s, "
        "analysis_id=%s, direction=%s, timeframe=%s",
        len(ranges),
        scenario_id,
        base_signal_id,
        analysis_id,
        direction,
        timeframe,
    )
    return ranges


# 🔸 Определение имени EMA-параметра по instance_id
def _resolve_ema_param_name(instance_id: int) -> Optional[str]:
    inst = get_indicator_instance(instance_id)
    if not inst:
        return None

    indicator = inst.get("indicator")
    params = inst.get("params") or {}
    if indicator != "ema":
        return None

    length_raw = params.get("length")
    if length_raw is None:
        return None

    try:
        length = int(str(length_raw))
    except Exception:
        return None

    # в indicators_v4 для EMA base/param_name = f"ema{length}"
    return f"ema{length}"


# 🔸 Чтение значения индикатора из Redis TS для конкретного бара
async def _get_indicator_value_ts(
    redis,
    symbol: str,
    timeframe: str,
    param_name: str,
    open_time: datetime,
) -> Optional[float]:
    ts_key = f"ts_ind:{symbol}:{timeframe}:{param_name}"
    ts_ms = int(open_time.timestamp() * 1000)

    rows = await _ts_range(redis, ts_key, ts_ms, ts_ms)
    if not rows:
        return None

    try:
        return float(rows[0][1])
    except Exception:
        return None


# 🔸 Чтение close-цены из Redis TS OHLCV по m5
async def _get_close_price_ts(
    redis,
    symbol: str,
    timeframe: str,
    open_time: datetime,
) -> Optional[float]:
    if timeframe != "m5":
        return None

    ts_key = f"bb:ts:{symbol}:{timeframe}:c"
    ts_ms = int(open_time.timestamp() * 1000)

    rows = await _ts_range(redis, ts_key, ts_ms, ts_ms)
    if not rows:
        return None

    try:
        return float(rows[0][1])
    except Exception:
        return None


# 🔸 Обёртки над TS.RANGE / TS.REVRANGE (минимальная совместимость)
async def _ts_range(
    redis,
    key: str,
    from_ts: int,
    to_ts: int,
) -> List[Tuple[int, float]]:
    try:
        # формат ответа: [[ts, value], ...]
        rows = await redis.execute_command("TS.RANGE", key, from_ts, to_ts)
        return rows or []
    except Exception as e:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: ошибка TS.RANGE key=%s, from=%s, to=%s: %s",
            key,
            from_ts,
            to_ts,
            e,
            exc_info=True,
        )
        return []


async def _ts_revrange(
    redis,
    key: str,
    from_ts: int,
    to_ts: int,
    count: int,
) -> List[Tuple[int, float]]:
    try:
        # формат ответа: [[ts, value], ...] в обратном порядке по времени
        rows = await redis.execute_command("TS.REVRANGE", key, from_ts, to_ts, "COUNT", count)
        return rows or []
    except Exception as e:
        log.error(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: ошибка TS.REVRANGE key=%s, from=%s, to=%s, count=%s: %s",
            key,
            from_ts,
            to_ts,
            count,
            e,
            exc_info=True,
        )
        return []


# 🔸 Вспомогательные функции чтения параметров
def _get_int_param(params: Dict[str, Any], name: str, default: Optional[int]) -> Optional[int]:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    try:
        return int(str(raw))
    except Exception:
        return default


def _get_str_param(params: Dict[str, Any], name: str, default: Optional[str]) -> Optional[str]:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    if raw is None:
        return default
    return str(raw)


def _get_int_param_from_analysis(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default
    try:
        # в analysis_params значение лежит внутри {"value": ...}
        return int(str(cfg.get("value")))
    except Exception:
        return default