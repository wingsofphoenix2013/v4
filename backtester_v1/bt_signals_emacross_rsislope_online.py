# bt_signals_emacross_rsislope_online.py — live EMA-cross + RSI-slope сигналы

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

# 🔸 Кеши backtester_v1
from backtester_config import (
    get_ticker_info,
    get_indicator_instance,
    get_analysis_instance,
)

# 🔸 Анализатор RSI (резолв RSI-инстанса)
from bt_analysis_rsi import _resolve_rsi_instance_id

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

        # резолвим instance_id RSI через ту же функцию, что и анализатор
        rsi_instance_id = _resolve_rsi_instance_id(rsi_timeframe, rsi_source_key)
        if rsi_instance_id is None:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: не удалось найти RSI instance_id для timeframe=%s, source_key=%s "
                "(analysis_id=%s, signal_id=%s)",
                rsi_timeframe,
                rsi_source_key,
                trigger_analysis_id,
                sid,
            )
            continue

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

    # реагируем только на m5, потому что триггер по EMA на m5
    if timeframe != "m5":
        return []

    live_signals: List[Dict[str, Any]] = []

    # пробегаем по всем live-конфигам rsislope
    for cfg in configs:
        # для данного конфига интересны только его fast/slow EMA
        fast_name = cfg["fast_indicator"]
        slow_name = cfg["slow_indicator"]

        if indicator not in (fast_name, slow_name):
            # это не тот индикатор, который участвует в EMA-кроссе для данного конфига
            continue

        bar_key = (symbol, open_time_str)
        ema_ready = cfg["ema_ready"].setdefault(bar_key, {"fast": False, "slow": False})

        # помечаем готовность fast/slow
        if indicator == fast_name:
            ema_ready["fast"] = True
        elif indicator == slow_name:
            ema_ready["slow"] = True

        # пока не готовы оба — дальше не идём
        if not (ema_ready["fast"] and ema_ready["slow"]):
            continue

        # защита от повторной обработки этого бара для данного конфига
        processed_bars: Set[Tuple[str, str]] = cfg["processed_bars"]
        if bar_key in processed_bars:
            # уже обработан этот бар для этого конфига
            continue

        # отмечаем бар как обработанный
        processed_bars.add(bar_key)

        # кросс считаем только один раз для этого бара и конфига
        try:
            lsigs = await _compute_live_signals_for_bar(
                cfg,
                symbol,
                open_time,
                pg,
                redis,
            )
        except Exception as e:
            log.error(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: ошибка расчёта live-сигнала для symbol=%s, time=%s, signal_id=%s: %s",
                symbol,
                open_time,
                cfg["signal_id"],
                e,
                exc_info=True,
            )
            lsigs = []

        # очищаем состояние ready по этому бару
        try:
            del cfg["ema_ready"][bar_key]
        except KeyError:
            # уже удалено — не страшно
            pass

        if lsigs:
            live_signals.extend(lsigs)

    return live_signals


# 🔸 Расчёт live-сигналов по одному бару m5 для конкретного конфига
async def _compute_live_signals_for_bar(
    cfg: Dict[str, Any],
    symbol: str,
    open_time: datetime,
    pg,
    redis,
) -> List[Dict[str, Any]]:
    # читаем EMA fast/slow из Redis TS
    fast_name = cfg["fast_indicator"]
    slow_name = cfg["slow_indicator"]
    timeframe = cfg["timeframe"]  # ожидается 'm5'

    fast_val = await _get_indicator_value_ts(redis, symbol, timeframe, fast_name, open_time)
    slow_val = await _get_indicator_value_ts(redis, symbol, timeframe, slow_name, open_time)

    if fast_val is None or slow_val is None:
        log.debug(
            "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: недостаточно данных EMA для %s на баре %s (fast=%s, slow=%s), "
            "signal_id=%s",
            symbol,
            open_time,
            fast_val,
            slow_val,
            cfg["signal_id"],
        )
        return []

    # epsilon = 1 * ticksize (как в обычном emacross)
    ticker_info = get_ticker_info(symbol) or {}
    ticksize = ticker_info.get("ticksize")
    try:
        epsilon = 1.0 * float(ticksize) if ticksize is not None else 0.0
    except Exception:
        epsilon = 0.0

    # классификация состояний и поиск кросса EMA для этого конфига и символа
    ema_prev_state: Dict[str, Optional[str]] = cfg["ema_prev_state"]
    prev_state = ema_prev_state.get(symbol)

    diff = fast_val - slow_val
    state = _classify_state(diff, epsilon)

    # зона неопределённости — состояние не меняем, сигнала нет
    if state == "neutral":
        return []

    # первая инициализация состояния
    if prev_state is None:
        ema_prev_state[symbol] = state
        return []

    live_signals: List[Dict[str, Any]] = []

    if state != prev_state:
        # фиксируем кросс с учётом смены состояния
        if prev_state == "below" and state == "above":
            direction = "long"
        elif prev_state == "above" and state == "below":
            direction = "short"
        else:
            ema_prev_state[symbol] = state
            return []

        # фильтр по маске направлений
        allowed_directions: Set[str] = cfg["allowed_directions"]
        if direction not in allowed_directions:
            ema_prev_state[symbol] = state
            return []

        # расчитываем RSI-slope для этого бара
        rsi_pair = await _compute_rsi_slope_for_bar(cfg, symbol, open_time, redis)
        if rsi_pair is None:
            # либо RSI ещё не готов (особенно на начале часа), либо недостаточно истории
            ema_prev_state[symbol] = state
            return []

        rsi_t, rsi_prev, slope = rsi_pair

        # проверяем попадание slope в кандидатные диапазоны
        candidate_ranges: List[Tuple[float, float]] = cfg["candidate_ranges"]
        if not _slope_in_ranges(slope, candidate_ranges):
            ema_prev_state[symbol] = state
            return []

        # достаём цену close на m5
        price = await _get_close_price_ts(redis, symbol, timeframe, open_time)
        if price is None:
            ema_prev_state[symbol] = state
            return []

        # формируем live-сигнал для публикации
        signal_id = cfg["signal_id"]
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
            "open_time": open_time.isoformat(),
            "direction": direction,
            "price": float(price),
            "rsi_timeframe": cfg["rsi_timeframe"],
            "rsi_source_key": cfg["rsi_indicator"],
            "slope_k": cfg["slope_k"],
            "rsi_t": float(rsi_t),
            "rsi_prev": float(rsi_prev),
            "slope": float(slope),
            "candidate_ranges": [
                {"from": r[0], "to": r[1]} for r in candidate_ranges
            ],
        }

        live_signals.append(
            {
                "signal": signal,
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "direction": direction,
                "open_time": open_time,
                "message": message,
                "raw_message": raw_message,
            }
        )

    # обновляем состояние EMA
    ema_prev_state[symbol] = state
    return live_signals


# 🔸 Расчёт RSI-slope для бара (учитываем особый случай начала часа)
async def _compute_rsi_slope_for_bar(
    cfg: Dict[str, Any],
    symbol: str,
    open_time: datetime,
    redis,
) -> Optional[Tuple[float, float, float]]:
    rsi_tf = cfg["rsi_timeframe"]
    rsi_name = cfg["rsi_indicator"]
    slope_k = cfg["slope_k"]

    # ключ Redis TS для RSI
    ts_key = f"ts_ind:{symbol}:{rsi_tf}:{rsi_name}"
    ts_ms = int(open_time.timestamp() * 1000)

    # особый случай: rsi_timeframe = h1 и начало часа — ждём новый бар до 3 раз по 20сек
    if rsi_tf.lower() == "h1" and open_time.minute == 0:
        for attempt in range(3):
            # пытаемся взять бар ровно с open_time == ts
            rows = await _ts_range(redis, ts_key, ts_ms, ts_ms)
            if rows:
                # нашли свежий h1-бар
                break
            # нет свежего бара — ждём и пробуем ещё
            if attempt < 2:
                await asyncio.sleep(20)

        # после ретраев ещё может не быть свежего бара — тогда просто скипаем
        rows = await _ts_range(redis, ts_key, ts_ms, ts_ms)
        if not rows:
            log.debug(
                "BT_SIG_EMA_CROSS_RSISLOPE_LIVE: для symbol=%s, time=%s, rsi_tf=%s, rsi_name=%s "
                "после ожидания нет нового h1-бара, сигнал скипнут",
                symbol,
                open_time,
                rsi_tf,
                rsi_name,
            )
            return None

        # текущий RSI на этом ts
        rsi_t = float(rows[0][1])

        # теперь получаем k баров назад (включая текущий) через REVRANGE
        rows_prev = await _ts_revrange(redis, ts_key, 0, ts_ms, slope_k + 1)
        if not rows_prev or len(rows_prev) < slope_k + 1:
            return None

        rsi_prev = float(rows_prev[slope_k][1])
        slope = rsi_t - rsi_prev
        return rsi_t, rsi_prev, slope

    # общий случай: берём последний бар <= ts и k баров назад
    rows = await _ts_revrange(redis, ts_key, 0, ts_ms, slope_k + 1)
    if not rows or len(rows) < slope_k + 1:
        return None

    # rows отсортирован в обратном порядке по времени: [текущий/последний<=ts, предыдущий, ...]
    rsi_t = float(rows[0][1])
    rsi_prev = float(rows[slope_k][1])
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

    log.debug(
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