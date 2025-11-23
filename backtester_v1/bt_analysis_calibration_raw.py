# bt_analysis_calibration_raw.py — сбор сырых значений фич (калибровочный слой анализа)

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (анализаторы и индикаторы)
from backtester_config import get_analysis_instance, get_all_indicator_instances

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_CALIB_RAW")

# 🔸 Константы стримов анализа
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
CALIB_READY_STREAM_KEY = "bt:analysis:calibration:ready"
CALIB_CONSUMER_GROUP = "bt_analysis_calib_raw"
CALIB_CONSUMER_NAME = "bt_analysis_calib_raw_main"

# 🔸 Настройки чтения стрима bt:analysis:ready
CALIB_STREAM_BATCH_SIZE = 10
CALIB_STREAM_BLOCK_MS = 5000

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Квантование до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Извлечение значения RSI из raw_stat с учётом TF и ключа
def _extract_rsi_value(
    raw_stat: Any,
    timeframe: str,
    source_key: str,
) -> Optional[float]:
    # если raw_stat пришёл как JSON-строка — разбираем
    if isinstance(raw_stat, str):
        try:
            raw_stat = json.loads(raw_stat)
        except Exception:
            return None

    if not isinstance(raw_stat, dict):
        return None

    tf_map = raw_stat.get("tf")
    if not isinstance(tf_map, dict):
        return None

    tf_lower: Dict[str, Any] = {str(k).lower(): v for k, v in tf_map.items()}
    tf_block = tf_lower.get(timeframe.lower())
    if not isinstance(tf_block, dict):
        return None

    indicators = tf_block.get("indicators")
    if not isinstance(indicators, dict):
        return None

    indicators_lower: Dict[str, Any] = {str(k).lower(): v for k, v in indicators.items()}
    rsi_block_raw = indicators_lower.get("rsi")
    if not isinstance(rsi_block_raw, dict):
        return None

    rsi_block: Dict[str, Any] = {str(k).lower(): v for k, v in rsi_block_raw.items()}
    rsi_val_raw = rsi_block.get(source_key.lower())
    if rsi_val_raw is None:
        return None

    try:
        return float(rsi_val_raw)
    except Exception:
        return None


# 🔸 Поиск instance_id для RSI по timeframe и source_key (rsi14 → length=14)
def _resolve_rsi_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        if source_key.lower().startswith("rsi"):
            length = int(source_key[3:])
    except Exception:
        length = None

    if length is None:
        return None

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "rsi" or tf != timeframe.lower():
            continue
        params = inst.get("params") or {}
        length_raw = params.get("length")
        try:
            length_inst = int(str(length_raw))
        except Exception:
            continue
        if length_inst == length:
            return iid

    return None


# 🔸 Загрузка исторического ряда RSI по instance_id для всех символов
async def _load_rsi_history_for_positions(
    pg,
    instance_id: int,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe)
    if not step_min:
        step_min = 5

    by_symbol: Dict[str, List[Any]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[Any, float]]] = {}

    async with pg.acquire() as conn:
        for symbol, times in by_symbol.items():
            if not times:
                continue

            min_entry = min(times)
            max_entry = max(times)

            delta = timedelta(minutes=step_min * window_bars)
            from_time = min_entry - delta
            to_time = max_entry

            rows = await conn.fetch(
                """
                SELECT open_time, value
                FROM indicator_values_v4
                WHERE instance_id = $1
                  AND symbol      = $2
                  AND open_time  BETWEEN $3 AND $4
                ORDER BY open_time
                """,
                instance_id,
                symbol,
                from_time,
                to_time,
            )

            if not rows:
                continue

            series: List[Tuple[Any, float]] = []
            for r in rows:
                try:
                    series.append((r["open_time"], float(r["value"])))
                except Exception:
                    continue

            if series:
                result[symbol] = series

    return result


# 🔸 Поиск индекса последнего бара с open_time <= entry_time
def _find_index_leq(series: List[Tuple[Any, float]], entry_time) -> Optional[int]:
    lo = 0
    hi = len(series) - 1
    idx = None

    while lo <= hi:
        mid = (lo + hi) // 2
        t = series[mid][0]
        if t <= entry_time:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return idx


# 🔸 Биннинг для rsi_dist_from_50 (используем только для понимания диапазона, но не записываем бин)
def _rsi_dist_raw(rsi: float) -> float:
    return abs(rsi - 50.0)


# 🔸 Биннинг для rsi_zone (возвращаем только метку, числа нас здесь не интересуют)
def _bin_rsi_zone(rsi: float) -> Tuple[str, float, float]:
    if rsi < 30.0:
        return "Z1_LT_30", 0.0, 30.0
    if 30.0 <= rsi < 40.0:
        return "Z2_30_40", 30.0, 40.0
    if 40.0 <= rsi <= 60.0:
        return "Z3_40_60", 40.0, 60.0
    if 60.0 < rsi <= 70.0:
        return "Z4_60_70", 60.0, 70.0
    return "Z5_GT_70", 70.0, 100.0


# 🔸 Биннинг для наклона/ускорения (используется для текущих бинов, но в raw мы пишем само значение)
def _bin_signed_value_5(v: float) -> str:
    if v >= 5.0:
        return "StrongUp"
    if 2.0 <= v < 5.0:
        return "ModerateUp"
    if -2.0 < v < 2.0:
        return "Flat"
    if -5.0 < v <= -2.0:
        return "ModerateDown"
    return "StrongDown"


# 🔸 Биннинг волатильности (для текущих бинов, в raw пишем vol)
def _bin_volatility(v: float) -> str:
    if v < 3.0:
        return "Vol_VeryLow"
    if v < 6.0:
        return "Vol_Low"
    if v < 10.0:
        return "Vol_Medium"
    if v < 15.0:
        return "Vol_High"
    return "Vol_VeryHigh"


# 🔸 Биннинг delta RSI vs MA(RSI) (для текущих бинов, в raw пишем delta)
def _bin_rsi_vs_ma(delta: float) -> str:
    if delta <= -10.0:
        return "StrongBelow"
    if -10.0 < delta <= -3.0:
        return "SlightBelow"
    if -3.0 < delta < 3.0:
        return "Near"
    if 3.0 <= delta < 10.0:
        return "SlightAbove"
    return "StrongAbove"


# 🔸 Биннинг количества баров с момента последнего касания уровня
def _bin_bars_since_level(bars: int) -> str:
    if bars == 0:
        return "JustNow"
    if 1 <= bars <= 3:
        return "VeryRecent"
    if 4 <= bars <= 10:
        return "Recent"
    if 11 <= bars <= 30:
        return "Old"
    return "VeryOld"

# 🔸 Публичная точка входа: воркер калибровки сырых фич
async def run_bt_analysis_calibration_raw(pg, redis):
    log.info("BT_ANALYSIS_CALIB_RAW: воркер калибровки сырых фич запущен")

    # подготавливаем consumer group для стрима bt:analysis:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и обработки
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_rows_written = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_ready_message(fields)
                    if not ctx:
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    analysis_ids = ctx["analysis_ids"]

                    log.info(
                        "BT_ANALYSIS_CALIB_RAW: получено сообщение о готовности анализа "
                        "scenario_id=%s, signal_id=%s, family=%s, analysis_ids=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        family_key,
                        analysis_ids,
                        entry_id,
                    )

                    # пока работаем только с RSI
                    if family_key != "rsi" or not analysis_ids:
                        await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)
                        continue

                    rows_written = await _process_family_raw(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        family_key=family_key,
                        analysis_ids=analysis_ids,
                    )
                    total_pairs += 1
                    total_rows_written += rows_written

                    # 🔸 Публикуем событие готовности калибровки в bt:analysis:calibration:ready
                    finished_at = datetime.utcnow()
                    try:
                        await redis.xadd(
                            CALIB_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "family_key": str(family_key),
                                "analysis_ids": ",".join(str(a) for a in analysis_ids),
                                "rows_written": str(rows_written),
                                "finished_at": finished_at.isoformat(),
                            },
                        )
                        log.info(
                            "BT_ANALYSIS_CALIB_RAW: опубликовано событие в '%s' для scenario_id=%s, signal_id=%s, "
                            "family=%s, analysis_ids=%s, rows_written=%s, finished_at=%s",
                            CALIB_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            analysis_ids,
                            rows_written,
                            finished_at,
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_CALIB_RAW: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, family=%s: %s",
                            CALIB_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            family_key,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(ANALYSIS_READY_STREAM_KEY, CALIB_CONSUMER_GROUP, entry_id)

                    log.info(
                        "BT_ANALYSIS_CALIB_RAW: сообщение stream_id=%s для scenario_id=%s, signal_id=%s "
                        "обработано, строк в bt_position_features_raw записано=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        rows_written,
                    )

            log.info(
                "BT_ANALYSIS_CALIB_RAW: пакет сообщений обработан — сообщений=%s, пар_сценарий_сигнал=%s, "
                "строк_в_bt_position_features_raw=%s",
                total_msgs,
                total_pairs,
                total_rows_written,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_CALIB_RAW: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)

# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=CALIB_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_CALIB_RAW: создана consumer group '%s' для стрима '%s'",
            CALIB_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_CALIB_RAW: consumer group '%s' для стрима '%s' уже существует",
                CALIB_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_CALIB_RAW: ошибка при создании consumer group '%s': %s",
                CALIB_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=CALIB_CONSUMER_GROUP,
        consumername=CALIB_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=CALIB_STREAM_BATCH_SIZE,
        block=CALIB_STREAM_BLOCK_MS,
    )

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:analysis:ready
def _parse_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        raw_ids = [s.strip() for s in analysis_ids_str.split(",") if s.strip()]
        analysis_ids: List[int] = []
        for s in raw_ids:
            try:
                analysis_ids.append(int(s))
            except Exception:
                continue

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "analysis_ids": analysis_ids,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_CALIB_RAW: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного семейства анализаторов для пары scenario_id/signal_id
async def _process_family_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    family_key: str,
    analysis_ids: List[int],
) -> int:
    # загружаем позиции этого сценария/сигнала, прошедшие постпроцессинг
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                symbol,
                direction,
                timeframe,
                entry_time,
                raw_stat,
                pnl_abs
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
            """,
            scenario_id,
            signal_id,
        )

    if not rows:
        log.debug(
            "BT_ANALYSIS_CALIB_RAW: нет позиций с postproc=true для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return 0

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "direction": r["direction"],
                "timeframe": r["timeframe"],
                "entry_time": r["entry_time"],
                "raw_stat": r["raw_stat"],
                "pnl_abs": r["pnl_abs"],
            }
        )

    # 🔸 Полная очистка сырых фич для этой связки и семьи
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_position_features_raw
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND family_key  = $3
            """,
            scenario_id,
            signal_id,
            family_key,
        )

    total_rows_written = 0

    # загружаем историю RSI один раз для всех history-based фич по каждому (timeframe, source_key)
    # сначала группируем analysis_instances по (timeframe, source_key)
    history_needed: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for aid in analysis_ids:
        inst = get_analysis_instance(aid)
        if not inst:
            continue
        if inst.get("family_key") != "rsi":
            continue

        key = inst.get("key")
        params = inst.get("params") or {}
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

        if key in (
            "rsi_zone_duration",
            "rsi_slope",
            "rsi_accel",
            "rsi_volatility",
            "rsi_avg_window",
            "rsi_since_cross_30",
            "rsi_since_cross_50",
            "rsi_since_cross_70",
            "rsi_vs_ma",
            "rsi_extremum",
        ):
            # для history-based используем общее окно из params или дефолт
            cfg_key = (timeframe, source_key)
            if cfg_key not in history_needed:
                history_needed[cfg_key] = {
                    "timeframe": timeframe,
                    "source_key": source_key,
                    "params": params,
                }

    rsi_history_by_key: Dict[Tuple[str, str], Dict[str, List[Tuple[Any, float]]]] = {}

    # загружаем историю для каждого (timeframe, source_key)
    for (timeframe, source_key), info in history_needed.items():
        params = info["params"]

        def _get_int_param(name: str, default: int) -> int:
            cfg = params.get(name)
            if cfg is None:
                return default
            try:
                return int(str(cfg.get("value")))
            except Exception:
                return default

        window_bars = _get_int_param("window_bars", 50)
        instance_id = _resolve_rsi_instance_id(timeframe, source_key)
        if instance_id is None:
            log.warning(
                "BT_ANALYSIS_CALIB_RAW: не найден instance_id RSI для timeframe=%s, source_key=%s",
                timeframe,
                source_key,
            )
            continue

        rsi_history = await _load_rsi_history_for_positions(
            pg=pg,
            instance_id=instance_id,
            timeframe=timeframe,
            positions=positions,
            window_bars=window_bars,
        )
        rsi_history_by_key[(timeframe, source_key)] = rsi_history

    # 🔸 Обрабатываем каждый анализатор по всем позициям
    async with pg.acquire() as conn:
        for aid in analysis_ids:
            inst = get_analysis_instance(aid)
            if not inst:
                log.warning(
                    "BT_ANALYSIS_CALIB_RAW: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != family_key:
                continue

            tf_cfg = params.get("timeframe")
            source_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

            feature_name = _resolve_feature_name_for_rsi(key=key, timeframe=timeframe, source_key=source_key)

            log.info(
                "BT_ANALYSIS_CALIB_RAW: сбор сырых фич для analysis_id=%s, family=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
                family_key,
                key,
                feature_name,
                timeframe,
                scenario_id,
                signal_id,
            )

            # подготавливаем вспомогательные параметры для history-based
            slope_k = 3
            if key in ("rsi_slope", "rsi_accel"):
                cfg = params.get("slope_k")
                if cfg is not None:
                    try:
                        slope_k = int(str(cfg.get("value")))
                    except Exception:
                        slope_k = 3

            window_bars = 50
            if key in (
                "rsi_zone_duration",
                "rsi_slope",
                "rsi_accel",
                "rsi_volatility",
                "rsi_avg_window",
                "rsi_since_cross_30",
                "rsi_since_cross_50",
                "rsi_since_cross_70",
                "rsi_vs_ma",
                "rsi_extremum",
            ):
                cfg = params.get("window_bars")
                if cfg is not None:
                    try:
                        window_bars = int(str(cfg.get("value")))
                    except Exception:
                        window_bars = 50

            level_param = params.get("level")

            # извлекаем историю для этого (timeframe, source_key), если нужно
            history_series = None
            if key in (
                "rsi_zone_duration",
                "rsi_slope",
                "rsi_accel",
                "rsi_volatility",
                "rsi_avg_window",
                "rsi_since_cross_30",
                "rsi_since_cross_50",
                "rsi_since_cross_70",
                "rsi_vs_ma",
                "rsi_extremum",
            ):
                history_series = rsi_history_by_key.get((timeframe, source_key))
                if history_series is None:
                    log.debug(
                        "BT_ANALYSIS_CALIB_RAW: нет истории RSI для key=%s timeframe=%s source_key=%s "
                        "— сырые значения фичи записаны не будут",
                        key,
                        timeframe,
                        source_key,
                    )
                    continue

            # собираем batch вставок для этого analysis_id
            rows_to_insert: List[Tuple[Any, ...]] = []

            for p in positions:
                position_id = p["id"]
                symbol = p["symbol"]
                direction = p["direction"]
                entry_time = p["entry_time"]
                raw_stat = p["raw_stat"]
                pnl_abs_raw = p["pnl_abs"]

                if direction is None or pnl_abs_raw is None:
                    continue

                try:
                    pnl_abs = Decimal(str(pnl_abs_raw))
                except Exception:
                    continue

                is_win = pnl_abs > 0

                feature_value: Optional[float] = None

                # расчёт feature_value по ключу
                if key == "rsi_value":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    feature_value = rsi_val

                elif key == "rsi_dist_from_50":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    feature_value = _rsi_dist_raw(rsi_val)

                elif key == "rsi_zone":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    zone_label, _, _ = _bin_rsi_zone(rsi_val)
                    # для калибровки можно хранить числовой RSI, а зону восстанавливать при анализе
                    feature_value = rsi_val

                elif key in (
                    "rsi_zone_duration",
                    "rsi_slope",
                    "rsi_accel",
                    "rsi_volatility",
                    "rsi_avg_window",
                    "rsi_since_cross_30",
                    "rsi_since_cross_50",
                    "rsi_since_cross_70",
                    "rsi_vs_ma",
                    "rsi_extremum",
                ):
                    series_for_symbol = history_series.get(symbol) if history_series else None
                    if not series_for_symbol:
                        continue

                    idx = _find_index_leq(series_for_symbol, entry_time)
                    if idx is None:
                        continue

                    rsi_t = series_for_symbol[idx][1]

                    if key == "rsi_zone_duration":
                        # длительность в барах, сколько RSI был в текущей зоне назад
                        zone_label, _, _ = _bin_rsi_zone(rsi_t)

                        def _in_zone(val: float, label: str) -> bool:
                            if label == "Z1_LT_30":
                                return val < 30.0
                            if label == "Z2_30_40":
                                return 30.0 <= val < 40.0
                            if label == "Z3_40_60":
                                return 40.0 <= val <= 60.0
                            if label == "Z4_60_70":
                                return 60.0 < val <= 70.0
                            if label == "Z5_GT_70":
                                return val > 70.0
                            return False

                            # здесь нельзя

                        duration = 1
                        j = idx - 1
                        while j >= 0 and duration < window_bars:
                            rsi_prev = series_for_symbol[j][1]
                            if not _in_zone(rsi_prev, zone_label):
                                break
                            duration += 1
                            j -= 1

                        feature_value = float(duration)

                    elif key == "rsi_slope":
                        if idx - slope_k < 0:
                            continue
                        rsi_prev = series_for_symbol[idx - slope_k][1]
                        slope = rsi_t - rsi_prev
                        feature_value = slope

                    elif key == "rsi_accel":
                        if idx - 2 * slope_k < 0:
                            continue
                        rsi_prev = series_for_symbol[idx - slope_k][1]
                        rsi_prev2 = series_for_symbol[idx - 2 * slope_k][1]
                        slope1 = rsi_t - rsi_prev
                        slope2 = rsi_prev - rsi_prev2
                        accel = slope1 - slope2
                        feature_value = accel

                    elif key == "rsi_volatility":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if len(window_vals) < 2:
                            continue
                        mean = sum(window_vals) / len(window_vals)
                        var = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
                        vol = var ** 0.5
                        feature_value = vol

                    elif key == "rsi_avg_window":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if not window_vals:
                            continue
                        avg_val = sum(window_vals) / len(window_vals)
                        feature_value = avg_val

                    elif key in ("rsi_since_cross_30", "rsi_since_cross_50", "rsi_since_cross_70"):
                        if level_param is not None:
                            try:
                                level = float(level_param.get("value"))
                            except Exception:
                                level = float(key.split("_")[-1])
                        else:
                            level = float(key.split("_")[-1])

                        bars_since = 0
                        j = idx - 1
                        while j >= 0 and bars_since < window_bars:
                            rsi_prev = series_for_symbol[j][1]
                            if (rsi_t >= level and rsi_prev <= level) or (rsi_t <= level and rsi_prev >= level):
                                break
                            bars_since += 1
                            j -= 1

                        feature_value = float(bars_since)

                    elif key == "rsi_vs_ma":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if not window_vals:
                            continue
                        ma_val = sum(window_vals) / len(window_vals)
                        delta = rsi_t - ma_val
                        feature_value = delta

                    elif key == "rsi_extremum":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if len(window_vals) < 3:
                            continue
                        current = rsi_t
                        feature_value = current

                    else:
                        continue

                else:
                    # неизвестный key — пропускаем
                    continue

                if feature_value is None:
                    continue

                rows_to_insert.append(
                    (
                        position_id,             # position_id
                        scenario_id,             # scenario_id
                        signal_id,               # signal_id
                        direction,               # direction
                        timeframe,               # timeframe (анализатора)
                        family_key,              # family_key ('rsi')
                        key,                     # key ('rsi_value', 'rsi_accel', ...)
                        feature_name,            # feature_name как в бинах
                        feature_value,           # feature_value (numeric)
                        pnl_abs,                 # pnl_abs
                        is_win,                  # is_win
                    )
                )

            if rows_to_insert:
                await conn.executemany(
                    """
                    INSERT INTO bt_position_features_raw (
                        position_id,
                        scenario_id,
                        signal_id,
                        direction,
                        timeframe,
                        family_key,
                        key,
                        feature_name,
                        feature_value,
                        pnl_abs,
                        is_win,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10,
                        $11, now()
                    )
                    """,
                    rows_to_insert,
                )

                total_rows_written += len(rows_to_insert)

                log.info(
                    "BT_ANALYSIS_CALIB_RAW: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    return total_rows_written


# 🔸 Разруливание feature_name для RSI по key/timeframe/source_key (как в bt_analysis_rsi)
def _resolve_feature_name_for_rsi(key: str, timeframe: str, source_key: str) -> str:
    if key == "rsi_value":
        return f"rsi_value_{timeframe}_{source_key}"
    if key == "rsi_dist_from_50":
        return f"rsi_dist_from_50_{timeframe}_{source_key}"
    if key == "rsi_zone":
        return f"rsi_zone_{timeframe}_{source_key}"
    return f"{key}_{timeframe}_{source_key}"