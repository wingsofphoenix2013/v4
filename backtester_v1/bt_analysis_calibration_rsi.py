# bt_analysis_calibration_rsi.py — калибровка сырых фич семейства RSI для backtester_v1

import json
import logging
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (анализаторы и индикаторы)
from backtester_config import get_analysis_instance, get_all_indicator_instances

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_CALIB_RSI")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


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


# 🔸 Биннинг для rsi_dist_from_50 (сырое значение dist для feature_value)
def _rsi_dist_raw(rsi: float) -> float:
    return abs(rsi - 50.0)


# 🔸 Биннинг для rsi_dist_from_50 (категориальный label, как в bt_analysis_rsi)
def _bin_rsi_dist_from_50(rsi: float) -> str:
    dist = abs(rsi - 50.0)

    if dist <= 5.0:
        return "Near_50"

    if rsi >= 50.0:
        if dist <= 10.0:
            return "Above_50_Weak"
        if dist <= 20.0:
            return "Above_50_Medium"
        return "Above_50_Strong"

    if dist <= 10.0:
        return "Below_50_Weak"
    if dist <= 20.0:
        return "Below_50_Medium"
    return "Below_50_Strong"


# 🔸 Биннинг для rsi_zone (возвращаем метку и диапазон)
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


# 🔸 Биннинг для наклона/ускорения (используется и для label)
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


# 🔸 Биннинг волатильности
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


# 🔸 Биннинг delta RSI vs MA(RSI)
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


# 🔸 Бины по абсолютному значению RSI (0–100) для rsi_value
def _default_rsi_value_bins() -> List[Tuple[float, float, str]]:
    return [
        (0.0, 20.0, "RSI_0_20"),
        (20.0, 30.0, "RSI_20_30"),
        (30.0, 40.0, "RSI_30_40"),
        (40.0, 50.0, "RSI_40_50"),
        (50.0, 60.0, "RSI_50_60"),
        (60.0, 70.0, "RSI_60_70"),
        (70.0, 100.0001, "RSI_70_100"),
    ]


# 🔸 Поиск bin_label для rsi_value
def _find_rsi_value_bin_label(value: float) -> Optional[str]:
    for b_from, b_to, label in _default_rsi_value_bins():
        if b_from <= value < b_to:
            return label
    return None


# 🔸 Публичная точка входа: калибровка сырых фич для семейства RSI
async def run_calibration_rsi_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    analysis_ids: List[int],
    positions: List[Dict[str, Any]],
) -> int:
    """
    Калибровка сырых фич для семейства RSI:
    - считает feature_value / bin_label для каждого analysis_id и позиции;
    - записывает строки в bt_position_features_raw;
    - возвращает общее количество записанных строк.
    """
    if not analysis_ids or not positions:
        log.debug(
            "BT_ANALYSIS_CALIB_RSI: scenario_id=%s, signal_id=%s — пустой набор analysis_ids или позиций",
            scenario_id,
            signal_id,
        )
        return 0

    log.debug(
        "BT_ANALYSIS_CALIB_RSI: старт калибровки сырых фич RSI для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, позиций=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        len(positions),
    )

    total_rows_written = 0

    # 🔸 Определяем, для каких (timeframe, source_key) нужны исторические ряды RSI
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
            cfg_key = (timeframe, source_key)
            if cfg_key not in history_needed:
                history_needed[cfg_key] = {
                    "timeframe": timeframe,
                    "source_key": source_key,
                    "params": params,
                }

    # 🔸 Загружаем историю RSI один раз для каждого (timeframe, source_key)
    rsi_history_by_key: Dict[Tuple[str, str], Dict[str, List[Tuple[Any, float]]]] = {}

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
                "BT_ANALYSIS_CALIB_RSI: не найден instance_id RSI для timeframe=%s, source_key=%s "
                "(scenario_id=%s, signal_id=%s)",
                timeframe,
                source_key,
                scenario_id,
                signal_id,
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
                    "BT_ANALYSIS_CALIB_RSI: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != "rsi":
                continue

            tf_cfg = params.get("timeframe")
            source_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

            feature_name = resolve_feature_name(
                family_key="rsi",
                key=key,
                timeframe=timeframe,
                source_key=source_key,
            )

            log.debug(
                "BT_ANALYSIS_CALIB_RSI: сбор сырых фич для analysis_id=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
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
                        "BT_ANALYSIS_CALIB_RSI: нет истории RSI для key=%s timeframe=%s source_key=%s "
                        "(scenario_id=%s, signal_id=%s) — сырые значения фичи записаны не будут",
                        key,
                        timeframe,
                        source_key,
                        scenario_id,
                        signal_id,
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
                bin_label: Optional[str] = None

                # расчёт feature_value и bin_label по ключу
                if key == "rsi_value":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    feature_value = rsi_val
                    bin_label = _find_rsi_value_bin_label(rsi_val)

                elif key == "rsi_dist_from_50":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    feature_value = _rsi_dist_raw(rsi_val)
                    bin_label = _bin_rsi_dist_from_50(rsi_val)

                elif key == "rsi_zone":
                    rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
                    if rsi_val is None:
                        continue
                    feature_value = rsi_val
                    zone_label, _, _ = _bin_rsi_zone(rsi_val)
                    bin_label = zone_label

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

                        duration = 1
                        j = idx - 1
                        while j >= 0 and duration < window_bars:
                            rsi_prev = series_for_symbol[j][1]
                            if not _in_zone(rsi_prev, zone_label):
                                break
                            duration += 1
                            j -= 1

                        feature_value = float(duration)

                        if duration <= 3:
                            dur_label = "D1_1_3"
                        elif duration <= 10:
                            dur_label = "D2_4_10"
                        else:
                            dur_label = "D3_GT_10"

                        bin_label = f"{zone_label}_{dur_label}"

                    elif key == "rsi_slope":
                        if idx - slope_k < 0:
                            continue
                        rsi_prev = series_for_symbol[idx - slope_k][1]
                        slope = rsi_t - rsi_prev
                        feature_value = slope
                        bin_label = _bin_signed_value_5(slope)

                    elif key == "rsi_accel":
                        if idx - 2 * slope_k < 0:
                            continue
                        rsi_prev = series_for_symbol[idx - slope_k][1]
                        rsi_prev2 = series_for_symbol[idx - 2 * slope_k][1]
                        slope1 = rsi_t - rsi_prev
                        slope2 = rsi_prev - rsi_prev2
                        accel = slope1 - slope2
                        feature_value = accel
                        bin_label = _bin_signed_value_5(accel)

                    elif key == "rsi_volatility":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if len(window_vals) < 2:
                            continue
                        mean = sum(window_vals) / len(window_vals)
                        var = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
                        vol = var ** 0.5
                        feature_value = vol
                        bin_label = _bin_volatility(vol)

                    elif key == "rsi_avg_window":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if not window_vals:
                            continue
                        avg_val = sum(window_vals) / len(window_vals)
                        feature_value = avg_val
                        zone_label, _, _ = _bin_rsi_zone(avg_val)
                        bin_label = zone_label

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
                        try:
                            bars_int = int(bars_since)
                        except Exception:
                            bars_int = 0
                        bin_label = _bin_bars_since_level(bars_int)

                    elif key == "rsi_vs_ma":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if not window_vals:
                            continue
                        ma_val = sum(window_vals) / len(window_vals)
                        delta = rsi_t - ma_val
                        feature_value = delta
                        bin_label = _bin_rsi_vs_ma(delta)

                    elif key == "rsi_extremum":
                        start_idx = max(0, idx - window_bars + 1)
                        window_vals = [v for _, v in series_for_symbol[start_idx : idx + 1]]
                        if len(window_vals) < 3:
                            continue

                        current = rsi_t
                        feature_value = current

                        local_min = current == min(window_vals)
                        local_max = current == max(window_vals)
                        spread = max(window_vals) - min(window_vals)

                        if not local_min and not local_max:
                            ext_type = "Flat"
                        else:
                            if local_min:
                                sorted_vals = sorted(window_vals)
                                second = sorted_vals[1]
                                diff = second - current
                                if diff >= 5.0 and spread >= 10.0:
                                    ext_type = "StrongMin"
                                else:
                                    ext_type = "WeakMin"
                            else:
                                sorted_vals = sorted(window_vals, reverse=True)
                                second = sorted_vals[1]
                                diff = current - second
                                if diff >= 5.0 and spread >= 10.0:
                                    ext_type = "StrongMax"
                                else:
                                    ext_type = "WeakMax"

                        bin_label = ext_type

                    else:
                        continue

                else:
                    # неизвестный key — пропускаем
                    continue

                if feature_value is None:
                    continue

                rows_to_insert.append(
                    (
                        position_id,   # position_id
                        scenario_id,   # scenario_id
                        signal_id,     # signal_id
                        direction,     # direction
                        timeframe,     # timeframe (анализатора)
                        "rsi",         # family_key
                        key,           # key ('rsi_value', 'rsi_accel', ...)
                        feature_name,  # feature_name как в бинах
                        bin_label,     # bin_label (может быть None)
                        feature_value, # feature_value (numeric)
                        pnl_abs,       # pnl_abs
                        is_win,        # is_win
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
                        bin_label,
                        feature_value,
                        pnl_abs,
                        is_win,
                        created_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, $8, $9, $10,
                        $11, $12, now()
                    )
                    """,
                    rows_to_insert,
                )

                total_rows_written += len(rows_to_insert)

                log.debug(
                    "BT_ANALYSIS_CALIB_RSI: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    log.info(
        "BT_ANALYSIS_CALIB_RSI: калибровка RSI завершена для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, всего_строк=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        total_rows_written,
    )

    return total_rows_written