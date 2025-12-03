# bt_analysis_calibration_lr.py — калибровка сырых фич семейства LR для backtester_v1

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Кеши backtester_v1 (анализаторы, сценарии и индикаторы)
from backtester_config import (
    get_analysis_instance,
    get_all_indicator_instances,
    get_scenario_instance,
)

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_CALIB_LR")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Расчёт cutoff_time: какие данные индикатора МОГЛИ БЫ БЫТЬ известны на момент открытия позиции
def _compute_cutoff_time(entry_time: datetime, pos_tf: str, ind_tf: str) -> datetime:
    pos_delta = _get_timeframe_timedelta(pos_tf)
    ind_delta = _get_timeframe_timedelta(ind_tf)

    # учитываем только те бары, которые реально могли быть известны:
    # open_time_ind + Δ_ind <= entry_time + Δ_pos
    if pos_delta.total_seconds() > 0 and ind_delta.total_seconds() > 0:
        decision_time = entry_time + pos_delta
        cutoff_time = decision_time - ind_delta
        return cutoff_time

    # fallback — старое поведение (<= entry_time)
    return entry_time


# 🔸 Поиск индекса последнего бара с open_time <= cutoff_time
def _find_index_leq(series: List[Tuple[datetime, Any]], cutoff_time: datetime) -> Optional[int]:
    lo = 0
    hi = len(series) - 1
    idx = None

    while lo <= hi:
        mid = (lo + hi) // 2
        t = series[mid][0]
        if t <= cutoff_time:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return idx


# 🔸 Безопасное извлечение int-параметров из inst.params
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default
    try:
        return int(str(cfg.get("value")))
    except Exception:
        return default


# 🔸 Безопасное извлечение float-параметров из inst.params
def _get_float_param(params: Dict[str, Any], name: str, default: float) -> float:
    cfg = params.get(name)
    if cfg is None:
        return default
    raw = cfg.get("value")
    try:
        return float(str(raw))
    except Exception:
        return default


# 🔸 Резолвинг instance_id для LR по timeframe и source_key (lr50 → length=50)
def _resolve_lr_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        sk = source_key.strip().lower()
        if sk.startswith("lr"):
            length = int(sk[2:])
    except Exception:
        length = None

    if length is None:
        return None

    tf_l = (timeframe or "").lower()

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "lr" or tf != tf_l:
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


# 🔸 Резолвинг instance_id для EMA по timeframe и source_key (ema50 → length=50)
def _resolve_ema_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        sk = source_key.strip().lower()
        if sk.startswith("ema"):
            length = int(sk[3:])
    except Exception:
        length = None

    if length is None:
        return None

    tf_l = (timeframe or "").lower()

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "ema" or tf != tf_l:
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


# 🔸 Резолвинг instance_id для ATR по timeframe и source_key (atr14 → length=14)
def _resolve_atr_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        sk = source_key.strip().lower()
        if sk.startswith("atr"):
            length = int(sk[3:])
    except Exception:
        length = None

    if length is None:
        return None

    tf_l = (timeframe or "").lower()

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "atr" or tf != tf_l:
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


# 🔸 Загрузка истории LR (angle/upper/lower/center) для всех символов по позициям
async def _load_lr_history_for_positions(
    pg,
    instance_id: int,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[datetime, Dict[str, float]]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get((timeframe or "").lower())
    if not step_min:
        step_min = 5

    by_symbol: Dict[str, List[datetime]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[datetime, Dict[str, float]]]] = {}

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
                SELECT open_time, param_name, value
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

            series_map: Dict[datetime, Dict[str, float]] = {}
            for r in rows:
                ts = r["open_time"]
                pname = str(r["param_name"] or "").lower()
                try:
                    v = float(r["value"])
                except Exception:
                    continue

                entry = series_map.setdefault(ts, {})
                if pname.endswith("_angle"):
                    entry["angle"] = v
                elif pname.endswith("_upper"):
                    entry["upper"] = v
                elif pname.endswith("_lower"):
                    entry["lower"] = v
                elif pname.endswith("_center"):
                    entry["center"] = v

            if not series_map:
                continue

            series_list = sorted(series_map.items(), key=lambda x: x[0])
            result[symbol] = series_list

    return result


# 🔸 Загрузка истории одиночного параметра индикатора (например EMA/ATR) для всех символов
async def _load_single_param_history_for_positions(
    pg,
    instance_id: int,
    positions: List[Dict[str, Any]],
) -> Dict[str, List[Tuple[datetime, float]]]:
    by_symbol: Dict[str, List[datetime]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[datetime, float]]] = {}

    async with pg.acquire() as conn:
        for symbol, times in by_symbol.items():
            if not times:
                continue

            min_entry = min(times)
            max_entry = max(times)

            # грубый буфер истории ~1 день
            from_time = min_entry - timedelta(days=1)
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

            series: List[Tuple[datetime, float]] = []
            for r in rows:
                try:
                    series.append((r["open_time"], float(r["value"])))
                except Exception:
                    continue

            if series:
                result[symbol] = series

    return result


# 🔸 Биннинг по знаку/силе (5 бинов)
def _bin_signed_5(v: float) -> str:
    if v >= 5.0:
        return "StrongUp"
    if 2.0 <= v < 5.0:
        return "ModerateUp"
    if -2.0 < v < 2.0:
        return "Flat"
    if -5.0 < v <= -2.0:
        return "ModerateDown"
    return "StrongDown"


# 🔸 Публичная точка входа: калибровка сырых фич для семейства LR
async def run_calibration_lr_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    analysis_ids: List[int],
    positions: List[Dict[str, Any]],
) -> int:
    """
    Калибровка сырых фич для семейства LR:
    - считает feature_value / bin_label для каждого analysis_id и позиции;
    - записывает строки в bt_position_features_raw;
    - возвращает общее количество записанных строк.
    """
    if not analysis_ids or not positions:
        log.debug(
            "BT_ANALYSIS_CALIB_LR: scenario_id=%s, signal_id=%s — пустой набор analysis_ids или позиций",
            scenario_id,
            signal_id,
        )
        return 0

    log.debug(
        "BT_ANALYSIS_CALIB_LR: старт калибровки сырых фич LR для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, позиций=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        len(positions),
    )

    total_rows_written = 0

    # 🔸 Достаём scenario, чтобы взять TP-процент (для RoomToOppositeBoundary)
    scenario = get_scenario_instance(scenario_id)
    tp_percent: Optional[Decimal] = None

    if scenario:
        params = scenario.get("params") or {}
        tp_type_cfg = params.get("tp_type")
        tp_value_cfg = params.get("tp_value")
        try:
            tp_type = (tp_type_cfg.get("value") or "").strip().lower() if tp_type_cfg else ""
        except Exception:
            tp_type = ""
        if tp_type == "percent" and tp_value_cfg is not None:
            try:
                tp_percent = Decimal(str(tp_value_cfg.get("value")))
            except Exception:
                tp_percent = None

    # 🔸 Дополняем позиции ценой входа (entry_price) по их id
    position_ids = [p["id"] for p in positions]
    if not position_ids:
        return 0

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, entry_price
            FROM bt_scenario_positions
            WHERE id = ANY($1::int[])
            """,
            position_ids,
        )

    price_by_id: Dict[int, Decimal] = {}
    for r in rows:
        try:
            price_by_id[r["id"]] = Decimal(str(r["entry_price"]))
        except Exception:
            continue

    for p in positions:
        pid = p["id"]
        p["entry_price"] = price_by_id.get(pid, Decimal("0"))

    # 🔸 Основной цикл по анализаторам
    async with pg.acquire() as conn:
        for aid in analysis_ids:
            inst = get_analysis_instance(aid)
            if not inst:
                log.warning(
                    "BT_ANALYSIS_CALIB_LR: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != "lr":
                continue

            tf_cfg = params.get("timeframe")
            source_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "lr50"

            feature_name = resolve_feature_name(
                family_key="lr",
                key=key,
                timeframe=timeframe,
                source_key=source_key,
            )

            log.debug(
                "BT_ANALYSIS_CALIB_LR: сбор сырых фич для analysis_id=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
                key,
                feature_name,
                timeframe,
                scenario_id,
                signal_id,
            )

            rows_to_insert: List[Tuple[Any, ...]] = []

            # 🔸 Фича 1: lr_angle_strength
            if key == "lr_angle_strength":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_angle_strength) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=5,
                    )

                    angle_small = _get_float_param(params, "angle_abs_small", 0.0)
                    angle_medium = _get_float_param(params, "angle_abs_medium", 0.0005)

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        pnl_abs_raw = p["pnl_abs"]

                        if not direction or pnl_abs_raw is None:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None:
                            continue

                        lr_vals = series[idx][1]
                        angle = lr_vals.get("angle")
                        if angle is None:
                            continue

                        try:
                            angle_f = float(angle)
                        except Exception:
                            continue

                        abs_angle = abs(angle_f)

                        if abs_angle <= angle_small:
                            strength = "Weak"
                        elif abs_angle <= angle_medium:
                            strength = "Medium"
                        else:
                            strength = "Strong"

                        if direction == "long":
                            if angle_f < 0:
                                bin_label = f"Opposite_{strength}"
                            else:
                                bin_label = f"Aligned_{strength}"
                        elif direction == "short":
                            if angle_f > 0:
                                bin_label = f"Opposite_{strength}"
                            else:
                                bin_label = f"Aligned_{strength}"
                        else:
                            bin_label = strength

                        feature_value = abs_angle

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 2: lr_channel_width_rel
            elif key == "lr_channel_width_rel":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_channel_width_rel) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=5,
                    )

                    width_small = _get_float_param(params, "width_rel_small", 0.005)
                    width_medium = _get_float_param(params, "width_rel_medium", 0.015)

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        entry_price = p.get("entry_price", Decimal("0"))
                        pnl_abs_raw = p["pnl_abs"]

                        if not direction or pnl_abs_raw is None or entry_price <= 0:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None:
                            continue

                        lr_vals = series[idx][1]
                        upper = lr_vals.get("upper")
                        lower = lr_vals.get("lower")
                        if upper is None or lower is None:
                            continue

                        try:
                            width = float(upper) - float(lower)
                            entry_f = float(entry_price)
                        except Exception:
                            continue

                        if width <= 0 or entry_f <= 0:
                            continue

                        width_rel = width / entry_f

                        if width_rel <= width_small:
                            bin_label = "Width_Narrow"
                        elif width_rel <= width_medium:
                            bin_label = "Width_Medium"
                        else:
                            bin_label = "Width_Wide"

                        feature_value = width_rel

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 3: lr_angle_stability
            elif key == "lr_angle_stability":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_angle_stability) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    window_bars = _get_int_param(params, "window_bars", 5)

                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=window_bars,
                    )

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        pnl_abs_raw = p["pnl_abs"]

                        if not direction or pnl_abs_raw is None:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None or idx - window_bars < 0:
                            continue

                        angle_curr = series[idx][1].get("angle")
                        angle_prev = series[idx - window_bars][1].get("angle")
                        if angle_curr is None or angle_prev is None:
                            continue

                        try:
                            diff = float(angle_curr) - float(angle_prev)
                        except Exception:
                            continue

                        bin_label = _bin_signed_5(diff)
                        feature_value = diff

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 4: lr_channel_width_trend
            elif key == "lr_channel_width_trend":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_channel_width_trend) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    window_bars = _get_int_param(params, "window_bars", 5)

                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=window_bars,
                    )

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        pnl_abs_raw = p["pnl_abs"]

                        if not direction or pnl_abs_raw is None:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None or idx - window_bars < 0:
                            continue

                        lr_curr = series[idx][1]
                        lr_prev = series[idx - window_bars][1]

                        upper_c = lr_curr.get("upper")
                        lower_c = lr_curr.get("lower")
                        upper_p = lr_prev.get("upper")
                        lower_p = lr_prev.get("lower")

                        if upper_c is None or lower_c is None or upper_p is None or lower_p is None:
                            continue

                        try:
                            width_curr = float(upper_c) - float(lower_c)
                            width_prev = float(upper_p) - float(lower_p)
                        except Exception:
                            continue

                        if width_prev <= 0 or width_curr <= 0:
                            continue

                        delta = width_curr - width_prev
                        bin_label = _bin_signed_5(delta)
                        feature_value = delta

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 5: lr_entry_distance_to_boundary
            elif key == "lr_entry_distance_to_boundary":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_entry_distance_to_boundary) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=5,
                    )

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        if direction not in ("long", "short"):
                            continue

                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        entry_price = p.get("entry_price", Decimal("0"))
                        pnl_abs_raw = p["pnl_abs"]

                        if pnl_abs_raw is None or entry_price <= 0:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None:
                            continue

                        lr_vals = series[idx][1]
                        upper = lr_vals.get("upper")
                        lower = lr_vals.get("lower")
                        if upper is None or lower is None:
                            continue

                        try:
                            upper_f = float(upper)
                            lower_f = float(lower)
                            entry_f = float(entry_price)
                        except Exception:
                            continue

                        H = upper_f - lower_f
                        if H <= 0:
                            continue

                        if direction == "long":
                            dist = entry_f - lower_f
                        else:
                            dist = upper_f - entry_f

                        ratio = dist / H

                        if ratio <= 0.2:
                            bin_label = "NearBoundary"
                        elif ratio <= 0.5:
                            bin_label = "MidZone"
                        else:
                            bin_label = "InnerChannel"

                        feature_value = ratio

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 6: lr_room_to_opposite_boundary
            elif key == "lr_room_to_opposite_boundary":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_room_to_opposite_boundary) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    lr_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=lr_instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=5,
                    )

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        if direction not in ("long", "short"):
                            continue

                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        entry_price = p.get("entry_price", Decimal("0"))
                        pnl_abs_raw = p["pnl_abs"]

                        if pnl_abs_raw is None or entry_price <= 0:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        series = lr_history.get(symbol)
                        if not series:
                            continue

                        cutoff_time = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        idx = _find_index_leq(series, cutoff_time)
                        if idx is None:
                            continue

                        lr_vals = series[idx][1]
                        upper = lr_vals.get("upper")
                        lower = lr_vals.get("lower")
                        if upper is None or lower is None:
                            continue

                        try:
                            upper_f = float(upper)
                            lower_f = float(lower)
                            entry_f = float(entry_price)
                        except Exception:
                            continue

                        if direction == "long":
                            room = upper_f - entry_f
                        else:
                            room = entry_f - lower_f

                        if room <= 0:
                            continue

                        if tp_percent is not None and tp_percent > 0:
                            base_move = entry_f * float(tp_percent) / 100.0
                            if base_move > 0:
                                ratio = room / base_move
                            else:
                                ratio = room
                        else:
                            H = upper_f - lower_f
                            if H > 0:
                                ratio = room / H
                            else:
                                ratio = room

                        if ratio < 1.0:
                            bin_label = "RoomLess1R"
                        elif ratio <= 2.0:
                            bin_label = "Room1to2R"
                        else:
                            bin_label = "RoomMore2R"

                        feature_value = ratio

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 7: lr_multitf_alignment
            elif key == "lr_multitf_alignment":
                higher_tf_cfg = params.get("higher_timeframe")
                higher_tf = str(higher_tf_cfg.get("value")).strip() if higher_tf_cfg is not None else "m15"

                higher_src_cfg = params.get("higher_source_key")
                higher_source_key = str(higher_src_cfg.get("value")).strip() if higher_src_cfg is not None else source_key

                base_lr_id = _resolve_lr_instance_id(timeframe, source_key)
                higher_lr_id = _resolve_lr_instance_id(higher_tf, higher_source_key)

                if base_lr_id is None or higher_lr_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_multitf_alignment) — не удалось найти LR instance_id "
                        "для base_tf=%s (%s) или higher_tf=%s (%s)",
                        aid,
                        timeframe,
                        source_key,
                        higher_tf,
                        higher_source_key,
                    )
                else:
                    base_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=base_lr_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=5,
                    )
                    higher_history = await _load_lr_history_for_positions(
                        pg=pg,
                        instance_id=higher_lr_id,
                        timeframe=higher_tf,
                        positions=positions,
                        window_bars=5,
                    )

                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pos_tf_raw = p.get("timeframe")
                        pos_tf = str(pos_tf_raw or "").lower()
                        pnl_abs_raw = p["pnl_abs"]

                        if not direction or pnl_abs_raw is None:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        is_win = pnl_abs > 0

                        base_series = base_history.get(symbol)
                        higher_series = higher_history.get(symbol)
                        if not base_series or not higher_series:
                            continue

                        cutoff_base = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                        cutoff_high = _compute_cutoff_time(entry_time, pos_tf, higher_tf)

                        idx_base = _find_index_leq(base_series, cutoff_base)
                        idx_high = _find_index_leq(higher_series, cutoff_high)

                        if idx_base is None or idx_high is None:
                            continue

                        angle_base = base_series[idx_base][1].get("angle")
                        angle_high = higher_series[idx_high][1].get("angle")
                        if angle_base is None or angle_high is None:
                            continue

                        try:
                            a_b = float(angle_base)
                            a_h = float(angle_high)
                        except Exception:
                            continue

                        if abs(a_h) < 1e-9:
                            ratio = 0.0
                            bin_label = "HigherFlat"
                        else:
                            ratio = a_b / a_h
                            same_sign = (a_b > 0 and a_h > 0) or (a_b < 0 and a_h < 0)
                            if same_sign:
                                bin_label = "Aligned"
                            else:
                                bin_label = "Opposite"

                        feature_value = ratio

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,  # используем TF базы
                                "lr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                is_win,
                            )
                        )

            # 🔸 Фича 8: lr_vs_ema_slope
            elif key == "lr_vs_ema_slope":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_vs_ema_slope) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    ema_src_cfg = params.get("ema_source_key")
                    ema_source_key = str(ema_src_cfg.get("value")).strip() if ema_src_cfg is not None else "ema50"

                    ema_instance_id = _resolve_ema_instance_id(timeframe, ema_source_key)
                    if ema_instance_id is None:
                        log.warning(
                            "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_vs_ema_slope) — не найден EMA instance_id "
                            "для timeframe=%s, source_key=%s",
                            aid,
                            timeframe,
                            ema_source_key,
                        )
                    else:
                        slope_k = _get_int_param(params, "ema_slope_bars", 3)

                        lr_history = await _load_lr_history_for_positions(
                            pg=pg,
                            instance_id=lr_instance_id,
                            timeframe=timeframe,
                            positions=positions,
                            window_bars=slope_k,
                        )
                        ema_history = await _load_single_param_history_for_positions(
                            pg=pg,
                            instance_id=ema_instance_id,
                            positions=positions,
                        )

                        for p in positions:
                            position_id = p["id"]
                            symbol = p["symbol"]
                            direction = p["direction"]
                            entry_time = p["entry_time"]
                            pos_tf_raw = p.get("timeframe")
                            pos_tf = str(pos_tf_raw or "").lower()
                            pnl_abs_raw = p["pnl_abs"]

                            if not direction or pnl_abs_raw is None:
                                continue

                            try:
                                pnl_abs = Decimal(str(pnl_abs_raw))
                            except Exception:
                                continue

                            is_win = pnl_abs > 0

                            lr_series = lr_history.get(symbol)
                            ema_series = ema_history.get(symbol)
                            if not lr_series or not ema_series:
                                continue

                            cutoff = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                            idx_lr = _find_index_leq(lr_series, cutoff)
                            idx_ema = _find_index_leq(ema_series, cutoff)

                            if idx_lr is None or idx_ema is None or idx_ema - slope_k < 0:
                                continue

                            angle = lr_series[idx_lr][1].get("angle")
                            if angle is None:
                                continue

                            try:
                                angle_f = float(angle)
                            except Exception:
                                continue

                            ema_now = ema_series[idx_ema][1]
                            ema_prev = ema_series[idx_ema - slope_k][1]
                            ema_slope = ema_now - ema_prev

                            if abs(ema_slope) < 1e-9:
                                ema_dir = "Flat"
                            elif ema_slope > 0:
                                ema_dir = "Up"
                            else:
                                ema_dir = "Down"

                            if abs(angle_f) < 1e-9:
                                lr_dir = "Flat"
                            elif angle_f > 0:
                                lr_dir = "Up"
                            else:
                                lr_dir = "Down"

                            if lr_dir == "Flat" or ema_dir == "Flat":
                                bin_label = f"LR_{lr_dir}_EMA_{ema_dir}"
                            elif lr_dir == ema_dir:
                                bin_label = "SameDirection"
                            else:
                                bin_label = "OppositeDirection"

                            feature_value = angle_f - ema_slope

                            rows_to_insert.append(
                                (
                                    position_id,
                                    scenario_id,
                                    signal_id,
                                    direction,
                                    timeframe,
                                    "lr",
                                    key,
                                    feature_name,
                                    bin_label,
                                    feature_value,
                                    pnl_abs,
                                    is_win,
                                )
                            )

            # 🔸 Фича 9: lr_channel_width_vs_atr
            elif key == "lr_channel_width_vs_atr":
                lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
                if lr_instance_id is None:
                    log.warning(
                        "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_channel_width_vs_atr) — не найден LR instance_id "
                        "для timeframe=%s, source_key=%s",
                        aid,
                        timeframe,
                        source_key,
                    )
                else:
                    atr_src_cfg = params.get("atr_source_key")
                    atr_source_key = str(atr_src_cfg.get("value")).strip() if atr_src_cfg is not None else "atr14"

                    atr_instance_id = _resolve_atr_instance_id(timeframe, atr_source_key)
                    if atr_instance_id is None:
                        log.warning(
                            "BT_ANALYSIS_CALIB_LR: analysis_id=%s (lr_channel_width_vs_atr) — не найден ATR instance_id "
                            "для timeframe=%s, source_key=%s",
                            aid,
                            timeframe,
                            atr_source_key,
                        )
                    else:
                        lr_history = await _load_lr_history_for_positions(
                            pg=pg,
                            instance_id=lr_instance_id,
                            timeframe=timeframe,
                            positions=positions,
                            window_bars=5,
                        )
                        atr_history = await _load_single_param_history_for_positions(
                            pg=pg,
                            instance_id=atr_instance_id,
                            positions=positions,
                        )

                        for p in positions:
                            position_id = p["id"]
                            symbol = p["symbol"]
                            direction = p["direction"]
                            entry_time = p["entry_time"]
                            pos_tf_raw = p.get("timeframe")
                            pos_tf = str(pos_tf_raw or "").lower()
                            pnl_abs_raw = p["pnl_abs"]

                            if not direction or pnl_abs_raw is None:
                                continue

                            try:
                                pnl_abs = Decimal(str(pnl_abs_raw))
                            except Exception:
                                continue

                            is_win = pnl_abs > 0

                            lr_series = lr_history.get(symbol)
                            atr_series = atr_history.get(symbol)
                            if not lr_series or not atr_series:
                                continue

                            cutoff = _compute_cutoff_time(entry_time, pos_tf, timeframe)
                            idx_lr = _find_index_leq(lr_series, cutoff)
                            idx_atr = _find_index_leq(atr_series, cutoff)

                            if idx_lr is None or idx_atr is None:
                                continue

                            lr_vals = lr_series[idx_lr][1]
                            upper = lr_vals.get("upper")
                            lower = lr_vals.get("lower")
                            if upper is None or lower is None:
                                continue

                            try:
                                width = float(upper) - float(lower)
                                atr_val = float(atr_series[idx_atr][1])
                            except Exception:
                                continue

                            if width <= 0 or atr_val <= 0:
                                continue

                            ratio = width / atr_val

                            if ratio < 1.0:
                                bin_label = "Width_LT_1ATR"
                            elif ratio <= 2.0:
                                bin_label = "Width_1_2_ATR"
                            else:
                                bin_label = "Width_GT_2ATR"

                            feature_value = ratio

                            rows_to_insert.append(
                                (
                                    position_id,
                                    scenario_id,
                                    signal_id,
                                    direction,
                                    timeframe,
                                    "lr",
                                    key,
                                    feature_name,
                                    bin_label,
                                    feature_value,
                                    pnl_abs,
                                    is_win,
                                )
                            )

            # 🔸 Вставка собранного batch для analysis_id
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
                    "BT_ANALYSIS_CALIB_LR: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    log.debug(
        "BT_ANALYSIS_CALIB_LR: калибровка LR завершена для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, всего_строк=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        total_rows_written,
    )

    log.info(
        "BT_ANALYSIS_CALIB_LR: завершена калибровка сырых фич LR для scenario_id=%s, signal_id=%s, сырых_строк=%s",
        scenario_id,
        signal_id,
        total_rows_written,
    )

    return total_rows_written