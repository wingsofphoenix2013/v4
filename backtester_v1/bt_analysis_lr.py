# bt_analysis_lr.py — анализатор фич семейства LR для backtester_v1

import logging
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, getcontext
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Кеши backtester_v1 (сценарии и индикаторы)
from backtester_config import get_scenario_instance, get_all_indicator_instances

# 🔸 Утилиты анализа фич
from bt_analysis_utils import resolve_feature_name, write_feature_bins

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_LR")

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


# 🔸 Поиск instance_id для LR по timeframe и source_key (lr50 → length=50)
def _resolve_lr_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        # source_key вида "lr50" / "LR50"
        if source_key.lower().startswith("lr"):
            length = int(source_key[2:])
    except Exception:
        length = None

    if length is None:
        return None

    tf_l = timeframe.lower()
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


# 🔸 Поиск instance_id для EMA по timeframe и source_key (ema50 → length=50)
def _resolve_ema_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        # source_key вида "ema50" / "EMA50"
        if source_key.lower().startswith("ema"):
            length = int(source_key[3:])
    except Exception:
        length = None

    if length is None:
        return None

    tf_l = timeframe.lower()
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


# 🔸 Загрузка истории LR для позиций (angle/upper/lower/center)
async def _load_lr_history_for_positions(
    pg,
    instance_id: int,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, Dict[str, float]]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower())
    if not step_min:
        step_min = 5

    # группируем позиции по символу и вычисляем диапазоны времени
    by_symbol: Dict[str, List[Any]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[Any, Dict[str, float]]]] = {}

    async with pg.acquire() as conn:
        for symbol, times in by_symbol.items():
            if not times:
                continue

            min_entry = min(times)
            max_entry = max(times)

            # запас по времени в прошлом — window_bars баров
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

            series_map: Dict[Any, Dict[str, float]] = {}
            for r in rows:
                ts = r["open_time"]
                pname = str(r["param_name"] or "")
                val = r["value"]

                entry = series_map.setdefault(ts, {})
                pname_l = pname.lower()
                try:
                    fval = float(val)
                except Exception:
                    continue

                if pname_l.endswith("_angle"):
                    entry["angle"] = fval
                elif pname_l.endswith("_upper"):
                    entry["upper"] = fval
                elif pname_l.endswith("_lower"):
                    entry["lower"] = fval
                elif pname_l.endswith("_center"):
                    entry["center"] = fval

            if not series_map:
                continue

            # сортируем по времени
            series_list: List[Tuple[Any, Dict[str, float]]] = sorted(series_map.items(), key=lambda x: x[0])
            result[symbol] = series_list

    return result


# 🔸 Загрузка истории EMA для позиций (один параметр value)
async def _load_ema_history_for_positions(
    pg,
    instance_id: int,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower())
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


# 🔸 Поиск индекса последнего бара с open_time <= cutoff_time
def _find_index_leq(series: List[Tuple[Any, Any]], cutoff_time) -> Optional[int]:
    # series отсортирован по времени
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


# 🔸 Нормализованный угол (в процентах от цены)
def _normalize_angle(angle: float, price: Decimal) -> float:
    try:
        p = float(price)
        if p <= 0:
            return 0.0
        return (angle / p) * 100.0
    except Exception:
        return 0.0


# 🔸 Публичная точка входа: анализ семейства LR для одного сценария+сигнала
async def run_analysis_lr(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.debug(
        "BT_ANALYSIS_LR: старт анализа LR для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_LR: для scenario_id=%s, signal_id=%s нет инстансов анализа LR",
            scenario_id,
            signal_id,
        )
        return

    # загружаем сценарий, чтобы взять deposit для расчёта ROI
    scenario = get_scenario_instance(scenario_id)
    deposit: Optional[Decimal] = None

    if scenario:
        params = scenario.get("params") or {}
        deposit_cfg = params.get("deposit")
        if deposit_cfg is not None:
            try:
                deposit = Decimal(str(deposit_cfg.get("value")))
            except Exception:
                deposit = None

    # грузим все позиции этого сценария/сигнала, уже прошедшие постпроцессинг
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                symbol,
                direction,
                timeframe,
                entry_time,
                entry_price,
                sl_price,
                tp_price,
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
            "BT_ANALYSIS_LR: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
            scenario_id,
            signal_id,
        )
        return

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "direction": r["direction"],
                "timeframe": r["timeframe"],
                "entry_time": r["entry_time"],
                "entry_price": Decimal(str(r["entry_price"])),
                "sl_price": Decimal(str(r["sl_price"])),
                "tp_price": Decimal(str(r["tp_price"])),
                "pnl_abs": r["pnl_abs"],
            }
        )

    log.debug(
        "BT_ANALYSIS_LR: для scenario_id=%s, signal_id=%s загружено позиций=%s",
        scenario_id,
        signal_id,
        len(positions),
    )

    # обрабатываем каждый инстанс анализа независимо
    for inst in analysis_instances:
        family_key = inst.get("family_key")
        key = inst.get("key")
        inst_id = inst.get("id")
        params = inst.get("params") or {}

        if family_key != "lr":
            continue

        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "lr50"

        if not timeframe or not source_key:
            log.warning(
                "BT_ANALYSIS_LR: inst_id=%s — некорректные параметры timeframe/source_key "
                "(timeframe=%s, source_key=%s)",
                inst_id,
                timeframe,
                source_key,
            )
            continue

        # проверка поддерживаемых TF
        if timeframe.lower() not in TF_STEP_MINUTES:
            log.warning(
                "BT_ANALYSIS_LR: inst_id=%s — неподдерживаемый timeframe=%s",
                inst_id,
                timeframe,
            )
            continue

        log.debug(
            "BT_ANALYSIS_LR: inst_id=%s — старт расчёта key=%s, timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )

        if key == "lr_angle_strength":
            await _analyze_lr_angle_strength(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_channel_width_rel_price":
            await _analyze_lr_channel_width_rel_price(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_angle_stability":
            await _analyze_lr_angle_stability(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_channel_width_trend":
            await _analyze_lr_channel_width_trend(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_bounce_depth":
            await _analyze_lr_bounce_depth(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_position_in_channel":
            await _analyze_lr_position_in_channel(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_multitf_alignment":
            await _analyze_lr_multitf_alignment(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_vs_ema_slope":
            await _analyze_lr_vs_ema_slope(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        elif key == "lr_room_to_opposite_boundary":
            await _analyze_lr_room_to_opposite_boundary(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        else:
            log.debug(
                "BT_ANALYSIS_LR: inst_id=%s (key=%s) пока не поддерживается анализатором LR",
                inst_id,
                key,
            )

    log.debug(
        "BT_ANALYSIS_LR: анализ LR завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )
    log.info(
        "BT_ANALYSIS_LR: завершён анализ LR для scenario_id=%s, signal_id=%s, позиций=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(positions),
        len(analysis_instances),
    )


# 🔸 Анализ lr_angle_strength — сила наклона канала
async def _analyze_lr_angle_strength(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_angle_strength", timeframe, source_key)

    # окно истории для оценки доступности баров
    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 10
    except Exception:
        window_bars = 10

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_angle_strength — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)

    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        # учитываем только те LR-бары, которые могли быть известны на момент решения
        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        angle = lr_t.get("angle")
        lower = lr_t.get("lower")
        upper = lr_t.get("upper")

        if angle is None or lower is None or upper is None:
            continue

        try:
            angle_f = float(angle)
        except Exception:
            continue

        angle_norm = _normalize_angle(angle_f, entry_price)
        abs_angle_norm = abs(angle_norm)

        # бинning по модулю нормализованного угла
        # пороги можно донастроить позже по калибровке
        if abs_angle_norm < 0.01:
            bin_label = "Angle_VeryWeak"
        elif abs_angle_norm < 0.03:
            bin_label = "Angle_Weak"
        elif abs_angle_norm < 0.06:
            bin_label = "Angle_Medium"
        else:
            bin_label = "Angle_Strong"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": angle_norm,
                "bin_to": angle_norm,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        # обновляем диапазон
        if angle_norm < bin_stat["bin_from"]:
            bin_stat["bin_from"] = angle_norm
        if angle_norm > bin_stat["bin_to"]:
            bin_stat["bin_to"] = angle_norm

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_angle_strength inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_channel_width_rel_price — относительная ширина канала
async def _analyze_lr_channel_width_rel_price(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_channel_width_rel_price", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 10
    except Exception:
        window_bars = 10

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_channel_width_rel_price — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        upper = lr_t.get("upper")
        lower = lr_t.get("lower")

        if upper is None or lower is None:
            continue

        try:
            upper_f = float(upper)
            lower_f = float(lower)
        except Exception:
            continue

        H = upper_f - lower_f
        if H <= 0:
            continue

        try:
            price_f = float(entry_price)
            if price_f <= 0:
                continue
            width_norm = (H / price_f) * 100.0
        except Exception:
            continue

        # биннинг по ширине канала в процентах от цены
        if width_norm < 0.2:
            bin_label = "Width_VeryNarrow"
        elif width_norm < 0.5:
            bin_label = "Width_Narrow"
        elif width_norm < 1.0:
            bin_label = "Width_Medium"
        elif width_norm < 2.0:
            bin_label = "Width_Wide"
        else:
            bin_label = "Width_VeryWide"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": width_norm,
                "bin_to": width_norm,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if width_norm < bin_stat["bin_from"]:
            bin_stat["bin_from"] = width_norm
        if width_norm > bin_stat["bin_to"]:
            bin_stat["bin_to"] = width_norm

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_channel_width_rel_price inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_angle_stability — стабильность наклона во времени
async def _analyze_lr_angle_stability(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_angle_stability", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 10
    except Exception:
        window_bars = 10

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_angle_stability — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        angle = lr_t.get("angle")
        if angle is None:
            continue

        try:
            angle_f = float(angle)
        except Exception:
            continue

        start_idx = max(0, idx - window_bars + 1)
        window_vals: List[float] = []
        for j in range(start_idx, idx + 1):
            ang_j = series[j][1].get("angle")
            if ang_j is None:
                continue
            try:
                window_vals.append(float(ang_j))
            except Exception:
                continue

        if len(window_vals) < 2:
            continue

        mean_angle = sum(window_vals) / len(window_vals)
        diff = angle_f - mean_angle
        diff_norm = _normalize_angle(diff, entry_price)

        # классификация стабильности наклона
        sign_current = 1 if angle_f > 0 else (-1 if angle_f < 0 else 0)
        sign_mean = 1 if mean_angle > 0 else (-1 if mean_angle < 0 else 0)

        threshold = 0.01  # 0.01% от цены – порог чувствительности

        if sign_current == 0 or sign_mean == 0:
            bin_label = "Angle_Uncertain"
        elif sign_current != sign_mean:
            bin_label = "Angle_Reversing"
        else:
            if diff_norm > threshold:
                bin_label = "Angle_Strengthening"
            elif diff_norm < -threshold:
                bin_label = "Angle_Weakening"
            else:
                bin_label = "Angle_Stable"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": diff_norm,
                "bin_to": diff_norm,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if diff_norm < bin_stat["bin_from"]:
            bin_stat["bin_from"] = diff_norm
        if diff_norm > bin_stat["bin_to"]:
            bin_stat["bin_to"] = diff_norm

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_angle_stability inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_channel_width_trend — тренд ширины канала
async def _analyze_lr_channel_width_trend(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_channel_width_trend", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 10
    except Exception:
        window_bars = 10

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_channel_width_trend — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        upper = lr_t.get("upper")
        lower = lr_t.get("lower")

        if upper is None or lower is None:
            continue

        try:
            H_now = float(upper) - float(lower)
        except Exception:
            continue

        if H_now <= 0:
            continue

        start_idx = max(0, idx - window_bars + 1)
        widths: List[float] = []
        for j in range(start_idx, idx + 1):
            up_j = series[j][1].get("upper")
            lo_j = series[j][1].get("lower")
            if up_j is None or lo_j is None:
                continue
            try:
                w = float(up_j) - float(lo_j)
            except Exception:
                continue
            if w > 0:
                widths.append(w)

        if len(widths) < 2:
            continue

        mean_width = sum(widths) / len(widths)
        if mean_width <= 0:
            continue

        width_ratio = H_now / mean_width  # >1 — расширение, <1 — сужение
        delta_ratio = width_ratio - 1.0

        # биннинг по тренду ширины
        # небольшие отклонения считаем стабильными
        if delta_ratio > 0.15:
            bin_label = "Width_Widening"
        elif delta_ratio < -0.15:
            bin_label = "Width_Narrowing"
        else:
            bin_label = "Width_Stable"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": delta_ratio,
                "bin_to": delta_ratio,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if delta_ratio < bin_stat["bin_from"]:
            bin_stat["bin_from"] = delta_ratio
        if delta_ratio > bin_stat["bin_to"]:
            bin_stat["bin_to"] = delta_ratio

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_channel_width_trend inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_bounce_depth — глубина входа от границы канала
async def _analyze_lr_bounce_depth(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_bounce_depth", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 5
    except Exception:
        window_bars = 5

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_bounce_depth — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        upper = lr_t.get("upper")
        lower = lr_t.get("lower")

        if upper is None or lower is None:
            continue

        try:
            upper_f = float(upper)
            lower_f = float(lower)
            price_f = float(entry_price)
        except Exception:
            continue

        H = upper_f - lower_f
        if H <= 0:
            continue

        # глубина входа относительно ближайшей "своей" границы
        if direction == "long":
            depth = (price_f - lower_f) / H
        elif direction == "short":
            depth = (upper_f - price_f) / H
        else:
            continue

        # немного ограничим разумный диапазон
        if depth < 0.0:
            depth = 0.0
        if depth > 2.0:
            depth = 2.0

        # биннинг по глубине
        if depth <= 0.05:
            bin_label = "Depth_VeryNearBoundary"
        elif depth <= 0.15:
            bin_label = "Depth_NearBoundary"
        elif depth <= 0.35:
            bin_label = "Depth_Mid"
        else:
            bin_label = "Depth_DeepInChannel"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": depth,
                "bin_to": depth,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if depth < bin_stat["bin_from"]:
            bin_stat["bin_from"] = depth
        if depth > bin_stat["bin_to"]:
            bin_stat["bin_to"] = depth

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_bounce_depth inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_position_in_channel — положение цены в канале на входе
async def _analyze_lr_position_in_channel(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_position_in_channel", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 5
    except Exception:
        window_bars = 5

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_position_in_channel — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        upper = lr_t.get("upper")
        lower = lr_t.get("lower")

        if upper is None or lower is None:
            continue

        try:
            upper_f = float(upper)
            lower_f = float(lower)
            price_f = float(entry_price)
        except Exception:
            continue

        H = upper_f - lower_f
        if H <= 0:
            continue

        pos = (price_f - lower_f) / H  # 0 — нижняя граница, 1 — верхняя
        if pos < 0.0:
            pos = 0.0
        if pos > 2.0:
            pos = 2.0

        # биннинг по зонам канала
        if pos <= 0.2:
            bin_label = "Zone_LowerOuter"
        elif pos <= 0.4:
            bin_label = "Zone_LowerInner"
        elif pos <= 0.6:
            bin_label = "Zone_Center"
        elif pos <= 0.8:
            bin_label = "Zone_UpperInner"
        else:
            bin_label = "Zone_UpperOuter"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": pos,
                "bin_to": pos,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if pos < bin_stat["bin_from"]:
            bin_stat["bin_from"] = pos
        if pos > bin_stat["bin_to"]:
            bin_stat["bin_to"] = pos

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_position_in_channel inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_multitf_alignment — согласованность наклона младшего и старшего TF
async def _analyze_lr_multitf_alignment(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_multitf_alignment", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 10
    except Exception:
        window_bars = 10

    htf_cfg = params.get("higher_timeframe")
    if htf_cfg is not None:
        higher_tf = str(htf_cfg.get("value") or "").strip().lower()
    else:
        tf_l = timeframe.lower()
        if tf_l == "m5":
            higher_tf = "m15"
        elif tf_l == "m15":
            higher_tf = "h1"
        else:
            higher_tf = "h1"

    if higher_tf not in TF_STEP_MINUTES:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_multitf_alignment — неподдерживаемый higher_timeframe=%s",
            inst_id,
            higher_tf,
        )
        return

    inst_l = _resolve_lr_instance_id(timeframe, source_key)
    inst_h = _resolve_lr_instance_id(higher_tf, source_key)

    if inst_l is None or inst_h is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_multitf_alignment — не найдены instance_id LR для TF=%s и higher_tf=%s",
            inst_id,
            timeframe,
            higher_tf,
        )
        return

    lr_l_history = await _load_lr_history_for_positions(pg, inst_l, timeframe, positions, window_bars)
    lr_h_history = await _load_lr_history_for_positions(pg, inst_h, higher_tf, positions, window_bars)

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta_l = _get_timeframe_timedelta(timeframe)
    ind_delta_h = _get_timeframe_timedelta(higher_tf)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series_l = lr_l_history.get(symbol)
        series_h = lr_h_history.get(symbol)
        if not series_l or not series_h:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        # cutoff для младшего TF
        if ind_delta_l.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time_l = entry_time + sig_delta
            cutoff_time_l = decision_time_l - ind_delta_l
        else:
            cutoff_time_l = entry_time

        # cutoff для старшего TF — используем тот же момент решения
        if ind_delta_h.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time_h = entry_time + sig_delta
            cutoff_time_h = decision_time_h - ind_delta_h
        else:
            cutoff_time_h = entry_time

        idx_l = _find_index_leq(series_l, cutoff_time_l)
        idx_h = _find_index_leq(series_h, cutoff_time_h)

        if idx_l is None or idx_h is None:
            continue

        lr_l = series_l[idx_l][1]
        lr_h = series_h[idx_h][1]

        angle_l = lr_l.get("angle")
        angle_h = lr_h.get("angle")

        if angle_l is None or angle_h is None:
            continue

        try:
            angle_l_f = float(angle_l)
            angle_h_f = float(angle_h)
        except Exception:
            continue

        angle_h_norm = _normalize_angle(angle_h_f, entry_price)
        abs_angle_h = abs(angle_h_norm)

        sign_l = 1 if angle_l_f > 0 else (-1 if angle_l_f < 0 else 0)
        sign_h = 1 if angle_h_f > 0 else (-1 if angle_h_f < 0 else 0)

        threshold_flat = 0.01  # 0.01% — считаем HTF плоским

        if abs_angle_h < threshold_flat or sign_h == 0:
            bin_label = "HTF_Flat"
        elif sign_l == 0:
            bin_label = "LTF_Flat"
        elif sign_l == sign_h:
            if abs_angle_h >= 0.05:
                bin_label = "Aligned_StrongHTF"
            else:
                bin_label = "Aligned"
        else:
            bin_label = "Opposite"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": angle_h_norm,
                "bin_to": angle_h_norm,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if angle_h_norm < bin_stat["bin_from"]:
            bin_stat["bin_from"] = angle_h_norm
        if angle_h_norm > bin_stat["bin_to"]:
            bin_stat["bin_to"] = angle_h_norm

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_multitf_alignment inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_vs_ema_slope — согласованность наклона LR и EMA
async def _analyze_lr_vs_ema_slope(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_vs_ema_slope", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 5
    except Exception:
        window_bars = 5

    slope_k_cfg = params.get("slope_k")
    try:
        slope_k = int(str(slope_k_cfg.get("value"))) if slope_k_cfg is not None else 3
    except Exception:
        slope_k = 3

    ema_src_cfg = params.get("ema_source_key")
    ema_source_key = str(ema_src_cfg.get("value")).strip() if ema_src_cfg is not None else "ema50"

    lr_instance_id = _resolve_lr_instance_id(timeframe, source_key)
    ema_instance_id = _resolve_ema_instance_id(timeframe, ema_source_key)

    if lr_instance_id is None or ema_instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_vs_ema_slope — не найдены instance_id LR (%s) или EMA (%s) для TF=%s",
            inst_id,
            source_key,
            ema_source_key,
            timeframe,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, lr_instance_id, timeframe, positions, window_bars + slope_k)
    ema_history = await _load_ema_history_for_positions(pg, ema_instance_id, timeframe, positions, window_bars + slope_k)

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series_lr = lr_history.get(symbol)
        series_ema = ema_history.get(symbol)
        if not series_lr or not series_ema:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx_lr = _find_index_leq(series_lr, cutoff_time)
        idx_ema = _find_index_leq(series_ema, cutoff_time)

        if idx_lr is None or idx_ema is None:
            continue

        lr_t = series_lr[idx_lr][1]
        angle = lr_t.get("angle")
        if angle is None:
            continue

        try:
            angle_f = float(angle)
        except Exception:
            continue

        # slope EMA_t - EMA_{t-k}
        if idx_ema - slope_k < 0:
            continue

        try:
            ema_t = float(series_ema[idx_ema][1])
            ema_prev = float(series_ema[idx_ema - slope_k][1])
        except Exception:
            continue

        angle_norm = _normalize_angle(angle_f, entry_price)

        try:
            price_f = float(entry_price)
            if price_f <= 0:
                continue
            ema_slope_norm = ((ema_t - ema_prev) / price_f) * 100.0
        except Exception:
            continue

        sign_lr = 1 if angle_norm > 0 else (-1 if angle_norm < 0 else 0)
        sign_ema = 1 if ema_slope_norm > 0 else (-1 if ema_slope_norm < 0 else 0)

        if sign_lr == 0 and sign_ema == 0:
            bin_label = "Flat_Both"
        elif sign_lr == sign_ema and sign_lr != 0:
            if abs(angle_norm) > abs(ema_slope_norm):
                bin_label = "Aligned_LR_Steeper"
            else:
                bin_label = "Aligned_EMA_SteeperOrEqual"
        elif sign_lr == 0:
            bin_label = "EMA_OnlyTrend"
        elif sign_ema == 0:
            bin_label = "LR_OnlyTrend"
        else:
            bin_label = "Disagree"

        # численное значение фичи — разница нормализованных наклонов
        slope_diff = angle_norm - ema_slope_norm

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": slope_diff,
                "bin_to": slope_diff,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if slope_diff < bin_stat["bin_from"]:
            bin_stat["bin_from"] = slope_diff
        if slope_diff > bin_stat["bin_to"]:
            bin_stat["bin_to"] = slope_diff

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_vs_ema_slope inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )


# 🔸 Анализ lr_room_to_opposite_boundary — запас хода до противоположной границы канала
async def _analyze_lr_room_to_opposite_boundary(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    feature_name = resolve_feature_name("lr", "lr_room_to_opposite_boundary", timeframe, source_key)

    window_bars_cfg = params.get("window_bars")
    try:
        window_bars = int(str(window_bars_cfg.get("value"))) if window_bars_cfg is not None else 5
    except Exception:
        window_bars = 5

    instance_id = _resolve_lr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_LR: inst_id=%s, key=lr_room_to_opposite_boundary — не найден instance_id LR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    lr_history = await _load_lr_history_for_positions(pg, instance_id, timeframe, positions, window_bars)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    ind_delta = _get_timeframe_timedelta(timeframe)
    total_trades = 0

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        sl_price = p["sl_price"]
        tp_price = p["tp_price"]
        pnl_abs_raw = p["pnl_abs"]
        pos_tf_raw = p["timeframe"]
        pos_tf = str(pos_tf_raw or "").lower()

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = lr_history.get(symbol)
        if not series:
            continue

        sig_delta = _get_timeframe_timedelta(pos_tf)

        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        lr_t = series[idx][1]
        upper = lr_t.get("upper")
        lower = lr_t.get("lower")

        if upper is None or lower is None:
            continue

        try:
            upper_f = float(upper)
            lower_f = float(lower)
            entry_f = float(entry_price)
            tp_f = float(tp_price)
        except Exception:
            continue

        H = upper_f - lower_f
        if H <= 0:
            continue

        # расстояние до цели TP и до противоположной границы
        if direction == "long":
            tp_target = max(tp_f - entry_f, 0.0)
            room_to_boundary = max(upper_f - entry_f, 0.0)
        elif direction == "short":
            tp_target = max(entry_f - tp_f, 0.0)
            room_to_boundary = max(entry_f - lower_f, 0.0)
        else:
            continue

        if tp_target <= 0.0:
            continue

        ratio = room_to_boundary / tp_target

        # ограничим разумный диапазон
        if ratio < 0.0:
            ratio = 0.0
        if ratio > 5.0:
            ratio = 5.0

        # биннинг по запасу хода
        if ratio < 0.5:
            bin_label = "Room_Limited"
        elif ratio <= 1.5:
            bin_label = "Room_Normal"
        else:
            bin_label = "Room_Large"

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": ratio,
                "bin_to": ratio,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        if ratio < bin_stat["bin_from"]:
            bin_stat["bin_from"] = ratio
        if ratio > bin_stat["bin_to"]:
            bin_stat["bin_to"] = ratio

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs
        total_trades += 1

    log.info(
        "BT_ANALYSIS_LR: lr_room_to_opposite_boundary inst_id=%s, feature=%s, trades=%s, bins=%s",
        inst_id,
        feature_name,
        total_trades,
        len(agg),
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )