# bt_analysis_calibration_atr.py — калибровка сырых фич семейства ATR для backtester_v1

# 🔸 Импорты стандартной библиотеки
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

log = logging.getLogger("BT_ANALYSIS_CALIB_ATR")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES: Dict[str, int] = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Таблицы OHLCV по TF
TF_TO_OHLCV_TABLE: Dict[str, str] = {
    "m5": "ohlcv_bb_m5",
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}


# 🔸 Поиск instance_id для ATR по timeframe и source_key (atr14 → length=14)
def _resolve_atr_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length: Optional[int] = None

    try:
        sk = source_key.strip().lower()
        if sk.startswith("atr"):
            length = int(sk[3:])
    except Exception:
        length = None

    if length is None:
        return None

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "atr" or tf != timeframe.lower():
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


# 🔸 Загрузка исторического ряда ATR по instance_id для всех символов
async def _load_atr_history_for_positions(
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


# 🔸 Загрузка истории OHLCV (close/high/low) по TF для всех символов
async def _load_ohlcv_history_for_positions(
    pg,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float, float, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower())
    if not step_min:
        step_min = 5

    table = TF_TO_OHLCV_TABLE.get(timeframe.lower())
    if not table:
        return {}

    by_symbol: Dict[str, List[Any]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[Any, float, float, float]]] = {}

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
                f"""
                SELECT open_time, "close", high, low
                FROM {table}
                WHERE symbol = $1
                  AND open_time BETWEEN $2 AND $3
                ORDER BY open_time
                """,
                symbol,
                from_time,
                to_time,
            )

            if not rows:
                continue

            series: List[Tuple[Any, float, float, float]] = []
            for r in rows:
                try:
                    series.append(
                        (
                            r["open_time"],
                            float(r["close"]),
                            float(r["high"]),
                            float(r["low"]),
                        )
                    )
                except Exception:
                    continue

            if series:
                result[symbol] = series

    return result


# 🔸 Поиск индекса последнего бара с open_time <= entry_time
def _find_index_leq(series: List[Tuple[Any, float]], entry_time) -> Optional[int]:
    lo = 0
    hi = len(series) - 1
    idx: Optional[int] = None

    while lo <= hi:
        mid = (lo + hi) // 2
        t = series[mid][0]
        if t <= entry_time:
            idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return idx


# 🔸 Биннинг для знаковых величин (5 бинов) — для atr_slope
def _bin_signed_value_5(v: float) -> str:
    if v >= 10.0:
        return "StrongUp"
    if 3.0 <= v < 10.0:
        return "ModerateUp"
    if -3.0 < v < 3.0:
        return "Flat"
    if -10.0 < v <= -3.0:
        return "ModerateDown"
    return "StrongDown"


# 🔸 Категоризация волатильности по atr_pct (для regime/persistence)
def _atr_regime_from_pct(atr_pct: float) -> str:
    if atr_pct < 0.5:
        return "Low"
    if atr_pct < 1.5:
        return "Medium"
    return "High"


# 🔸 Публичная точка входа: калибровка сырых фич для семейства ATR
async def run_calibration_atr_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    analysis_ids: List[int],
    positions: List[Dict[str, Any]],
) -> int:
    """
    Калибровка сырых фич для семейства ATR:
    - считает feature_value / bin_label для каждого analysis_id и позиции;
    - записывает строки в bt_position_features_raw;
    - возвращает общее количество записанных строк.
    """
    if not analysis_ids or not positions:
        log.debug(
            "BT_ANALYSIS_CALIB_ATR: scenario_id=%s, signal_id=%s — пустой набор analysis_ids или позиций",
            scenario_id,
            signal_id,
        )
        return 0

    log.debug(
        "BT_ANALYSIS_CALIB_ATR: старт калибровки сырых фич ATR для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, позиций=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        len(positions),
    )

    total_rows_written = 0

    # 🔸 Обрабатываем каждый анализатор по всем позициям (без попыток "экономить" запросы)
    async with pg.acquire() as conn:
        for aid in analysis_ids:
            inst = get_analysis_instance(aid)
            if not inst:
                log.warning(
                    "BT_ANALYSIS_CALIB_ATR: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != "atr":
                continue

            tf_cfg = params.get("timeframe")
            source_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "atr14"

            feature_name = resolve_feature_name(
                family_key="atr",
                key=key,
                timeframe=timeframe,
                source_key=source_key,
            )

            log.debug(
                "BT_ANALYSIS_CALIB_ATR: сбор сырых фич для analysis_id=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
                key,
                feature_name,
                timeframe,
                scenario_id,
                signal_id,
            )

            # общие параметры окон
            def _get_int_param(name: str, default: int) -> int:
                cfg = params.get(name)
                if cfg is None:
                    return default
                try:
                    return int(str(cfg.get("value")))
                except Exception:
                    return default

            window_bars = _get_int_param("window_bars", 50)
            slope_k = _get_int_param("slope_k", 5)

            rows_to_insert: List[Tuple[Any, ...]] = []
            eps = 1e-9

            # history / OHLCV для конкретного ключа
            atr_history: Dict[str, List[Tuple[Any, float]]] = {}
            ohlcv_history: Dict[str, List[Tuple[Any, float, float, float]]] = {}
            ohlcv_lookup: Dict[str, Dict[Any, Tuple[float, float, float]]] = {}

            # sl_pct — размер стопа в процентах (для atr_stop_units)
            sl_pct = 1.0
            if key == "atr_stop_units":
                sl_cfg = params.get("sl_pct")
                if sl_cfg is not None:
                    try:
                        sl_pct = float(str(sl_cfg.get("value")))
                    except Exception:
                        sl_pct = 1.0

            # atr_pct / atr_stop_units / atr_slope / atr_normalized_range / atr_regime_persistence / atr_tf_ratio
            if key in (
                "atr_pct",
                "atr_stop_units",
                "atr_slope",
                "atr_normalized_range",
                "atr_regime_persistence",
                "atr_tf_ratio",
            ):
                # atr_tf_ratio использует TF анализатора и базовый TF m5
                if key == "atr_tf_ratio":
                    inst_tf = _resolve_atr_instance_id(timeframe, source_key)
                    inst_base = _resolve_atr_instance_id("m5", source_key)
                    if inst_tf is None or inst_base is None:
                        log.warning(
                            "BT_ANALYSIS_CALIB_ATR: analysis_id=%s, key=atr_tf_ratio — "
                            "не найдены instance_id ATR для timeframe=%s или base_tf=%s, source_key=%s",
                            aid,
                            timeframe,
                            "m5",
                            source_key,
                        )
                        # перейти к следующему analysis_id
                        continue

                    atr_tf_history = await _load_atr_history_for_positions(
                        pg=pg,
                        instance_id=inst_tf,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=10,
                    )
                    atr_base_history = await _load_atr_history_for_positions(
                        pg=pg,
                        instance_id=inst_base,
                        timeframe="m5",
                        positions=positions,
                        window_bars=10,
                    )

                    # расчёт feature_value как в анализаторе
                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pnl_abs_raw = p["pnl_abs"]

                        if direction is None or pnl_abs_raw is None:
                            continue

                        series_tf = atr_tf_history.get(symbol)
                        series_base = atr_base_history.get(symbol)
                        if not series_tf or not series_base:
                            continue

                        idx_tf = _find_index_leq(series_tf, entry_time)
                        idx_base = _find_index_leq(series_base, entry_time)
                        if idx_tf is None or idx_base is None:
                            continue

                        atr_tf_now = series_tf[idx_tf][1]
                        atr_base_now = series_base[idx_base][1]

                        denom = atr_base_now if abs(atr_base_now) > eps else eps
                        ratio = atr_tf_now / denom

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        if ratio < 0.5:
                            bin_label = "ATR_TF_Ratio_MuchLower"
                        elif ratio < 0.8:
                            bin_label = "ATR_TF_Ratio_Lower"
                        elif ratio < 1.2:
                            bin_label = "ATR_TF_Ratio_Similar"
                        elif ratio < 2.0:
                            bin_label = "ATR_TF_Ratio_Higher"
                        else:
                            bin_label = "ATR_TF_Ratio_MuchHigher"

                        feature_value = float(ratio)

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "atr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                pnl_abs > 0,
                            )
                        )

                else:
                    # все остальные ключи используют TF analysis_id
                    instance_id = _resolve_atr_instance_id(timeframe, source_key)
                    if instance_id is None:
                        log.warning(
                            "BT_ANALYSIS_CALIB_ATR: analysis_id=%s, key=%s — не найден instance_id ATR "
                            "для timeframe=%s, source_key=%s",
                            aid,
                            key,
                            timeframe,
                            source_key,
                        )
                        continue

                    # для atr_pct и atr_stop_units и regime_persistence нужна цена (OHLCV)
                    need_price = key in ("atr_pct", "atr_stop_units", "atr_regime_persistence")

                    atr_history = await _load_atr_history_for_positions(
                        pg=pg,
                        instance_id=instance_id,
                        timeframe=timeframe,
                        positions=positions,
                        window_bars=window_bars,
                    )

                    if need_price:
                        ohlcv_history = await _load_ohlcv_history_for_positions(
                            pg=pg,
                            timeframe=timeframe,
                            positions=positions,
                            window_bars=window_bars,
                        )
                        for sym, series in ohlcv_history.items():
                            local: Dict[Any, Tuple[float, float, float]] = {}
                            for ot, cl, hi, lo in series:
                                local[ot] = (cl, hi, lo)
                            ohlcv_lookup[sym] = local

                    # расчёт feature_value как в анализаторе
                    for p in positions:
                        position_id = p["id"]
                        symbol = p["symbol"]
                        direction = p["direction"]
                        entry_time = p["entry_time"]
                        pnl_abs_raw = p["pnl_abs"]

                        if direction is None or pnl_abs_raw is None:
                            continue

                        series = atr_history.get(symbol)
                        if not series:
                            continue

                        idx = _find_index_leq(series, entry_time)
                        if idx is None:
                            continue

                        try:
                            pnl_abs = Decimal(str(pnl_abs_raw))
                        except Exception:
                            continue

                        feature_value: Optional[float] = None
                        bin_label: Optional[str] = None

                        # atr_pct
                        if key == "atr_pct":
                            ot = series[idx][0]
                            atr_now = series[idx][1]
                            sym_ohlcv = ohlcv_lookup.get(symbol)
                            if not sym_ohlcv:
                                continue
                            cl_tuple = sym_ohlcv.get(ot)
                            if cl_tuple is None:
                                continue
                            close_now, _, _ = cl_tuple
                            if close_now == 0:
                                continue

                            atr_pct = atr_now / close_now * 100.0
                            feature_value = float(atr_pct)

                            if atr_pct < 0.3:
                                bin_label = "ATR_Low_VeryLow"
                            elif atr_pct < 0.8:
                                bin_label = "ATR_Low"
                            elif atr_pct < 1.5:
                                bin_label = "ATR_Medium"
                            elif atr_pct < 3.0:
                                bin_label = "ATR_High"
                            else:
                                bin_label = "ATR_VeryHigh"

                        # atr_stop_units
                        elif key == "atr_stop_units":
                            ot = series[idx][0]
                            atr_now = series[idx][1]
                            sym_ohlcv = ohlcv_lookup.get(symbol)
                            if not sym_ohlcv:
                                continue
                            cl_tuple = sym_ohlcv.get(ot)
                            if cl_tuple is None:
                                continue
                            close_now, _, _ = cl_tuple
                            if close_now == 0:
                                continue

                            atr_pct = atr_now / close_now * 100.0
                            denom = atr_pct if atr_pct > eps else eps
                            stop_units = sl_pct / denom
                            feature_value = float(stop_units)

                            if stop_units < 0.5:
                                bin_label = "StopUnits_VeryTight"
                            elif stop_units < 1.0:
                                bin_label = "StopUnits_Tight"
                            elif stop_units < 2.0:
                                bin_label = "StopUnits_Normal"
                            elif stop_units < 4.0:
                                bin_label = "StopUnits_Wide"
                            else:
                                bin_label = "StopUnits_VeryWide"

                        # atr_slope
                        elif key == "atr_slope":
                            if idx - slope_k < 0:
                                continue
                            atr_now = series[idx][1]
                            atr_prev = series[idx - slope_k][1]
                            if atr_prev == 0:
                                continue
                            slope_pct = (atr_now - atr_prev) / atr_prev * 100.0
                            feature_value = float(slope_pct)
                            bin_label = _bin_signed_value_5(slope_pct)

                        # atr_normalized_range: ATR_now / mean(ATR_window)
                        elif key == "atr_normalized_range":
                            start_idx = max(0, idx - window_bars + 1)
                            window_vals = [v for _, v in series[start_idx : idx + 1]]
                            if not window_vals:
                                continue
                            atr_now = series[idx][1]
                            mean_atr = sum(window_vals) / len(window_vals)
                            if mean_atr == 0:
                                continue
                            ratio = atr_now / mean_atr
                            feature_value = float(ratio)
                            bin_label = "ATR_Range"

                        # atr_regime_persistence: длительность текущего режима ATR% (ATR/close*100)
                        elif key == "atr_regime_persistence":
                            sym_ohlcv = ohlcv_lookup.get(symbol)
                            if not sym_ohlcv:
                                continue

                            ot_now = series[idx][0]
                            atr_now = series[idx][1]
                            cl_tuple = sym_ohlcv.get(ot_now)
                            if cl_tuple is None:
                                continue
                            close_now, _, _ = cl_tuple
                            if close_now == 0:
                                continue

                            atr_pct_now = atr_now / close_now * 100.0
                            current_regime = _atr_regime_from_pct(atr_pct_now)

                            persistence = 1
                            j = idx - 1
                            while j >= 0 and persistence < window_bars:
                                ot_prev = series[j][0]
                                atr_prev_val = series[j][1]
                                cl_prev_tuple = sym_ohlcv.get(ot_prev)
                                if cl_prev_tuple is None:
                                    break
                                close_prev, _, _ = cl_prev_tuple
                                if close_prev == 0:
                                    break
                                atr_prev_pct = atr_prev_val / close_prev * 100.0
                                prev_regime = _atr_regime_from_pct(atr_prev_pct)
                                if prev_regime != current_regime:
                                    break
                                persistence += 1
                                j -= 1

                            feature_value = float(persistence)
                            if persistence <= 3:
                                bin_label = f"Regime_{current_regime}_Short"
                            elif persistence <= 10:
                                bin_label = f"Regime_{current_regime}_Medium"
                            else:
                                bin_label = f"Regime_{current_regime}_Long"

                        else:
                            continue

                        if feature_value is None:
                            continue

                        rows_to_insert.append(
                            (
                                position_id,
                                scenario_id,
                                signal_id,
                                direction,
                                timeframe,
                                "atr",
                                key,
                                feature_name,
                                bin_label,
                                feature_value,
                                pnl_abs,
                                pnl_abs > 0,
                            )
                        )

            else:
                # неизвестный key — пропускаем
                continue

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
                    "BT_ANALYSIS_CALIB_ATR: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    log.info(
        "BT_ANALYSIS_CALIB_ATR: калибровка ATR завершена для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, всего_строк=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        total_rows_written,
    )

    return total_rows_written