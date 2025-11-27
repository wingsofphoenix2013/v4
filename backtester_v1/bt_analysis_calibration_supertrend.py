# bt_analysis_calibration_supertrend.py — калибровка сырых фич семейства Supertrend для backtester_v1

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

log = logging.getLogger("BT_ANALYSIS_CALIB_SUPERTREND")

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

# 🔸 Значения по умолчанию для окон Supertrend
DEFAULT_ST_LOOKBACK_BARS: int = 200
DEFAULT_ST_SLOPE_K: int = 3
DEFAULT_ST_ACCEL_K: int = 3


# 🔸 Вспомогательный парсер source_key → (length, mult)
def _parse_supertrend_source_key(source_key: str) -> Tuple[Optional[int], Optional[float]]:
    length: Optional[int] = None
    mult: Optional[float] = None

    try:
        sk = source_key.strip().lower()
        if not sk.startswith("supertrend"):
            return None, None
        core = sk[len("supertrend") :]
        # ожидаем формат "10_3_0"
        parts = core.split("_")
        if not parts:
            return None, None
        length = int(parts[0])
        if len(parts) >= 2:
            mult_str = ".".join(parts[1:])
            mult = float(mult_str)
    except Exception:
        return None, None

    return length, mult


# 🔸 Поиск instance_id для Supertrend по timeframe и source_key (supertrend10_3_0 → length=10, mult=3.0)
def _resolve_supertrend_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length_cfg, mult_cfg = _parse_supertrend_source_key(source_key)
    if length_cfg is None or mult_cfg is None:
        return None

    tf_lower = timeframe.lower()

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "supertrend" or tf != tf_lower:
            continue

        params = inst.get("params") or {}
        length_raw = params.get("length")
        mult_raw = params.get("mult")

        try:
            length_inst = int(str(length_raw))
        except Exception:
            continue

        try:
            mult_inst = float(str(mult_raw))
        except Exception:
            continue

        # небольшой допуск по сравнению float
        if length_inst == length_cfg and abs(mult_inst - mult_cfg) < 1e-9:
            return iid

    return None


# 🔸 Загрузка исторического ряда Supertrend (trend/line) по instance_id и param_name для всех символов
async def _load_st_history_for_positions(
    pg,
    instance_id: int,
    param_name: str,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower()) or 5

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
                  AND param_name  = $3
                  AND open_time  BETWEEN $4 AND $5
                ORDER BY open_time
                """,
                instance_id,
                symbol,
                param_name,
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


# 🔸 Загрузка истории OHLCV (close) по TF для всех символов
async def _load_ohlcv_history_for_positions(
    pg,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower()) or 5
    table = TF_TO_OHLCV_TABLE.get(timeframe.lower())
    if not table:
        return {}

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
                f"""
                SELECT open_time, "close"
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

            series: List[Tuple[Any, float]] = []
            for r in rows:
                try:
                    series.append((r["open_time"], float(r["close"])))
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


def _dir_sign(direction: str) -> int:
    return 1 if (direction or "").lower() == "long" else -1


def _is_win(pnl_abs: float) -> bool:
    return pnl_abs > 0


# 🔸 Публичная точка входа: калибровка сырых фич для семейства Supertrend
async def run_calibration_supertrend_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    analysis_ids: List[int],
    positions: List[Dict[str, Any]],
) -> int:
    """
    Калибровка сырых фич для семейства Supertrend:
    - считает feature_value / bin_label для каждого analysis_id и позиции;
    - записывает строки в bt_position_features_raw;
    - возвращает общее количество записанных строк.
    """
    if not analysis_ids or not positions:
        log.debug(
            "BT_ANALYSIS_CALIB_SUPERTREND: scenario_id=%s, signal_id=%s — пустой набор analysis_ids или позиций",
            scenario_id,
            signal_id,
        )
        return 0

    log.debug(
        "BT_ANALYSIS_CALIB_SUPERTREND: старт калибровки сырых фич Supertrend для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, позиций=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        len(positions),
    )

    total_rows_written = 0
    eps = 1e-9

    async with pg.acquire() as conn:
        for aid in analysis_ids:
            inst = get_analysis_instance(aid)
            if not inst:
                log.warning(
                    "BT_ANALYSIS_CALIB_SUPERTREND: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != "supertrend":
                continue

            tf_cfg = params.get("timeframe")
            src_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(src_cfg.get("value")).strip() if src_cfg is not None else "supertrend10_3_0"

            feature_name = resolve_feature_name(
                family_key="supertrend",
                key=key,
                timeframe=timeframe,
                source_key=source_key,
            )

            log.debug(
                "BT_ANALYSIS_CALIB_SUPERTREND: сбор сырых фич для analysis_id=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
                key,
                feature_name,
                timeframe,
                scenario_id,
                signal_id,
            )

            def _get_int_param(name: str, default: int) -> int:
                cfg = params.get(name)
                if cfg is None:
                    return default
                try:
                    return int(str(cfg.get("value")))
                except Exception:
                    return default

            window_bars = _get_int_param("window_bars", DEFAULT_ST_LOOKBACK_BARS)
            slope_k = _get_int_param("slope_k", DEFAULT_ST_SLOPE_K)
            accel_k = _get_int_param("accel_k", DEFAULT_ST_ACCEL_K)

            rows_to_insert: List[Tuple[Any, ...]] = []

            # 🔸 Расчёт фич по ключам семейства supertrend
            if key == "align_mtf":
                rows_to_insert.extend(
                    await _calib_align_mtf(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                    )
                )

            elif key == "cushion_stop_units":
                rows_to_insert.extend(
                    await _calib_cushion_stop_units(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        eps=eps,
                    )
                )

            elif key == "age_bars":
                rows_to_insert.extend(
                    await _calib_age_bars(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        window_bars=window_bars,
                    )
                )

            elif key == "whipsaw_index":
                rows_to_insert.extend(
                    await _calib_whipsaw_index(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        window_bars=window_bars,
                    )
                )

            elif key == "pullback_depth":
                rows_to_insert.extend(
                    await _calib_pullback_depth(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        window_bars=window_bars,
                    )
                )

            elif key == "slope_pct":
                rows_to_insert.extend(
                    await _calib_slope_pct(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        slope_k=slope_k,
                        window_bars=window_bars,
                    )
                )

            elif key == "accel_pct":
                rows_to_insert.extend(
                    await _calib_accel_pct(
                        pg=pg,
                        positions=positions,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        feature_name=feature_name,
                        timeframe=timeframe,
                        source_key=source_key,
                        accel_k=accel_k,
                        window_bars=window_bars,
                    )
                )

            else:
                log.debug(
                    "BT_ANALYSIS_CALIB_SUPERTREND: неизвестный key='%s' для семейства supertrend, analysis_id=%s",
                    key,
                    aid,
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
                    "BT_ANALYSIS_CALIB_SUPERTREND: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    log.debug(
        "BT_ANALYSIS_CALIB_SUPERTREND: калибровка Supertrend завершена для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, всего_строк=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        total_rows_written,
    )

    return total_rows_written


# 🔸 Калибровка: st_align_mtf_sum
async def _calib_align_mtf(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    # условия расчёта: нужен ST для m5/m15/h1
    i_m5 = _resolve_supertrend_instance_id("m5", source_key)
    i_m15 = _resolve_supertrend_instance_id("m15", source_key)
    i_h1 = _resolve_supertrend_instance_id("h1", source_key)

    if i_m5 is None or i_m15 is None or i_h1 is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: align_mtf — не найдены instance_id ST для всех TF (m5=%s, m15=%s, h1=%s)",
            i_m5,
            i_m15,
            i_h1,
        )
        return rows_to_insert

    hist_m5 = await _load_st_history_for_positions(
        pg=pg,
        instance_id=i_m5,
        param_name=f"{source_key}_trend",
        timeframe="m5",
        positions=positions,
        window_bars=10,
    )
    hist_m15 = await _load_st_history_for_positions(
        pg=pg,
        instance_id=i_m15,
        param_name=f"{source_key}_trend",
        timeframe="m15",
        positions=positions,
        window_bars=10,
    )
    hist_h1 = await _load_st_history_for_positions(
        pg=pg,
        instance_id=i_h1,
        param_name=f"{source_key}_trend",
        timeframe="h1",
        positions=positions,
        window_bars=10,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series_m5 = hist_m5.get(symbol)
        series_m15 = hist_m15.get(symbol)
        series_h1 = hist_h1.get(symbol)
        if not series_m5 or not series_m15 or not series_h1:
            continue

        idx_m5 = _find_index_leq(series_m5, entry_time)
        idx_m15 = _find_index_leq(series_m15, entry_time)
        idx_h1 = _find_index_leq(series_h1, entry_time)
        if idx_m5 is None or idx_m15 is None or idx_h1 is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        dir_sign = _dir_sign(direction)

        trend_m5 = series_m5[idx_m5][1]
        trend_m15 = series_m15[idx_m15][1]
        trend_h1 = series_h1[idx_h1][1]

        align_m5 = trend_m5 * dir_sign
        align_m15 = trend_m15 * dir_sign
        align_h1 = trend_h1 * dir_sign

        st_align_mtf_sum = (align_m5 + align_m15 + align_h1) / 3.0
        bin_label, _, _ = _bin_st_align_mtf(st_align_mtf_sum)

        feature_value = float(st_align_mtf_sum)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,  # TF фичи — из параметров анализатора
                "supertrend",
                "align_mtf",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_cushion_stop_units
async def _calib_cushion_stop_units(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    eps: float,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: cushion_stop_units — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    line_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=source_key,
        timeframe=timeframe,
        positions=positions,
        window_bars=10,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        sl_price = p["sl_price"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None or entry_price is None or sl_price is None:
            continue

        series = line_history.get(symbol)
        if not series or entry_price == 0:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        st_line = series[idx][1]
        dir_sign = _dir_sign(direction)

        stop_pct = abs(entry_price - sl_price) / entry_price * 100.0 if abs(entry_price) > eps else 0.0
        if stop_pct <= 0:
            continue

        dist_pct_signed = (entry_price - st_line) / entry_price * 100.0 * dir_sign
        cushion_units = dist_pct_signed / stop_pct

        bin_label, _, _ = _bin_st_cushion(cushion_units, dist_pct_signed)
        feature_value = float(cushion_units)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "cushion_stop_units",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_age_bars
async def _calib_age_bars(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    window_bars: int,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: age_bars — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    trend_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=f"{source_key}_trend",
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series = trend_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        trend_now = series[idx][1]

        age_bars = 1
        j = idx - 1
        # условия подсчёта возраста
        while j >= 0 and age_bars < window_bars:
            if series[j][1] != trend_now:
                break
            age_bars += 1
            j -= 1

        bin_label, _, _ = _bin_st_age(age_bars)
        feature_value = float(age_bars)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "age_bars",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_whipsaw_index
async def _calib_whipsaw_index(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    window_bars: int,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: whipsaw_index — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    trend_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=f"{source_key}_trend",
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series = trend_history.get(symbol)
        if not series or len(series) < 2:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None or idx < 1:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        # берём окно до idx включительно
        window = series[max(0, idx - window_bars + 1) : idx + 1]
        if len(window) < 2:
            continue

        flips = 0
        last = window[0][1]
        for _, v in window[1:]:
            if v != last:
                flips += 1
                last = v

        whipsaw_index = flips / (len(window) - 1)
        bin_label, _, _ = _bin_st_whipsaw(whipsaw_index)
        feature_value = float(whipsaw_index)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "whipsaw_index",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_pullback_depth_pct
async def _calib_pullback_depth(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    window_bars: int,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: pullback_depth — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    trend_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=f"{source_key}_trend",
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )
    close_history = await _load_ohlcv_history_for_positions(
        pg=pg,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None or entry_price is None:
            continue

        series_trend = trend_history.get(symbol)
        series_close = close_history.get(symbol)
        if not series_trend or not series_close:
            continue

        idx_trend = _find_index_leq(series_trend, entry_time)
        idx_close = _find_index_leq(series_close, entry_time)
        if idx_trend is None or idx_close is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        _, trend_now = series_trend[idx_trend]

        closes_in_trend: List[float] = []

        # выравнивание по индексу назад
        j_trend = idx_trend
        j_close = idx_close

        while j_trend >= 0 and j_close >= 0 and len(closes_in_trend) < window_bars:
            t_trend, v_trend = series_trend[j_trend]
            t_close, v_close = series_close[j_close]
            if t_trend != t_close:
                break
            if v_trend != trend_now:
                break
            closes_in_trend.append(v_close)
            j_trend -= 1
            j_close -= 1

        if not closes_in_trend:
            continue

        if direction.lower() == "long":
            swing_high = max(closes_in_trend)
            if swing_high == 0:
                continue
            depth_pct = (swing_high - entry_price) / swing_high * 100.0
        else:
            swing_low = min(closes_in_trend)
            if swing_low == 0:
                continue
            depth_pct = (entry_price - swing_low) / swing_low * 100.0

        bin_label, _, _ = _bin_st_pullback_depth(depth_pct)
        feature_value = float(depth_pct)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "pullback_depth",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_slope_pct
async def _calib_slope_pct(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    slope_k: int,
    window_bars: int,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: slope_pct — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    line_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=source_key,
        timeframe=timeframe,
        positions=positions,
        window_bars=max(window_bars, slope_k + 1),
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None or entry_price is None or entry_price == 0:
            continue

        series = line_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None or idx - slope_k < 0:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        dir_sign = _dir_sign(direction)

        st_now = series[idx][1]
        st_prev = series[idx - slope_k][1]

        slope_pct = (st_now - st_prev) / entry_price * 100.0 * dir_sign
        bin_label, _, _ = _bin_st_slope(slope_pct)
        feature_value = float(slope_pct)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "slope_pct",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Калибровка: st_accel_pct
async def _calib_accel_pct(
    pg,
    positions: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    feature_name: str,
    timeframe: str,
    source_key: str,
    accel_k: int,
    window_bars: int,
) -> List[Tuple[Any, ...]]:
    rows_to_insert: List[Tuple[Any, ...]] = []

    instance_id = _resolve_supertrend_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_CALIB_SUPERTREND: accel_pct — не найден instance_id для timeframe=%s, source_key=%s",
            timeframe,
            source_key,
        )
        return rows_to_insert

    need_bars = max(window_bars, 2 * accel_k + 1)

    line_history = await _load_st_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        param_name=source_key,
        timeframe=timeframe,
        positions=positions,
        window_bars=need_bars,
    )

    for p in positions:
        position_id = p["id"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None or entry_price is None or entry_price == 0:
            continue

        series = line_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None or idx - 2 * accel_k < 0:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        dir_sign = _dir_sign(direction)

        st_t = series[idx][1]
        st_t_k = series[idx - accel_k][1]
        st_t_2k = series[idx - 2 * accel_k][1]

        slope1 = (st_t - st_t_k) / entry_price * 100.0 * dir_sign
        slope2 = (st_t_k - st_t_2k) / entry_price * 100.0 * dir_sign

        accel_pct = slope1 - slope2
        bin_label, _, _ = _bin_st_accel(accel_pct)
        feature_value = float(accel_pct)

        rows_to_insert.append(
            (
                position_id,
                scenario_id,
                signal_id,
                direction,
                timeframe,
                "supertrend",
                "accel_pct",
                feature_name,
                bin_label,
                feature_value,
                pnl_abs,
                pnl_abs > 0,
            )
        )

    return rows_to_insert


# 🔸 Биннеры для значений Supertrend (должны быть согласованы с V1-анализатором)


def _bin_st_align_mtf(value: float) -> Tuple[str, float, float]:
    # биннинг MTF-конфлюэнса
    if value <= -0.5:
        return "ST_MTF_AllAgainst", -1.0, -0.5
    if value < 0.0:
        return "ST_MTF_MostlyAgainst", -0.5, 0.0
    if value < 0.5:
        return "ST_MTF_MostlyWith", 0.0, 0.5
    return "ST_MTF_AllWith", 0.5, 1.0


def _bin_st_cushion(cushion_units: float, dist_pct_signed: float) -> Tuple[str, Optional[float], Optional[float]]:
    # биннинг запаса до ST в единицах стопа
    if dist_pct_signed <= 0:
        return "ST_Cushion_Negative", None, 0.0
    if cushion_units <= 0.5:
        return "ST_Cushion_VeryThin", 0.0, 0.5
    if cushion_units <= 1.0:
        return "ST_Cushion_Thin", 0.5, 1.0
    if cushion_units <= 2.0:
        return "ST_Cushion_Normal", 1.0, 2.0
    return "ST_Cushion_Thick", 2.0, float("inf")


def _bin_st_age(age_bars: int) -> Tuple[str, int, Optional[int]]:
    # биннинг возраста ST-тренда
    if age_bars <= 3:
        return "ST_Age_VeryFresh", 1, 3
    if age_bars <= 10:
        return "ST_Age_Fresh", 4, 10
    if age_bars <= 30:
        return "ST_Age_Mature", 11, 30
    return "ST_Age_Old", 31, None


def _bin_st_whipsaw(index: float) -> Tuple[str, float, float]:
    # биннинг whipsaw-индекса
    if index < 0.02:
        return "ST_Whipsaw_Stable", 0.0, 0.02
    if index < 0.08:
        return "ST_Whipsaw_Moderate", 0.02, 0.08
    return "ST_Whipsaw_Choppy", 0.08, 1.0


def _bin_st_pullback_depth(depth_pct: float) -> Tuple[str, float, Optional[float]]:
    # биннинг глубины отката
    if depth_pct < 0.3:
        return "PB_Depth_None", 0.0, 0.3
    if depth_pct < 1.0:
        return "PB_Depth_Shallow", 0.3, 1.0
    if depth_pct < 2.5:
        return "PB_Depth_Normal", 1.0, 2.5
    if depth_pct < 5.0:
        return "PB_Depth_Deep", 2.5, 5.0
    return "PB_Depth_VeryDeep", 5.0, None


def _bin_st_slope(slope_pct: float) -> Tuple[str, float, Optional[float]]:
    # биннинг наклона ST
    if slope_pct <= -1.5:
        return "ST_Slope_AgainstStrong", float("-inf"), -1.5
    if slope_pct <= -0.5:
        return "ST_Slope_AgainstWeak", -1.5, -0.5
    if slope_pct < 0.5:
        return "ST_Slope_WithFlat", -0.5, 0.5
    if slope_pct < 1.5:
        return "ST_Slope_WithNormal", 0.5, 1.5
    return "ST_Slope_WithStrong", 1.5, None


def _bin_st_accel(accel_pct: float) -> Tuple[str, float, Optional[float]]:
    # биннинг ускорения ST
    if accel_pct <= -0.5:
        return "ST_Accel_Decelerating", float("-inf"), -0.5
    if accel_pct < 0.5:
        return "ST_Accel_Flat", -0.5, 0.5
    return "ST_Accel_Accelerating", 0.5, None