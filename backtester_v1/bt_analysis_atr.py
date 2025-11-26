# bt_analysis_atr.py — анализатор фич семейства ATR для backtester_v1

# 🔸 Импорты стандартной библиотеки
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

log = logging.getLogger("BT_ANALYSIS_ATR")

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

    # группируем позиции по символу и вычисляем диапазоны времени
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

            # запас по времени в прошлом — window_bars баров
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
    # series отсортирован по времени
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


# 🔸 Публичная точка входа: анализ семейства ATR для одного сценария+сигнала
async def run_analysis_atr(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.debug(
        "BT_ANALYSIS_ATR: старт анализа ATR для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_ATR: для scenario_id=%s, signal_id=%s нет инстансов анализа ATR",
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
            "BT_ANALYSIS_ATR: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
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
                "entry_price": r["entry_price"],
                "raw_stat": r["raw_stat"],
                "pnl_abs": r["pnl_abs"],
            }
        )

    log.debug(
        "BT_ANALYSIS_ATR: для scenario_id=%s, signal_id=%s загружено позиций=%s",
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

        if family_key != "atr":
            continue

        # общие параметры инстанса
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "atr14"

        if not timeframe or not source_key:
            log.warning(
                "BT_ANALYSIS_ATR: inst_id=%s — некорректные параметры timeframe/source_key "
                "(timeframe=%s, source_key=%s)",
                inst_id,
                timeframe,
                source_key,
            )
            continue

        if timeframe.lower() not in TF_STEP_MINUTES:
            log.warning(
                "BT_ANALYSIS_ATR: inst_id=%s — неподдерживаемый timeframe=%s",
                inst_id,
                timeframe,
            )
            continue

        log.debug(
            "BT_ANALYSIS_ATR: inst_id=%s — старт расчёта key=%s, timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )

        # снапшотные фичи: atr_pct / atr_tf_ratio / atr_stop_units
        if key == "atr_pct":
            await _analyze_atr_pct(
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
        elif key == "atr_tf_ratio":
            await _analyze_atr_tf_ratio(
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
        elif key == "atr_stop_units":
            await _analyze_atr_stop_units(
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
        # history-based фичи: atr_slope / atr_normalized_range / atr_regime_persistence
        elif key in ("atr_slope", "atr_normalized_range", "atr_regime_persistence"):
            await _analyze_atr_history_based(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                key=key,
                params=params,
            )
        else:
            log.debug(
                "BT_ANALYSIS_ATR: inst_id=%s (key=%s) пока не поддерживается анализатором ATR",
                inst_id,
                key,
            )

    log.debug(
        "BT_ANALYSIS_ATR: анализ ATR завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )


# 🔸 Анализ atr_pct (режим волатильности в % от цены закрытия)
async def _analyze_atr_pct(
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
    feature_name = resolve_feature_name("atr", "atr_pct", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # окно для подгрузки истории (для привязки ATR к барам)
    window_bars = 10

    # ищем instance_id ATR
    instance_id = _resolve_atr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=atr_pct — не найден instance_id ATR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    # загружаем историю ATR и OHLCV
    atr_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )
    ohlcv_history = await _load_ohlcv_history_for_positions(
        pg=pg,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    # строим lookup по open_time для OHLCV
    ohlcv_lookup: Dict[str, Dict[Any, Tuple[float, float, float]]] = {}
    for sym, series in ohlcv_history.items():
        local: Dict[Any, Tuple[float, float, float]] = {}
        for ot, close, high, low in series:
            local[ot] = (close, high, low)
        ohlcv_lookup[sym] = local

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
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

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        atr_pct = atr_now / close_now * 100.0

        # бин по atr_pct
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

        bin_from = float(atr_pct)
        bin_to = float(atr_pct)

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": bin_from,
                "bin_to": bin_to,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs

    total_trades = sum(b["trades"] for b in agg.values())
    log.debug(
        "BT_ANALYSIS_ATR: inst_id=%s, feature=%s, bins=%s, trades=%s",
        inst_id,
        feature_name,
        len(agg),
        total_trades,
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


# 🔸 Анализ atr_tf_ratio (локальная ATR / ATR базового TF m5)
async def _analyze_atr_tf_ratio(
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
    feature_name = resolve_feature_name("atr", "atr_tf_ratio", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    base_tf = "m5"
    window_bars = 10
    eps = 1e-9

    # instance_id для TF и базового TF
    inst_tf = _resolve_atr_instance_id(timeframe, source_key)
    inst_base = _resolve_atr_instance_id(base_tf, source_key)

    if inst_tf is None or inst_base is None:
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=atr_tf_ratio — не найдены instance_id ATR "
            "для timeframe=%s или base_tf=%s, source_key=%s",
            inst_id,
            timeframe,
            base_tf,
            source_key,
        )
        return

    atr_tf_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=inst_tf,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )
    atr_base_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=inst_base,
        timeframe=base_tf,
        positions=positions,
        window_bars=window_bars,
    )

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
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

        bin_from = float(ratio)
        bin_to = float(ratio)

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": bin_from,
                "bin_to": bin_to,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs

    total_trades = sum(b["trades"] for b in agg.values())
    log.debug(
        "BT_ANALYSIS_ATR: inst_id=%s, feature=%s, bins=%s, trades=%s",
        inst_id,
        feature_name,
        len(agg),
        total_trades,
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


# 🔸 Анализ atr_stop_units (размер стопа в единицах ATR)
async def _analyze_atr_stop_units(
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
    feature_name = resolve_feature_name("atr", "atr_stop_units", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # sl_pct — размер стопа в процентах от цены (по умолчанию 1.0)
    sl_pct = 1.0
    sl_cfg = params.get("sl_pct")
    if sl_cfg is not None:
        try:
            sl_pct = float(str(sl_cfg.get("value")))
        except Exception:
            sl_pct = 1.0

    window_bars = 10
    eps = 1e-9

    instance_id = _resolve_atr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=atr_stop_units — не найден instance_id ATR для timeframe=%s, source_key=%s",
            inst_id,
            timeframe,
            source_key,
        )
        return

    atr_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )
    ohlcv_history = await _load_ohlcv_history_for_positions(
        pg=pg,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    ohlcv_lookup: Dict[str, Dict[Any, Tuple[float, float, float]]] = {}
    for sym, series in ohlcv_history.items():
        local: Dict[Any, Tuple[float, float, float]] = {}
        for ot, close, high, low in series:
            local[ot] = (close, high, low)
        ohlcv_lookup[sym] = local

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
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

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        atr_pct = atr_now / close_now * 100.0
        denom = atr_pct if atr_pct > eps else eps
        stop_units = sl_pct / denom

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

        bin_from = float(stop_units)
        bin_to = float(stop_units)

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": bin_from,
                "bin_to": bin_to,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs

    total_trades = sum(b["trades"] for b in agg.values())
    log.debug(
        "BT_ANALYSIS_ATR: inst_id=%s, feature=%s, bins=%s, trades=%s",
        inst_id,
        feature_name,
        len(agg),
        total_trades,
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


# 🔸 Анализ history-based фич: atr_slope / atr_normalized_range / atr_regime_persistence
async def _analyze_atr_history_based(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    key: str,
    params: Dict[str, Any],
) -> None:
    instance_id = _resolve_atr_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=%s — не найден instance_id ATR для timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )
        return

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

    atr_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    # OHLCV нужна только для regime_persistence (для реального ATR%)
    ohlcv_history: Dict[str, List[Tuple[Any, float, float, float]]] = {}
    ohlcv_lookup: Dict[str, Dict[Any, Tuple[float, float, float]]] = {}
    if key == "atr_regime_persistence":
        ohlcv_history = await _load_ohlcv_history_for_positions(
            pg=pg,
            timeframe=timeframe,
            positions=positions,
            window_bars=window_bars,
        )
        for sym, series in ohlcv_history.items():
            local: Dict[Any, Tuple[float, float, float]] = {}
            for ot, close, high, low in series:
                local[ot] = (close, high, low)
            ohlcv_lookup[sym] = local

    feature_name = resolve_feature_name("atr", key, timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
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

        bin_label: Optional[str] = None
        bin_from: float = 0.0
        bin_to: float = 0.0

        # atr_slope: изменение ATR_t - ATR_{t-k} в %
        if key == "atr_slope":
            if idx - slope_k < 0:
                continue
            atr_now = series[idx][1]
            atr_prev = series[idx - slope_k][1]
            if atr_prev == 0:
                continue
            slope_pct = (atr_now - atr_prev) / atr_prev * 100.0
            bin_label = _bin_signed_value_5(slope_pct)
            bin_from = float(slope_pct)
            bin_to = float(slope_pct)

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
            bin_label = "ATR_Range"
            bin_from = float(ratio)
            bin_to = float(ratio)

        # atr_regime_persistence: длительность текущего режима ATR (Low/Medium/High) по ATR% = ATR/close*100
        elif key == "atr_regime_persistence":
            sym_ohlcv = ohlcv_lookup.get(symbol)
            if not sym_ohlcv:
                continue

            # текущий бар
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

            # считаем назад длительность в этом же режиме
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

            if persistence <= 3:
                bin_label = f"Regime_{current_regime}_Short"
            elif persistence <= 10:
                bin_label = f"Regime_{current_regime}_Medium"
            else:
                bin_label = f"Regime_{current_regime}_Long"

            bin_from = float(persistence)
            bin_to = float(persistence)

        else:
            # неизвестный key — защищаемся
            continue

        if bin_label is None:
            continue

        key_tuple = (direction, bin_label)
        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": bin_from,
                "bin_to": bin_to,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_abs_total": Decimal("0"),
            }
            agg[key_tuple] = bin_stat

        bin_stat["trades"] += 1
        if pnl_abs > 0:
            bin_stat["wins"] += 1
        elif pnl_abs < 0:
            bin_stat["losses"] += 1
        bin_stat["pnl_abs_total"] += pnl_abs

    total_trades = sum(b["trades"] for b in agg.values())
    log.debug(
        "BT_ANALYSIS_ATR: inst_id=%s, feature=%s, key=%s, bins=%s, trades=%s",
        inst_id,
        feature_name,
        key,
        len(agg),
        total_trades,
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