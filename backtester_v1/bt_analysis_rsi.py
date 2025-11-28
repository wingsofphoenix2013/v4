# bt_analysis_rsi.py — анализатор фич семейства RSI для backtester_v1

import json
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

log = logging.getLogger("BT_ANALYSIS_RSI")

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

    # приводим ключи TF к lower()
    tf_lower: Dict[str, Any] = {str(k).lower(): v for k, v in tf_map.items()}
    tf_block = tf_lower.get(timeframe.lower())
    if not isinstance(tf_block, dict):
        return None

    indicators = tf_block.get("indicators")
    if not isinstance(indicators, dict):
        return None

    # приводим семьи индикаторов к lower()
    indicators_lower: Dict[str, Any] = {str(k).lower(): v for k, v in indicators.items()}
    rsi_block_raw = indicators_lower.get("rsi")
    if not isinstance(rsi_block_raw, dict):
        return None

    # приводим ключи внутри RSI к lower()
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
        # source_key вида "rsi14" / "RSI14"
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


# 🔸 Поиск индекса последнего бара с open_time <= entry_time
def _find_index_leq(series: List[Tuple[Any, float]], entry_time) -> Optional[int]:
    # series отсортирован по времени
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


# 🔸 Бины по абсолютному значению RSI (0–100)
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


# 🔸 Поиск бина для конкретного значения RSI
def _find_bin(value: float, bins: List[Tuple[float, float, str]]) -> Optional[Tuple[float, float, str]]:
    for b_from, b_to, label in bins:
        if b_from <= value < b_to:
            return b_from, b_to, label
    return None


# 🔸 Биннинг для rsi_dist_from_50
def _bin_rsi_dist_from_50(rsi: float) -> Tuple[str, float, float]:
    dist = abs(rsi - 50.0)

    if dist <= 5.0:
        return "Near_50", 45.0, 55.0

    if rsi >= 50.0:
        # выше 50
        if dist <= 10.0:
            return "Above_50_Weak", 50.0, 60.0
        if dist <= 20.0:
            return "Above_50_Medium", 50.0, 70.0
        return "Above_50_Strong", 50.0, 100.0

    # ниже 50
    if dist <= 10.0:
        return "Below_50_Weak", 40.0, 50.0
    if dist <= 20.0:
        return "Below_50_Medium", 30.0, 50.0
    return "Below_50_Strong", 0.0, 50.0


# 🔸 Биннинг для rsi_zone
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


# 🔸 Биннинг для наклона/ускорения (5 бинов)
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


# 🔸 Биннинг волатильности (5 бинов)
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


# 🔸 Биннинг delta RSI vs MA(RSI) (5 бинов)
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


# 🔸 Биннинг типа экстремума
def _bin_extremum_type(ext_type: str) -> str:
    # ext_type из: "StrongMin", "WeakMin", "Flat", "WeakMax", "StrongMax"
    return ext_type


# 🔸 Публичная точка входа: анализ семейства RSI для одного сценария+сигнала
async def run_analysis_rsi(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.debug(
        "BT_ANALYSIS_RSI: старт анализа RSI для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s нет инстансов анализа RSI",
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
            "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
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
                "raw_stat": r["raw_stat"],
                "pnl_abs": r["pnl_abs"],
            }
        )

    log.debug(
        "BT_ANALYSIS_RSI: для scenario_id=%s, signal_id=%s загружено позиций=%s",
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

        if family_key != "rsi":
            continue

        # общие параметры инстанса
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "rsi14"

        if not timeframe or not source_key:
            log.warning(
                "BT_ANALYSIS_RSI: inst_id=%s — некорректные параметры timeframe/source_key "
                "(timeframe=%s, source_key=%s)",
                inst_id,
                timeframe,
                source_key,
            )
            continue

        # праздная проверка TF мапы
        if timeframe.lower() not in TF_STEP_MINUTES:
            log.warning(
                "BT_ANALYSIS_RSI: inst_id=%s — неподдерживаемый timeframe=%s",
                inst_id,
                timeframe,
            )
            continue

        log.debug(
            "BT_ANALYSIS_RSI: inst_id=%s — старт расчёта key=%s, timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )

        # ветка анализа по типу key
        if key == "rsi_value":
            await _analyze_rsi_value(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "rsi_dist_from_50":
            await _analyze_rsi_dist_from_50(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "rsi_zone":
            await _analyze_rsi_zone(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
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
            await _analyze_rsi_history_based(
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
                "BT_ANALYSIS_RSI: inst_id=%s (key=%s) пока не поддерживается анализатором",
                inst_id,
                key,
            )

    log.debug(
        "BT_ANALYSIS_RSI: анализ RSI завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )
    log.info(
        "BT_ANALYSIS_RSI: завершён анализ RSI для scenario_id=%s, signal_id=%s, позиций=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(positions),
        len(analysis_instances),
    )


# 🔸 Анализ rsi_value (текущее значение RSI по бинам)
async def _analyze_rsi_value(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("rsi", "rsi_value", timeframe, source_key)
    bins = _default_rsi_value_bins()
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
        if rsi_val is None:
            continue

        bin_def = _find_bin(rsi_val, bins)
        if bin_def is None:
            continue

        b_from, b_to, bin_label = bin_def
        key_tuple = (direction, bin_label)

        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": b_from,
                "bin_to": b_to,
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

    total_trades = sum(stat["trades"] for stat in agg.values())
    log.info(
        "BT_ANALYSIS_RSI: rsi_value inst_id=%s, feature=%s, trades=%s, bins=%s",
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


# 🔸 Анализ rsi_dist_from_50
async def _analyze_rsi_dist_from_50(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("rsi", "rsi_dist_from_50", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
        if rsi_val is None:
            continue

        bin_label, b_from, b_to = _bin_rsi_dist_from_50(rsi_val)
        key_tuple = (direction, bin_label)

        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": b_from,
                "bin_to": b_to,
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

    total_trades = sum(stat["trades"] for stat in agg.values())
    log.info(
        "BT_ANALYSIS_RSI: rsi_dist_from_50 inst_id=%s, feature=%s, trades=%s, bins=%s",
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


# 🔸 Анализ rsi_zone (только зона)
async def _analyze_rsi_zone(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("rsi", "rsi_zone", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        rsi_val = _extract_rsi_value(raw_stat, timeframe, source_key)
        if rsi_val is None:
            continue

        bin_label, b_from, b_to = _bin_rsi_zone(rsi_val)
        key_tuple = (direction, bin_label)

        bin_stat = agg.get(key_tuple)
        if bin_stat is None:
            bin_stat = {
                "bin_from": b_from,
                "bin_to": b_to,
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

    total_trades = sum(stat["trades"] for stat in agg.values())
    log.info(
        "BT_ANALYSIS_RSI: rsi_zone inst_id=%s, feature=%s, trades=%s, bins=%s",
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


# 🔸 Анализ фич, требующих истории RSI
async def _analyze_rsi_history_based(
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
    # пробуем найти instance_id для RSI
    instance_id = _resolve_rsi_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_RSI: inst_id=%s, key=%s — не найден instance_id RSI для timeframe=%s, source_key=%s",
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
    slope_k = _get_int_param("slope_k", 3)
    level_param = params.get("level")

    # загружаем историю RSI для всех символов, учитывая окно
    rsi_history = await _load_rsi_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("rsi", key, timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # длительность TF индикатора (RSI)
    ind_delta = _get_timeframe_timedelta(timeframe)

    for p in positions:
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series = rsi_history.get(symbol)
        if not series:
            continue

        # длительность TF позиции: используем timeframe самой позиции
        pos_tf_raw = p.get("timeframe")
        pos_tf = str(pos_tf_raw or "").lower()
        sig_delta = _get_timeframe_timedelta(pos_tf)

        # учитываем только те RSI-бары, которые реально могли быть известны
        # к моменту закрытия бара позиции:
        # open_time_RSI + Δ_RSI <= entry_time + Δ_sig
        if ind_delta.total_seconds() > 0 and sig_delta.total_seconds() > 0:
            decision_time = entry_time + sig_delta
            cutoff_time = decision_time - ind_delta
        else:
            # неизвестный TF — возвращаемся к старому поведению (<= entry_time)
            cutoff_time = entry_time

        idx = _find_index_leq(series, cutoff_time)
        if idx is None:
            continue

        # текущий RSI (на последнем баре, доступном к моменту cutoff_time)
        rsi_t = series[idx][1]

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        # вычисляем значение фичи и бин по ключу
        bin_label: Optional[str] = None
        bin_from: float = 0.0
        bin_to: float = 0.0

        # rsi_zone_duration: зона + длительность в зоне (по истории назад)
        if key == "rsi_zone_duration":
            zone_label, _, _ = _bin_rsi_zone(rsi_t)

            # считаем длительность (баров подряд) в этой зоне назад
            duration = 1

            # условия зоны — по rsi_t
            z1_low, z1_high = 0.0, 30.0
            z2_low, z2_high = 30.0, 40.0
            z3_low, z3_high = 40.0, 60.0
            z4_low, z4_high = 60.0, 70.0
            z5_low, z5_high = 70.0, 1000.0

            def _in_zone(val: float, label: str) -> bool:
                if label == "Z1_LT_30":
                    return val < z1_high
                if label == "Z2_30_40":
                    return z2_low <= val < z2_high
                if label == "Z3_40_60":
                    return z3_low <= val <= z3_high
                if label == "Z4_60_70":
                    return z4_low < val <= z4_high
                if label == "Z5_GT_70":
                    return val > z5_low
                return False

            # считаем назад
            j = idx - 1
            while j >= 0 and duration < window_bars:
                rsi_prev = series[j][1]
                if not _in_zone(rsi_prev, zone_label):
                    break
                duration += 1
                j -= 1

            # длительность → 3 класса
            if duration <= 3:
                dur_label = "D1_1_3"
            elif duration <= 10:
                dur_label = "D2_4_10"
            else:
                dur_label = "D3_GT_10"

            bin_label = f"{zone_label}_{dur_label}"
            bin_from = 0.0
            bin_to = float(duration)

        # rsi_slope: RSI_t - RSI_{t-k}
        elif key == "rsi_slope":
            if idx - slope_k < 0:
                continue
            rsi_prev = series[idx - slope_k][1]
            slope = rsi_t - rsi_prev
            bin_label = _bin_signed_value_5(slope)
            bin_from = float(slope)
            bin_to = float(slope)

        # rsi_accel: (RSI_t - RSI_{t-k}) - (RSI_{t-k} - RSI_{t-2k})
        elif key == "rsi_accel":
            if idx - 2 * slope_k < 0:
                continue
            rsi_prev = series[idx - slope_k][1]
            rsi_prev2 = series[idx - 2 * slope_k][1]
            slope1 = rsi_t - rsi_prev
            slope2 = rsi_prev - rsi_prev2
            accel = slope1 - slope2
            bin_label = _bin_signed_value_5(accel)
            bin_from = float(accel)
            bin_to = float(accel)

        # rsi_volatility: std за окно
        elif key == "rsi_volatility":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if len(window_vals) < 2:
                continue
            mean = sum(window_vals) / len(window_vals)
            var = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
            vol = var ** 0.5
            bin_label = _bin_volatility(vol)
            bin_from = float(vol)
            bin_to = float(vol)

        # rsi_avg_window: среднее RSI за окно
        elif key == "rsi_avg_window":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if not window_vals:
                continue
            avg_val = sum(window_vals) / len(window_vals)
            z_label, z_from, z_to = _bin_rsi_zone(avg_val)
            bin_label = z_label
            bin_from = z_from
            bin_to = z_to

        # rsi_since_cross_XX: "уровень" берём либо из param level, либо из ключа
        elif key in ("rsi_since_cross_30", "rsi_since_cross_50", "rsi_since_cross_70"):
            if level_param is not None:
                try:
                    level = float(level_param.get("value"))
                except Exception:
                    level = float(key.split("_")[-1])
            else:
                level = float(key.split("_")[-1])

            # считаем количество баров с момента последнего касания уровня
            bars_since = 0
            j = idx - 1
            while j >= 0 and bars_since < window_bars:
                rsi_prev = series[j][1]
                if (rsi_t >= level and rsi_prev <= level) or (rsi_t <= level and rsi_prev >= level):
                    break
                bars_since += 1
                j -= 1

            bin_label = _bin_bars_since_level(bars_since)
            bin_from = float(bars_since)
            bin_to = float(bars_since)

        # rsi_vs_ma: delta RSI_t - MA(RSI) за окно
        elif key == "rsi_vs_ma":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if not window_vals:
                continue
            ma_val = sum(window_vals) / len(window_vals)
            delta = rsi_t - ma_val
            bin_label = _bin_rsi_vs_ma(delta)
            bin_from = float(delta)
            bin_to = float(delta)

        # rsi_extremum: тип экстремума в окне назад
        elif key == "rsi_extremum":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if len(window_vals) < 3:
                continue
            current = rsi_t
            local_min = current == min(window_vals)
            local_max = current == max(window_vals)
            spread = max(window_vals) - min(window_vals)

            if not local_min and not local_max:
                ext_type = "Flat"
            else:
                # "сила" экстремума
                if local_min:
                    second = sorted(window_vals)[1]
                    diff = second - current
                    if diff >= 5.0 and spread >= 10.0:
                        ext_type = "StrongMin"
                    else:
                        ext_type = "WeakMin"
                else:
                    second = sorted(window_vals, reverse=True)[1]
                    diff = current - second
                    if diff >= 5.0 and spread >= 10.0:
                        ext_type = "StrongMax"
                    else:
                        ext_type = "WeakMax"

            bin_label = _bin_extremum_type(ext_type)
            bin_from = float(current)
            bin_to = float(current)

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

    total_trades = sum(stat["trades"] for stat in agg.values())
    log.info(
        "BT_ANALYSIS_RSI: %s inst_id=%s, feature=%s, trades=%s, bins=%s",
        key,
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