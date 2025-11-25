# bt_analysis_adx.py — анализатор фич семейства ADX/DMI для backtester_v1

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

log = logging.getLogger("BT_ANALYSIS_ADX")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES: Dict[str, int] = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Константы для DMI/ADX
EPS_DMI = 1e-6
SLOPE_BARS = 3  # N=3 для всех TF, как договорились


# 🔸 Поиск индекса последнего бара с open_time <= entry_time
def _find_index_leq(series: List[Tuple[Any, Any]], entry_time) -> Optional[int]:
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


# 🔸 Вытягиваем int-параметр из params инстанса
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default
    try:
        return int(str(cfg.get("value")))
    except Exception:
        return default


# 🔸 Парсим длину ADX/DMI из source_key (например, "adx14" → 14)
def _parse_adx_length_from_source_key(source_key: str) -> Optional[int]:
    sk = (source_key or "").strip().lower()
    # ожидаем что-то вроде "adx14" / "adx21"
    if sk.startswith("adx"):
        tail = sk[3:]
        try:
            return int(tail)
        except Exception:
            return None
    return None


# 🔸 Поиск instance_id для ADX_DMI по timeframe и length
def _resolve_adx_dmi_instance_id(timeframe: str, length: int) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "adx_dmi" or tf != timeframe.lower():
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


# 🔸 Загрузка истории ADX (param_name = adx_dmi{length}_adx) для всех символов
async def _load_adx_history_for_positions(
    pg,
    instance_id: int,
    adx_param_name: str,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower()) or 5

    # группируем позиции по символу и собираем времена входа
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

            # запас по времени назад — window_bars баров
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
                adx_param_name,
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


# 🔸 Загрузка истории DMI (+DI/-DI) для всех символов
async def _load_dmi_history_for_positions(
    pg,
    instance_id: int,
    length: int,
    timeframe: str,
    positions: List[Dict[str, Any]],
    window_bars: int,
) -> Dict[str, List[Tuple[Any, float, float]]]:
    if window_bars <= 0:
        window_bars = 1

    step_min = TF_STEP_MINUTES.get(timeframe.lower()) or 5

    plus_param = f"adx_dmi{length}_plus_di"
    minus_param = f"adx_dmi{length}_minus_di"

    # группируем позиции по символу и собираем времена входа
    by_symbol: Dict[str, List[Any]] = defaultdict(list)
    for p in positions:
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        by_symbol[symbol].append(entry_time)

    result: Dict[str, List[Tuple[Any, float, float]]] = {}

    async with pg.acquire() as conn:
        for symbol, times in by_symbol.items():
            if not times:
                continue

            min_entry = min(times)
            max_entry = max(times)

            # запас по времени назад — window_bars баров
            delta = timedelta(minutes=step_min * window_bars)
            from_time = min_entry - delta
            to_time = max_entry

            rows = await conn.fetch(
                """
                SELECT open_time, param_name, value
                FROM indicator_values_v4
                WHERE instance_id = $1
                  AND symbol      = $2
                  AND param_name IN ($3, $4)
                  AND open_time BETWEEN $5 AND $6
                ORDER BY open_time
                """,
                instance_id,
                symbol,
                plus_param,
                minus_param,
                from_time,
                to_time,
            )

            if not rows:
                continue

            # группируем по open_time
            tmp: Dict[Any, Dict[str, float]] = defaultdict(dict)
            for r in rows:
                ot = r["open_time"]
                pn = r["param_name"]
                try:
                    val = float(r["value"])
                except Exception:
                    continue
                tmp[ot][pn] = val

            series: List[Tuple[Any, float, float]] = []
            for ot in sorted(tmp.keys()):
                vals = tmp[ot]
                if plus_param in vals and minus_param in vals:
                    series.append((ot, vals[plus_param], vals[minus_param]))

            if series:
                result[symbol] = series

    return result


# 🔸 Биннинг силы тренда ADX
def _bin_adx_strength(adx_value: float) -> Tuple[str, float, float]:
    if adx_value < 10.0:
        return "no_trend", 0.0, 10.0
    if adx_value < 20.0:
        return "weak", 10.0, 20.0
    if adx_value < 30.0:
        return "normal", 20.0, 30.0
    if adx_value < 40.0:
        return "strong", 30.0, 40.0
    return "extreme", 40.0, 1000.0


# 🔸 Биннинг DMI-направленности относительно сделки
def _bin_dmi_dominance(effective_dom: float) -> Tuple[str, float, float]:
    if effective_dom <= -0.4:
        return "strong_against", -1.0, -0.4
    if -0.4 < effective_dom <= -0.1:
        return "weak_against", -0.4, -0.1
    if -0.1 < effective_dom < 0.1:
        return "neutral", -0.1, 0.1
    if 0.1 <= effective_dom < 0.4:
        return "weak_with", 0.1, 0.4
    return "strong_with", 0.4, 1.0


# 🔸 Биннинг наклона ADX (slope)
def _bin_adx_slope(slope: float) -> Tuple[str, float, float]:
    if slope <= -5.0:
        return "sharp_falling", -1000.0, -5.0
    if -5.0 < slope <= -1.0:
        return "falling", -5.0, -1.0
    if -1.0 < slope < 1.0:
        return "flat", -1.0, 1.0
    if 1.0 <= slope < 5.0:
        return "rising", 1.0, 5.0
    return "sharp_rising", 5.0, 1000.0


# 🔸 Биннинг возраста DMI-тренда
def _bin_dmi_trend_age(age_bars: int, has_trend: bool) -> Tuple[str, float, float]:
    if not has_trend or age_bars <= 0:
        return "no_trend", 0.0, 0.0
    if age_bars <= 3:
        return "very_fresh", 1.0, 3.0
    if age_bars <= 10:
        return "fresh", 4.0, 10.0
    if age_bars <= 30:
        return "mature", 11.0, 30.0
    return "old", 31.0, 1000.0


# 🔸 Биннинг пилы DMI (chop index)
def _bin_dmi_chop(chop_index: float) -> Tuple[str, float, float]:
    if chop_index < 0.05:
        return "very_trendy", 0.0, 0.05
    if chop_index < 0.15:
        return "moderate", 0.05, 0.15
    return "choppy", 0.15, 1.0


# 🔸 Биннинг разницы fast vs slow ADX (14 vs 21)
def _bin_adx_fast_slow_diff(diff: float) -> Tuple[str, float, float]:
    if diff <= -5.0:
        return "late_weakening", -1000.0, -5.0
    if -5.0 < diff <= -1.0:
        return "weakening", -5.0, -1.0
    if -1.0 < diff < 1.0:
        return "balanced", -1.0, 1.0
    if 1.0 <= diff < 5.0:
        return "early_strength", 1.0, 5.0
    return "surge_strength", 5.0, 1000.0


# 🔸 Публичная точка входа: анализ семейства ADX/DMI для одного сценария+сигнала
async def run_analysis_adx(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.debug(
        "BT_ANALYSIS_ADX: старт анализа ADX для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_ADX: для scenario_id=%s, signal_id=%s нет инстансов анализа ADX",
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
            "BT_ANALYSIS_ADX: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
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
        "BT_ANALYSIS_ADX: для scenario_id=%s, signal_id=%s загружено позиций=%s",
        scenario_id,
        signal_id,
        len(positions),
    )

    # обрабатываем каждый инстанс анализа независимо
    for inst in analysis_instances:
        family_key = (inst.get("family_key") or "").lower()
        key = (inst.get("key") or "").lower()
        inst_id = inst.get("id")
        params = inst.get("params") or {}

        if family_key != "adx":
            continue

        # общие параметры инстанса
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "adx14"

        if not timeframe:
            log.warning(
                "BT_ANALYSIS_ADX: inst_id=%s — некорректный timeframe (%s)",
                inst_id,
                timeframe,
            )
            continue

        if timeframe.lower() not in TF_STEP_MINUTES:
            log.warning(
                "BT_ANALYSIS_ADX: inst_id=%s — неподдерживаемый timeframe=%s",
                inst_id,
                timeframe,
            )
            continue

        log.debug(
            "BT_ANALYSIS_ADX: inst_id=%s — старт расчёта key=%s, timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )

        # ветка анализа по типу key
        if key == "strength":
            await _analyze_adx_strength(
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
        elif key == "dominance":
            await _analyze_dmi_dominance(
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
        elif key == "slope":
            await _analyze_adx_slope(
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
        elif key == "trend_age":
            await _analyze_dmi_trend_age(
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
        elif key == "chop":
            await _analyze_dmi_chop(
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
        elif key == "fast_slow_diff":
            await _analyze_adx_fast_slow_diff(
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
                "BT_ANALYSIS_ADX: inst_id=%s (key=%s) пока не поддерживается анализатором ADX",
                inst_id,
                key,
            )

    log.debug(
        "BT_ANALYSIS_ADX: анализ ADX завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )


# 🔸 Анализ strength (сила тренда по ADX)
async def _analyze_adx_strength(
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
    length = _parse_adx_length_from_source_key(source_key) or 14
    instance_id = _resolve_adx_dmi_instance_id(timeframe, length)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=strength — не найден instance_id ADX_DMI для timeframe=%s, length=%s",
            inst_id,
            timeframe,
            length,
        )
        return

    adx_param = f"adx_dmi{length}_adx"
    window_bars = 1

    adx_history = await _load_adx_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        adx_param_name=adx_param,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "strength", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = adx_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        adx_val = series[idx][1]
        bin_label, b_from, b_to = _bin_adx_strength(adx_val)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=strength timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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


# 🔸 Анализ dominance (направление DMI относительно сделки)
async def _analyze_dmi_dominance(
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
    length = _parse_adx_length_from_source_key(source_key) or 14
    instance_id = _resolve_adx_dmi_instance_id(timeframe, length)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=dominance — не найден instance_id ADX_DMI для timeframe=%s, length=%s",
            inst_id,
            timeframe,
            length,
        )
        return

    window_bars = 1

    dmi_history = await _load_dmi_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        length=length,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "dominance", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        dir_lower = str(direction).lower()
        if dir_lower not in ("long", "short"):
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = dmi_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        _, plus_di, minus_di = series[idx]
        denom = plus_di + minus_di
        if denom <= EPS_DMI:
            continue

        raw_dom = (plus_di - minus_di) / denom
        if dir_lower == "long":
            effective_dom = raw_dom
        else:
            effective_dom = -raw_dom

        bin_label, b_from, b_to = _bin_dmi_dominance(effective_dom)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=dominance timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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


# 🔸 Анализ slope (наклон ADX за 3 бара)
async def _analyze_adx_slope(
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
    length = _parse_adx_length_from_source_key(source_key) or 14
    instance_id = _resolve_adx_dmi_instance_id(timeframe, length)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=slope — не найден instance_id ADX_DMI для timeframe=%s, length=%s",
            inst_id,
            timeframe,
            length,
        )
        return

    window_bars = _get_int_param(params, "window_bars", 50)
    if window_bars < SLOPE_BARS + 1:
        window_bars = SLOPE_BARS + 1

    adx_param = f"adx_dmi{length}_adx"

    adx_history = await _load_adx_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        adx_param_name=adx_param,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "slope", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = adx_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None or idx - SLOPE_BARS < 0:
            continue

        adx_now = series[idx][1]
        adx_prev = series[idx - SLOPE_BARS][1]
        slope = adx_now - adx_prev

        bin_label, b_from, b_to = _bin_adx_slope(slope)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=slope timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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


# 🔸 Анализ trend_age (возраст DMI-тренда)
async def _analyze_dmi_trend_age(
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
    length = _parse_adx_length_from_source_key(source_key) or 14
    instance_id = _resolve_adx_dmi_instance_id(timeframe, length)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=trend_age — не найден instance_id ADX_DMI для timeframe=%s, length=%s",
            inst_id,
            timeframe,
            length,
        )
        return

    window_bars = _get_int_param(params, "window_bars", 50)

    dmi_history = await _load_dmi_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        length=length,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "trend_age", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = dmi_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        _, plus_now, minus_now = series[idx]
        diff_now = plus_now - minus_now

        if abs(diff_now) <= EPS_DMI:
            has_trend = False
            age_bars = 0
        else:
            has_trend = True
            sign_entry = 1 if diff_now > 0 else -1
            age_bars = 0

            # считаем назад
            j = idx - 1
            while j >= 0 and age_bars < window_bars:
                _, plus_j, minus_j = series[j]
                diff_j = plus_j - minus_j
                if abs(diff_j) <= EPS_DMI:
                    break
                sign_j = 1 if diff_j > 0 else -1
                if sign_j != sign_entry:
                    break
                age_bars += 1
                j -= 1

        bin_label, b_from, b_to = _bin_dmi_trend_age(age_bars, has_trend)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=trend_age timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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


# 🔸 Анализ chop (DMI chop index — пила/нет)
async def _analyze_dmi_chop(
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
    length = _parse_adx_length_from_source_key(source_key) or 14
    instance_id = _resolve_adx_dmi_instance_id(timeframe, length)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=chop — не найден instance_id ADX_DMI для timeframe=%s, length=%s",
            inst_id,
            timeframe,
            length,
        )
        return

    window_bars = _get_int_param(params, "window_bars", 50)
    if window_bars <= 1:
        window_bars = 10  # минимум какой-то разумный

    dmi_history = await _load_dmi_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        length=length,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "chop", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series = dmi_history.get(symbol)
        if not series:
            continue

        idx = _find_index_leq(series, entry_time)
        if idx is None:
            continue

        # окно [start_idx .. idx) — бары до входа
        start_idx = max(0, idx - window_bars)
        if start_idx >= idx:
            continue

        prev_sign: Optional[int] = None
        changes = 0
        bars_count = 0

        j = start_idx
        while j < idx:
            _, plus_j, minus_j = series[j]
            diff_j = plus_j - minus_j
            if abs(diff_j) <= EPS_DMI:
                # нет явного тренда — пропускаем, но считаем бар
                bars_count += 1
                j += 1
                continue

            sign_j = 1 if diff_j > 0 else -1
            if prev_sign is None:
                prev_sign = sign_j
            else:
                if sign_j != prev_sign:
                    changes += 1
                    prev_sign = sign_j
            bars_count += 1
            j += 1

        if bars_count <= 0:
            continue

        chop_index = changes / float(bars_count)
        bin_label, b_from, b_to = _bin_dmi_chop(chop_index)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=chop timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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


# 🔸 Анализ fast_slow_diff (ADX14 vs ADX21)
async def _analyze_adx_fast_slow_diff(
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
    # длины fast/slow фиксируем как 14 и 21
    fast_len = 14
    slow_len = 21

    fast_instance_id = _resolve_adx_dmi_instance_id(timeframe, fast_len)
    slow_instance_id = _resolve_adx_dmi_instance_id(timeframe, slow_len)

    if fast_instance_id is None or slow_instance_id is None:
        log.warning(
            "BT_ANALYSIS_ADX: inst_id=%s, key=fast_slow_diff — не найдены instance_id для fast_len=%s/slow_len=%s, timeframe=%s",
            inst_id,
            fast_len,
            slow_len,
            timeframe,
        )
        return

    window_bars = 1

    fast_param = f"adx_dmi{fast_len}_adx"
    slow_param = f"adx_dmi{slow_len}_adx"

    fast_history = await _load_adx_history_for_positions(
        pg=pg,
        instance_id=fast_instance_id,
        adx_param_name=fast_param,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )
    slow_history = await _load_adx_history_for_positions(
        pg=pg,
        instance_id=slow_instance_id,
        adx_param_name=slow_param,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("adx", "fast_slow_diff", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        symbol = p["symbol"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        series_fast = fast_history.get(symbol)
        series_slow = slow_history.get(symbol)
        if not series_fast or not series_slow:
            continue

        idx_fast = _find_index_leq(series_fast, entry_time)
        idx_slow = _find_index_leq(series_slow, entry_time)
        if idx_fast is None or idx_slow is None:
            continue

        adx_fast = series_fast[idx_fast][1]
        adx_slow = series_slow[idx_slow][1]
        diff = adx_fast - adx_slow

        bin_label, b_from, b_to = _bin_adx_fast_slow_diff(diff)

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
        "BT_ANALYSIS_ADX: inst_id=%s key=fast_slow_diff timeframe=%s source_key=%s bins=%s trades=%s",
        inst_id,
        timeframe,
        source_key,
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