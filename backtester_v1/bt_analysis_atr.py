# bt_analysis_atr.py — анализатор фич семейства ATR для backtester_v1

# 🔸 Импорты
import json
import logging
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Tuple, Optional

# 🔸 Кеши backtester_v1 (сценарии и индикаторы)
from backtester_config import get_scenario_instance, get_all_indicator_instances

# 🔸 Настройки Decimal
getcontext().prec = 28

log = logging.getLogger("BT_ANALYSIS_ATR")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Квантование чисел до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)


# 🔸 Безопасное деление
def _safe_div(n: Decimal, d: Decimal) -> Decimal:
    if d == 0:
        return Decimal("0")
    return n / d


# 🔸 Бины для ATR% от цены (проценты)
def _default_atr_value_bins_pct() -> List[Tuple[float, Optional[float], str]]:
    return [
        (0.0, 0.25, "ATRpct_0_0.25"),
        (0.25, 0.50, "ATRpct_0.25_0.50"),
        (0.50, 0.75, "ATRpct_0.50_0.75"),
        (0.75, 1.00, "ATRpct_0.75_1.00"),
        (1.00, None, "ATRpct_GE_1.00"),
    ]


# 🔸 Бины для delta ATR vs MA(ATR) в процентах
def _default_atr_vs_ma_bins_pct() -> List[Tuple[Optional[float], Optional[float], str]]:
    return [
        (None, -20.0, "ATRvsMA_LE_-20"),
        (-20.0, -10.0, "ATRvsMA_-20_-10"),
        (-10.0, 10.0, "ATRvsMA_-10_10"),
        (10.0, 20.0, "ATRvsMA_10_20"),
        (20.0, None, "ATRvsMA_GE_20"),
    ]


# 🔸 Бины для мульти-TF отношения ATR
def _default_atr_multiscale_ratio_bins() -> List[Tuple[Optional[float], Optional[float], str]]:
    return [
        (None, 0.7, "ATR_ratio_LT_0.7"),
        (0.7, 1.0, "ATR_ratio_0.7_1.0"),
        (1.0, 1.3, "ATR_ratio_1.0_1.3"),
        (1.3, None, "ATR_ratio_GE_1.3"),
    ]


# 🔸 Поиск бина по числовому значению
def _find_numeric_bin(
    value: float,
    bins: List[Tuple[Optional[float], Optional[float], str]],
) -> Optional[Tuple[Optional[float], Optional[float], str]]:
    for b_from, b_to, label in bins:
        if b_from is not None and value < b_from:
            continue
        if b_to is not None and value >= b_to:
            continue
        return b_from, b_to, label
    return None


# 🔸 Биннинг подписанного значения (как в RSI: StrongUp/Flat/StrongDown)
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


# 🔸 Биннинг "волатильности волатильности"
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


# 🔸 Извлечение значения ATR из raw_stat с учётом TF и source_key (например atr14)
def _extract_atr_value(
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
    atr_block_raw = indicators_lower.get("atr")
    if not isinstance(atr_block_raw, dict):
        return None

    atr_block: Dict[str, Any] = {str(k).lower(): v for k, v in atr_block_raw.items()}
    atr_val_raw = atr_block.get(source_key.lower())
    if atr_val_raw is None:
        return None

    try:
        return float(atr_val_raw)
    except Exception:
        return None


# 🔸 Поиск instance_id для ATR по timeframe и source_key (atr14 → length=14)
def _resolve_atr_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length = None

    try:
        if source_key.lower().startswith("atr"):
            length = int(source_key[3:])
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


# 🔸 Универсальная запись агрегатов в bt_scenario_feature_bins
async def _write_bins(
    pg,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    feature_name: str,
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    if not agg:
        log.debug(
            "BT_ANALYSIS_ATR: inst_id=%s, feature_name=%s — нет данных для записи (agg пустой), "
            "очищаем старые бины",
            inst_id,
            feature_name,
        )
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_scenario_feature_bins
                WHERE scenario_id  = $1
                  AND signal_id    = $2
                  AND timeframe    = $3
                  AND feature_name = $4
                """,
                scenario_id,
                signal_id,
                timeframe,
                feature_name,
            )
        return

    rows_to_insert: List[Tuple[Any, ...]] = []

    for (direction, bin_label), stat in agg.items():
        trades = stat["trades"]
        wins = stat["wins"]
        losses = stat["losses"]
        pnl_abs_total: Decimal = stat["pnl_abs_total"]
        bin_from = stat["bin_from"]
        bin_to = stat["bin_to"]

        if trades <= 0:
            continue

        winrate = _safe_div(Decimal(wins), Decimal(trades))
        if deposit is not None and deposit != 0:
            roi = _safe_div(pnl_abs_total, deposit)
        else:
            roi = Decimal("0")

        rows_to_insert.append(
            (
                scenario_id,
                signal_id,
                direction,
                timeframe,
                feature_name,
                bin_label,
                bin_from,
                bin_to,
                trades,
                wins,
                losses,
                _q4(pnl_abs_total),
                _q4(winrate),
                _q4(roi),
                "v1",
            )
        )

    async with pg.acquire() as conn:
        # сначала удаляем старые бины по этой фиче/TF для данного сценария/сигнала
        await conn.execute(
            """
            DELETE FROM bt_scenario_feature_bins
            WHERE scenario_id  = $1
              AND signal_id    = $2
              AND timeframe    = $3
              AND feature_name = $4
              AND version      = 'v1'
            """,
            scenario_id,
            signal_id,
            timeframe,
            feature_name,
        )

        if rows_to_insert:
            await conn.executemany(
                """
                INSERT INTO bt_scenario_feature_bins (
                    scenario_id,
                    signal_id,
                    direction,
                    timeframe,
                    feature_name,
                    bin_label,
                    bin_from,
                    bin_to,
                    trades,
                    wins,
                    losses,
                    pnl_abs_total,
                    winrate,
                    roi,
                    version
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15
                )
                """,
                rows_to_insert,
            )

    log.debug(
        "BT_ANALYSIS_ATR: inst_id=%s, feature_name=%s, timeframe=%s — бинов записано=%s",
        inst_id,
        feature_name,
        timeframe,
        len(rows_to_insert),
    )


# 🔸 Анализ atr_value_pct (ATR% от цены, по бинам)
async def _analyze_atr_value_pct(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = f"atr_value_pct_{timeframe}_{source_key}"
    bins = _default_atr_value_bins_pct()
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]
        entry_price_raw = p["entry_price"]

        if direction is None or raw_stat is None or pnl_abs_raw is None or entry_price_raw is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
            entry_price = Decimal(str(entry_price_raw))
        except Exception:
            continue

        if entry_price <= 0:
            continue

        atr_val = _extract_atr_value(raw_stat, timeframe, source_key)
        if atr_val is None:
            continue

        # ATR% от цены
        atr_pct = float((Decimal(str(atr_val)) / entry_price) * Decimal("100"))

        bin_def = _find_numeric_bin(atr_pct, bins)
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

    await _write_bins(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
    )


# 🔸 Анализ atr_multiscale_ratio (отношение ATR между TF)
async def _analyze_atr_multiscale_ratio(
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
    # базовый TF берём из timeframe анализатора
    base_tf = timeframe
    other_tf_cfg = params.get("higher_timeframe") or params.get("other_timeframe")
    if other_tf_cfg is None:
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=atr_multiscale_ratio — не задан higher_timeframe/other_timeframe",
            inst_id,
        )
        return

    other_tf = str(other_tf_cfg.get("value") if isinstance(other_tf_cfg, dict) else other_tf_cfg).strip()
    if other_tf not in ("m5", "m15", "h1"):
        log.warning(
            "BT_ANALYSIS_ATR: inst_id=%s, key=atr_multiscale_ratio — неподдерживаемый higher_timeframe=%s",
            inst_id,
            other_tf,
        )
        return

    feature_name = f"atr_multiscale_ratio_{base_tf}_{other_tf}_{source_key}"
    bins = _default_atr_multiscale_ratio_bins()
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

        atr_base = _extract_atr_value(raw_stat, base_tf, source_key)
        atr_other = _extract_atr_value(raw_stat, other_tf, source_key)

        if atr_base is None or atr_other is None:
            continue

        if atr_other <= 0:
            continue

        ratio = float(atr_base / atr_other)

        bin_def = _find_numeric_bin(ratio, bins)
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

    await _write_bins(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=base_tf,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
    )


# 🔸 Анализ фич, требующих истории ATR (atr_vs_ma, atr_slope, atr_volatility)
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
    # пробуем найти instance_id для ATR
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
    slope_k = _get_int_param("slope_k", 3)

    # загружаем историю ATR для всех символов
    atr_history = await _load_atr_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = f"{key}_{timeframe}_{source_key}"
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

        atr_t = series[idx][1]

        feature_value: Optional[float] = None
        bin_label: Optional[str] = None
        bin_from: Optional[float] = None
        bin_to: Optional[float] = None

        # atr_vs_ma: (ATR_t - MA(ATR)) / MA(ATR) * 100
        if key == "atr_vs_ma":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if not window_vals:
                continue
            ma_val = sum(window_vals) / len(window_vals)
            if ma_val == 0:
                continue
            delta_pct = (atr_t - ma_val) / ma_val * 100.0
            feature_value = delta_pct

            bin_def = _find_numeric_bin(delta_pct, _default_atr_vs_ma_bins_pct())
            if bin_def is None:
                continue
            b_from, b_to, bin_label = bin_def
            bin_from = b_from
            bin_to = b_to

        # atr_slope: ATR_t - ATR_{t-k}
        elif key == "atr_slope":
            if idx - slope_k < 0:
                continue
            atr_prev = series[idx - slope_k][1]
            slope = atr_t - atr_prev
            feature_value = slope
            bin_label = _bin_signed_value_5(slope)
            bin_from = slope
            bin_to = slope

        # atr_volatility: std(ATR) за окно
        elif key == "atr_volatility":
            start_idx = max(0, idx - window_bars + 1)
            window_vals = [v for _, v in series[start_idx : idx + 1]]
            if len(window_vals) < 2:
                continue
            mean = sum(window_vals) / len(window_vals)
            var = sum((v - mean) ** 2 for v in window_vals) / (len(window_vals) - 1)
            vol = var ** 0.5
            feature_value = vol
            bin_label = _bin_volatility(vol)
            bin_from = vol
            bin_to = vol

        else:
            # неизвестный key — на всякий случай пропускаем
            continue

        if feature_value is None or bin_label is None:
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

    await _write_bins(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe=timeframe,
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
    )


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

    # грузим сценарий, чтобы взять deposit для расчёта ROI
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
        family_key = (inst.get("family_key") or "").lower()
        key = inst.get("key")
        inst_id = inst.get("id")
        params = inst.get("params") or {}

        if family_key != "atr":
            continue

        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "atr14"

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

        # ветка анализа по типу key
        if key == "atr_value_pct":
            await _analyze_atr_value_pct(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "atr_multiscale_ratio":
            await _analyze_atr_multiscale_ratio(
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
        elif key in ("atr_vs_ma", "atr_slope", "atr_volatility"):
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