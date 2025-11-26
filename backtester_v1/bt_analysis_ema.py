# bt_analysis_ema.py — анализатор фич семейства EMA для backtester_v1

# 🔸 Импорты стандартной библиотеки
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

log = logging.getLogger("BT_ANALYSIS_EMA")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES: Dict[str, int] = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Извлечение блока EMA из raw_stat с учётом TF
def _extract_ema_block(
    raw_stat: Any,
    timeframe: str,
) -> Optional[Dict[str, float]]:
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
    ema_block_raw = indicators_lower.get("ema")
    if not isinstance(ema_block_raw, dict):
        return None

    # приводим ключи внутри EMA к lower() и конвертируем значения в float
    ema_block: Dict[str, float] = {}
    for k, v in ema_block_raw.items():
        key = str(k).lower()
        try:
            ema_block[key] = float(v)
        except Exception:
            continue

    if not ema_block:
        return None

    return ema_block


# 🔸 Поиск instance_id для EMA по timeframe и source_key (ema9 → length=9)
def _resolve_ema_instance_id(timeframe: str, source_key: str) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    length: Optional[int] = None

    try:
        # source_key вида "ema9" / "EMA9"
        sk = source_key.strip().lower()
        if sk.startswith("ema"):
            length = int(sk[3:])
    except Exception:
        length = None

    if length is None:
        return None

    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "ema" or tf != timeframe.lower():
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


# 🔸 Поиск instance_id для EMA длины length на заданном TF
def _resolve_ema_instance_id_by_length(timeframe: str, length: int) -> Optional[int]:
    all_instances = get_all_indicator_instances()
    for iid, inst in all_instances.items():
        indicator = (inst.get("indicator") or "").lower()
        tf = (inst.get("timeframe") or "").lower()
        if indicator != "ema" or tf != timeframe.lower():
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


# 🔸 Загрузка исторического ряда EMA по instance_id для всех символов
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


# 🔸 Биннинг для знаковых величин (5 бинов)
def _bin_signed_value_5(v: float) -> str:
    if v >= 2.0:
        return "StrongUp"
    if 0.5 <= v < 2.0:
        return "ModerateUp"
    if -0.5 < v < 0.5:
        return "Flat"
    if -2.0 < v <= -0.5:
        return "ModerateDown"
    return "StrongDown"


# 🔸 Публичная точка входа: анализ семейства EMA для одного сценария+сигнала
async def run_analysis_ema(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    log.debug(
        "BT_ANALYSIS_EMA: старт анализа EMA для scenario_id=%s, signal_id=%s, инстансов=%s",
        scenario_id,
        signal_id,
        len(analysis_instances),
    )

    if not analysis_instances:
        log.debug(
            "BT_ANALYSIS_EMA: для scenario_id=%s, signal_id=%s нет инстансов анализа EMA",
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
            "BT_ANALYSIS_EMA: для scenario_id=%s, signal_id=%s нет позиций с postproc=true",
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
        "BT_ANALYSIS_EMA: для scenario_id=%s, signal_id=%s загружено позиций=%s",
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

        if family_key != "ema":
            continue

        # общие параметры инстанса
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "ema9"

        if not timeframe or not source_key:
            log.warning(
                "BT_ANALYSIS_EMA: inst_id=%s — некорректные параметры timeframe/source_key "
                "(timeframe=%s, source_key=%s)",
                inst_id,
                timeframe,
                source_key,
            )
            continue

        if timeframe.lower() not in TF_STEP_MINUTES:
            log.warning(
                "BT_ANALYSIS_EMA: inst_id=%s — неподдерживаемый timeframe=%s",
                inst_id,
                timeframe,
            )
            continue

        log.debug(
            "BT_ANALYSIS_EMA: inst_id=%s — старт расчёта key=%s, timeframe=%s, source_key=%s",
            inst_id,
            key,
            timeframe,
            source_key,
        )

        # ветка анализа по типу key (снапшотные фичи)
        if key == "ema_trend_alignment":
            await _analyze_ema_trend_alignment(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "ema_stack_spread":
            await _analyze_ema_stack_spread(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "ema_overextension":
            await _analyze_ema_overextension(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "ema_band_9_21":
            await _analyze_ema_band_9_21(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        elif key == "ema_inner_outer_ratio":
            await _analyze_ema_inner_outer_ratio(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                timeframe=timeframe,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
            )
        # исторические фичи на базе EMA
        elif key in (
            "ema200_slope",
            "ema_pullback_depth",
            "ema_pullback_duration",
            "ema_cross_count_window",
        ):
            await _analyze_ema_history_based(
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
        elif key == "ema_tf_confluence":
            await _analyze_ema_tf_confluence(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                positions=positions,
                source_key=source_key,
                deposit=deposit,
                inst_id=inst_id,
                params=params,
            )
        else:
            log.debug(
                "BT_ANALYSIS_EMA: inst_id=%s (key=%s) пока не поддерживается анализатором EMA",
                inst_id,
                key,
            )

    log.debug(
        "BT_ANALYSIS_EMA: анализ EMA завершён для scenario_id=%s, signal_id=%s",
        scenario_id,
        signal_id,
    )


# 🔸 Анализ ema_trend_alignment
async def _analyze_ema_trend_alignment(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("ema", "ema_trend_alignment", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        ema_block = _extract_ema_block(raw_stat, timeframe)
        if not ema_block:
            continue

        # достаём значения EMA
        ema9 = ema_block.get("ema9")
        ema21 = ema_block.get("ema21")
        ema50 = ema_block.get("ema50")
        ema100 = ema_block.get("ema100")
        ema200 = ema_block.get("ema200")

        if None in (ema9, ema21, ema50, ema100, ema200):
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        # условия выравнивания стека
        if direction == "long":
            pairs = [
                ema9 > ema21,
                ema21 > ema50,
                ema50 > ema100,
                ema100 > ema200,
            ]
            dir_sign = 1.0
        elif direction == "short":
            pairs = [
                ema9 < ema21,
                ema21 < ema50,
                ema50 < ema100,
                ema100 < ema200,
            ]
            dir_sign = -1.0
        else:
            continue

        correct_count = sum(1 for cond in pairs if cond)
        alignment_score = (correct_count / 4.0) * dir_sign

        # бин по alignment_score
        if alignment_score <= -0.5:
            bin_label = "Align_StrongOpposite"
        elif -0.5 < alignment_score < 0.0:
            bin_label = "Align_MixedOpposite"
        elif 0.0 <= alignment_score < 0.5:
            bin_label = "Align_WeakAligned"
        elif 0.5 <= alignment_score < 0.85:
            bin_label = "Align_GoodAligned"
        else:
            bin_label = "Align_PerfectAligned"

        bin_from = float(alignment_score)
        bin_to = float(alignment_score)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, bins=%s, trades=%s",
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


# 🔸 Анализ ema_stack_spread
async def _analyze_ema_stack_spread(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("ema", "ema_stack_spread", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    eps = 1e-9

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        ema_block = _extract_ema_block(raw_stat, timeframe)
        if not ema_block:
            continue

        ema9 = ema_block.get("ema9")
        ema200 = ema_block.get("ema200")

        if ema9 is None or ema200 is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        denom = abs(ema200) if abs(ema200) > eps else eps
        spread_pct = abs(ema9 - ema200) / denom * 100.0

        # бин по spread_pct
        if spread_pct < 0.3:
            bin_label = "Spread_VeryLow"
        elif spread_pct < 0.7:
            bin_label = "Spread_Low"
        elif spread_pct < 1.5:
            bin_label = "Spread_Medium"
        elif spread_pct < 3.0:
            bin_label = "Spread_High"
        else:
            bin_label = "Spread_VeryHigh"

        bin_from = float(spread_pct)
        bin_to = float(spread_pct)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, bins=%s, trades=%s",
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


# 🔸 Анализ ema_overextension
async def _analyze_ema_overextension(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("ema", "ema_overextension", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    eps = 1e-9

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        ema_block = _extract_ema_block(raw_stat, timeframe)
        if not ema_block:
            continue

        ema9 = ema_block.get("ema9")
        ema50 = ema_block.get("ema50")
        ema100 = ema_block.get("ema100")
        ema200 = ema_block.get("ema200")

        if None in (ema9, ema50, ema100, ema200):
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        denom50 = abs(ema50) if abs(ema50) > eps else eps
        denom100 = abs(ema100) if abs(ema100) > eps else eps
        denom200 = abs(ema200) if abs(ema200) > eps else eps

        d50 = (ema9 - ema50) / denom50 * 100.0
        d100 = (ema9 - ema100) / denom100 * 100.0
        d200 = (ema9 - ema200) / denom200 * 100.0

        overext = 0.5 * d50 + 0.3 * d100 + 0.2 * d200

        # бин по overext
        if overext <= -5.0:
            bin_label = "Overext_DeepBelow"
        elif overext <= -2.0:
            bin_label = "Overext_Below"
        elif overext < 2.0:
            bin_label = "Overext_Neutral"
        elif overext < 5.0:
            bin_label = "Overext_Above"
        else:
            bin_label = "Overext_DeepAbove"

        bin_from = float(overext)
        bin_to = float(overext)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, bins=%s, trades=%s",
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


# 🔸 Анализ ema_band_9_21
async def _analyze_ema_band_9_21(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("ema", "ema_band_9_21", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    eps = 1e-9

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        ema_block = _extract_ema_block(raw_stat, timeframe)
        if not ema_block:
            continue

        ema9 = ema_block.get("ema9")
        ema21 = ema_block.get("ema21")

        if ema9 is None or ema21 is None:
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        denom = abs(ema21) if abs(ema21) > eps else eps
        band_pct = abs(ema9 - ema21) / denom * 100.0

        if band_pct < 0.1:
            bin_label = "Band9_21_VeryNarrow"
        elif band_pct < 0.3:
            bin_label = "Band9_21_Narrow"
        elif band_pct < 0.7:
            bin_label = "Band9_21_Medium"
        elif band_pct < 1.5:
            bin_label = "Band9_21_Wide"
        else:
            bin_label = "Band9_21_VeryWide"

        bin_from = float(band_pct)
        bin_to = float(band_pct)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, bins=%s, trades=%s",
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


# 🔸 Анализ ema_inner_outer_ratio
async def _analyze_ema_inner_outer_ratio(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    timeframe: str,
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
) -> None:
    feature_name = resolve_feature_name("ema", "ema_inner_outer_ratio", timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    eps = 1e-9

    for p in positions:
        direction = p["direction"]
        raw_stat = p["raw_stat"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or raw_stat is None or pnl_abs_raw is None:
            continue

        ema_block = _extract_ema_block(raw_stat, timeframe)
        if not ema_block:
            continue

        ema9 = ema_block.get("ema9")
        ema21 = ema_block.get("ema21")
        ema50 = ema_block.get("ema50")

        if None in (ema9, ema21, ema50):
            continue

        try:
            pnl_abs = Decimal(str(pnl_abs_raw))
        except Exception:
            continue

        denom21 = abs(ema21) if abs(ema21) > eps else eps
        denom50 = abs(ema50) if abs(ema50) > eps else eps

        band_9_21 = abs(ema9 - ema21) / denom21 * 100.0
        band_21_50 = abs(ema21 - ema50) / denom50 * 100.0

        denom_ratio = band_21_50 if band_21_50 > eps else eps
        ratio = band_9_21 / denom_ratio

        if ratio < 0.3:
            bin_label = "InnerMuchNarrower"
        elif ratio < 0.7:
            bin_label = "InnerNarrower"
        elif ratio < 1.3:
            bin_label = "InnerSimilar"
        elif ratio < 2.5:
            bin_label = "InnerWider"
        else:
            bin_label = "InnerMuchWider"

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, bins=%s, trades=%s",
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


# 🔸 Анализ фич, требующих истории EMA (без TF-конфлюэнса)
async def _analyze_ema_history_based(
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
    # пробуем найти instance_id для EMA по source_key (например, ema200)
    instance_id = _resolve_ema_instance_id(timeframe, source_key)
    if instance_id is None:
        log.warning(
            "BT_ANALYSIS_EMA: inst_id=%s, key=%s — не найден instance_id EMA для timeframe=%s, source_key=%s",
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

    # загружаем историю EMA для всех символов, учитывая окно
    ema_history = await _load_ema_history_for_positions(
        pg=pg,
        instance_id=instance_id,
        timeframe=timeframe,
        positions=positions,
        window_bars=window_bars,
    )

    feature_name = resolve_feature_name("ema", key, timeframe, source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # для pullback и cross_count нужна также история ema9/ema21
    ema9_history: Dict[str, List[Tuple[Any, float]]] = {}
    ema21_history: Dict[str, List[Tuple[Any, float]]] = {}
    need_pullback = key in ("ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window")

    if need_pullback:
        iid_ema9 = _resolve_ema_instance_id_by_length(timeframe, 9)
        iid_ema21 = _resolve_ema_instance_id_by_length(timeframe, 21)
        if iid_ema9 is None or iid_ema21 is None:
            log.warning(
                "BT_ANALYSIS_EMA: inst_id=%s, key=%s — не найдены EMA9/EMA21 для pullback/cross_count (tf=%s)",
                inst_id,
                key,
                timeframe,
            )
            return

        ema9_history = await _load_ema_history_for_positions(
            pg=pg,
            instance_id=iid_ema9,
            timeframe=timeframe,
            positions=positions,
            window_bars=window_bars,
        )
        ema21_history = await _load_ema_history_for_positions(
            pg=pg,
            instance_id=iid_ema21,
            timeframe=timeframe,
            positions=positions,
            window_bars=window_bars,
        )

    for p in positions:
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series = ema_history.get(symbol)
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

        # ema200_slope: изменение EMA_t - EMA_{t-k}
        if key == "ema200_slope":
            if idx - slope_k < 0:
                continue
            ema_t = series[idx][1]
            ema_prev = series[idx - slope_k][1]
            if ema_prev == 0:
                continue
            slope_pct = (ema_t - ema_prev) / ema_prev * 100.0
            bin_label = _bin_signed_value_5(slope_pct)
            bin_from = float(slope_pct)
            bin_to = float(slope_pct)

        # ema_pullback_depth и ema_pullback_duration: глубина и длительность отката по отношению ema9/ema21
        elif key in ("ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window"):
            series9 = ema9_history.get(symbol)
            series21 = ema21_history.get(symbol)
            if not series9 or not series21:
                continue

            idx9 = _find_index_leq(series9, entry_time)
            idx21 = _find_index_leq(series21, entry_time)
            if idx9 is None or idx21 is None:
                continue

            # выравнивание по времени (предполагаем одинаковую сетку open_time)
            # условия достаточности
            if idx9 < 0 or idx21 < 0:
                continue

            # готовим окно назад
            start_idx9 = max(0, idx9 - window_bars + 1)
            start_idx21 = max(0, idx21 - window_bars + 1)
            window_len = min(idx9 - start_idx9 + 1, idx21 - start_idx21 + 1)

            if window_len <= 0:
                continue

            # рассчитываем отношение ema9/ema21 - 1
            ratios: List[float] = []
            for j in range(window_len):
                v9 = series9[start_idx9 + j][1]
                v21 = series21[start_idx21 + j][1]
                if v21 == 0:
                    continue
                ratios.append(v9 / v21 - 1.0)

            if not ratios:
                continue

            # ema_pullback_depth
            if key == "ema_pullback_depth":
                if direction == "long":
                    min_ratio = min(ratios)
                    depth = abs(min_ratio) * 100.0
                elif direction == "short":
                    max_ratio = max(ratios)
                    depth = abs(max_ratio) * 100.0
                else:
                    continue

                if depth < 0.3:
                    bin_label = "PB_Depth_None"
                elif depth < 1.0:
                    bin_label = "PB_Depth_Shallow"
                elif depth < 2.5:
                    bin_label = "PB_Depth_Normal"
                elif depth < 5.0:
                    bin_label = "PB_Depth_Deep"
                else:
                    bin_label = "PB_Depth_VeryDeep"

                bin_from = float(depth)
                bin_to = float(depth)

            # ema_pullback_duration
            elif key == "ema_pullback_duration":
                # ищем индекс экстремума в окне
                if direction == "long":
                    target_ratio = min(ratios)
                elif direction == "short":
                    target_ratio = max(ratios)
                else:
                    continue

                # последний индекс экстремума в окне
                ext_idx_rel = None
                for j in range(len(ratios) - 1, -1, -1):
                    if ratios[j] == target_ratio:
                        ext_idx_rel = j
                        break

                if ext_idx_rel is None:
                    continue

                # индекс экстремума относительно текущего
                duration_bars = window_len - 1 - ext_idx_rel

                if duration_bars == 0:
                    bin_label = "PB_Dur_0"
                elif 1 <= duration_bars <= 3:
                    bin_label = "PB_Dur_1_3"
                elif 4 <= duration_bars <= 10:
                    bin_label = "PB_Dur_4_10"
                else:
                    bin_label = "PB_Dur_GT_10"

                bin_from = float(duration_bars)
                bin_to = float(duration_bars)

            # ema_cross_count_window
            elif key == "ema_cross_count_window":
                # считаем знаки разности ema9 - ema21
                signs: List[int] = []
                for j in range(window_len):
                    v9 = series9[start_idx9 + j][1]
                    v21 = series21[start_idx21 + j][1]
                    diff = v9 - v21
                    if diff > 0:
                        s = 1
                    elif diff < 0:
                        s = -1
                    else:
                        s = 0
                    signs.append(s)

                cross_count = 0
                prev = None
                for s in signs:
                    if s == 0:
                        continue
                    if prev is None:
                        prev = s
                        continue
                    if s != prev:
                        cross_count += 1
                        prev = s

                if cross_count <= 2:
                    bin_label = "CrossCount_VeryLow"
                elif cross_count <= 5:
                    bin_label = "CrossCount_Low"
                elif cross_count <= 10:
                    bin_label = "CrossCount_Medium"
                elif cross_count <= 20:
                    bin_label = "CrossCount_High"
                else:
                    bin_label = "CrossCount_VeryHigh"

                bin_from = float(cross_count)
                bin_to = float(cross_count)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, key=%s, bins=%s, trades=%s",
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


# 🔸 Анализ ema_tf_confluence (конфлюэнс наклонов EMA200 по TF m5/m15/h1)
async def _analyze_ema_tf_confluence(
    pg,
    scenario_id: int,
    signal_id: int,
    positions: List[Dict[str, Any]],
    source_key: str,
    deposit: Optional[Decimal],
    inst_id: int,
    params: Dict[str, Any],
) -> None:
    # параметры окна в барах на каждом TF
    def _get_int_param(name: str, default: int) -> int:
        cfg = params.get(name)
        if cfg is None:
            return default
        try:
            return int(str(cfg.get("value")))
        except Exception:
            return default

    window_bars_m5 = _get_int_param("window_bars_m5", 20)
    window_bars_m15 = _get_int_param("window_bars_m15", 20)
    window_bars_h1 = _get_int_param("window_bars_h1", 20)

    # instance_id для EMA200 на каждом TF
    iid_m5 = _resolve_ema_instance_id_by_length("m5", 200)
    iid_m15 = _resolve_ema_instance_id_by_length("m15", 200)
    iid_h1 = _resolve_ema_instance_id_by_length("h1", 200)

    if iid_m5 is None or iid_m15 is None or iid_h1 is None:
        log.warning(
            "BT_ANALYSIS_EMA: inst_id=%s, key=ema_tf_confluence — не найдены EMA200 для m5/m15/h1",
            inst_id,
        )
        return

    # загружаем историю EMA200 по всем TF
    ema_m5 = await _load_ema_history_for_positions(
        pg=pg,
        instance_id=iid_m5,
        timeframe="m5",
        positions=positions,
        window_bars=window_bars_m5,
    )
    ema_m15 = await _load_ema_history_for_positions(
        pg=pg,
        instance_id=iid_m15,
        timeframe="m15",
        positions=positions,
        window_bars=window_bars_m15,
    )
    ema_h1 = await _load_ema_history_for_positions(
        pg=pg,
        instance_id=iid_h1,
        timeframe="h1",
        positions=positions,
        window_bars=window_bars_h1,
    )

    feature_name = resolve_feature_name("ema", "ema_tf_confluence", "m5", source_key)
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # вспомогательная функция для вычисления slope% и знака
    def _compute_slope(series: List[Tuple[Any, float]], idx: int, window: int) -> Optional[float]:
        if idx - window < 0:
            return None
        v_now = series[idx][1]
        v_prev = series[idx - window][1]
        if v_prev == 0:
            return None
        return (v_now - v_prev) / v_prev * 100.0

    for p in positions:
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        pnl_abs_raw = p["pnl_abs"]

        if direction is None or pnl_abs_raw is None:
            continue

        series_m5 = ema_m5.get(symbol)
        series_m15 = ema_m15.get(symbol)
        series_h1 = ema_h1.get(symbol)

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

        slope_m5 = _compute_slope(series_m5, idx_m5, window_bars_m5)
        slope_m15 = _compute_slope(series_m15, idx_m15, window_bars_m15)
        slope_h1 = _compute_slope(series_h1, idx_h1, window_bars_h1)

        if slope_m5 is None or slope_m15 is None or slope_h1 is None:
            continue

        # определяем знак наклона
        def _sign_from_slope(v: float) -> int:
            if v >= 0.5:
                return 1
            if v <= -0.5:
                return -1
            return 0

        sign_m5 = _sign_from_slope(slope_m5)
        sign_m15 = _sign_from_slope(slope_m15)
        sign_h1 = _sign_from_slope(slope_h1)

        if direction == "long":
            dir_sign = 1.0
        elif direction == "short":
            dir_sign = -1.0
        else:
            continue

        confluence = (sign_m5 + sign_m15 + sign_h1) * dir_sign / 3.0

        # бин по confluence
        if confluence == 1.0:
            bin_label = "TFConf_AllAligned"
        elif confluence > 0.33:
            bin_label = "TFConf_MostlyAligned"
        elif -0.33 <= confluence <= 0.33:
            bin_label = "TFConf_Mixed"
        elif confluence >= -1.0 and confluence > -1.0:
            # -1 < confluence < -0.33
            bin_label = "TFConf_MostlyOpposite"
        else:
            bin_label = "TFConf_AllOpposite"

        bin_from = float(confluence)
        bin_to = float(confluence)

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
    log.info(
        "BT_ANALYSIS_EMA: inst_id=%s, feature=%s, key=ema_tf_confluence, bins=%s, trades=%s",
        inst_id,
        feature_name,
        len(agg),
        total_trades,
    )

    await write_feature_bins(
        pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        timeframe="m5",
        feature_name=feature_name,
        agg=agg,
        deposit=deposit,
        inst_id=inst_id,
        logger=log,
    )