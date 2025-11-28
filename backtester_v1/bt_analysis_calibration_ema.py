# bt_analysis_calibration_ema.py — калибровка сырых фич семейства EMA для backtester_v1

# 🔸 Импорты стандартной библиотеки
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

log = logging.getLogger("BT_ANALYSIS_CALIB_EMA")

# 🔸 Таймшаги TF (в минутах) для расчёта окон по барам
TF_STEP_MINUTES: Dict[str, int] = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Вспомогательная функция: длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").strip().lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if step_min is not None:
        return timedelta(minutes=step_min)
    if tf.startswith("m"):
        try:
            return timedelta(minutes=int(tf[1:]))
        except Exception:
            return timedelta(0)
    if tf.startswith("h"):
        try:
            return timedelta(hours=int(tf[1:]))
        except Exception:
            return timedelta(0)
    if tf.startswith("d"):
        try:
            return timedelta(days=int(tf[1:]))
        except Exception:
            return timedelta(0)
    return timedelta(0)


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


# 🔸 Публичная точка входа: калибровка сырых фич для семейства EMA
async def run_calibration_ema_raw(
    pg,
    scenario_id: int,
    signal_id: int,
    analysis_ids: List[int],
    positions: List[Dict[str, Any]],
) -> int:
    """
    Калибровка сырых фич для семейства EMA:
    - считает feature_value / bin_label для каждого analysis_id и позиции;
    - записывает строки в bt_position_features_raw;
    - возвращает общее количество записанных строк.
    """
    if not analysis_ids or not positions:
        log.debug(
            "BT_ANALYSIS_CALIB_EMA: scenario_id=%s, signal_id=%s — пустой набор analysis_ids или позиций",
            scenario_id,
            signal_id,
        )
        return 0

    log.debug(
        "BT_ANALYSIS_CALIB_EMA: старт калибровки сырых фич EMA для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, позиций=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        len(positions),
    )

    total_rows_written = 0

    # 🔸 Определяем, для каких (timeframe, source_key) нужны исторические ряды EMA
    history_needed_generic: Dict[Tuple[str, str], Dict[str, Any]] = {}
    ema9_needed_tfs: Dict[str, Dict[str, Any]] = {}
    ema21_needed_tfs: Dict[str, Dict[str, Any]] = {}
    tf_confluence_needed = False

    for aid in analysis_ids:
        inst = get_analysis_instance(aid)
        if not inst:
            continue
        if inst.get("family_key") != "ema":
            continue

        key = inst.get("key")
        params = inst.get("params") or {}
        tf_cfg = params.get("timeframe")
        source_cfg = params.get("source_key")

        timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
        source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "ema9"

        # history-based по одному EMA (ema200_slope и др.)
        if key in ("ema200_slope", "ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window"):
            cfg_key = (timeframe, source_key)
            if cfg_key not in history_needed_generic:
                history_needed_generic[cfg_key] = {
                    "timeframe": timeframe,
                    "source_key": source_key,
                    "params": params,
                }

        # для pullback / cross_count нужны ema9/ema21 по TF
        if key in ("ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window"):
            if timeframe not in ema9_needed_tfs:
                ema9_needed_tfs[timeframe] = {"timeframe": timeframe}
            if timeframe not in ema21_needed_tfs:
                ema21_needed_tfs[timeframe] = {"timeframe": timeframe}

        # для ema_tf_confluence нужен отдельный путь
        if key == "ema_tf_confluence":
            tf_confluence_needed = True

    # 🔸 Загружаем историю EMA по (timeframe, source_key)
    ema_history_by_key: Dict[Tuple[str, str], Dict[str, List[Tuple[Any, float]]]] = {}

    for (timeframe, source_key), info in history_needed_generic.items():
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
        instance_id = _resolve_ema_instance_id(timeframe, source_key)
        if instance_id is None:
            log.warning(
                "BT_ANALYSIS_CALIB_EMA: не найден instance_id EMA для timeframe=%s, source_key=%s "
                "(scenario_id=%s, signal_id=%s)",
                timeframe,
                source_key,
                scenario_id,
                signal_id,
            )
            continue

        ema_history = await _load_ema_history_for_positions(
            pg=pg,
            instance_id=instance_id,
            timeframe=timeframe,
            positions=positions,
            window_bars=window_bars,
        )
        ema_history_by_key[(timeframe, source_key)] = ema_history

    # 🔸 Загружаем историю EMA9/EMA21 по TF (для pullback / cross_count)
    ema9_history_by_tf: Dict[str, Dict[str, List[Tuple[Any, float]]]] = {}
    ema21_history_by_tf: Dict[str, Dict[str, List[Tuple[Any, float]]]] = {}

    for timeframe in ema9_needed_tfs.keys():
        iid_ema9 = _resolve_ema_instance_id_by_length(timeframe, 9)
        if iid_ema9 is None:
            log.warning(
                "BT_ANALYSIS_CALIB_EMA: не найден instance_id EMA9 для timeframe=%s "
                "(scenario_id=%s, signal_id=%s)",
                timeframe,
                scenario_id,
                signal_id,
            )
            continue
        ema9_history_by_tf[timeframe] = await _load_ema_history_for_positions(
            pg=pg,
            instance_id=iid_ema9,
            timeframe=timeframe,
            positions=positions,
            window_bars=50,
        )

    for timeframe in ema21_needed_tfs.keys():
        iid_ema21 = _resolve_ema_instance_id_by_length(timeframe, 21)
        if iid_ema21 is None:
            log.warning(
                "BT_ANALYSIS_CALIB_EMA: не найден instance_id EMA21 для timeframe=%s "
                "(scenario_id=%s, signal_id=%s)",
                timeframe,
                scenario_id,
                signal_id,
            )
            continue
        ema21_history_by_tf[timeframe] = await _load_ema_history_for_positions(
            pg=pg,
            instance_id=iid_ema21,
            timeframe=timeframe,
            positions=positions,
            window_bars=50,
        )

    # 🔸 История EMA200 по TF для ema_tf_confluence (m5/m15/h1), если нужна
    ema200_m5: Dict[str, List[Tuple[Any, float]]] = {}
    ema200_m15: Dict[str, List[Tuple[Any, float]]] = {}
    ema200_h1: Dict[str, List[Tuple[Any, float]]] = {}

    if tf_confluence_needed:
        iid_m5 = _resolve_ema_instance_id_by_length("m5", 200)
        iid_m15 = _resolve_ema_instance_id_by_length("m15", 200)
        iid_h1 = _resolve_ema_instance_id_by_length("h1", 200)

        if iid_m5 is None or iid_m15 is None or iid_h1 is None:
            log.warning(
                "BT_ANALYSIS_CALIB_EMA: ema_tf_confluence запрошен, но нет EMA200 для m5/m15/h1 "
                "(scenario_id=%s, signal_id=%s)",
                scenario_id,
                signal_id,
            )
            tf_confluence_needed = False
        else:
            ema200_m5 = await _load_ema_history_for_positions(
                pg=pg,
                instance_id=iid_m5,
                timeframe="m5",
                positions=positions,
                window_bars=50,
            )
            ema200_m15 = await _load_ema_history_for_positions(
                pg=pg,
                instance_id=iid_m15,
                timeframe="m15",
                positions=positions,
                window_bars=50,
            )
            ema200_h1 = await _load_ema_history_for_positions(
                pg=pg,
                instance_id=iid_h1,
                timeframe="h1",
                positions=positions,
                window_bars=50,
            )

    # 🔸 Обрабатываем каждый анализатор по всем позициям
    async with pg.acquire() as conn:
        for aid in analysis_ids:
            inst = get_analysis_instance(aid)
            if not inst:
                log.warning(
                    "BT_ANALYSIS_CALIB_EMA: analysis_id=%s не найден в кеше, scenario_id=%s, signal_id=%s",
                    aid,
                    scenario_id,
                    signal_id,
                )
                continue

            inst_family = inst.get("family_key")
            key = inst.get("key")
            params = inst.get("params") or {}

            if inst_family != "ema":
                continue

            tf_cfg = params.get("timeframe")
            source_cfg = params.get("source_key")

            timeframe = str(tf_cfg.get("value")).strip() if tf_cfg is not None else "m5"
            source_key = str(source_cfg.get("value")).strip() if source_cfg is not None else "ema9"

            # для ema_tf_confluence жестко используем "m5" как timeframe фичи (как в bt_analysis_ema)
            feature_tf = "m5" if key == "ema_tf_confluence" else timeframe

            feature_name = resolve_feature_name(
                family_key="ema",
                key=key,
                timeframe=feature_tf,
                source_key=source_key,
            )

            log.debug(
                "BT_ANALYSIS_CALIB_EMA: сбор сырых фич для analysis_id=%s, key=%s, "
                "feature_name=%s, timeframe=%s, scenario_id=%s, signal_id=%s",
                aid,
                key,
                feature_name,
                feature_tf,
                scenario_id,
                signal_id,
            )

            # вспомогательные параметры
            def _get_int_param(name: str, default: int) -> int:
                cfg = params.get(name)
                if cfg is None:
                    return default
                try:
                    return int(str(cfg.get("value")))
                except Exception:
                    return default

            slope_k = _get_int_param("slope_k", 3)
            window_bars = _get_int_param("window_bars", 50)

            history_series = None
            if key in ("ema200_slope", "ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window"):
                history_series = ema_history_by_key.get((timeframe, source_key))

            rows_to_insert: List[Tuple[Any, ...]] = []

            for p in positions:
                position_id = p["id"]
                symbol = p["symbol"]
                direction = p["direction"]
                entry_time = p["entry_time"]
                raw_stat = p["raw_stat"]
                pnl_abs_raw = p["pnl_abs"]
                pos_tf = str(p["timeframe"] or "").lower()

                if direction is None or pnl_abs_raw is None:
                    continue

                try:
                    pnl_abs = Decimal(str(pnl_abs_raw))
                except Exception:
                    continue

                is_win = pnl_abs > 0

                feature_value: Optional[float] = None
                bin_label: Optional[str] = None

                # снапшотные фичи (через raw_stat)
                if key in (
                    "ema_trend_alignment",
                    "ema_stack_spread",
                    "ema_overextension",
                    "ema_band_9_21",
                    "ema_inner_outer_ratio",
                ):
                    ema_block = _extract_ema_block(raw_stat, timeframe)
                    if not ema_block:
                        continue

                    eps = 1e-9

                    if key == "ema_trend_alignment":
                        ema9 = ema_block.get("ema9")
                        ema21 = ema_block.get("ema21")
                        ema50 = ema_block.get("ema50")
                        ema100 = ema_block.get("ema100")
                        ema200 = ema_block.get("ema200")
                        if None in (ema9, ema21, ema50, ema100, ema200):
                            continue

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
                        feature_value = alignment_score

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

                    elif key == "ema_stack_spread":
                        ema9 = ema_block.get("ema9")
                        ema200 = ema_block.get("ema200")
                        if ema9 is None or ema200 is None:
                            continue
                        denom = abs(ema200) if abs(ema200) > eps else eps
                        spread_pct = abs(ema9 - ema200) / denom * 100.0
                        feature_value = spread_pct

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

                    elif key == "ema_overextension":
                        ema9 = ema_block.get("ema9")
                        ema50 = ema_block.get("ema50")
                        ema100 = ema_block.get("ema100")
                        ema200 = ema_block.get("ema200")
                        if None in (ema9, ema50, ema100, ema200):
                            continue
                        denom50 = abs(ema50) if abs(ema50) > eps else eps
                        denom100 = abs(ema100) if abs(ema100) > eps else eps
                        denom200 = abs(ema200) if abs(ema200) > eps else eps

                        d50 = (ema9 - ema50) / denom50 * 100.0
                        d100 = (ema9 - ema100) / denom100 * 100.0
                        d200 = (ema9 - ema200) / denom200 * 100.0

                        overext = 0.5 * d50 + 0.3 * d100 + 0.2 * d200
                        feature_value = overext

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

                    elif key == "ema_band_9_21":
                        ema9 = ema_block.get("ema9")
                        ema21 = ema_block.get("ema21")
                        if ema9 is None or ema21 is None:
                            continue
                        denom = abs(ema21) if abs(ema21) > eps else eps
                        band_pct = abs(ema9 - ema21) / denom * 100.0
                        feature_value = band_pct

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

                    elif key == "ema_inner_outer_ratio":
                        ema9 = ema_block.get("ema9")
                        ema21 = ema_block.get("ema21")
                        ema50 = ema_block.get("ema50")
                        if None in (ema9, ema21, ema50):
                            continue
                        denom21 = abs(ema21) if abs(ema21) > eps else eps
                        denom50 = abs(ema50) if abs(ema50) > eps else eps

                        band_9_21 = abs(ema9 - ema21) / denom21 * 100.0
                        band_21_50 = abs(ema21 - ema50) / denom50 * 100.0

                        denom_ratio = band_21_50 if band_21_50 > eps else eps
                        ratio = band_9_21 / denom_ratio
                        feature_value = ratio

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

                    else:
                        continue

                # ema200_slope (history-based по одной EMA)
                elif key == "ema200_slope":
                    series = history_series.get(symbol) if history_series else None
                    if not series:
                        continue

                    sig_delta = _get_timeframe_timedelta(pos_tf)
                    ema_delta = _get_timeframe_timedelta(timeframe)
                    if sig_delta.total_seconds() > 0 and ema_delta.total_seconds() > 0:
                        decision_time = entry_time + sig_delta
                        cutoff_time = decision_time - ema_delta
                    else:
                        cutoff_time = entry_time

                    idx = _find_index_leq(series, cutoff_time)
                    if idx is None or idx - slope_k < 0:
                        continue

                    v_now = series[idx][1]
                    v_prev = series[idx - slope_k][1]
                    if v_prev == 0:
                        continue

                    slope_pct = (v_now - v_prev) / v_prev * 100.0
                    feature_value = slope_pct
                    bin_label = _bin_signed_value_5(slope_pct)

                # ema_pullback_* и ema_cross_count_window (history-based по EMA9/EMA21)
                elif key in ("ema_pullback_depth", "ema_pullback_duration", "ema_cross_count_window"):
                    series9_map = ema9_history_by_tf.get(timeframe)
                    series21_map = ema21_history_by_tf.get(timeframe)
                    if not series9_map or not series21_map:
                        continue

                    series9 = series9_map.get(symbol)
                    series21 = series21_map.get(symbol)
                    if not series9 or not series21:
                        continue

                    sig_delta = _get_timeframe_timedelta(pos_tf)
                    ema_delta = _get_timeframe_timedelta(timeframe)
                    if sig_delta.total_seconds() > 0 and ema_delta.total_seconds() > 0:
                        decision_time = entry_time + sig_delta
                        cutoff_time = decision_time - ema_delta
                    else:
                        cutoff_time = entry_time

                    idx9 = _find_index_leq(series9, cutoff_time)
                    idx21 = _find_index_leq(series21, cutoff_time)
                    if idx9 is None or idx21 is None:
                        continue

                    start_idx9 = max(0, idx9 - window_bars + 1)
                    start_idx21 = max(0, idx21 - window_bars + 1)
                    window_len = min(idx9 - start_idx9 + 1, idx21 - start_idx21 + 1)
                    if window_len <= 0:
                        continue

                    ratios: List[float] = []
                    for j in range(window_len):
                        v9 = series9[start_idx9 + j][1]
                        v21 = series21[start_idx21 + j][1]
                        if v21 == 0:
                            continue
                        ratios.append(v9 / v21 - 1.0)

                    if not ratios:
                        continue

                    if key == "ema_pullback_depth":
                        if direction == "long":
                            min_ratio = min(ratios)
                            depth = abs(min_ratio) * 100.0
                        elif direction == "short":
                            max_ratio = max(ratios)
                            depth = abs(max_ratio) * 100.0
                        else:
                            continue

                        feature_value = depth

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

                    elif key == "ema_pullback_duration":
                        if direction == "long":
                            target_ratio = min(ratios)
                        elif direction == "short":
                            target_ratio = max(ratios)
                        else:
                            continue

                        ext_idx_rel = None
                        for j in range(len(ratios) - 1, -1, -1):
                            if ratios[j] == target_ratio:
                                ext_idx_rel = j
                                break

                        if ext_idx_rel is None:
                            continue

                        duration_bars = window_len - 1 - ext_idx_rel
                        feature_value = float(duration_bars)

                        if duration_bars == 0:
                            bin_label = "PB_Dur_0"
                        elif 1 <= duration_bars <= 3:
                            bin_label = "PB_Dur_1_3"
                        elif 4 <= duration_bars <= 10:
                            bin_label = "PB_Dur_4_10"
                        else:
                            bin_label = "PB_Dur_GT_10"

                    elif key == "ema_cross_count_window":
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
                        prev_sign = None
                        for s in signs:
                            if s == 0:
                                continue
                            if prev_sign is None:
                                prev_sign = s
                                continue
                            if s != prev_sign:
                                cross_count += 1
                                prev_sign = s

                        feature_value = float(cross_count)

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

                    else:
                        continue

                # ema_tf_confluence (history-based по EMA200 на m5/m15/h1)
                elif key == "ema_tf_confluence" and tf_confluence_needed:
                    series_m5 = ema200_m5.get(symbol)
                    series_m15 = ema200_m15.get(symbol)
                    series_h1 = ema200_h1.get(symbol)
                    if not series_m5 or not series_m15 or not series_h1:
                        continue

                    sig_delta = _get_timeframe_timedelta(pos_tf)
                    delta_m5 = _get_timeframe_timedelta("m5")
                    delta_m15 = _get_timeframe_timedelta("m15")
                    delta_h1 = _get_timeframe_timedelta("h1")

                    if sig_delta.total_seconds() > 0 and delta_m5.total_seconds() > 0:
                        decision_time = entry_time + sig_delta
                        cutoff_m5 = decision_time - delta_m5
                    else:
                        cutoff_m5 = entry_time

                    if sig_delta.total_seconds() > 0 and delta_m15.total_seconds() > 0:
                        decision_time = entry_time + sig_delta
                        cutoff_m15 = decision_time - delta_m15
                    else:
                        cutoff_m15 = entry_time

                    if sig_delta.total_seconds() > 0 and delta_h1.total_seconds() > 0:
                        decision_time = entry_time + sig_delta
                        cutoff_h1 = decision_time - delta_h1
                    else:
                        cutoff_h1 = entry_time

                    idx_m5 = _find_index_leq(series_m5, cutoff_m5)
                    idx_m15 = _find_index_leq(series_m15, cutoff_m15)
                    idx_h1 = _find_index_leq(series_h1, cutoff_h1)
                    if idx_m5 is None or idx_m15 is None or idx_h1 is None:
                        continue

                    def _get_int_param_local(name: str, default: int) -> int:
                        cfg = params.get(name)
                        if cfg is None:
                            return default
                        try:
                            return int(str(cfg.get("value")))
                        except Exception:
                            return default

                    window_bars_m5 = _get_int_param_local("window_bars_m5", 20)
                    window_bars_m15 = _get_int_param_local("window_bars_m15", 20)
                    window_bars_h1 = _get_int_param_local("window_bars_h1", 20)

                    def _compute_slope(series: List[Tuple[Any, float]], idx: int, window: int) -> Optional[float]:
                        if idx - window < 0:
                            return None
                        v_now = series[idx][1]
                        v_prev = series[idx - window][1]
                        if v_prev == 0:
                            return None
                        return (v_now - v_prev) / v_prev * 100.0

                    slope_m5 = _compute_slope(series_m5, idx_m5, window_bars_m5)
                    slope_m15 = _compute_slope(series_m15, idx_m15, window_bars_m15)
                    slope_h1 = _compute_slope(series_h1, idx_h1, window_bars_h1)

                    if slope_m5 is None or slope_m15 is None or slope_h1 is None:
                        continue

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
                    feature_value = confluence

                    if confluence == 1.0:
                        bin_label = "TFConf_AllAligned"
                    elif confluence > 0.33:
                        bin_label = "TFConf_MostlyAligned"
                    elif -0.33 <= confluence <= 0.33:
                        bin_label = "TFConf_Mixed"
                    elif confluence > -1.0:
                        bin_label = "TFConf_MostlyOpposite"
                    else:
                        bin_label = "TFConf_AllOpposite"

                else:
                    continue

                if feature_value is None or bin_label is None:
                    continue

                rows_to_insert.append(
                    (
                        position_id,    # position_id
                        scenario_id,    # scenario_id
                        signal_id,      # signal_id
                        direction,      # direction
                        feature_tf,     # timeframe (анализатора / фичи)
                        "ema",          # family_key
                        key,            # key
                        feature_name,   # feature_name
                        bin_label,      # bin_label
                        feature_value,  # feature_value
                        pnl_abs,        # pnl_abs
                        is_win,         # is_win
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
                    "BT_ANALYSIS_CALIB_EMA: для analysis_id=%s, feature_name=%s записано сырых строк=%s",
                    aid,
                    feature_name,
                    len(rows_to_insert),
                )

    log.debug(
        "BT_ANALYSIS_CALIB_EMA: калибровка EMA завершена для scenario_id=%s, signal_id=%s, "
        "analysis_ids=%s, всего_строк=%s",
        scenario_id,
        signal_id,
        analysis_ids,
        total_rows_written,
    )

    return total_rows_written