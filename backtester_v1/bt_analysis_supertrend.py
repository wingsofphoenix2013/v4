# bt_analysis_supertrend.py — V1-анализатор семейства supertrend (агрегаты в bt_scenario_feature_bins)

# 🔸 Импорты стандартной библиотеки
import logging
from typing import Any, Dict, List, Tuple, Optional

from decimal import Decimal

# 🔸 Импорты внутренних утилит
from bt_analysis_utils import resolve_feature_name, write_feature_bins

log = logging.getLogger(__name__)


# 🔸 Константы семейства supertrend
DEFAULT_ST_LOOKBACK_BARS: int = 200
DEFAULT_ST_SLOPE_K: int = 3
DEFAULT_ST_ACCEL_K: int = 3


# 🔸 Типы данных
PositionRow = Dict[str, Any]
BinKey = Tuple[str, str]  # (direction, bin_label)


# 🔸 Публичный вход V1-анализа supertrend
async def run_analysis_supertrend(
    scenario_id: int,
    signal_id: int,
    analysis_instances: List[Dict[str, Any]],
    pg,
) -> None:
    """
    V1-анализатор семейства supertrend:
    считает агрегаты фич в bt_scenario_feature_bins для пары (scenario_id, signal_id).
    """
    # фильтруем только supertrend-анализаторы на всякий случай
    st_instances = [inst for inst in analysis_instances if inst.get("family_key") == "supertrend"]
    if not st_instances:
        log.info(
            "run_analysis_supertrend: нет активных инстансов семейства supertrend для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        return

    async with pg.acquire() as conn:
        # 🔸 Загрузка позиций для анализа
        positions: List[PositionRow] = await _load_positions_for_pair(conn, scenario_id, signal_id)
        if not positions:
            log.info(
                "run_analysis_supertrend: нет позиций для анализа (scenario_id=%s, signal_id=%s)",
                scenario_id,
                signal_id,
            )
            return

        total_bins_written = 0
        total_features_processed = 0

        for inst in st_instances:
            key = inst.get("key")
            params = inst.get("params") or {}
            timeframe = params.get("timeframe") or inst.get("timeframe")
            source_key = params.get("source_key")

            if not timeframe or not source_key:
                log.info(
                    "run_analysis_supertrend: пропуск инстанса id=%s — нет timeframe/source_key",
                    inst.get("id"),
                )
                continue

            # позиции данного TF
            positions_tf = [p for p in positions if p["timeframe"] == timeframe]
            if not positions_tf:
                log.info(
                    "run_analysis_supertrend: нет позиций для timeframe=%s (scenario_id=%s, signal_id=%s)",
                    timeframe,
                    scenario_id,
                    signal_id,
                )
                continue

            feature_name = resolve_feature_name("supertrend", key, timeframe, source_key)

            # выбор вычислителя по ключу фичи
            if key == "align_mtf":
                bins = await _compute_bins_align_mtf(conn, positions_tf, timeframe, source_key)
            elif key == "cushion_stop_units":
                bins = await _compute_bins_cushion_stop_units(conn, positions_tf, timeframe, source_key)
            elif key == "age_bars":
                bins = await _compute_bins_age_bars(conn, positions_tf, timeframe, source_key)
            elif key == "whipsaw_index":
                bins = await _compute_bins_whipsaw_index(conn, positions_tf, timeframe, source_key)
            elif key == "pullback_depth":
                bins = await _compute_bins_pullback_depth(conn, positions_tf, timeframe, source_key)
            elif key == "slope_pct":
                bins = await _compute_bins_slope_pct(conn, positions_tf, timeframe, source_key)
            elif key == "accel_pct":
                bins = await _compute_bins_accel_pct(conn, positions_tf, timeframe, source_key)
            else:
                log.info(
                    "run_analysis_supertrend: неизвестный key='%s' для семейства supertrend, пропуск",
                    key,
                )
                continue

            if not bins:
                log.info(
                    "run_analysis_supertrend: не удалось посчитать фичу '%s' (feature_name=%s) — пустые бины",
                    key,
                    feature_name,
                )
                continue

            # 🔸 Запись агрегатов в bt_scenario_feature_bins через утилиту
            # ожидается, что write_feature_bins знает формат records (см. RSI-реализацию)
            await write_feature_bins(
                conn=conn,
                scenario_id=scenario_id,
                signal_id=signal_id,
                feature_name=feature_name,
                timeframe=timeframe,
                version="v1",
                records=list(bins.values()),
            )

            total_bins_written += len(bins)
            # оценки количества позиций в фиче (по сумме trades)
            total_features_processed += sum(rec["trades"] for rec in bins.values())

            log.info(
                "run_analysis_supertrend: feature '%s' (feature_name=%s, tf=%s, src=%s) — bins=%s, trades=%s",
                key,
                feature_name,
                timeframe,
                source_key,
                len(bins),
                sum(rec["trades"] for rec in bins.values()),
            )

        log.info(
            "run_analysis_supertrend: завершено для scenario_id=%s, signal_id=%s — всего фич обработано=%s, всего бинов=%s",
            scenario_id,
            signal_id,
            total_features_processed,
            total_bins_written,
        )


# 🔸 Загрузка позиций для пары (scenario, signal)
async def _load_positions_for_pair(conn, scenario_id: int, signal_id: int) -> List[PositionRow]:
    # выбираем только постпроцесснутые позиции
    rows = await conn.fetch(
        """
        SELECT
            id,
            symbol,
            timeframe,
            direction,
            entry_time,
            entry_price,
            sl_price,
            pnl_abs
        FROM bt_scenario_positions
        WHERE scenario_id = $1
          AND signal_id = $2
          AND postproc = TRUE
        """,
        scenario_id,
        signal_id,
    )

    positions: List[PositionRow] = []
    for r in rows:
        positions.append(
            {
                "id": r["id"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "entry_time": r["entry_time"],
                "entry_price": float(r["entry_price"]),
                "sl_price": float(r["sl_price"]),
                "pnl_abs": float(r["pnl_abs"]),
            }
        )
    return positions


# 🔸 Вспомогательные функции для загрузки supertrend и OHLCV


async def _load_last_trend_point(
    conn,
    symbol: str,
    timeframe: str,
    source_key: str,
    entry_time,
) -> Optional[int]:
    # загружаем тренд supertrend для символа/TF на баре входа
    param_name = f"{source_key}_trend"
    row = await conn.fetchrow(
        """
        SELECT value
        FROM indicator_values_v4
        WHERE symbol = $1
          AND timeframe = $2
          AND param_name = $3
          AND open_time <= $4
        ORDER BY open_time DESC
        LIMIT 1
        """,
        symbol,
        timeframe,
        param_name,
        entry_time,
    )
    if not row:
        return None
    return int(row["value"])


async def _load_trend_history(
    conn,
    symbol: str,
    timeframe: str,
    source_key: str,
    entry_time,
    limit_bars: int,
) -> List[Tuple]:
    # загружаем историю тренда supertrend до бара входа
    param_name = f"{source_key}_trend"
    rows = await conn.fetch(
        """
        SELECT open_time, value
        FROM indicator_values_v4
        WHERE symbol = $1
          AND timeframe = $2
          AND param_name = $3
          AND open_time <= $4
        ORDER BY open_time DESC
        LIMIT $5
        """,
        symbol,
        timeframe,
        param_name,
        entry_time,
        limit_bars,
    )
    # переворачиваем в прямо-временной порядок
    series = [(r["open_time"], float(r["value"])) for r in reversed(rows)]
    return series


async def _load_line_history(
    conn,
    symbol: str,
    timeframe: str,
    source_key: str,
    entry_time,
    limit_bars: int,
) -> List[Tuple]:
    # загружаем историю линии supertrend до бара входа
    param_name = source_key
    rows = await conn.fetch(
        """
        SELECT open_time, value
        FROM indicator_values_v4
        WHERE symbol = $1
          AND timeframe = $2
          AND param_name = $3
          AND open_time <= $4
        ORDER BY open_time DESC
        LIMIT $5
        """,
        symbol,
        timeframe,
        param_name,
        entry_time,
        limit_bars,
    )
    series = [(r["open_time"], float(r["value"])) for r in reversed(rows)]
    return series


async def _load_close_history(
    conn,
    symbol: str,
    timeframe: str,
    entry_time,
    limit_bars: int,
) -> List[Tuple]:
    # загружаем историю close по TF до бара входа
    table = _ohlcv_table_for_tf(timeframe)
    rows = await conn.fetch(
        f"""
        SELECT open_time, "close"
        FROM {table}
        WHERE symbol = $1
          AND open_time <= $2
        ORDER BY open_time DESC
        LIMIT $3
        """,
        symbol,
        entry_time,
        limit_bars,
    )
    series = [(r["open_time"], float(r["close"])) for r in reversed(rows)]
    return series


def _ohlcv_table_for_tf(timeframe: str) -> str:
    tf = timeframe.lower()
    if tf == "m5":
        return "ohlcv_bb_m5"
    if tf == "m15":
        return "ohlcv_bb_m15"
    if tf == "h1":
        return "ohlcv_bb_h1"
    # дефолт на m5, чтобы не падать
    return "ohlcv_bb_m5"


def _dir_sign(direction: str) -> int:
    return 1 if direction.lower() == "long" else -1


def _is_win(pnl_abs: float) -> bool:
    # считаем, что pnl_abs > 0 — выигрыш, остальное — не выигрыш
    return pnl_abs > 0


def _init_bin_record(
    direction: str,
    bin_label: str,
    bin_from: Optional[float],
    bin_to: Optional[float],
) -> Dict[str, Any]:
    return {
        "direction": direction,
        "bin_label": bin_label,
        "bin_from": Decimal(str(bin_from)) if bin_from is not None else None,
        "bin_to": Decimal(str(bin_to)) if bin_to is not None else None,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "pnl_abs_total": Decimal("0"),
    }


def _update_bin_record(
    rec: Dict[str, Any],
    pnl_abs: float,
    win: bool,
) -> None:
    rec["trades"] += 1
    rec["pnl_abs_total"] += Decimal(str(pnl_abs))
    if win:
        rec["wins"] += 1
    else:
        rec["losses"] += 1


# 🔸 Реализации фич семейства supertrend


async def _compute_bins_align_mtf(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_align_mtf_sum — MTF-конфлюэнс тренда supertrend на m5/m15/h1.
    Для feature_name таймфрейм берётся из анализа, но сам расчёт
    использует все три TF (m5/m15/h1).
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        dir_sign = _dir_sign(direction)
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)

        # тренды на трёх TF
        m5_trend = await _load_last_trend_point(conn, symbol, "m5", source_key, entry_time)
        m15_trend = await _load_last_trend_point(conn, symbol, "m15", source_key, entry_time)
        h1_trend = await _load_last_trend_point(conn, symbol, "h1", source_key, entry_time)

        if m5_trend is None or m15_trend is None or h1_trend is None:
            continue

        align_m5 = m5_trend * dir_sign
        align_m15 = m15_trend * dir_sign
        align_h1 = h1_trend * dir_sign

        st_align_mtf_sum = (align_m5 + align_m15 + align_h1) / 3.0

        # бин по st_align_mtf_sum
        bin_label, bin_from, bin_to = _bin_st_align_mtf(st_align_mtf_sum)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_cushion_stop_units(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_cushion_stop_units — расстояние до линии supertrend в единицах стопа.
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        entry_price = pos["entry_price"]
        sl_price = pos["sl_price"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)
        dir_sign = _dir_sign(direction)

        stop_pct = abs(entry_price - sl_price) / entry_price * 100 if entry_price != 0 else 0.0
        if stop_pct <= 0:
            continue

        # линия ST на TF позиции
        line_series = await _load_line_history(conn, symbol, timeframe, source_key, entry_time, 1)
        if not line_series:
            continue
        _, st_line = line_series[-1]

        dist_pct_signed = (entry_price - st_line) / entry_price * 100 * dir_sign if entry_price != 0 else 0.0
        if dist_pct_signed == 0 and stop_pct == 0:
            continue

        cushion_units = dist_pct_signed / stop_pct

        bin_label, bin_from, bin_to = _bin_st_cushion(cushion_units, dist_pct_signed)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_age_bars(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_age_bars — возраст текущего тренда supertrend в барах.
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)

        series = await _load_trend_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            source_key=source_key,
            entry_time=entry_time,
            limit_bars=DEFAULT_ST_LOOKBACK_BARS,
        )
        if not series:
            continue

        # текущий тренд — последний элемент
        _, trend_now = series[-1]

        age_bars = 1
        # условия подсчёта возраста
        for _, v in reversed(series[:-1]):
            if v != trend_now:
                break
            age_bars += 1

        bin_label, bin_from, bin_to = _bin_st_age(age_bars)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_whipsaw_index(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_whipsaw_index — доля флипов тренда supertrend за окно.
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)

        series = await _load_trend_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            source_key=source_key,
            entry_time=entry_time,
            limit_bars=DEFAULT_ST_LOOKBACK_BARS,
        )
        if len(series) < 2:
            continue

        # считаем флипы
        flips = 0
        last = series[0][1]
        for _, val in series[1:]:
            if val != last:
                flips += 1
                last = val

        whipsaw_index = flips / (len(series) - 1)

        bin_label, bin_from, bin_to = _bin_st_whipsaw(whipsaw_index)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_pullback_depth(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_pullback_depth_pct — глубина отката от экстремума внутри текущего ST-тренда.
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        entry_price = pos["entry_price"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)

        trend_series = await _load_trend_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            source_key=source_key,
            entry_time=entry_time,
            limit_bars=DEFAULT_ST_LOOKBACK_BARS,
        )
        close_series = await _load_close_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            entry_time=entry_time,
            limit_bars=DEFAULT_ST_LOOKBACK_BARS,
        )

        if not trend_series or not close_series:
            continue

        # выравниваем по времени (простая версия: берём последние N, предполагая совпадение сетки)
        # текущий тренд
        _, trend_now = trend_series[-1]

        # собираем close внутри текущего тренда
        closes_in_trend: List[float] = []
        # условия накопления
        for (t_trend, v_trend), (t_close, v_close) in zip(reversed(trend_series), reversed(close_series)):
            if t_trend != t_close:
                # в реальном коде тут лучше аккуратно совместить по времени; сейчас предполагаем совпадение
                continue
            if v_trend != trend_now:
                break
            closes_in_trend.append(v_close)

        if not closes_in_trend:
            continue

        if direction.lower() == "long":
            swing_high = max(closes_in_trend)
            if swing_high == 0:
                continue
            depth_pct = (swing_high - entry_price) / swing_high * 100
        else:
            swing_low = min(closes_in_trend)
            if swing_low == 0:
                continue
            depth_pct = (entry_price - swing_low) / swing_low * 100

        bin_label, bin_from, bin_to = _bin_st_pullback_depth(depth_pct)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_slope_pct(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_slope_pct — наклон линии supertrend за K баров, в %% от цены входа.
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    K = DEFAULT_ST_SLOPE_K

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        entry_price = pos["entry_price"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)
        dir_sign = _dir_sign(direction)

        line_series = await _load_line_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            source_key=source_key,
            entry_time=entry_time,
            limit_bars=K + 1,
        )
        if len(line_series) <= K or entry_price == 0:
            continue

        st_now = line_series[-1][1]
        st_prev = line_series[-1 - K][1]

        slope_pct = (st_now - st_prev) / entry_price * 100 * dir_sign

        bin_label, bin_from, bin_to = _bin_st_slope(slope_pct)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


async def _compute_bins_accel_pct(
    conn,
    positions: List[PositionRow],
    timeframe: str,
    source_key: str,
) -> Dict[BinKey, Dict[str, Any]]:
    """
    st_accel_pct — изменение наклона supertrend (ускорение/замедление).
    """
    bins: Dict[BinKey, Dict[str, Any]] = {}

    K = DEFAULT_ST_ACCEL_K

    for pos in positions:
        symbol = pos["symbol"]
        entry_time = pos["entry_time"]
        direction = pos["direction"]
        entry_price = pos["entry_price"]
        pnl_abs = pos["pnl_abs"]
        win = _is_win(pnl_abs)
        dir_sign = _dir_sign(direction)

        line_series = await _load_line_history(
            conn=conn,
            symbol=symbol,
            timeframe=timeframe,
            source_key=source_key,
            entry_time=entry_time,
            limit_bars=2 * K + 1,
        )
        if len(line_series) <= 2 * K or entry_price == 0:
            continue

        st_t = line_series[-1][1]
        st_t_k = line_series[-1 - K][1]
        st_t_2k = line_series[-1 - 2 * K][1]

        slope1 = (st_t - st_t_k) / entry_price * 100 * dir_sign
        slope2 = (st_t_k - st_t_2k) / entry_price * 100 * dir_sign

        accel_pct = slope1 - slope2

        bin_label, bin_from, bin_to = _bin_st_accel(accel_pct)
        key: BinKey = (direction, bin_label)
        if key not in bins:
            bins[key] = _init_bin_record(direction, bin_label, bin_from, bin_to)
        _update_bin_record(bins[key], pnl_abs, win)

    return bins


# 🔸 Биннеры для каждой фичи


def _bin_st_align_mtf(value: float) -> Tuple[str, float, float]:
    # условия биннинга MTF-конфлюэнса
    if value <= -0.5:
        return "ST_MTF_AllAgainst", -1.0, -0.5
    if value < 0.0:
        return "ST_MTF_MostlyAgainst", -0.5, 0.0
    if value < 0.5:
        return "ST_MTF_MostlyWith", 0.0, 0.5
    return "ST_MTF_AllWith", 0.5, 1.0


def _bin_st_cushion(cushion_units: float, dist_pct_signed: float) -> Tuple[str, Optional[float], Optional[float]]:
    # биннинг по запасу до ST в стопах
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
    # биннинг возраста тренда
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