# bt_complex_ema_position.py — комплексный анализатор: поведение цены относительно EMA в окне истории

import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Логгер модуля
log = logging.getLogger("BT_COMPLEX_EMA_POSITION")


# 🔸 Публичная точка входа комплекса EMA/position
async def run_complex_ema_position(
    complex_cfg: Dict[str, Any],
    complex_ctx: Dict[str, Any],
    pg,
    redis,  # оставляем для совместимости сигнатур, здесь не используется
) -> Dict[str, Any]:
    complex_id = complex_cfg.get("id")
    family_key = str(complex_cfg.get("family_key") or "").strip()
    complex_key = str(complex_cfg.get("key") or "").strip()
    name = complex_cfg.get("name")

    params = complex_cfg.get("params") or {}
    scenario_id = complex_ctx.get("scenario_id")
    signal_id = complex_ctx.get("signal_id")

    # 🔸 Базовые параметры комплекса
    tf = _get_str_param(params, "tf", default="m5")  # m5 / m15 / h1
    ema_instance_id = _get_int_param(params, "ema_instance_id", default=0)
    window_bars = _get_int_param(params, "window_bars", default=30)

    # eps в процентах (0.01 = 0.01%), переводим в долю
    eps_pct = _get_decimal_param(params, "eps_pct", default=Decimal("0.01"))
    eps = (eps_pct / Decimal("100")).copy_abs()

    # порог тренда по дистанции (в долях, не в процентах), по умолчанию 0.2 = 20%
    trend_threshold = _get_decimal_param(
        params,
        "trend_threshold",
        default=Decimal("0.2"),
    ).copy_abs()

    if ema_instance_id <= 0:
        log.error(
            "BT_COMPLEX_EMA_POSITION: некорректный ema_instance_id=%s для комплекса id=%s (family=%s, key=%s, name=%s)",
            ema_instance_id,
            complex_id,
            family_key,
            complex_key,
            name,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
            },
        }

    log.debug(
        "BT_COMPLEX_EMA_POSITION: старт комплекса id=%s (family=%s, key=%s, name=%s) "
        "для scenario_id=%s, signal_id=%s, tf=%s, ema_instance_id=%s, window_bars=%s, "
        "eps_pct=%s, trend_threshold=%s",
        complex_id,
        family_key,
        complex_key,
        name,
        scenario_id,
        signal_id,
        tf,
        ema_instance_id,
        window_bars,
        eps_pct,
        trend_threshold,
    )

    # 🔸 Загружаем позиции сценария/сигнала с good_state=true
    positions = await _load_good_positions_for_pair(pg, scenario_id, signal_id)
    if not positions:
        log.info(
            "BT_COMPLEX_EMA_POSITION: нет good-позиций для комплекса id=%s, scenario_id=%s, signal_id=%s",
            complex_id,
            scenario_id,
            signal_id,
        )
        return {
            "rows": [],
            "summary": {
                "positions_total": 0,
                "positions_used": 0,
                "positions_skipped": 0,
            },
        }

    rows: List[Dict[str, Any]] = []
    positions_total = 0
    positions_used = 0
    positions_skipped = 0

    for p in positions:
        positions_total += 1

        position_uid = p["position_uid"]
        symbol = p["symbol"]
        direction = p["direction"]
        entry_time = p["entry_time"]
        entry_price = p["entry_price"]
        pnl_abs = p["pnl_abs"]

        try:
            classification = await _classify_position_ema_state(
                pg=pg,
                symbol=symbol,
                tf=tf,
                ema_instance_id=ema_instance_id,
                window_bars=window_bars,
                eps=eps,
                trend_threshold=trend_threshold,
                entry_time=entry_time,
                entry_price=entry_price,
            )
        except Exception as e:
            log.error(
                "BT_COMPLEX_EMA_POSITION: ошибка классификации позиции position_uid=%s "
                "(symbol=%s, tf=%s, ema_instance_id=%s, window_bars=%s): %s",
                position_uid,
                symbol,
                tf,
                ema_instance_id,
                window_bars,
                e,
                exc_info=True,
            )
            positions_skipped += 1
            continue

        if classification is None:
            # недостаточно истории или некорректные данные — пропускаем позицию
            positions_skipped += 1
            continue

        bin_name, signed_dist = classification

        rows.append(
            {
                "position_uid": position_uid,
                "timeframe": tf,
                "direction": direction,
                "bin_name": bin_name,
                "value": signed_dist,  # нормированное (entry_price - ema)/entry_price на последнем закрытом баре
                "pnl_abs": pnl_abs,
            }
        )
        positions_used += 1

    log.info(
        "BT_COMPLEX_EMA_POSITION: комплекс id=%s (family=%s, key=%s, name=%s), "
        "scenario_id=%s, signal_id=%s — позиций всего=%s, использовано=%s, пропущено=%s, строк_в_результате=%s",
        complex_id,
        family_key,
        complex_key,
        name,
        scenario_id,
        signal_id,
        positions_total,
        positions_used,
        positions_skipped,
        len(rows),
    )

    return {
        "rows": rows,
        "summary": {
            "positions_total": positions_total,
            "positions_used": positions_used,
            "positions_skipped": positions_skipped,
        },
    }


# 🔸 Загрузка позиций сценария/сигнала с good_state=true
async def _load_good_positions_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                p.position_uid,
                p.symbol,
                p.direction,
                p.entry_time,
                p.entry_price,
                p.pnl_abs
            FROM bt_analysis_positions_postproc ap
            JOIN bt_scenario_positions p
              ON p.position_uid = ap.position_uid
            WHERE ap.scenario_id = $1
              AND ap.signal_id   = $2
              AND ap.good_state  = true
              AND p.postproc     = true
            ORDER BY p.entry_time
            """,
            scenario_id,
            signal_id,
        )

    positions: List[Dict[str, Any]] = []
    for r in rows:
        positions.append(
            {
                "position_uid": r["position_uid"],
                "symbol": r["symbol"],
                "direction": r["direction"],
                "entry_time": r["entry_time"],
                "entry_price": _safe_decimal(r["entry_price"]),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
            }
        )

    log.debug(
        "BT_COMPLEX_EMA_POSITION: загружено good-позиций для комплекса, scenario_id=%s, signal_id=%s: %s",
        scenario_id,
        signal_id,
        len(positions),
    )
    return positions


# 🔸 Классификация одной позиции: бин + динамика EMA
async def _classify_position_ema_state(
    pg,
    symbol: str,
    tf: str,
    ema_instance_id: int,
    window_bars: int,
    eps: Decimal,
    trend_threshold: Decimal,
    entry_time: datetime,
    entry_price: Decimal,
) -> Optional[Tuple[str, Decimal]]:
    # считаем open-время последнего ЗАКРЫТОГО бара по заданному TF
    last_bar_open = _get_last_closed_bar_open(entry_time, tf)

    # загружаем окно истории EMA и цены (по close)
    window = await _load_ema_window_for_position(
        pg=pg,
        symbol=symbol,
        tf=tf,
        ema_instance_id=ema_instance_id,
        last_bar_open=last_bar_open,
        window_bars=window_bars,
    )

    # по договорённости: если истории меньше N баров — позицию не используем
    if len(window) < window_bars:
        log.debug(
            "BT_COMPLEX_EMA_POSITION: недостаточно истории EMA для symbol=%s, tf=%s, ema_instance_id=%s, "
            "entry_time=%s (есть баров=%s, нужно=%s)",
            symbol,
            tf,
            ema_instance_id,
            entry_time,
            len(window),
            window_bars,
        )
        return None

    # расстояния и знаки price - ema (по close для всей истории)
    dists: List[Decimal] = []
    signs: List[int] = []

    for item in window:
        price_close = item["price"]
        ema = item["ema"]

        if price_close <= 0:
            return None

        diff = _ema_safe_sub(price_close, ema)
        dist = (diff.copy_abs() / price_close).quantize(Decimal("0.0000001"), rounding=ROUND_HALF_UP)
        dists.append(dist)

        if diff > 0:
            signs.append(1)
        elif diff < 0:
            signs.append(-1)
        else:
            # если diff == 0, используем предыдущий знак (если есть), иначе 0
            signs.append(signs[-1] if signs else 0)

    if not dists:
        return None

    # комментарий: статическое положение считаем по entry_price относительно EMA последнего закрытого бара
    last_ema = window[-1]["ema"]
    price_entry = entry_price

    if price_entry <= 0:
        return None

    last_diff_entry = _ema_safe_sub(price_entry, last_ema)
    last_dist_entry = (last_diff_entry.copy_abs() / price_entry).quantize(
        Decimal("0.0000001"),
        rounding=ROUND_HALF_UP,
    )

    # статическое положение цены относительно EMA: around / above / below
    if last_dist_entry <= eps:
        position_zone = "around"
    elif last_diff_entry > 0:
        position_zone = "above"
    else:
        position_zone = "below"

    # считаем динамику дистанции по истории (по close): сравниваем начало и конец окна
    head_size = min(5, len(dists))
    tail_size = min(5, len(dists))

    d_first = sum(dists[:head_size]) / Decimal(head_size)
    d_last = sum(dists[-tail_size:]) / Decimal(tail_size)

    base = d_first.copy_abs()
    if base <= Decimal("0"):
        base = Decimal("0.0000001")

    delta = (d_last - d_first) / base

    # классификация тренда по delta
    if delta <= -trend_threshold:
        trend_state = "approach"
    elif delta >= trend_threshold:
        trend_state = "away"
        # значение delta > 0: дистанция растёт
    else:
        trend_state = "flat"

    # считаем "пилу" вокруг EMA: количество смен знака price - ema
    sign_changes = _count_sign_changes(signs)

    # если around — делим на around_flat / around_choppy
    if position_zone == "around":
        # простое правило: если смен знака много — choppy, иначе flat
        choppy_threshold = 3
        if sign_changes >= choppy_threshold:
            bin_name = "around_choppy"
        else:
            bin_name = "around_flat"
    else:
        # not around: below_* или above_*
        if position_zone == "below":
            prefix = "below"
        else:
            prefix = "above"

        if trend_state == "approach":
            bin_name = f"{prefix}_approach"
        elif trend_state == "away":
            bin_name = f"{prefix}_away"
        else:
            bin_name = f"{prefix}_flat"

    # value — знак (entry_price - ema)/entry_price на последнем закрытом баре
    signed_dist = (last_diff_entry / price_entry).quantize(
        Decimal("0.0000001"),
        rounding=ROUND_HALF_UP,
    )

    return bin_name, signed_dist


# 🔸 Расчёт open-времени последнего закрытого бара для TF
def _get_last_closed_bar_open(entry_time: datetime, tf: str) -> datetime:
    # карта длительности бара в минутах
    tf_minutes_map = {
        "m5": 5,
        "m15": 15,
        "h1": 60,
    }
    minutes = tf_minutes_map.get(tf)
    if not minutes:
        raise ValueError(f"Unsupported timeframe for EMA complex: {tf}")

    # open текущего бара, в котором находится entry_time
    delta_to_floor = timedelta(
        minutes=entry_time.minute % minutes,
        seconds=entry_time.second,
        microseconds=entry_time.microsecond,
    )
    current_bar_open = entry_time - delta_to_floor

    # последний закрытый бар — предыдущий относительно current_bar_open
    last_bar_open = current_bar_open - timedelta(minutes=minutes)
    return last_bar_open


# 🔸 Загрузка окна истории EMA и цены для позиции
async def _load_ema_window_for_position(
    pg,
    symbol: str,
    tf: str,
    ema_instance_id: int,
    last_bar_open: datetime,
    window_bars: int,
) -> List[Dict[str, Any]]:
    # выбор таблицы OHLCV по TF
    if tf == "m5":
        ohlcv_table = "ohlcv_bb_m5"
    elif tf == "m15":
        ohlcv_table = "ohlcv_bb_m15"
    elif tf == "h1":
        ohlcv_table = "ohlcv_bb_h1"
    else:
        raise ValueError(f"Unsupported timeframe for EMA complex: {tf}")

    query = f"""
        SELECT DISTINCT ON (iv.open_time)
            iv.open_time AS bar_time,
            c.close      AS price,
            iv.value     AS ema
        FROM indicator_values_v4 iv
        JOIN {ohlcv_table} c
          ON c.symbol    = iv.symbol
         AND c.open_time = iv.open_time
        WHERE iv.symbol      = $1
          AND iv.instance_id = $2
          AND iv.open_time  <= $3
        ORDER BY iv.open_time DESC
        LIMIT $4
    """

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            query,
            symbol,
            ema_instance_id,
            last_bar_open,
            window_bars,
        )

    window: List[Dict[str, Any]] = []
    # переворачиваем в хронологический порядок (от старых к новым)
    for r in reversed(rows):
        price = _safe_decimal(r["price"])
        ema = _safe_decimal(r["ema"])
        window.append(
            {
                "bar_time": r["bar_time"],
                "price": price,
                "ema": ema,
            }
        )

    log.debug(
        "BT_COMPLEX_EMA_POSITION: загружено баров EMA окна для symbol=%s, tf=%s, ema_instance_id=%s: %s",
        symbol,
        tf,
        ema_instance_id,
        len(window),
    )
    return window


# 🔸 Подсчёт числа смен знака
def _count_sign_changes(signs: List[int]) -> int:
    if not signs:
        return 0

    last = signs[0]
    changes = 0

    for s in signs[1:]:
        if s == 0:
            continue
        if last == 0:
            last = s
            continue
        if s != last:
            changes += 1
            last = s

    return changes


# 🔸 Вспомогательная функция: безопасное чтение str-параметра
def _get_str_param(params: Dict[str, Any], name: str, default: str) -> str:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    return str(raw).strip()


# 🔸 Вспомогательная функция: безопасное чтение int-параметра
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return int(str(raw))
    except (ValueError, TypeError):
        return default


# 🔸 Вспомогательная функция: безопасное чтение Decimal-параметра
def _get_decimal_param(params: Dict[str, Any], name: str, default: Decimal) -> Decimal:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    if raw is None:
        return default

    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return default


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: разность для Decimal
def _ema_safe_sub(a: Decimal, b: Decimal) -> Decimal:
    try:
        return a - b
    except Exception:
        return Decimal("0")