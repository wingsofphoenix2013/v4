# packs/ema_pack.py — on-demand построитель пакета EMA (позиция цены vs EMA и динамики: strict + smooth)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("EMA_PACK")

# 🔸 Конфигурация порогов (в процентах)
EQ_EPS_PCT = 0.05  # зона "равно" для |d_t| (одинакова для всех TF)
MOVE_EPS_PCT = {   # антидребезг по изменению дистанции Δ (в процентах)
    "m5":  0.03,
    "m15": 0.05,
    "h1":  0.10,
}

# 🔸 Smooth: сколько закрытых баров учитывать в среднем |d|
SMOOTH_N = {"m5": 10, "m15": 6, "h1": 4}

# 🔸 KV/TS префиксы
IND_KV_PREFIX = "ind"     # ind:{symbol}:{tf}:{param_name}
TS_IND_PREFIX = "ts_ind"  # ts_ind:{symbol}:{tf}:{param_name}
BB_TS_PREFIX  = "bb:ts"   # bb:ts:{symbol}:{tf}:c
MARK_PRICE    = "bb:price:{symbol}"


# 🔸 Цена live: markPrice → фоллбэк последняя close
async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    mp = await redis.get(MARK_PRICE.format(symbol=symbol))
    if mp:
        try:
            return float(mp)
        except Exception:
            pass
    try:
        res = await redis.execute_command("TS.GET", f"{BB_TS_PREFIX}:{symbol}:{tf}:c")
        if res and len(res) == 2:
            return float(res[1])
    except Exception:
        pass
    return None

# 🔸 Закрытая цена на конкретном баре (по open_time в ms)
async def fetch_closed_close(redis, symbol: str, tf: str, closed_ms: int) -> float | None:
    try:
        res = await redis.execute_command("TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c", closed_ms, closed_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 Прочитать закрытую EMA из KV ind:{symbol}:{tf}:ema{length}
async def fetch_closed_ema(redis, symbol: str, tf: str, length: int) -> float | None:
    key = f"{IND_KV_PREFIX}:{symbol}:{tf}:ema{length}"
    try:
        s = await redis.get(key)
        return float(s) if s is not None else None
    except Exception:
        return None

# 🔸 История закрытых EMA и CLOSE за N баров → среднее |d| (в %)
async def fetch_mean_abs_d(redis, symbol: str, tf: str, length: int, last_closed_ms: int, n: int) -> float | None:
    """
    mean(|d|), d = (Close - EMA)/EMA * 100, по N закрытым барам [last-N+1 .. last]
    """
    if n <= 0:
        return None
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        ema_series = await redis.execute_command(
            "TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:ema{length}", start, last_closed_ms
        )
        close_series = await redis.execute_command(
            "TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c", start, last_closed_ms
        )
        if not ema_series or not close_series:
            return None
        ema_map = {int(ts): float(v) for ts, v in ema_series}
        close_map = {int(ts): float(v) for ts, v in close_series}
        xs = sorted(set(ema_map.keys()) & set(close_map.keys()))
        if not xs:
            return None
        vals = []
        for t in xs:
            ema_v = ema_map.get(t)
            c_v = close_map.get(t)
            if ema_v is None or c_v is None or ema_v == 0:
                continue
            d = (c_v - ema_v) / ema_v * 100.0
            vals.append(abs(d))
        if not vals:
            return None
        # усечём до последних N точек на всякий случай
        vals = vals[-n:]
        return sum(vals) / len(vals)
    except Exception:
        return None

# 🔸 Классификация динамики EMA (strict и smooth)
def classify_ema_dynamic(d_t: float, d_c: float, tf: str) -> tuple[str, str, float]:
    """
    d_t, d_c — нормированные дистанции в процентах:
      d = (Price - EMA)/EMA * 100
    Возвращает (side, dynamic_strict, delta_abs),
      side ∈ {"above","equal","below"},
      dynamic_strict ∈ {"equal","above_away","above_stable","above_approaching","below_away","below_stable","below_approaching"}
    """
    eq_eps = EQ_EPS_PCT
    move_eps = MOVE_EPS_PCT.get(tf, 0.05)

    # сторона относительно EMA на сейчас
    if abs(d_t) <= eq_eps:
        return "equal", "equal", 0.0

    side = "above" if d_t > 0 else "below"

    # изменение модуля дистанции
    delta_abs = abs(d_t) - abs(d_c)

    if abs(delta_abs) <= move_eps:
        dynamic = f"{side}_stable"
    elif delta_abs > 0:
        dynamic = f"{side}_away"
    else:
        dynamic = f"{side}_approaching"

    return side, dynamic, delta_abs


# 🔸 Построить пакет EMA для конкретного length
async def build_ema_pack(symbol: str, tf: str, length: int, now_ms: int,
                         precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "ema{L}", "pack": {...}} либо None.
    """
    # нормализуем время к началу текущего бара
    bar_open_ms = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    # загрузка OHLCV и live-расчёт EMA
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[EMA_PACK] {symbol}/{tf} ema{length}: no ohlcv")
        return None

    inst = {
        "indicator": "ema",
        "params": {"length": str(length)},
        "timeframe": tf,
    }
    base = f"ema{length}"

    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[EMA_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    sval = values.get(base)
    if sval is None:
        log.warning(f"[EMA_PACK] {symbol}/{tf} {base}: no value")
        return None

    try:
        ema_live = float(sval)
    except Exception:
        log.warning(f"[EMA_PACK] {symbol}/{tf} {base}: bad value {sval}")
        return None

    # текущая цена и дистанция d_t
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    if price_live is None or ema_live == 0:
        log.warning(f"[EMA_PACK] {symbol}/{tf} {base}: no live price or zero EMA")
        return None

    d_t = (price_live - ema_live) / ema_live * 100.0

    # референс (закрытый бар)
    ema_closed = await fetch_closed_ema(redis, symbol, tf, length)
    price_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms) if last_closed_ms is not None else None

    # если нет закрытого значения — прерываем
    if ema_closed is None or price_closed is None or ema_closed == 0:
        return None

    # strict
    if ema_closed is None or price_closed is None or ema_closed == 0:
        # нет референса — отдаём только side/eq
        side = "above" if d_t > EQ_EPS_PCT else ("below" if d_t < -EQ_EPS_PCT else "equal")
        dynamic_strict = "equal" if side == "equal" else f"{side}_stable"
        dynamic_smooth = dynamic_strict
        delta_abs = None
        delta_smooth = None
    else:
        d_c = (price_closed - ema_closed) / ema_closed * 100.0
        side, dynamic_strict, delta_abs = classify_ema_dynamic(d_t, d_c, tf)

        # smooth: сравниваем |d_t| с SMA_N(|d|) по последним закрытым барам
        n = SMOOTH_N.get(tf, 6)
        mean_abs_d = await fetch_mean_abs_d(redis, symbol, tf, length, last_closed_ms, n)
        if mean_abs_d is None:
            dynamic_smooth = dynamic_strict
            delta_smooth = None
        else:
            move_eps = MOVE_EPS_PCT.get(tf, 0.05)
            delta_smooth_val = abs(d_t) - mean_abs_d
            if abs(delta_smooth_val) <= move_eps:
                dynamic_smooth = f"{side}_stable"
            elif delta_smooth_val > 0:
                dynamic_smooth = f"{side}_away"
            else:
                dynamic_smooth = f"{side}_approaching"
            delta_smooth = delta_smooth_val

    pack = {
        "base": base,
        "pack": {
            "value": f"{ema_live:.{precision}f}",
            "price": f"{price_live:.{precision}f}",
            "dist_pct": f"{d_t:.2f}",
            "delta_dist_pct": (f"{delta_abs:.2f}" if delta_abs is not None else None),
            "side": side,                    # above / equal / below
            "dynamic": dynamic_strict,       # alias на strict для совместимости
            "dynamic_strict": dynamic_strict,
            "dynamic_smooth": dynamic_smooth,
            "delta_smooth_pct": (f"{delta_smooth:.2f}" if delta_smooth is not None else None),
            "ref": "closed" if (ema_closed is not None and price_closed is not None) else "closed_missing",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack