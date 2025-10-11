# packs/macd_pack.py — on-demand построитель пакета MACD (режим/кроссы, гистограмма в %, корзины и тренды strict/smooth)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("MACD_PACK")

# 🔸 Пороги (в процентах от цены)
ZERO_EPS_PCT = {"m5": 0.03, "m15": 0.05, "h1": 0.10}   # near_zero зона для MACD относительно 0
HIST_MOVE_EPS_PCT = {"m5": 0.02, "m15": 0.03, "h1": 0.05}  # антидребезг тренда гистограммы (в п.п.)
SMOOTH_N = {"m5": 10, "m15": 6, "h1": 4}  # сглаживание по N закрытым барам для hist_pct

# 🔸 KV/TS префиксы
IND_KV_PREFIX = "ind"     # ind:{symbol}:{tf}:{base}_{macd|macd_signal|macd_hist}
TS_IND_PREFIX = "ts_ind"  # ts_ind:{symbol}:{tf}:{base}_macd_hist
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

# 🔸 Прочитать закрытые MACD/Signal/Hist из KV ind:*
async def fetch_closed_macd(redis, symbol: str, tf: str, base: str):
    try:
        macd_s   = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_macd")
        signal_s = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_macd_signal")
        hist_s   = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_macd_hist")
        if macd_s is None or signal_s is None or hist_s is None:
            return None
        return float(macd_s), float(signal_s), float(hist_s)
    except Exception:
        return None

# 🔸 Среднее hist_pct за N закрытых баров
async def fetch_mean_hist_pct(redis, symbol: str, tf: str, base: str, last_closed_ms: int, n: int) -> float | None:
    if n <= 0:
        return None
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        hist_series  = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_macd_hist", start, last_closed_ms)
        close_series = await redis.execute_command("TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c",               start, last_closed_ms)
        if not hist_series or not close_series:
            return None
        h_map = {int(ts): float(v) for ts, v in hist_series}
        c_map = {int(ts): float(v) for ts, v in close_series}
        xs = sorted(set(h_map.keys()) & set(c_map.keys()))
        if not xs:
            return None
        vals = []
        for t in xs:
            price = c_map[t]
            if price is None or price == 0:
                continue
            vals.append((h_map[t] / price) * 100.0)
        if not vals:
            return None
        vals = vals[-n:]
        return sum(vals) / len(vals)
    except Exception:
        return None

# 🔸 Корзина гистограммы в % (нижняя граница, шаг 0.05%)
def hist_bucket_low_pct(hist_pct: float) -> float:
    # квантуем с симметрией вокруг нуля
    step = 0.05
    # для отрицательных корректно сработает floor в сторону -inf
    import math
    b = step * math.floor(hist_pct / step)
    # округляем до двух знаков для стабильного вывода
    return round(b, 2)

# 🔸 Классификация тренда гистограммы
def classify_hist_trend(delta_pp: float, tf: str) -> str:
    eps = HIST_MOVE_EPS_PCT.get(tf, 0.03)
    if abs(delta_pp) <= eps:
        return "stable"
    return "rising" if delta_pp > 0 else "falling"

# 🔸 Сторона относительно нуля для MACD
def classify_zero_side(macd_pct: float, tf: str) -> str:
    eps = ZERO_EPS_PCT.get(tf, 0.05)
    if abs(macd_pct) <= eps:
        return "near_zero"
    return "above_zero" if macd_pct > 0 else "below_zero"


# 🔸 Построить пакет MACD для (fast[, slow, signal]) — base = macd{fast}
async def build_macd_pack(symbol: str, tf: str, fast: int, now_ms: int,
                          precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "macd{fast}", "pack": {...}} либо None.
    Замечание: base определяется fast-периодом (как в твоей системе).
    """
    # нормализуем время к началу текущего бара
    bar_open_ms = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    # загрузка OHLCV и live-расчёт MACD
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[MACD_PACK] {symbol}/{tf} macd{fast}: no ohlcv")
        return None

    inst = {
        "indicator": "macd",
        "params": {"fast": str(fast)},  # slow/signal задаются в инстансе; base строится от fast
        "timeframe": tf,
    }
    base = f"macd{fast}"

    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[MACD_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    try:
        macd_live   = float(values.get(f"{base}_macd"))
        signal_live = float(values.get(f"{base}_macd_signal"))
        hist_live   = float(values.get(f"{base}_macd_hist"))
    except Exception:
        log.warning(f"[MACD_PACK] {symbol}/{tf} {base}: missing live params")
        return None

    # текущая цена
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    if price_live is None or price_live == 0:
        log.warning(f"[MACD_PACK] {symbol}/{tf} {base}: no live price")
        return None

    # нормализации (в % от цены)
    hist_pct_live   = (hist_live / price_live) * 100.0
    spread_pct_live = ((macd_live - signal_live) / price_live) * 100.0
    macd_zero_pct   = (macd_live / price_live) * 100.0

    # режим/кросс/сторона нуля
    mode = "bull" if (macd_live - signal_live) >= 0 else "bear"
    zero_side = classify_zero_side(macd_zero_pct, tf)

    # закрытые значения
    closed = await fetch_closed_macd(redis, symbol, tf, base)
    if closed is None:
        return None
    macd_c, signal_c, hist_c = closed
    spread_c = macd_c - signal_c

    # кросс (strict)
    prev_mode = "bull" if spread_c >= 0 else "bear"
    if mode == "bull" and prev_mode == "bear":
        cross = "bull_cross"
    elif mode == "bear" and prev_mode == "bull":
        cross = "bear_cross"
    else:
        cross = "none"

    # закрытая цена для нормализации hist
    price_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms)
    if price_closed is None or price_closed == 0:
        return None
    hist_pct_closed = (hist_c / price_closed) * 100.0

    # корзина гистограммы (нижняя граница, шаг 0.05%)
    bucket_low = hist_bucket_low_pct(hist_pct_live)

    # тренды гистограммы: strict и smooth
    delta_hist_pp = hist_pct_live - hist_pct_closed
    hist_trend_strict = classify_hist_trend(delta_hist_pp, tf)

    n = SMOOTH_N.get(tf, 6)
    hist_mean = await fetch_mean_hist_pct(redis, symbol, tf, base, last_closed_ms, n)
    if hist_mean is None:
        hist_trend_smooth = hist_trend_strict
        delta_smooth_pp = None
    else:
        delta_smooth_pp = hist_pct_live - hist_mean
        hist_trend_smooth = classify_hist_trend(delta_smooth_pp, tf)

    # сборка пакета
    pack = {
        "base": base,
        "pack": {
            # режим/кроссы/ноль
            "mode": mode,                        # bull / bear (по спреду)
            "cross": cross,                      # bull_cross / bear_cross / none
            "zero_side": zero_side,              # above_zero / near_zero / below_zero

            # гистограмма (нормированная)
            "hist_pct": f"{hist_pct_live:.2f}",
            "hist_bucket_low_pct": f"{bucket_low:.2f}",
            "hist_trend_strict": hist_trend_strict,     # rising / falling / stable
            "hist_trend_smooth": hist_trend_smooth,     # rising / falling / stable
            "delta_hist_pct": f"{delta_hist_pp:.2f}",
            "delta_hist_smooth_pct": (f"{delta_smooth_pp:.2f}" if delta_smooth_pp is not None else None),

            # амплитуда сигнала (по желанию стратегии)
            "spread_pct": f"{spread_pct_live:.2f}",

            # линии (для прозрачности; можно убрать, если лишнее)
            "macd": f"{macd_live:.{precision}f}",
            "signal": f"{signal_live:.{precision}f}",

            "ref": "closed",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack