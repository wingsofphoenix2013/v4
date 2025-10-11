# packs/bb_pack.py — on-demand построитель пакета BB (12-корзинная позиция, bucket_delta, тренды ширины strict/smooth)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("BB_PACK")

# 🔸 Порог для относительной динамики ширины полосы (строгий/сглаженный)
BW_EPS_REL = {"m5": 0.05, "m15": 0.04, "h1": 0.03}
BW_SMOOTH_N = 5  # сглаживание по N закрытым барам

# 🔸 Префиксы ключей Redis (закрытые значения и TS истории)
IND_KV_PREFIX = "ind"     # ind:{symbol}:{tf}:{param_name}
TS_IND_PREFIX = "ts_ind"  # ts_ind:{symbol}:{tf}:{param_name}
BB_TS_PREFIX  = "bb:ts"   # bb:ts:{symbol}:{tf}:c
MARK_PRICE    = "bb:price:{symbol}"


# 🔸 Имя base для BB (ровно как в compute_and_store.get_expected_param_names)
def bb_base(length: int, std: float) -> str:
    std_raw = round(float(std), 2)
    std_str = str(std_raw).replace(".", "_")
    return f"bb{int(length)}_{std_str}"


# 🔸 Корзины: 12 сегментов сверху вниз (0..11)
def compute_bucket_12(price: float, lower: float, upper: float) -> int | None:
    width = upper - lower
    if width <= 0:
        return None
    seg = width / 8.0
    top2 = upper + 2 * seg
    if price >= top2:
        return 0
    if price >= upper:
        return 1
    if price >= lower:
        # внутри: 0..7 сверху вниз → 2..9
        k = int((upper - price) // seg)
        if k < 0: k = 0
        if k > 7: k = 7
        return 2 + k
    # под полосой
    bot1 = lower - seg
    if price >= bot1:
        return 10
    return 11


def classify_bucket_delta(d: int) -> str:
    if d == 0: return "no_change"
    if d == 1: return "up_1"
    if d == 2: return "up_2"
    if d >= 3: return "up_strong"
    if d == -1: return "down_1"
    if d == -2: return "down_2"
    if d <= -3: return "down_strong"
    return "no_change"


def classify_bw_trend(rel_diff: float, eps: float) -> str:
    if rel_diff >= eps:
        return "expanding"
    if rel_diff <= -eps:
        return "contracting"
    return "stable"


# 🔸 Цена live: markPrice → фоллбэк последняя close
async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    mp = await redis.get(MARK_PRICE.format(symbol=symbol))
    if mp is not None:
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


# 🔸 Закрытые BB границы (KV ind:*)
async def fetch_closed_bb(redis, symbol: str, tf: str, base: str) -> tuple[float | None, float | None]:
    try:
        up = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_upper")
        lo = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_lower")
        return (float(up) if up is not None else None,
                float(lo) if lo is not None else None)
    except Exception:
        return (None, None)


# 🔸 Средняя ширина за N закрытых баров (TS индикаторов)
async def fetch_smooth_bw(redis, symbol: str, tf: str, base: str, last_closed_ms: int, n: int) -> float | None:
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        up_series = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_upper", start, last_closed_ms)
        lo_series = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_lower", start, last_closed_ms)
        if not up_series or not lo_series:
            return None
        up_map = {int(ts): float(v) for ts, v in up_series}
        lo_map = {int(ts): float(v) for ts, v in lo_series}
        xs = sorted(set(up_map.keys()) & set(lo_map.keys()))
        if not xs:
            return None
        widths = [up_map[t] - lo_map[t] for t in xs]
        if not widths:
            return None
        return sum(widths) / len(widths)
    except Exception:
        return None


# 🔸 Построить пакет BB для (length, std)
async def build_bb_pack(symbol: str, tf: str, length: int, std: float, now_ms: int,
                        precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": <bb base>, "pack": {...}} либо None.
    """
    # нормализуем время к началу текущего бара
    bar_open_ms = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    # загрузка OHLCV и live-расчёт BB
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[BB_PACK] {symbol}/{tf} bb{length}/{std}: no ohlcv")
        return None

    inst = {
        "indicator": "bb",
        "params": {"length": str(length), "std": str(std)},
        "timeframe": tf,
    }
    base = bb_base(length, std)

    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[BB_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    try:
        up_live = float(values.get(f"{base}_upper"))
        lo_live = float(values.get(f"{base}_lower"))
    except Exception:
        log.warning(f"[BB_PACK] {symbol}/{tf} {base}: live upper/lower missing")
        return None

    # live bucket (если есть цена)
    live_price = await fetch_mark_or_last_close(redis, symbol, tf)
    bucket_live = compute_bucket_12(live_price, lo_live, up_live) if live_price is not None else None

    # closed bucket (рассчитаем «на лету»)
    up_closed = lo_closed = None
    bucket_closed = None
    up_closed, lo_closed = await fetch_closed_bb(redis, symbol, tf, base)
    if (up_closed is not None) and (lo_closed is not None) and (last_closed_ms is not None):
        close_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms)
        if close_closed is not None:
            bucket_closed = compute_bucket_12(close_closed, lo_closed, up_closed)

    # delta по корзине
    if (bucket_live is not None) and (bucket_closed is not None):
        d = bucket_live - bucket_closed
        bucket_delta = classify_bucket_delta(int(d))
    else:
        bucket_delta = "unknown"

    # тренды ширины
    width_live = up_live - lo_live
    eps_rel = BW_EPS_REL.get(tf, 0.04)

    # strict: live vs последняя закрытая ширина
    bw_trend_strict = "stable"
    if (up_closed is not None) and (lo_closed is not None):
        width_closed = up_closed - lo_closed
        if width_closed and width_closed > 0:
            rel = (width_live - width_closed) / width_closed
            bw_trend_strict = classify_bw_trend(rel, eps_rel)

    # smooth: live vs средняя за N закрытых
    bw_trend_smooth = "stable"
    if last_closed_ms is not None:
        bw_mean = await fetch_smooth_bw(redis, symbol, tf, base, last_closed_ms, BW_SMOOTH_N)
        if bw_mean and bw_mean > 0:
            rel2 = (width_live - bw_mean) / bw_mean
            bw_trend_smooth = classify_bw_trend(rel2, eps_rel)

    # сборка пакета
    pack = {
        "base": base,
        "pack": {
            "bucket": bucket_live if bucket_live is not None else None,
            "bucket_delta": bucket_delta,
            "bw_trend_strict": bw_trend_strict,
            "bw_trend_smooth": bw_trend_smooth,
            "price": f"{live_price:.8f}" if live_price is not None else None,
            "lower": f"{lo_live:.8f}",
            "upper": f"{up_live:.8f}",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack