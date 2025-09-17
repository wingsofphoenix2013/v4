# packs/lr_pack.py — on-demand построитель пакета LR (12 корзин, bucket_delta, angle_trend ±5%)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("LR_PACK")

ANGLE_FLAT_REL = 0.05
ANGLE_ABS_EPS  = 1e-6
IND_KV_PREFIX  = "ind"
TS_IND_PREFIX  = "ts_ind"
BB_TS_PREFIX   = "bb:ts"
MARK_PRICE     = "bb:price:{symbol}"

def compute_bucket_12(price: float, lower: float, upper: float) -> int | None:
    width = upper - lower
    if width <= 0: return None
    seg = width / 8.0
    top2 = upper + 2 * seg
    if price >= top2: return 0
    if price >= upper: return 1
    if price >= lower:
        k = int((upper - price) // seg)
        if k < 0: k = 0
        if k > 7: k = 7
        return 2 + k
    if price >= lower - seg: return 10
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

def classify_angle_trend(angle_live: float, angle_closed: float) -> tuple[str,float]:
    delta = angle_live - angle_closed
    denom = abs(angle_closed)
    if denom < ANGLE_ABS_EPS:
        if abs(delta) <= ANGLE_ABS_EPS: return "flat", delta
        return ("up" if delta > 0 else "down"), delta
    rel = abs(delta)/denom
    if rel <= ANGLE_FLAT_REL: return "flat", delta
    return ("up" if delta > 0 else "down"), delta

async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    mp = await redis.get(MARK_PRICE.format(symbol=symbol))
    if mp:
        try: return float(mp)
        except: pass
    try:
        res = await redis.execute_command("TS.GET", f"{BB_TS_PREFIX}:{symbol}:{tf}:c")
        if res and len(res)==2: return float(res[1])
    except: pass
    return None

async def fetch_closed_close(redis, symbol: str, tf: str, closed_ms: int) -> float | None:
    try:
        res = await redis.execute_command("TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c", closed_ms, closed_ms)
        if res: return float(res[0][1])
    except: pass
    return None

async def fetch_closed_lr(redis, symbol: str, tf: str, base: str) -> dict:
    out = {}
    try:
        up = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_upper")
        lo = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_lower")
        ct = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_center")
        ang = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_angle")
        if up: out["upper"]=float(up)
        if lo: out["lower"]=float(lo)
        if ct: out["center"]=float(ct)
        if ang: out["angle"]=float(ang)
    except: pass
    return out

async def build_lr_pack(symbol: str, tf: str, length: int, now_ms: int,
                        precision: int, redis, compute_fn) -> dict | None:
    bar_open_ms = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[LR_PACK] {symbol}/{tf} lr{length}: no ohlcv")
        return None

    inst = {"indicator":"lr","params":{"length":str(length)},"timeframe":tf}
    base = f"lr{length}"

    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[LR_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    try:
        up_live = float(values.get(f"{base}_upper"))
        lo_live = float(values.get(f"{base}_lower"))
        ct_live = float(values.get(f"{base}_center"))
        ang_live= float(values.get(f"{base}_angle"))
    except Exception:
        log.warning(f"[LR_PACK] {symbol}/{tf} {base}: missing values")
        return None

    live_price = await fetch_mark_or_last_close(redis, symbol, tf)
    bucket_live = compute_bucket_12(live_price, lo_live, up_live) if live_price is not None else None

    closed = await fetch_closed_lr(redis, symbol, tf, base)
    bucket_closed = None
    if "upper" in closed and "lower" in closed:
        close_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms)
        if close_closed is not None:
            bucket_closed = compute_bucket_12(close_closed, closed["lower"], closed["upper"])

    bucket_delta = "unknown"
    if bucket_live is not None and bucket_closed is not None:
        bucket_delta = classify_bucket_delta(bucket_live - bucket_closed)

    angle_trend="unknown"; angle_delta=None
    if "angle" in closed:
        angle_trend, angle_delta = classify_angle_trend(ang_live, closed["angle"])

    pack = {
        "base": base,
        "pack": {
            "bucket": bucket_live,
            "bucket_delta": bucket_delta,
            "angle": f"{ang_live:.5f}",
            "angle_delta": f"{angle_delta:.5f}" if angle_delta is not None else None,
            "angle_trend": angle_trend,
            "price": f"{live_price:.8f}" if live_price is not None else None,
            "lower": f"{lo_live:.8f}",
            "upper": f"{up_live:.8f}",
            "center": f"{ct_live:.8f}",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack