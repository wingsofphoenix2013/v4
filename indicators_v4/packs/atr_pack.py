# packs/atr_pack.py — on-demand построитель пакета ATR (нормализация в %, корзины 0.1%, bucket_delta)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("ATR_PACK")

# 🔸 KV-префиксы
IND_KV_PREFIX  = "ind"
BB_TS_PREFIX   = "bb:ts"
MARK_PRICE     = "bb:price:{symbol}"

# 🔸 Корзина ATR%: шаг 0.1% → bucket = int(ATR% / 0.1) + 1
def atr_bucket(atr_pct: float) -> int:
    return int(atr_pct / 0.1) + 1

# 🔸 Сравнение корзин: возвращает up_X / down_X / stable
def classify_bucket_delta(live_bucket: int | None, closed_bucket: int | None) -> str:
    if live_bucket is None or closed_bucket is None:
        return "unknown"
    d = live_bucket - closed_bucket
    if d == 0: return "stable"
    if d > 0:  return f"up_{d}"
    return f"down_{abs(d)}"

# 🔸 Добыть markPrice или close
async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    mp = await redis.get(MARK_PRICE.format(symbol=symbol))
    if mp:
        try: return float(mp)
        except Exception: pass
    try:
        res = await redis.execute_command("TS.GET", f"{BB_TS_PREFIX}:{symbol}:{tf}:c")
        if res and len(res) == 2:
            return float(res[1])
    except Exception:
        pass
    return None

# 🔸 Закрытый ATR из KV
async def fetch_closed_atr(redis, symbol: str, tf: str, length: int) -> float | None:
    key = f"{IND_KV_PREFIX}:{symbol}:{tf}:atr{length}"
    try:
        s = await redis.get(key)
        return float(s) if s is not None else None
    except Exception:
        return None

# 🔸 Построить пакет ATR
async def build_atr_pack(symbol: str, tf: str, length: int, now_ms: int,
                         precision: int, redis, compute_fn) -> dict | None:
    bar_open_ms   = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[ATR_PACK] {symbol}/{tf} atr{length}: no ohlcv")
        return None

    inst = {"indicator": "atr", "params": {"length": str(length)}, "timeframe": tf}
    base = f"atr{length}"

    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[ATR_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    sval = values.get(base)
    if sval is None:
        log.warning(f"[ATR_PACK] {symbol}/{tf} {base}: no value")
        return None

    try:
        v_live = float(sval)
    except Exception:
        log.warning(f"[ATR_PACK] {symbol}/{tf} {base}: bad value {sval}")
        return None

    # нормализация в %
    live_price = await fetch_mark_or_last_close(redis, symbol, tf)
    atr_pct = (v_live / live_price * 100) if live_price and live_price > 0 else None
    bucket_live = atr_bucket(atr_pct) if atr_pct is not None else None

    # закрытый ATR% → корзина
    v_closed = await fetch_closed_atr(redis, symbol, tf, length)
    closed_bucket = None
    if v_closed and live_price:
        closed_pct = v_closed / live_price * 100
        closed_bucket = atr_bucket(closed_pct)

    bucket_delta = classify_bucket_delta(bucket_live, closed_bucket)

    pack = {
        "base": base,
        "pack": {
            "value": f"{v_live:.{precision}f}",
            "value_pct": f"{atr_pct:.2f}" if atr_pct is not None else None,
            "bucket": bucket_live,
            "bucket_delta": bucket_delta,
            "ref": "closed",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack