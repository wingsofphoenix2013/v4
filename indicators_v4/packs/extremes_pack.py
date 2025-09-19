# packs/extremes_pack.py — on-demand построитель пакета EXTREMES (live на текущем баре)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

# 🔸 Общие правила MarketWatch (Extremes)
from indicator_mw_shared import (
    load_prev_state,
    ext_thresholds,
    apply_ext_hysteresis_and_dwell,
)

# 🔸 Логгер
log = logging.getLogger("EXTREMES_PACK")

# 🔸 Пороги (синхронизированы с MW_EXT)
RSI_OVERBOUGHT = {"m5": 70.0, "m15": 70.0, "h1": 70.0}
RSI_OVERSOLD   = {"m5": 30.0, "m15": 30.0, "h1": 30.0}
MFI_OVERBOUGHT = {"m5": 80.0, "m15": 80.0, "h1": 80.0}
MFI_OVERSOLD   = {"m5": 20.0, "m15": 20.0, "h1": 20.0}

LR_UP_ANGLE_EPS   = {"m5": 1e-4, "m15": 8e-4, "h1": 2e-3}
LR_DOWN_ANGLE_EPS = {"m5": -1e-4, "m15": -8e-4, "h1": -2e-3}

BB_TS_PREFIX  = "bb:ts"     # bb:ts:{symbol}:{tf}:c
TS_IND_PREFIX = "ts_ind"    # ts_ind:{symbol}:{tf}:{param}

# 🔸 Утилиты для TS
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 BB 12-корзин (как в bb_pack/mw_extremes)
def bb_bucket_12(price: float, lower: float, upper: float) -> int | None:
    width = upper - lower
    if width <= 0:
        return None
    seg = width / 8.0
    top2 = upper + 2 * seg
    if price >= top2: return 0
    if price >= upper: return 1
    if price >= lower:
        k = int((upper - price) // seg)
        if k < 0: k = 0
        if k > 7: k = 7
        return 2 + k
    if price >= (lower - seg): return 10
    return 11

def r2(x): return None if x is None else round(float(x), 2)
def r5(x): return None if x is None else round(float(x), 5)

# 🔸 Построить live EXTREMES-пакет (единые пороги + hysteresis/dwell)
async def build_extremes_pack(symbol: str, tf: str, now_ms: int,
                              precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "extremes", "pack": {...}} либо None.
    """
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[EXTREMES_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # RSI/MFI live
    def get_val(ind, L):
        inst = {"indicator": ind, "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if vals:
            try: return float(vals.get(f"{ind}{L}"))
            except Exception: return None
        return None

    rsi14 = await get_val("rsi",14); rsi21 = await get_val("rsi",21)
    mfi14 = await get_val("mfi",14); mfi21 = await get_val("mfi",21)

    # BB live (upper/lower) и prev из TS
    base_bb = "bb20_2_0"
    bb_up = bb_lo = None
    vals_bb = await compute_fn({"indicator":"bb","params":{"length":"20","std":"2.0"},"timeframe":tf}, symbol, df, precision)
    if vals_bb:
        try:
            bb_up = float(vals_bb.get(f"{base_bb}_upper"))
            bb_lo = float(vals_bb.get(f"{base_bb}_lower"))
        except Exception:
            pass

    async def ts_get_at(key, ts_ms):
        try:
            res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
            if res: return float(res[0][1])
        except Exception:
            pass
        return None

    price_cur  = await ts_get_at(f"{BB_TS_PREFIX}:{symbol}:{tf}:c", bar_open_ms)
    price_prev = await ts_get_at(f"{BB_TS_PREFIX}:{symbol}:{tf}:c", prev_ms)
    bb_up_prev = await ts_get_at(f"{TS_IND_PREFIX}:{symbol}:{tf}:{base_bb}_upper", prev_ms)
    bb_lo_prev = await ts_get_at(f"{TS_IND_PREFIX}:{symbol}:{tf}:{base_bb}_lower", prev_ms)

    def bb_bucket_12(price, lower, upper):
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
        if price >= (lower - seg): return 10
        return 11

    bb_bucket_cur = bb_bucket_12(price_cur, bb_lo, bb_up) if None not in (price_cur, bb_lo, bb_up) else None
    bb_bucket_prev = bb_bucket_12(price_prev, bb_lo_prev, bb_up_prev) if None not in (price_prev, bb_lo_prev, bb_up_prev) else None
    bb_bucket_delta = (bb_bucket_cur - bb_bucket_prev) if (bb_bucket_cur is not None and bb_bucket_prev is not None) else None

    # LR углы live + prev (конфликт → нейтральный тренд)
    def get_lr_angle(L):
        vals = await compute_fn({"indicator":"lr","params":{"length":str(L)},"timeframe":tf}, symbol, df, precision)
        if vals:
            try: return float(vals.get(f"lr{L}_angle"))
            except Exception: return None
        return None
    ang50 = await get_lr_angle(50); ang100 = await get_lr_angle(100)
    lr_conflict = (ang50 is not None and ang100 is not None and ((ang50 > 0 and ang100 < 0) or (ang50 < 0 and ang100 > 0)))
    uptrend   = False if lr_conflict else ((ang50 is not None and ang50 > 0) or (ang100 is not None and ang100 > 0))
    downtrend = False if lr_conflict else ((ang50 is not None and ang50 < 0) or (ang100 is not None and ang100 < 0))

    # флаги OB/OS — те же пороги, что у воркера
    ob = ((rsi14 is not None and rsi14 >= RSI_OVERBOUGHT[tf]) or
          (rsi21 is not None and rsi21 >= RSI_OVERBOUGHT[tf]) or
          (mfi14 is not None and mfi14 >= MFI_OVERBOUGHT[tf]) or
          (mfi21 is not None and mfi21 >= MFI_OVERBOUGHT[tf]))
    os = ((rsi14 is not None and rsi14 <= RSI_OVERSOLD[tf]) or
          (rsi21 is not None and rsi21 <= RSI_OVERSOLD[tf]) or
          (mfi14 is not None and mfi14 <= MFI_OVERSOLD[tf]) or
          (mfi21 is not None and mfi21 <= MFI_OVERSOLD[tf]))

    # raw state
    if ob and bb_bucket_cur is not None and bb_bucket_cur <= 3:
        raw_state = "overbought_extension"
    elif ob and (bb_bucket_delta is not None and bb_bucket_delta >= 1):
        raw_state = "overbought_extension"
    elif os and bb_bucket_cur is not None and bb_bucket_cur >= 8:
        raw_state = "oversold_extension"
    elif os and (bb_bucket_delta is not None and bb_bucket_delta <= -1):
        raw_state = "oversold_extension"
    elif uptrend and (bb_bucket_delta is not None and bb_bucket_delta <= -1):
        raw_state = "pullback_in_uptrend"
    elif downtrend and (bb_bucket_delta is not None and bb_bucket_delta >= +1):
        raw_state = "pullback_in_downtrend"
    else:
        raw_state = "none"

    # hysteresis + dwell
    prev_state, prev_streak = await load_prev_state(redis, kind="extremes", symbol=symbol, tf=tf)
    thr = ext_thresholds(tf)
    final_state, new_streak = apply_ext_hysteresis_and_dwell(
        prev_state=prev_state,
        raw_state=raw_state,
        features={"bb_delta": bb_bucket_delta},
        thr=thr,
        prev_streak=prev_streak,
    )

    pack = {
        "base": "extremes",
        "pack": {
            "state": final_state,
            "ref": "live",
            "open_time": bar_open_iso(bar_open_ms),
            "used_bases": ["rsi14","rsi21","mfi14","mfi21","bb20_2_0_upper","bb20_2_0_lower","lr50_angle","lr100_angle","close"],
            "rsi": {"rsi14": r2(rsi14), "rsi21": r2(rsi21)},
            "mfi": {"mfi14": r2(mfi14), "mfi21": r2(mfi21)},
            "bb":  {"bucket_cur": bb_bucket_cur, "bucket_prev": bb_bucket_prev, "bucket_delta": bb_bucket_delta,
                    "upper": r5(bb_up), "lower": r5(bb_lo)},
            "lr":  {"ang50": r5(ang50), "ang100": r5(ang100), "uptrend": bool(uptrend), "downtrend": bool(downtrend), "conflict": bool(lr_conflict)},
            "prev_state": prev_state,
            "raw_state": raw_state,
            "streak_preview": new_streak,
        },
    }
    return pack