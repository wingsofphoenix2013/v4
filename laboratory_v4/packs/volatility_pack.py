# packs/volatility_pack.py — on-demand VOLATILITY (live: low_squeeze / normal / expanding / high) с общими правилами hysteresis+dwell

import logging

# 🔸 Общие правила MarketWatch (Volatility)
from laboratory_mw_shared import (
    load_prev_state,
    vol_thresholds,
    apply_vol_hysteresis_and_dwell,
)

from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("VOL_PACK")

# 🔸 Префиксы Redis (цена/TS индикаторов)
BB_TS_PREFIX  = "bb:ts"     # bb:ts:{symbol}:{tf}:c
TS_IND_PREFIX = "ts_ind"    # ts_ind:{symbol}:{tf}:{param}

# 🔸 Прочитать точку TS по exact open_time
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 Цена live: markPrice → фоллбэк close текущего бара
async def fetch_mark_or_last_close(redis, symbol: str, tf: str, bar_open_ms: int) -> float | None:
    mp = await redis.get(f"bb:price:{symbol}")
    if mp:
        try:
            return float(mp)
        except Exception:
            pass
    return await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", bar_open_ms)

# 🔸 Метрики (как в воркере)
def atr_pct(atr: float | None, close: float | None) -> float | None:
    if atr is None or close is None or close == 0:
        return None
    return (atr / close) * 100.0

def atr_bucket(atr_pct_val: float | None) -> int | None:
    if atr_pct_val is None:
        return None
    return int(atr_pct_val / 0.1) + 1  # шаг 0.1% → 1,2,3,...

def classify_bw_phase(tf: str, bw_cur: float | None, bw_prev: float | None) -> tuple[str, float | None]:
    if bw_cur is None or bw_prev is None or bw_prev == 0:
        return "unknown", None
    rel = (bw_cur - bw_prev) / bw_prev
    if rel > 0:
        return "expanding", rel
    if rel < 0:
        return "contracting", rel
    return "stable", 0.0

def r2(x): return None if x is None else round(float(x), 2)
def r6(x): return None if x is None else round(float(x), 6)


# 🔸 Построить live VOLATILITY-пакет (единые пороги + hysteresis/dwell)
async def build_volatility_pack(symbol: str, tf: str, now_ms: int,
                                precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "volatility", "pack": {...}} либо None.
    """
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV для live-расчётов (ATR/BB)
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[VOL_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live цена (markPrice → fallback close текущего бара)
    price_live = await fetch_mark_or_last_close(redis, symbol, tf, bar_open_ms)
    if price_live is None:
        log.warning(f"[VOL_PACK] {symbol}/{tf}: no live price")
        return None

    # ATR(14) live через compute_fn
    atr14 = None
    inst_atr = {"indicator": "atr", "params": {"length": "14"}, "timeframe": tf}
    vals_atr = await compute_fn(inst_atr, symbol, df, precision)
    if vals_atr:
        try:
            atr14 = float(vals_atr.get("atr14"))
        except Exception:
            pass

    atr_pct_cur = atr_pct(atr14, price_live)

    # BB20/2.0 live (upper/lower) через compute_fn
    bb_upper = bb_lower = None
    inst_bb = {"indicator": "bb", "params": {"length": "20", "std": "2.0"}, "timeframe": tf}
    vals_bb = await compute_fn(inst_bb, symbol, df, precision)
    if vals_bb:
        try:
            bb_upper = float(vals_bb.get("bb20_2_0_upper"))
            bb_lower = float(vals_bb.get("bb20_2_0_lower"))
        except Exception:
            pass

    bw_cur = None
    if bb_upper is not None and bb_lower is not None:
        bw_cur = bb_upper - bb_lower

    # prev из TS
    price_prev    = await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", prev_ms)
    atr_prev      = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:atr14", prev_ms)
    bb_upper_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper", prev_ms)
    bb_lower_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower", prev_ms)

    atr_pct_prev = atr_pct(atr_prev, price_prev)
    atr_b_cur    = atr_bucket(atr_pct_cur)
    atr_b_prev   = atr_bucket(atr_pct_prev)
    atr_b_delta  = None if (atr_b_cur is None or atr_b_prev is None) else (atr_b_cur - atr_b_prev)

    bw_prev = None
    if bb_upper_prev is not None and bb_lower_prev is not None:
        bw_prev = bb_upper_prev - bb_lower_prev

    bw_phase, bw_rel = classify_bw_phase(tf, bw_cur, bw_prev)

    # пороги и прошлое состояние
    thr = vol_thresholds(tf)
    prev_state, prev_streak = await load_prev_state(redis, kind="volatility", symbol=symbol, tf=tf)

    # raw-состояние (включающее сравнение для low)
    is_low  = (atr_pct_cur is not None and atr_pct_cur <= thr["atr_low"])
    is_high = (atr_pct_cur is not None and atr_pct_cur >  thr["atr_high"])

    if is_low and bw_phase == "contracting":
        raw_state = "low_squeeze"
    elif is_high:
        raw_state = "high"
    elif bw_rel is not None and bw_rel >= thr["bw_exp_in"]:
        raw_state = "expanding"
    else:
        raw_state = "normal"

    # hysteresis + dwell
    final_state, new_streak = apply_vol_hysteresis_and_dwell(
        prev_state=prev_state,
        raw_state=raw_state,
        features={"rel_diff": bw_rel, "atr_pct": atr_pct_cur},
        thr=thr,
        prev_streak=prev_streak,
    )

    # сборка пакета
    pack = {
        "base": "volatility",
        "pack": {
            "state": final_state,
            "ref": "live",
            "open_time": bar_open_iso(bar_open_ms),
            "used_bases": ["atr14", "bb20_2_0_upper", "bb20_2_0_lower", "close"],
            "atr_pct": r2(atr_pct_cur),
            "atr_bucket": atr_b_cur,
            "atr_bucket_delta": atr_b_delta,
            "bw": {
                "cur": r6(bw_cur),
                "prev": r6(bw_prev),
                "rel_diff": r6(bw_rel),
                "phase": bw_phase,
            },
            "flags": {
                "is_low": bool(is_low),
                "is_high": bool(is_high),
                "is_expanding": bw_phase == "expanding",
                "is_contracting": bw_phase == "contracting",
            },
            "prev_state": prev_state,
            "raw_state": raw_state,
            "streak_preview": new_streak,
        },
    }
    return pack