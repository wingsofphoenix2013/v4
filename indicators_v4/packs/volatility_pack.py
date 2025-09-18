# packs/volatility_pack.py — on-demand построитель пакета VOLATILITY (live на текущем баре: low_squeeze / normal / expanding / high)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

# 🔸 Логгер
log = logging.getLogger("VOL_PACK")

# 🔸 Константы и пороги (синхронизированы с indicator_mw_volatility.py)
ATR_LOW_PCT   = {"m5": 0.30, "m15": 0.40, "h1": 0.60}
ATR_HIGH_PCT  = {"m5": 0.80, "m15": 1.00, "h1": 1.50}

BW_EXPAND_EPS = {"m5": 0.04, "m15": 0.03, "h1": 0.02}
BW_CONTR_EPS  = {"m5": -0.04, "m15": -0.03, "h1": -0.02}

# 🔸 Префиксы Redis (цена/TS индикаторов)
BB_TS_PREFIX  = "bb:ts"            # bb:ts:{symbol}:{tf}:c
TS_IND_PREFIX = "ts_ind"           # ts_ind:{symbol}:{tf}:{param}
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

# 🔸 Прочитать из TS точку по exact open_time
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 Метрики
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
    if rel >= BW_EXPAND_EPS.get(tf, 0.03):
        return "expanding", rel
    if rel <= BW_CONTR_EPS.get(tf, -0.03):
        return "contracting", rel
    return "stable", rel


# 🔸 Построить live VOLATILITY-пакет (в стиле MW_VOL)
async def build_volatility_pack(symbol: str, tf: str, now_ms: int,
                                precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "volatility", "pack": {...}} либо None.
    """
    # нормализуем время
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV для live-расчётов (для ATR через compute_fn)
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[VOL_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live / prev close (для нормализации ATR%)
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    if price_live is None:
        log.warning(f"[VOL_PACK] {symbol}/{tf}: no live price")
        return None
    price_prev = await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", prev_ms)

    # ATR(14) live
    atr14 = None
    inst_atr = {"indicator": "atr", "params": {"length": "14"}, "timeframe": tf}
    vals_atr = await compute_fn(inst_atr, symbol, df, precision)
    if vals_atr:
        try:
            atr14 = float(vals_atr.get("atr14"))
        except Exception:
            pass

    atr_pct_cur  = atr_pct(atr14, price_live)
    atr_pct_prev = None
    if price_prev is not None:
        # prev ATR из TS (закрытое значение)
        atr_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:atr14", prev_ms)
        atr_pct_prev = atr_pct(atr_prev, price_prev)

    atr_b_cur   = atr_bucket(atr_pct_cur)
    atr_b_prev  = atr_bucket(atr_pct_prev)
    atr_b_delta = None if (atr_b_cur is None or atr_b_prev is None) else (atr_b_cur - atr_b_prev)

    # BB20/2.0 ширина: live из compute_fn (upper/lower), prev из TS
    bb_upper = bb_lower = None
    inst_bb = {"indicator": "bb", "params": {"length": "20", "std": "2.0"}, "timeframe": tf}
    vals_bb = await compute_fn(inst_bb, symbol, df, precision)
    if vals_bb:
        try:
            base = "bb20_2_0"
            bb_upper = float(vals_bb.get(f"{base}_upper"))
            bb_lower = float(vals_bb.get(f"{base}_lower"))
        except Exception:
            pass

    bw_cur = None
    if bb_upper is not None and bb_lower is not None:
        bw_cur = bb_upper - bb_lower

    bb_upper_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper", prev_ms)
    bb_lower_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower", prev_ms)
    bw_prev = None
    if bb_upper_prev is not None and bb_lower_prev is not None:
        bw_prev = bb_upper_prev - bb_lower_prev

    bw_phase, bw_rel = classify_bw_phase(tf, bw_cur, bw_prev)

    # классификация состояния (приоритет)
    low_th  = ATR_LOW_PCT.get(tf, 0.30)
    high_th = ATR_HIGH_PCT.get(tf, 0.80)
    is_low  = (atr_pct_cur is not None and atr_pct_cur < low_th)
    is_high = (atr_pct_cur is not None and atr_pct_cur > high_th)

    if is_low and bw_phase == "contracting":
        state = "low_squeeze"
    elif is_high:
        state = "high"
    elif bw_phase == "expanding":
        state = "expanding"
    else:
        state = "normal"

    # детали (округлим как в воркере)
    def r2(x): return None if x is None else round(float(x), 2)
    def r6(x): return None if x is None else round(float(x), 6)

    pack = {
        "base": "volatility",
        "pack": {
            "state": state,
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
                "phase": bw_phase
            },
            "flags": {
                "is_low": bool(is_low),
                "is_high": bool(is_high),
                "is_expanding": bw_phase == "expanding",
                "is_contracting": bw_phase == "contracting",
            },
        },
    }
    return pack