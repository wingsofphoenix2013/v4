# packs/momentum_pack.py — on-demand построитель пакета MOMENTUM (live на текущем баре: bull/bear_impulse, overbought/oversold, divergence_flat)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

# 🔸 Логгер
log = logging.getLogger("MOMENTUM_PACK")

# 🔸 Пороги (синхронизированы c MW_MOM)
RSI_OVERBOUGHT = {"m5": 70.0, "m15": 70.0, "h1": 70.0}
RSI_OVERSOLD   = {"m5": 30.0, "m15": 30.0, "h1": 30.0}
MFI_OVERBOUGHT = {"m5": 80.0, "m15": 80.0, "h1": 80.0}
MFI_OVERSOLD   = {"m5": 20.0, "m15": 20.0, "h1": 20.0}

MACD_ZERO_EPS_PCT = {"m5": 0.03, "m15": 0.05, "h1": 0.10}
HIST_MOVE_EPS_PCT = {"m5": 0.03, "m15": 0.04, "h1": 0.05}

# 🔸 Префиксы Redis (TS ключи)
BB_TS_PREFIX  = "bb:ts"     # bb:ts:{symbol}:{tf}:c
TS_IND_PREFIX = "ts_ind"    # ts_ind:{symbol}:{tf}:{param}

# 🔸 Вспомогательные
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

def hist_pct(v_hist: float | None, price: float | None) -> float | None:
    if v_hist is None or price is None or price == 0:
        return None
    return (v_hist / price) * 100.0

def dpp(cur, prev):
    if cur is None or prev is None:
        return None
    return cur - prev

def r2(x): return None if x is None else round(float(x), 2)

def is_overbought(tf: str, rsi: float | None, mfi: float | None) -> bool:
    return (rsi is not None and rsi >= RSI_OVERBOUGHT.get(tf, 70.0)) or \
           (mfi is not None and mfi >= MFI_OVERBOUGHT.get(tf, 80.0))

def is_oversold(tf: str, rsi: float | None, mfi: float | None) -> bool:
    return (rsi is not None and rsi <= RSI_OVERSOLD.get(tf, 30.0)) or \
           (mfi is not None and mfi <= MFI_OVERSOLD.get(tf, 20.0))


# 🔸 Построить live MOMENTUM-пакет
async def build_momentum_pack(symbol: str, tf: str, now_ms: int,
                              precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "momentum", "pack": {...}} либо None.
    """
    # нормализуем время бара
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV (для live-расчётов compute_fn)
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[MOMENTUM_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live/prev цена
    price_live = await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", bar_open_ms)
    price_prev = await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", prev_ms)
    if price_live is None:
        log.warning(f"[MOMENTUM_PACK] {symbol}/{tf}: no live close")
        return None

    # MACD live: 12/26/9 и 5/35/5
    m12_macd = m12_sig = m12_hist = None
    m5_macd  = m5_sig  = m5_hist  = None
    for fast in (12, 5):
        inst = {"indicator": "macd", "params": {"fast": str(fast)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals: 
            continue
        try:
            base = f"macd{fast}"
            macd   = float(vals.get(f"{base}_macd"))
            signal = float(vals.get(f"{base}_macd_signal"))
            hist   = float(vals.get(f"{base}_macd_hist"))
            if fast == 12:
                m12_macd, m12_sig, m12_hist = macd, signal, hist
            else:
                m5_macd, m5_sig, m5_hist = macd, signal, hist
        except Exception:
            pass

    # RSI/MFI live: 14/21
    rsi14 = rsi21 = mfi14 = mfi21 = None
    for (ind, L) in (("rsi",14),("rsi",21),("mfi",14),("mfi",21)):
        inst = {"indicator": ind, "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals:
            continue
        try:
            v = float(vals.get(f"{ind}{L}"))
            if ind == "rsi":
                if L == 14: rsi14 = v
                else:       rsi21 = v
            else:
                if L == 14: mfi14 = v
                else:       mfi21 = v
        except Exception:
            pass

    # prev из TS (для дельт)
    # hist prev
    m12_hist_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd_hist", prev_ms)
    m5_hist_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd_hist",  prev_ms)
    # rsi/mfi prev (минимум для 14-периодных)
    rsi14_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi14", prev_ms)
    mfi14_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi14", prev_ms)

    # нормировки
    m12_hist_pct_cur  = hist_pct(m12_hist, price_live)
    m12_hist_pct_prev = hist_pct(m12_hist_prev, price_prev)
    m5_hist_pct_cur   = hist_pct(m5_hist,  price_live)
    m5_hist_pct_prev  = hist_pct(m5_hist_prev,  price_prev)

    d_m12_hist_pp = dpp(m12_hist_pct_cur, m12_hist_pct_prev)
    d_m5_hist_pp  = dpp(m5_hist_pct_cur,  m5_hist_pct_prev)

    # MACD режимы (спред)
    m12_mode = None if (m12_macd is None or m12_sig is None) else ("bull" if (m12_macd - m12_sig) >= 0 else "bear")
    m5_mode  = None if (m5_macd  is None or m5_sig  is None) else ("bull" if (m5_macd  - m5_sig)  >= 0 else "bear")

    # near-zero по MACD12
    macd12_zero_pct = None if (m12_macd is None or price_live is None or price_live == 0) \
        else (m12_macd / price_live) * 100.0
    near_zero = (macd12_zero_pct is not None and abs(macd12_zero_pct) <= MACD_ZERO_EPS_PCT.get(tf, 0.05))

    # RSI/MFI дельты
    drsi14 = None if (rsi14 is None or rsi14_prev is None) else (rsi14 - rsi14_prev)
    dmfi14 = None if (mfi14 is None or mfi14_prev is None) else (mfi14 - mfi14_prev)

    # зоны
    overbought = is_overbought(tf, rsi14, mfi14) or is_overbought(tf, rsi21, mfi21)
    oversold   = is_oversold(tf,   rsi14, mfi14) or is_oversold(tf,   rsi21, mfi21)

    # импульсные признаки (антидребезг)
    hist_eps = HIST_MOVE_EPS_PCT.get(tf, 0.03)
    m12_up   = (d_m12_hist_pp is not None and d_m12_hist_pp >  hist_eps)
    m12_down = (d_m12_hist_pp is not None and d_m12_hist_pp < -hist_eps)
    m5_up    = (d_m5_hist_pp  is not None and d_m5_hist_pp  >  hist_eps)
    m5_down  = (d_m5_hist_pp  is not None and d_m5_hist_pp  < -hist_eps)

    # классификация (приоритет)
    if overbought:
        state = "overbought"
    elif oversold:
        state = "oversold"
    elif (m12_mode == "bull" and m5_mode == "bull" and (m12_up or m5_up) and (drsi14 is None or drsi14 >= 0) and (dmfi14 is None or dmfi14 >= 0)):
        state = "bull_impulse"
    elif (m12_mode == "bear" and m5_mode == "bear" and (m12_down or m5_down) and (drsi14 is None or drsi14 <= 0) and (dmfi14 is None or dmfi14 <= 0)):
        state = "bear_impulse"
    else:
        state = "divergence_flat"  # нет явного импульса

    # сборка пакета (диагностика совпадает с воркером)
    pack = {
        "base": "momentum",
        "pack": {
            "state": state,
            "ref": "live",
            "open_time": bar_open_iso(bar_open_ms),
            "used_bases": ["macd12","macd5","rsi14","rsi21","mfi14","mfi21","close"],
            "macd": {
                "mode12": m12_mode,
                "mode5":  m5_mode,
                "hist12_pct": r2(m12_hist_pct_cur),
                "hist12_delta_pp": r2(d_m12_hist_pp),
                "hist5_pct":  r2(m5_hist_pct_cur),
                "hist5_delta_pp":  r2(d_m5_hist_pp),
                "near_zero_pct": r2(macd12_zero_pct),
            },
            "rsi": {"rsi14": r2(rsi14), "drsi14": r2(drsi14), "rsi21": r2(rsi21)},
            "mfi": {"mfi14": r2(mfi14), "dmfi14": r2(dmfi14), "mfi21": r2(mfi21)},
            "flags": {
                "overbought": bool(overbought),
                "oversold":   bool(oversold),
                "m12_up": m12_up, "m12_down": m12_down,
                "m5_up":  m5_up,  "m5_down":  m5_down,
            },
        },
    }
    return pack