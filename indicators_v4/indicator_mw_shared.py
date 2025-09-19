# indicator_mw_shared.py — общий слой правил для MarketWatch (гистерезис + dwell) и доступ к прошлому состоянию

import json
from datetime import datetime

# 🔸 KV-ключ для MarketWatch
def kv_key(kind: str, symbol: str, tf: str) -> str:
    return f"ind_mw:{symbol}:{tf}:{kind}"


# 🔸 Загрузить предыдущее состояние из KV (state, streak)
async def load_prev_state(redis, kind: str, symbol: str, tf: str) -> tuple[str | None, int]:
    """
    Читает KV ind_mw:{symbol}:{tf}:{kind}.
    Возвращает (prev_state | None, prev_streak:int).
    """
    try:
        key = kv_key(kind, symbol, tf)
        raw = await redis.get(key)
        if not raw:
            return None, 0
        data = json.loads(raw)
        prev_state = data.get("state")
        details = data.get("details") or {}
        prev_streak = int(details.get("streak") or 0)
        return prev_state, prev_streak
    except Exception:
        return None, 0


# 🔸 -------------------- Trend: thresholds + hysteresis/dwell --------------------

TREND_ADX_SIDEWAYS_IN  = 12.0   # вход во флет:   max(ADX) <  IN
TREND_ADX_SIDEWAYS_OUT = 14.0   # выход из флэта: max(ADX) ≥ OUT

TREND_MIN_STREAK = {"m5": 2, "m15": 1, "h1": 1}

def trend_thresholds(tf: str) -> dict:
    return {
        "adx_in": TREND_ADX_SIDEWAYS_IN,
        "adx_out": TREND_ADX_SIDEWAYS_OUT,
        "min_streak": TREND_MIN_STREAK.get(tf, 2),
    }

def apply_trend_hysteresis_and_dwell(
    prev_state: str | None,
    raw_state: str,
    features: dict,     # {"max_adx": float}
    thresholds: dict,   # из trend_thresholds(tf)
    prev_streak: int,
) -> tuple[str, int]:
    max_adx = features.get("max_adx")
    adx_in  = thresholds["adx_in"]
    adx_out = thresholds["adx_out"]
    min_streak = thresholds["min_streak"]

    if prev_state is None:
        return raw_state, 1

    prev_is_sideways = (prev_state == "sideways")
    raw_is_sideways  = (raw_state == "sideways")
    candidate = raw_state

    if raw_is_sideways and not prev_is_sideways:
        if max_adx is not None and max_adx >= adx_in:
            candidate = prev_state
    elif (not raw_is_sideways) and prev_is_sideways:
        if max_adx is not None and max_adx < adx_out:
            candidate = "sideways"

    if candidate == prev_state:
        return prev_state, prev_streak + 1

    if prev_streak + 1 < min_streak:
        return prev_state, prev_streak + 1

    return candidate, 1


# 🔸 ------------------ Volatility: thresholds + hysteresis/dwell ------------------

VOL_ATR_LOW_PCT     = {"m5": 0.30, "m15": 0.40, "h1": 0.60}   # ≤ low
VOL_ATR_HIGH_PCT    = {"m5": 0.80, "m15": 1.00, "h1": 1.50}

VOL_BW_EXPAND_IN    = {"m5": 0.040, "m15": 0.030, "h1": 0.020}
VOL_BW_EXPAND_OUT   = {"m5": 0.025, "m15": 0.020, "h1": 0.015}
VOL_BW_CONTR_IN     = {"m5": -0.040,"m15": -0.030,"h1": -0.020}
VOL_BW_CONTR_OUT    = {"m5": -0.025,"m15": -0.020,"h1": -0.015}

VOL_MIN_STREAK      = {"m5": 2, "m15": 1, "h1": 1}

def vol_thresholds(tf: str) -> dict:
    return {
        "atr_low":   VOL_ATR_LOW_PCT[tf],
        "atr_high":  VOL_ATR_HIGH_PCT[tf],
        "bw_exp_in":  VOL_BW_EXPAND_IN[tf],
        "bw_exp_out": VOL_BW_EXPAND_OUT[tf],
        "bw_con_in":  VOL_BW_CONTR_IN[tf],
        "bw_con_out": VOL_BW_CONTR_OUT[tf],
        "min_streak": VOL_MIN_STREAK.get(tf, 2),
    }

def apply_vol_hysteresis_and_dwell(
    prev_state: str | None,
    raw_state: str,              # "low_squeeze" | "high" | "expanding" | "normal"
    features: dict,              # {"rel_diff": float | None, "atr_pct": float | None}
    thr: dict,                   # из vol_thresholds(tf)
    prev_streak: int,
) -> tuple[str, int]:
    rel = features.get("rel_diff")
    atr = features.get("atr_pct")
    min_streak = thr["min_streak"]

    if prev_state is None:
        return raw_state, 1

    if raw_state in ("low_squeeze", "high"):
        return (raw_state, prev_streak + 1) if raw_state == prev_state else (raw_state, 1)

    candidate = raw_state

    if prev_state == "expanding":
        candidate = "normal" if (rel is not None and rel < thr["bw_exp_out"]) else "expanding"
    elif prev_state == "normal":
        if rel is not None and rel >= thr["bw_exp_in"]:
            candidate = "expanding"
        else:
            candidate = "low_squeeze" if (rel is not None and rel <= thr["bw_con_in"] and atr is not None and atr <= thr["atr_low"]) else "normal"
    elif prev_state == "low_squeeze":
        candidate = "normal" if (rel is not None and rel > thr["bw_con_out"]) else "low_squeeze"

    if candidate == prev_state:
        return prev_state, prev_streak + 1

    if prev_streak + 1 < min_streak and candidate not in ("low_squeeze", "high"):
        return prev_state, prev_streak + 1

    return candidate, 1


# 🔸 ------------------ TODO: Momentum thresholds + hysteresis/dwell ------------------
# Зарезервировано под будущие обновления (MW_MOM).

# 🔸 ------------------ TODO: Extremes thresholds + hysteresis/dwell ------------------
# Зарезервировано под будущие обновления (MW_EXT).