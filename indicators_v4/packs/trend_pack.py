# packs/trend_pack.py — on-demand построитель пакета TREND (live на текущем баре: up/down/sideways + strong)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

# 🔸 Логгер
log = logging.getLogger("TREND_PACK")

# 🔸 Пороговые константы
ANGLE_EPS  = 0.0   # угол LR: >0 вверх, <0 вниз
ADX_STRONG = 25.0  # сила тренда по ADX (максимум из 14/21)

# 🔸 Префиксы Redis (цена)
BB_TS_PREFIX  = "bb:ts"            # bb:ts:{symbol}:{tf}:c
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

# 🔸 Определение направления тренда по EMA и LR
def infer_direction(price: float | None,
                    ema21: float | None, ema50: float | None, ema200: float | None,
                    ang50: float | None, ang100: float | None) -> str:
    # голоса EMA (цена vs EMA)
    up_votes = 0
    down_votes = 0

    # условия достаточности
    if price is not None and ema21 is not None:
        if price > ema21: up_votes += 1
        elif price < ema21: down_votes += 1
    if price is not None and ema50 is not None:
        if price > ema50: up_votes += 1
        elif price < ema50: down_votes += 1
    if price is not None and ema200 is not None:
        if price > ema200: up_votes += 1
        elif price < ema200: down_votes += 1

    # голоса LR (угол канала)
    if ang50 is not None:
        if ang50 > ANGLE_EPS: up_votes += 1
        elif ang50 < -ANGLE_EPS: down_votes += 1
    if ang100 is not None:
        if ang100 > ANGLE_EPS: up_votes += 1
        elif ang100 < -ANGLE_EPS: down_votes += 1

    # решение по голосам
    if up_votes >= 3 and up_votes > down_votes:
        return "up"
    if down_votes >= 3 and down_votes > up_votes:
        return "down"
    return "sideways"

# 🔸 Определение силы тренда по ADX
def infer_strength(adx14: float | None, adx21: float | None) -> bool:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False
    return max(vals) >= ADX_STRONG

# 🔸 Построить live TREND-пакет для текущего бара
async def build_trend_pack(symbol: str, tf: str, now_ms: int,
                           precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "trend", "pack": {...}} либо None.
    """
    # нормализация времени
    bar_open_ms = floor_to_bar(now_ms, tf)

    # грузим OHLCV
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live цена
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    if price_live is None:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no live price")
        return None

    # собираем значения из compute_fn (EMA 21/50/200; LR 50/100; ADX 14/21)
    used_bases = ["ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"]

    # EMA
    ema21 = ema50 = ema200 = None
    for L in (21, 50, 200):
        inst = {"indicator": "ema", "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals: continue
        try:
            v = float(vals.get(f"ema{L}"))
            if L == 21: ema21 = v
            elif L == 50: ema50 = v
            else: ema200 = v
        except Exception:
            pass

    # LR (углы)
    ang50 = ang100 = None
    for L in (50, 100):
        inst = {"indicator": "lr", "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals: continue
        try:
            v = float(vals.get(f"lr{L}_angle"))
            if L == 50: ang50 = v
            else: ang100 = v
        except Exception:
            pass

    # ADX (значение ADX)
    adx14 = adx21 = None
    for L in (14, 21):
        inst = {"indicator": "adx_dmi", "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals: continue
        try:
            v = float(vals.get(f"adx_dmi{L}_adx"))
            if L == 14: adx14 = v
            else: adx21 = v
        except Exception:
            pass

    # классификация
    direction = infer_direction(price_live, ema21, ema50, ema200, ang50, ang100)
    strong = infer_strength(adx14, adx21)
    state = "sideways" if direction == "sideways" else f"{direction}_{'strong' if strong else 'weak'}"

    # сборка пакета
    pack = {
        "base": "trend",
        "pack": {
            "state": state,
            "direction": direction,
            "strong": bool(strong),
            "ref": "live",
            "open_time": bar_open_iso(bar_open_ms),
            "used_bases": used_bases,
        },
    }
    return pack