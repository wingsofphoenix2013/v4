# packs/trend_pack.py — on-demand TREND (live на текущем баре: up/down/sideways + strong с учётом дельт)

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
ANGLE_EPS  = 0.0     # угол LR: >0 вверх, <0 вниз
ADX_STRONG = 25.0    # сила тренда по ADX (максимум из 14/21)

# 🔸 Пороговые дельты (по TF) — «острота» силы
ADX_DROP_EPS        = {"m5": 0.5, "m15": 0.7, "h1": 1.0}     # падение max(ADX) ≤ −eps → ослабляем
EMA_DIST_DROP_EPS   = {"m5": 0.15, "m15": 0.20, "h1": 0.30}  # уменьшение |(Close-EMA50)/EMA50| (п.п.) → ослабляем
LR_FLATTEN_ALLOW    = {"m5": 0.0, "m15": 0.0, "h1": 0.0}     # Δугла ≤ 0 → ослабляем

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

# 🔸 Закрытая цена на конкретном баре (по open_time в ms)
async def fetch_closed_close(redis, symbol: str, tf: str, closed_ms: int) -> float | None:
    try:
        res = await redis.execute_command("TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c", closed_ms, closed_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 Прочитать из TS точку индикатора на exact open_time
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

# 🔸 Направление по текущему бару: EMA vs цена + LR углы
def infer_direction_now(price: float | None,
                        ema21: float | None, ema50: float | None, ema200: float | None,
                        ang50: float | None, ang100: float | None) -> str:
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

    if ang50 is not None:
        if ang50 > ANGLE_EPS: up_votes += 1
        elif ang50 < -ANGLE_EPS: down_votes += 1
    if ang100 is not None:
        if ang100 > ANGLE_EPS: up_votes += 1
        elif ang100 < -ANGLE_EPS: down_votes += 1

    if up_votes >= 3 and up_votes > down_votes:
        return "up"
    if down_votes >= 3 and down_votes > up_votes:
        return "down"
    return "sideways"

# 🔸 Сила по текущему бару: уровень ADX
def base_strength_now(adx14: float | None, adx21: float | None) -> bool:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False
    return max(vals) >= ADX_STRONG

# 🔸 Коррекция силы по дельтам (ослабление strong → weak)
def weaken_by_deltas(tf: str,
                     adx14_cur: float | None, adx14_prev: float | None,
                     adx21_cur: float | None, adx21_prev: float | None,
                     ema50_cur: float | None, ema50_prev: float | None,
                     close_cur: float | None, close_prev: float | None,
                     ang50_cur: float | None, ang50_prev: float | None,
                     ang100_cur: float | None, ang100_prev: float | None) -> dict:
    adx_drop_eps = ADX_DROP_EPS.get(tf, 0.7)
    ema_drop_eps = EMA_DIST_DROP_EPS.get(tf, 0.2)
    lr_flat_allow = LR_FLATTEN_ALLOW.get(tf, 0.0)

    # ΔADX по максимуму
    max_adx_cur = max([v for v in (adx14_cur, adx21_cur) if v is not None], default=None)
    max_adx_prev = max([v for v in (adx14_prev, adx21_prev) if v is not None], default=None)
    d_adx = None
    adx_is_falling = False
    if max_adx_cur is not None and max_adx_prev is not None:
        d_adx = max_adx_cur - max_adx_prev
        adx_is_falling = (d_adx <= -adx_drop_eps)

    # Δ|dist EMA50|
    d_abs_dist = None
    abs_dist_is_shrinking = False
    if (ema50_cur is not None and ema50_cur != 0 and close_cur is not None and
        ema50_prev is not None and ema50_prev != 0 and close_prev is not None):
        dist_cur = abs((close_cur - ema50_cur) / ema50_cur) * 100.0
        dist_prev = abs((close_prev - ema50_prev) / ema50_prev) * 100.0
        d_abs_dist = dist_cur - dist_prev
        abs_dist_is_shrinking = (d_abs_dist <= -ema_drop_eps)

    # Δуглов LR
    d_ang50 = (ang50_cur - ang50_prev) if (ang50_cur is not None and ang50_prev is not None) else None
    d_ang100 = (ang100_cur - ang100_prev) if (ang100_cur is not None and ang100_prev is not None) else None
    lr_is_flatten = False
    conds = []
    if d_ang50 is not None: conds.append(d_ang50 <= lr_flat_allow)
    if d_ang100 is not None: conds.append(d_ang100 <= lr_flat_allow)
    if conds: lr_is_flatten = all(conds)

    weaken = adx_is_falling or abs_dist_is_shrinking or lr_is_flatten

    return {
        "weaken": weaken,
        "d_adx": d_adx,
        "d_abs_dist_pct": d_abs_dist,
        "d_lr50_angle": d_ang50,
        "d_lr100_angle": d_ang100,
        "flags": {
            "adx_is_falling": adx_is_falling,
            "abs_dist_is_shrinking": abs_dist_is_shrinking,
            "lr_is_flatten": lr_is_flatten,
        }
    }

# 🔸 Построить live TREND-пакет (с учётом дельт одного бара назад)
async def build_trend_pack(symbol: str, tf: str, now_ms: int,
                           precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "trend", "pack": {...}} либо None.
    """
    # нормализуем время
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live цена и закрытая цена на предыдущем баре
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    price_prev = await fetch_closed_close(redis, symbol, tf, prev_ms)
    if price_live is None:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no live price")
        return None

    # EMA (текущие) — 21/50/200
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

    # LR углы (текущие) — 50/100
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

    # ADX (текущие) — 14/21
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

    # направление/базовая сила по текущему бару
    direction = infer_direction_now(price_live, ema21, ema50, ema200, ang50, ang100)
    strong = base_strength_now(adx14, adx21)

    # предыдущие значения из TS для дельт
    ema50_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:ema50", prev_ms)
    ang50_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle", prev_ms)
    ang100_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle", prev_ms)
    adx14_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi14_adx", prev_ms)
    adx21_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi21_adx", prev_ms)

    deltas = weaken_by_deltas(
        tf,
        adx14, adx14_prev,
        adx21, adx21_prev,
        ema50, ema50_prev,
        price_live, price_prev,
        ang50, ang50_prev,
        ang100, ang100_prev,
    )
    if strong and deltas["weaken"]:
        strong = False

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
            "used_bases": ["ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"],
            # диагностические дельты (по желанию стратегий можно игнорировать)
            "d_adx": (None if deltas["d_adx"] is None else f"{deltas['d_adx']:.2f}"),
            "d_abs_dist_pct": (None if deltas["d_abs_dist_pct"] is None else f"{deltas['d_abs_dist_pct']:.2f}"),
            "d_lr50_angle": (None if deltas["d_lr50_angle"] is None else f"{deltas['d_lr50_angle']:.5f}"),
            "d_lr100_angle": (None if deltas["d_lr100_angle"] is None else f"{deltas['d_lr100_angle']:.5f}"),
        },
    }
    return pack