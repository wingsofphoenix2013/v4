# packs/trend_pack.py — on-demand TREND (live на текущем баре: up/down/sideways + strong/weak) с дельтами и «смягчённым» голосованием

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

# 🔸 Логгер
log = logging.getLogger("TREND_PACK")

# 🔸 Константы и пороги (синхронизированы с indicator_mw_trend.py)
ADX_STRONG_LEVEL    = 25.0                       # базовый порог strong
ADX_SIDEWAYS_LEVEL  = 12.0                       # если ниже — считаем sideways независимо от голосов

ADX_DROP_EPS        = {"m5": 0.5, "m15": 0.7, "h1": 1.0}     # падение max(ADX) на бар → ослабляем
EMA_DIST_DROP_EPS   = {"m5": 0.15, "m15": 0.20, "h1": 0.30}  # уменьшение |(Close-EMA50)/EMA50| в п.п.
LR_FLATTEN_ALLOW    = {"m5": 0.0, "m15": 0.0, "h1": 0.0}     # Δугла <= 0 → сглаживание

EMA_EQ_EPS_PCT      = {"m5": 0.05, "m15": 0.07, "h1": 0.10}  # |price-EMA|/EMA*100 <= eps → нейтрально
ANGLE_EPS           = {"m5": 1e-4, "m15": 8e-4, "h1": 2e-3}  # |угол| <= eps → нейтрально

BB_TS_PREFIX  = "bb:ts"                                     # bb:ts:{symbol}:{tf}:c
TS_IND_PREFIX = "ts_ind"                                    # ts_ind:{symbol}:{tf}:{param}
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


# 🔸 «Смягчённое» голосование направления по текущему бару
def infer_direction_soft(tf: str,
                         price: float | None,
                         ema21: float | None, ema50: float | None, ema200: float | None,
                         ang50: float | None, ang100: float | None) -> str:
    up_ema = 0
    down_ema = 0
    up_lr = 0
    down_lr = 0

    eps_pct = EMA_EQ_EPS_PCT.get(tf, 0.05)

    def vote_ema(p, e):
        if p is None or e is None or e == 0:
            return 0, 0
        dist_pct = (p - e) / e * 100.0
        if abs(dist_pct) <= eps_pct:
            return 0, 0
        return (1, 0) if dist_pct > 0 else (0, 1)

    u, d = vote_ema(price, ema21); up_ema += u; down_ema += d
    u, d = vote_ema(price, ema50); up_ema += u; down_ema += d
    u, d = vote_ema(price, ema200); up_ema += u; down_ema += d

    aeps = ANGLE_EPS.get(tf, 1e-4)

    def vote_angle(a):
        if a is None:
            return 0, 0
        if abs(a) <= aeps:
            return 0, 0
        return (1, 0) if a > 0 else (0, 1)

    u, d = vote_angle(ang50);  up_lr += u;  down_lr += d
    u, d = vote_angle(ang100); up_lr += u;  down_lr += d

    # правило согласованности (делаем больше «sideways» в спорных местах):
    # up → минимум 2 из 3 EMA в плюс И минимум 1 LR в плюс
    # down → минимум 2 из 3 EMA в минус И минимум 1 LR в минус
    if up_ema >= 2 and up_lr >= 1:
        return "up"
    if down_ema >= 2 and down_lr >= 1:
        return "down"
    return "sideways"


# 🔸 Базовая сила по уровню ADX
def base_strength_now(adx14: float | None, adx21: float | None) -> tuple[bool, float]:
    vals = [v for v in (adx14, adx21) if v is not None]
    if not vals:
        return False, 0.0
    max_adx = max(vals)
    return max_adx >= ADX_STRONG_LEVEL, max_adx


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

    max_adx_cur = max([v for v in (adx14_cur, adx21_cur) if v is not None], default=None)
    max_adx_prev = max([v for v in (adx14_prev, adx21_prev) if v is not None], default=None)

    d_adx = None
    adx_is_falling = False
    if max_adx_cur is not None and max_adx_prev is not None:
        d_adx = max_adx_cur - max_adx_prev
        adx_is_falling = (d_adx <= -adx_drop_eps)

    d_abs_dist = None
    abs_dist_is_shrinking = False
    if (ema50_cur is not None and ema50_cur != 0 and close_cur is not None and
        ema50_prev is not None and ema50_prev != 0 and close_prev is not None):
        dist_cur = abs((close_cur - ema50_cur) / ema50_cur) * 100.0
        dist_prev = abs((close_prev - ema50_prev) / ema50_prev) * 100.0
        d_abs_dist = dist_cur - dist_prev
        abs_dist_is_shrinking = (d_abs_dist <= -ema_drop_eps)

    d_ang50 = (ang50_cur - ang50_prev) if (ang50_cur is not None and ang50_prev is not None) else None
    d_ang100 = (ang100_cur - ang100_prev) if (ang100_cur is not None and ang100_prev is not None) else None
    lr_is_flatten = False
    conds = []
    if d_ang50 is not None:  conds.append(d_ang50 <= lr_flat_allow)
    if d_ang100 is not None: conds.append(d_ang100 <= lr_flat_allow)
    if conds: lr_is_flatten = all(conds)

    weaken = adx_is_falling or abs_dist_is_shrinking or lr_is_flatten

    # округление для деталей
    def r2(x): return None if x is None else round(float(x), 2)
    def r5(x): return None if x is None else round(float(x), 5)

    return {
        "weaken": weaken,
        "d_adx": r2(d_adx),
        "d_abs_dist_pct": r2(d_abs_dist),
        "d_lr50_angle": r5(d_ang50),
        "d_lr100_angle": r5(d_ang100),
        "flags": {
            "adx_is_falling": adx_is_falling,
            "abs_dist_is_shrinking": abs_dist_is_shrinking,
            "lr_is_flatten": lr_is_flatten,
        }
    }


# 🔸 Построить live TREND-пакет (в стиле MW_TREND)
async def build_trend_pack(symbol: str, tf: str, now_ms: int,
                           precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "trend", "pack": {...}} либо None.
    """
    # нормализуем время
    bar_open_ms = floor_to_bar(now_ms, tf)
    prev_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV для live-расчётов
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no ohlcv")
        return None

    # live цена и закрытая цена на предыдущем баре
    price_live = await fetch_mark_or_last_close(redis, symbol, tf)
    if price_live is None:
        log.warning(f"[TREND_PACK] {symbol}/{tf}: no live price")
        return None
    price_prev = await ts_get_at(redis, f"{BB_TS_PREFIX}:{symbol}:{tf}:c", prev_ms)

    # EMA (текущие) — 21/50/200
    ema21 = ema50 = ema200 = None
    for L in (21, 50, 200):
        inst = {"indicator": "ema", "params": {"length": str(L)}, "timeframe": tf}
        vals = await compute_fn(inst, symbol, df, precision)
        if not vals:
            continue
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
        if not vals:
            continue
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
        if not vals:
            continue
        try:
            v = float(vals.get(f"adx_dmi{L}_adx"))
            if L == 14: adx14 = v
            else: adx21 = v
        except Exception:
            pass

    # предыдущие значения из TS (для дельт)
    ema50_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:ema50", prev_ms)
    ang50_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle", prev_ms)
    ang100_prev = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle", prev_ms)
    adx14_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi14_adx", prev_ms)
    adx21_prev  = await ts_get_at(redis, f"{TS_IND_PREFIX}:{symbol}:{tf}:adx_dmi21_adx", prev_ms)

    # дельты считаем всегда (как в воркере)
    deltas = weaken_by_deltas(
        tf,
        adx14, adx14_prev,
        adx21, adx21_prev,
        ema50, ema50_prev,
        price_live, price_prev,
        ang50, ang50_prev,
        ang100, ang100_prev,
    )

    # guard на флэт по ADX
    _, max_adx = base_strength_now(adx14, adx21)
    if max_adx < ADX_SIDEWAYS_LEVEL:
        pack = {
            "base": "trend",
            "pack": {
                "state": "sideways",
                "direction": "sideways",
                "strong": False,
                "ref": "live",
                "open_time": bar_open_iso(bar_open_ms),
                "used_bases": ["ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"],
                "max_adx": round(max_adx, 2),
                "deltas": {
                    "d_adx": deltas["d_adx"],
                    "d_abs_dist_pct": deltas["d_abs_dist_pct"],
                    "d_lr50_angle": deltas["d_lr50_angle"],
                    "d_lr100_angle": deltas["d_lr100_angle"],
                    **deltas["flags"],
                },
            },
        }
        return pack

    # направление по «мягкому» голосованию (deadband + согласованность)
    direction = infer_direction_soft(tf, price_live, ema21, ema50, ema200, ang50, ang100)

    # сила: базово strong по уровню, затем ослабляем по дельтам
    strong, _ = base_strength_now(adx14, adx21)
    if strong and deltas["weaken"]:
        strong = False

    state = "sideways" if direction == "sideways" else f"{direction}_{'strong' if strong else 'weak'}"

    # сборка пакета (диагностика включена)
    pack = {
        "base": "trend",
        "pack": {
            "state": state,
            "direction": direction,
            "strong": bool(strong),
            "ref": "live",
            "open_time": bar_open_iso(bar_open_ms),
            "used_bases": ["ema21", "ema50", "ema200", "lr50", "lr100", "adx_dmi14", "adx_dmi21"],
            "max_adx": round(max_adx, 2),
            "deltas": {
                "d_adx": deltas["d_adx"],
                "d_abs_dist_pct": deltas["d_abs_dist_pct"],
                "d_lr50_angle": deltas["d_lr50_angle"],
                "d_lr100_angle": deltas["d_lr100_angle"],
                **deltas["flags"],
            },
        },
    }
    return pack