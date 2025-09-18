# packs/adx_dmi_pack.py — on-demand построитель пакета ADX/DMI (корзины по 5, strict/smooth динамики для ADX и Gap)

import logging
from .pack_utils import (
    STEP_MS,
    floor_to_bar,
    load_ohlcv_df,
    bar_open_iso,
)

log = logging.getLogger("ADX_DMI_PACK")

# 🔸 Префиксы ключей Redis
IND_KV_PREFIX = "ind"      # ind:{symbol}:{tf}:{param_name}
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param_name}

# 🔸 Эпсилоны (антидребезг) для трендов — в «пунктах» индикатора
ADX_EPS = {"m5": 0.5, "m15": 0.7, "h1": 1.0}
GAP_EPS = {"m5": 1.0, "m15": 1.5, "h1": 2.0}

# 🔸 Окна сглаживания (кол-во закрытых баров в среднем)
SMOOTH_N = {"m5": 10, "m15": 6, "h1": 4}


# 🔸 Вспомогательные функции корзин (нижняя граница)
def adx_bucket_low(value: float) -> int:
    if value is None:
        return 0
    v = max(0.0, min(99.9999, float(value)))
    return int(v // 5) * 5  # 0,5,10,…,95

def gap_bucket_low(gap: float) -> int:
    if gap is None:
        return 0
    # клампим к (-100 .. 100)
    g = max(-100.0, min(99.9999, float(gap)))
    # шаг 5 с нижней границей и возвращаем в исходную ось
    return int(((g + 100.0) // 5) * 5) - 100  # …,-100,-95,…,0,5,…,95


# 🔸 Классификация трендов (strict/smooth)
def classify_trend(delta: float, eps: float, up_label: str, down_label: str, stable_label: str) -> str:
    if abs(delta) <= eps:
        return stable_label
    return up_label if delta > 0 else down_label


# 🔸 Чтение «закрытых» значений ADX/DMI из KV ind:*
async def fetch_closed_adx_dmi(redis, symbol: str, tf: str, base: str):
    try:
        adx_s  = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_adx")
        pdi_s  = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_plus_di")
        mdi_s  = await redis.get(f"{IND_KV_PREFIX}:{symbol}:{tf}:{base}_minus_di")
        if adx_s is None or pdi_s is None or mdi_s is None:
            return None
        return float(adx_s), float(pdi_s), float(mdi_s)
    except Exception:
        return None


# 🔸 Среднее ADX за N закрытых баров (TS.RANGE)
async def fetch_mean_adx(redis, symbol: str, tf: str, base: str, last_closed_ms: int, n: int) -> float | None:
    if n <= 0:
        return None
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        series = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_adx", start, last_closed_ms)
        if not series:
            return None
        vals = [float(v) for _, v in series][-n:]
        return sum(vals) / len(vals) if vals else None
    except Exception:
        return None


# 🔸 Среднее Gap за N закрытых баров (Gap = +DI − −DI)
async def fetch_mean_gap(redis, symbol: str, tf: str, base: str, last_closed_ms: int, n: int) -> float | None:
    if n <= 0:
        return None
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        p_series = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_plus_di",  start, last_closed_ms)
        m_series = await redis.execute_command("TS.RANGE", f"{TS_IND_PREFIX}:{symbol}:{tf}:{base}_minus_di", start, last_closed_ms)
        if not p_series or not m_series:
            return None
        p_map = {int(ts): float(v) for ts, v in p_series}
        m_map = {int(ts): float(v) for ts, v in m_series}
        xs = sorted(set(p_map.keys()) & set(m_map.keys()))
        if not xs:
            return None
        vals = [(p_map[t] - m_map[t]) for t in xs][-n:]
        return sum(vals) / len(vals) if vals else None
    except Exception:
        return None


# 🔸 Построить пакет ADX/DMI для конкретного length
async def build_adx_dmi_pack(symbol: str, tf: str, length: int, now_ms: int,
                             precision: int, redis, compute_fn) -> dict | None:
    """
    Возвращает {"base": "adx_dmi{L}", "pack": {...}} либо None.
    """
    # нормализуем время к началу текущего бара
    bar_open_ms   = floor_to_bar(now_ms, tf)
    last_closed_ms = bar_open_ms - STEP_MS[tf]

    # грузим OHLCV
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[ADX_DMI_PACK] {symbol}/{tf} adx_dmi{length}: no ohlcv")
        return None

    base = f"adx_dmi{length}"
    inst = {"indicator": "adx_dmi", "params": {"length": str(length)}, "timeframe": tf}

    # live-расчёт
    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[ADX_DMI_PACK] {symbol}/{tf} {base}: compute failed")
        return None

    try:
        adx_live = float(values.get(f"{base}_adx"))
        pdi_live = float(values.get(f"{base}_plus_di"))
        mdi_live = float(values.get(f"{base}_minus_di"))
    except Exception:
        log.warning(f"[ADX_DMI_PACK] {symbol}/{tf} {base}: missing live params")
        return None

    gap_live = pdi_live - mdi_live

    # закрытые значения
    closed = await fetch_closed_adx_dmi(redis, symbol, tf, base)
    if closed is None:
        # если инстанс не активен/нет закрытого — считаем, что результата нет
        return None
    adx_closed, pdi_closed, mdi_closed = closed
    gap_closed = pdi_closed - mdi_closed

    # корзины (нижняя граница)
    adx_bl  = adx_bucket_low(adx_live)
    gap_bl  = gap_bucket_low(gap_live)

    # strict динамики
    adx_eps = ADX_EPS.get(tf, 0.7)
    gap_eps = GAP_EPS.get(tf, 1.5)

    d_adx   = adx_live - adx_closed
    d_gap   = gap_live - gap_closed

    adx_dyn_strict = classify_trend(d_adx, adx_eps, "adx_up", "adx_down", "adx_stable")
    gap_dyn_strict = classify_trend(d_gap, gap_eps, "gap_up", "gap_down", "gap_stable")

    # smooth динамики (среднее за N закрытых)
    n = SMOOTH_N.get(tf, 6)
    adx_mean = await fetch_mean_adx(redis, symbol, tf, base, last_closed_ms, n)
    gap_mean = await fetch_mean_gap(redis, symbol, tf, base, last_closed_ms, n)

    if adx_mean is None:
        adx_dyn_smooth = adx_dyn_strict
        d_adx_smooth = None
    else:
        d_adx_smooth_val = adx_live - adx_mean
        adx_dyn_smooth = classify_trend(d_adx_smooth_val, adx_eps, "adx_up", "adx_down", "adx_stable")
        d_adx_smooth = d_adx_smooth_val

    if gap_mean is None:
        gap_dyn_smooth = gap_dyn_strict
        d_gap_smooth = None
    else:
        d_gap_smooth_val = gap_live - gap_mean
        gap_dyn_smooth = classify_trend(d_gap_smooth_val, gap_eps, "gap_up", "gap_down", "gap_stable")
        d_gap_smooth = d_gap_smooth_val

    # сборка пакета
    pack = {
        "base": base,
        "pack": {
            "adx_value": f"{adx_live:.2f}",
            "adx_bucket_low": adx_bl,
            "adx_dynamic_strict": adx_dyn_strict,
            "adx_dynamic_smooth": adx_dyn_smooth,

            "gap_value": f"{gap_live:.2f}",
            "gap_bucket_low": gap_bl,
            "gap_dynamic_strict": gap_dyn_strict,
            "gap_dynamic_smooth": gap_dyn_smooth,

            "ref": "closed",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }
    return pack