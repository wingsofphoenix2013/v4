# packs/pack_utils.py — утилиты для on-demand пакетов (RSI/MFI/…): время бара, загрузка OHLCV из TS, корзины и тренды

import asyncio
import logging
from datetime import datetime
import pandas as pd

# 🔸 Логгер модуля
log = logging.getLogger("PACK_UTILS")

# 🔸 Таймшаги TF (мс и минуты)
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
STEP_MS  = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Префиксы Redis
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}
IND_KV_PREFIX = "ind"   # ind:{symbol}:{tf}:{param_name}

# 🔸 Нормализация времени к началу бара
def floor_to_bar(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step

# 🔸 Загрузка OHLCV из Redis TS (одним батчем) и сборка DataFrame
async def load_ohlcv_df(redis, symbol: str, tf: str, end_ts_ms: int, bars: int = 800):
    if tf not in STEP_MS:
        return None

    step = STEP_MS[tf]
    start_ts = end_ts_ms - (bars - 1) * step

    fields = ["o", "h", "l", "c", "v"]
    keys = {f: f"{BB_TS_PREFIX}:{symbol}:{tf}:{f}" for f in fields}

    # один батч на 5 TS.RANGE
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, end_ts_ms) for f in fields}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for f, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            log.warning(f"[TS] RANGE {keys[f]} error: {res}")
            continue
        if res:
            try:
                series[f] = {int(ts): float(val) for ts, val in res if val is not None}
            except Exception as e:
                log.warning(f"[TS] parse {keys[f]} error: {e}")

    if not series or "c" not in series or not series["c"]:
        return None

    # общий индекс по меткам времени (по close)
    idx = sorted(series["c"].keys())
    df = None
    for f in fields:
        col_map = series.get(f, {})
        s = pd.Series({ts: col_map.get(ts) for ts in idx})
        s.index = pd.to_datetime(s.index, unit="ms")
        s.name = f
        df = s.to_frame() if df is None else df.join(s, how="outer")

    if df is None or df.empty:
        return None

    df.index.name = "open_time"
    return df.sort_index()

# 🔸 RSI: корзина (нижняя граница, шаг 5)
def rsi_bucket_low(value: float) -> int:
    x = max(0.0, min(99.9999, float(value)))
    return int((int(x) // 5) * 5)

# 🔸 RSI: пороги «мертвого коридора» по TF (абсолютные пункты RSI)
RSI_EPS = {"m5": 0.3, "m15": 0.4, "h1": 0.6}

# 🔸 RSI: классификация тренда по абсолютной дельте (up/flat/down)
def classify_abs_delta(delta: float, tf: str) -> str:
    eps = RSI_EPS.get(tf, 0.4)
    if delta >= eps:
        return "up"
    if delta <= -eps:
        return "down"
    return "flat"

# 🔸 Прочитать закрытое значение RSI из KV ind:{symbol}:{tf}:rsi{length}
async def get_closed_rsi(redis, symbol: str, tf: str, length: int) -> float | None:
    key = f"{IND_KV_PREFIX}:{symbol}:{tf}:rsi{length}"
    try:
        s = await redis.get(key)
        return float(s) if s is not None else None
    except Exception:
        return None

# 🔸 Вспомогательное: ISO-время открытия бара по ms
def bar_open_iso(bar_open_ms: int) -> str:
    return datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()