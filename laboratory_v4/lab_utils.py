# lab_utils.py — утилиты laboratory_v4: нормализация времени бара и загрузка OHLCV из Redis TS в DataFrame

# 🔸 Импорты
import asyncio
import logging
from typing import Optional
import pandas as pd

# 🔸 Логгер модуля
log = logging.getLogger("LAB_UTILS")

# 🔸 Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Префиксы Redis
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}

# 🔸 Нормализация времени к началу бара
def floor_to_bar(ts_ms: int, tf: str) -> int:
    """
    Округляет метку времени ts_ms вниз к началу бара TF.
    """
    step = STEP_MS.get(tf)
    if step is None:
        raise ValueError(f"unsupported timeframe: {tf}")
    return (int(ts_ms) // step) * step

# 🔸 Загрузка OHLCV из Redis TS и сборка pandas.DataFrame
async def load_ohlcv_df(redis, symbol: str, tf: str, end_ts_ms: int, bars: int = 800) -> Optional[pd.DataFrame]:
    """
    Загружает OHLCV ряды для {symbol, tf} из RedisTimeSeries ключей bb:ts:{symbol}:{tf}:{o|h|l|c|v}
    в диапазоне [start_ts, end_ts_ms], затем собирает их в DataFrame с индексом open_time.
    Возвращает DataFrame или None, если данных недостаточно.
    """
    step = STEP_MS.get(tf)
    if step is None:
        log.warning("load_ohlcv_df: unsupported tf=%s", tf)
        return None

    # диапазон времени
    start_ts = int(end_ts_ms) - (bars - 1) * step

    # формируем ключи TS
    fields = ["o", "h", "l", "c", "v"]
    keys = {f: f"{BB_TS_PREFIX}:{symbol}:{tf}:{f}" for f in fields}

    # асинхронный батч на 5 TS.RANGE
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, end_ts_ms) for f in fields}
    try:
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    except Exception as e:
        log.warning("[TS] batch RANGE error %s/%s: %s", symbol, tf, e)
        return None

    # парсинг серий
    series = {}
    for f, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            log.warning("[TS] RANGE %s error: %s", keys[f], res)
            continue
        if res:
            try:
                # res: [[ts, value], ...] → {ts:int -> float(value)}
                series[f] = {int(ts): float(val) for ts, val in res if val is not None}
            except Exception as e:
                log.warning("[TS] parse %s error: %s", keys[f], e)

    # должны быть хотя бы закрытия
    if not series or "c" not in series or not series["c"]:
        return None

    # общий индекс по таймстампам close
    idx = sorted(series["c"].keys())
    if not idx:
        return None

    # собираем DataFrame по общему индексу (outer join по остальным полям)
    df = None
    for f in fields:
        col_map = series.get(f, {})
        s = pd.Series({ts: col_map.get(ts) for ts in idx})
        s.index = pd.to_datetime(s.index, unit="ms")
        s.name = f
        df = s.to_frame() if df is None else df.join(s, how="outer")

    if df is None or df.empty:
        return None

    # финальная подготовка
    df.index.name = "open_time"
    df = df.sort_index()

    # проверка длины (могут отсутствовать некоторые последние бары)
    if len(df) < min(bars // 2, 100):  # нижняя граница разумности, чтобы не возвращать совсем крохи
        log.debug("load_ohlcv_df: too few rows (%d) for %s/%s", len(df), symbol, tf)
        return None

    return df