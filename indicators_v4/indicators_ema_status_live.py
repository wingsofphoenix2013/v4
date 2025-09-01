# indicators_ema_status_live.py — ежеминутный on-demand EMA-status: Этап 3 (t−1 из TS + retry)

import os
import asyncio
import logging
from datetime import datetime
import pandas as pd

log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL_SEC", "60"))
REQUIRED_TFS = ("m5", "m15", "h1")
REQUIRED_BARS_DEFAULT = int(os.getenv("EMA_STATUS_LIVE_REQUIRED_BARS", "800"))
RETRY_SEC = int(os.getenv("EMA_STATUS_LIVE_RETRY_SEC", "15"))

# 🔸 Вспомогательные мапы для шага времени
_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}
_FIELDS = ("o", "h", "l", "c", "v")

# 🔸 Ключи Redis TS для t−1 (готовые значения)
def _k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def _k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def _k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def _k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

# 🔸 EMA длины и пороги (как у остальных воркеров; будем использовать на Этапе 4)
def _parse_ema_lens(raw: str) -> list[int]:
    out = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.append(int(p))
        except:
            pass
    return out or [9, 21, 50, 100, 200]

EMA_LENS = _parse_ema_lens(os.getenv("EMA_STATUS_LIVE_EMA_LENS", "9,21,50,100,200"))

# 🔸 Флор к началу бара TF (UTC, мс)
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = _STEP_MS[tf]
    return (ts_ms // step) * step

# 🔸 Чтение одной точки ровно на open_time из TS
async def _ts_get_exact(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Загрузка OHLCV из Redis TS до bar_open_ms включительно (последние N баров)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> pd.DataFrame | None:
    step_ms = _STEP_MS[tf]
    start_ts = bar_open_ms - (depth - 1) * step_ms
    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in _FIELDS}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in _FIELDS}
    res = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for f, r in zip(tasks.keys(), res):
        if isinstance(r, Exception):
            log.debug("[TSERR] %s err=%s", keys[f], r)
            continue
        if r:
            series[f] = {int(ts): float(v) for ts, v in r if v is not None}

    if "c" not in series or not series["c"]:
        return None

    idx = sorted(series["c"].keys())
    data = {f: [series.get(f, {}).get(ts) for ts in idx] for f in _FIELDS}
    df = pd.DataFrame(data, index=pd.to_datetime(idx, unit="ms"))
    df.index.name = "open_time"
    return df

# 🔸 Проверка готовности t−1 из TS с одним отложенным retry
async def _check_prev_ready_with_retry(redis, symbol: str, tf: str, bar_open_ms: int) -> bool:
    step = _STEP_MS[tf]
    prev_ms = bar_open_ms - step

    # close prev
    close_prev = await _ts_get_exact(redis, _k_close(symbol, tf), prev_ms)
    # scale prev (ATR приоритетен на m5/m15, иначе BB width; на h1 — BB width)
    if tf in ("m5", "m15"):
        atr_prev = await _ts_get_exact(redis, _k_atr(symbol, tf), prev_ms)
        if atr_prev is not None and atr_prev > 0.0:
            scale_prev_ready = True
        else:
            bbu = await _ts_get_exact(redis, _k_bb(symbol, tf, "upper"), prev_ms)
            bbl = await _ts_get_exact(redis, _k_bb(symbol, tf, "lower"), prev_ms)
            scale_prev_ready = (bbu is not None and bbl is not None and (bbu - bbl) > 0.0)
    else:
        bbu = await _ts_get_exact(redis, _k_bb(symbol, tf, "upper"), prev_ms)
        bbl = await _ts_get_exact(redis, _k_bb(symbol, tf, "lower"), prev_ms)
        scale_prev_ready = (bbu is not None and bbl is not None and (bbu - bbl) > 0.0)

    # ema prev (для всех L нужно иметь точку)
    ema_prev_ready = True
    for L in EMA_LENS:
        v = await _ts_get_exact(redis, _k_ema(symbol, tf, L), prev_ms)
        if v is None:
            ema_prev_ready = False
            break

    ready = (close_prev is not None) and scale_prev_ready and ema_prev_ready
    if ready:
        log.info("[PREV] ok %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
        return True

    # один retry через RETRY_SEC
    log.info("[PREV] miss %s/%s retry in %ds", symbol, tf, RETRY_SEC)
    await asyncio.sleep(RETRY_SEC)

    # повторная проверка
    close_prev = await _ts_get_exact(redis, _k_close(symbol, tf), prev_ms)
    if tf in ("m5", "m15"):
        atr_prev = await _ts_get_exact(redis, _k_atr(symbol, tf), prev_ms)
        if atr_prev is not None and atr_prev > 0.0:
            scale_prev_ready = True
        else:
            bbu = await _ts_get_exact(redis, _k_bb(symbol, tf, "upper"), prev_ms)
            bbl = await _ts_get_exact(redis, _k_bb(symbol, tf, "lower"), prev_ms)
            scale_prev_ready = (bbu is not None and bbl is not None and (bbu - bbl) > 0.0)
    else:
        bbu = await _ts_get_exact(redis, _k_bb(symbol, tf, "upper"), prev_ms)
        bbl = await _ts_get_exact(redis, _k_bb(symbol, tf, "lower"), prev_ms)
        scale_prev_ready = (bbu is not None and bbl is not None and (bbu - bbl) > 0.0)

    ema_prev_ready = True
    for L in EMA_LENS:
        v = await _ts_get_exact(redis, _k_ema(symbol, tf, L), prev_ms)
        if v is None:
            ema_prev_ready = False
            break

    ready = (close_prev is not None) and scale_prev_ready and ema_prev_ready
    if ready:
        log.info("[PREV] ok-after-retry %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
    else:
        log.info("[PREV] still-miss %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
    return ready

# 🔸 Основной воркер (Этап 3: DF + проверка готовности t−1; логи Этапа 2 → debug)
async def run_indicators_ema_status_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    while True:
        try:
            tick_iso = datetime.utcnow().isoformat()
            symbols = list(get_active_symbols() or [])
            planned = 0
            df_ok = 0
            prev_ok = 0
            skipped = 0

            log.debug("[TICK] start @ %s, symbols=%d", tick_iso, len(symbols))

            now_ms = int(datetime.utcnow().timestamp() * 1000)

            for sym in symbols:
                for tf in REQUIRED_TFS:
                    planned += 1
                    bar_open_ms = _floor_to_bar_ms(now_ms, tf)

                    # Этап 2 → debug
                    df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                    if df is None or df.empty:
                        skipped += 1
                        log.debug("[DF] miss %s/%s @ %s", sym, tf, datetime.utcfromtimestamp(bar_open_ms/1000).isoformat())
                        continue
                    df_ok += 1
                    log.debug("[DF] ok %s/%s bars=%d @ %s", sym, tf, len(df), datetime.utcfromtimestamp(bar_open_ms/1000).isoformat())

                    # Этап 3 → info
                    if await _check_prev_ready_with_retry(redis, sym, tf, bar_open_ms):
                        prev_ok += 1

            log.debug("[TICK] end, planned=%d df_ok=%d prev_ok=%d skipped=%d", planned, df_ok, prev_ok, skipped)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)