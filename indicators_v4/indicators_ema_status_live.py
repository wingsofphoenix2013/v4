# indicators_ema_status_live.py — 🔸 LIVE перерасчёт EMA-status по принципу PIS (bar-anchored, scale = high−low, prev через snapshot)

import os
import asyncio
import logging
from datetime import datetime
import pandas as pd

from indicators.compute_and_store import compute_snapshot_values_async
from indicators_ema_status import _classify_with_prev, EPS0, EPS1  # синхронизация порогов и формулы с PIS

log = logging.getLogger("EMA_STATUS_LIVE_SYNC_PIS")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL_SEC", "30"))
REQUIRED_TFS = ("m5", "m15", "h1")
REQUIRED_BARS_DEFAULT = int(os.getenv("EMA_STATUS_LIVE_REQUIRED_BARS", "800"))

MAX_CONCURRENCY = int(os.getenv("EMA_STATUS_LIVE_MAX_CONCURRENCY", "45"))
MAX_PER_SYMBOL = int(os.getenv("EMA_STATUS_LIVE_MAX_PER_SYMBOL", "3"))
TTL_SEC = int(os.getenv("EMA_STATUS_LIVE_TTL_SEC", "60"))

# 🔸 Таймшаги/поля
_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}
_FIELDS = ("o", "h", "l", "c", "v")

# 🔸 EMA длины
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

# 🔸 Загрузка OHLCV из Redis TS до bar_open_ms включительно (последние N баров)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> pd.DataFrame | None:
    start_ts = bar_open_ms - (depth - 1) * _STEP_MS[tf]
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

# 🔸 Подбор нужных EMA-инстансов по длинам
def _pick_ema_instances(instances: list, ema_lens: list[int]):
    by_len = {}
    for inst in instances:
        if inst.get("indicator") != "ema":
            continue
        params = inst.get("params", {}) or {}
        try:
            L = int(params.get("length"))
            if L in ema_lens and L not in by_len:
                by_len[L] = inst
        except Exception:
            continue
    return by_len

# 🔸 Вычислить на t через snapshot-инстансы: close_t, ema_t_map, scale_t (scale = high − low)
async def _features_t_snapshot(instances_tf: list, symbol: str, df: pd.DataFrame, precision: int):
    ema_by_len = _pick_ema_instances(instances_tf, EMA_LENS)
    close_t = float(df["c"].iloc[-1])
    try:
        scale_t = float(df["h"].iloc[-1]) - float(df["l"].iloc[-1])
    except Exception:
        scale_t = None

    ema_t_map = {}
    for L, inst in ema_by_len.items():
        vals = await compute_snapshot_values_async(inst, symbol, df, precision)
        key = f"ema{L}"
        if vals and key in vals:
            try:
                ema_t_map[L] = float(vals[key])
            except Exception:
                pass

    return close_t, ema_t_map, scale_t

# 🔸 Основной воркер: запись individual KV + композиты (синхронизировано с PIS)
async def run_indicators_ema_status_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

            triplet_cache: dict[tuple, dict] = {}
            cache_lock = asyncio.Lock()

            async def handle_pair(sym: str, tf: str):
                async with gate:
                    if sym not in symbol_semaphores:
                        symbol_semaphores[sym] = asyncio.Semaphore(MAX_PER_SYMBOL)
                    async with symbol_semaphores[sym]:
                        try:
                            bar_open_ms = _floor_to_bar_ms(now_ms, tf)
                            precision = get_precision(sym)

                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                return

                            instances_tf = get_instances_by_tf(tf)
                            if not instances_tf:
                                return

                            # текущие значения (t) через snapshot
                            close_t, ema_t_map, scale_t = await _features_t_snapshot(instances_tf, sym, df, precision)

                            # prev из df (как в PIS): close_prev, ema_prev via snapshot(df[:-1]), scale_prev = high−low_prev
                            if len(df) < 2:
                                return
                            close_prev = float(df["c"].iloc[-2])
                            try:
                                scale_prev = float(df["h"].iloc[-2]) - float(df["l"].iloc[-2])
                            except Exception:
                                scale_prev = None

                            ema_prev_map = {}
                            ema_by_len = _pick_ema_instances(instances_tf, EMA_LENS)
                            df_prev = df.iloc[:-1]
                            for L, inst in ema_by_len.items():
                                try:
                                    v_prev = await compute_snapshot_values_async(inst, sym, df_prev, precision)
                                    key = f"ema{L}"
                                    if v_prev and key in v_prev:
                                        ema_prev_map[L] = float(v_prev[key])
                                except Exception:
                                    pass

                            open_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

                            for L in EMA_LENS:
                                ema_t = ema_t_map.get(L)
                                ema_p = ema_prev_map.get(L)
                                if None in (close_t, close_prev, ema_t, ema_p, scale_t, scale_prev):
                                    log.debug("[STATE] miss %s/%s ema%d (insufficient features) @ %s", sym, tf, L, open_iso)
                                    continue

                                # та же функция, что использует PIS
                                cls = _classify_with_prev(close_t, close_prev, ema_t, ema_p, scale_t, scale_prev, EPS0, EPS1, None)
                                if cls is None:
                                    log.debug("[STATE] miss %s/%s ema%d (classify None) @ %s", sym, tf, L, open_iso)
                                    continue

                                code, _, _, _, _ = cls
                                key_ind = f"ind_live:{sym}:{tf}:ema{L}_status"
                                try:
                                    await redis.setex(key_ind, TTL_SEC, str(code))
                                except Exception as e:
                                    log.error("[SETERR] %s err=%s", key_ind, e)

                                async with cache_lock:
                                    triplet_cache.setdefault((sym, L), {})[tf] = code

                        except Exception as e:
                            log.error("[PAIR] error %s/%s: %s", sym, tf, e, exc_info=True)

            tasks = [asyncio.create_task(handle_pair(sym, tf)) for sym in symbols for tf in REQUIRED_TFS]
            await asyncio.gather(*tasks)

            # 🔸 Формирование и запись композитов (m5-m15-h1)
            async with cache_lock:
                for (sym, L), tf_map in triplet_cache.items():
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        key_trip = f"ind_live:{sym}:ema{L}_status_triplet"
                        try:
                            await redis.setex(key_trip, TTL_SEC, trip)
                        except Exception as e:
                            log.error("[SETERR] %s err=%s", key_trip, e)

            log.debug("[TICK] end")

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)