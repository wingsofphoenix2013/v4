# indicators_ema_status_live.py — ежеминутный on-demand EMA-status: Этап 5 (KV + композиты m5-m15-h1)

import os
import asyncio
import logging
from datetime import datetime
import pandas as pd

from indicators.compute_and_store import compute_snapshot_values_async

log = logging.getLogger("EMA_STATUS_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("EMA_STATUS_LIVE_INTERVAL_SEC", "30"))
REQUIRED_TFS = ("m5", "m15", "h1")
REQUIRED_BARS_DEFAULT = int(os.getenv("EMA_STATUS_LIVE_REQUIRED_BARS", "800"))
RETRY_SEC = int(os.getenv("EMA_STATUS_LIVE_RETRY_SEC", "15"))

EPS0 = float(os.getenv("EMA_STATUS_LIVE_EPS0", "0.05"))
EPS1 = float(os.getenv("EMA_STATUS_LIVE_EPS1", "0.02"))

MAX_CONCURRENCY = int(os.getenv("EMA_STATUS_LIVE_MAX_CONCURRENCY", "45"))
MAX_PER_SYMBOL = int(os.getenv("EMA_STATUS_LIVE_MAX_PER_SYMBOL", "3"))
TTL_SEC = int(os.getenv("EMA_STATUS_LIVE_TTL_SEC", "60"))

# 🔸 Вспомогательные мапы
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

# 🔸 Маппинг кода → label (для логов)
STATE_LABELS = {
    0: "below_away",
    1: "below_towards",
    2: "equal",
    3: "above_towards",
    4: "above_away",
}

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

# 🔸 Ключи Redis TS (prev)
def _k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def _k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def _k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def _k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

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

# 🔸 Проверка готовности t−1 из TS с одним отложенным retry
async def _check_prev_ready_with_retry(redis, symbol: str, tf: str, bar_open_ms: int) -> bool:
    step = _STEP_MS[tf]
    prev_ms = bar_open_ms - step

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
        log.debug("[PREV] ok %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
        return True

    log.debug("[PREV] miss %s/%s retry in %ds", symbol, tf, RETRY_SEC)
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
        log.debug("[PREV] ok-after-retry %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
    else:
        log.debug("[PREV] still-miss %s/%s t-1=%s", symbol, tf, datetime.utcfromtimestamp(prev_ms/1000).isoformat())
    return ready

# 🔸 Поиск нужных инстансов под EMA, ATR(14), BB(20,2) на данном TF
def _pick_required_instances(instances: list, ema_lens: list[int]):
    ema_by_len = {}
    atr14 = None
    bb_20_2 = None

    for inst in instances:
        ind = inst.get("indicator")
        params = inst.get("params", {})
        if ind == "ema":
            try:
                L = int(params.get("length"))
                if L in ema_lens and L not in ema_by_len:
                    ema_by_len[L] = inst
            except Exception:
                continue
        elif ind == "atr":
            try:
                if int(params.get("length", 0)) == 14 and atr14 is None:
                    atr14 = inst
            except Exception:
                continue
        elif ind == "bb":
            try:
                length_ok = int(params.get("length", 0)) == 20
                std_raw = float(params.get("std", 0))
                std_ok = abs(std_raw - 2.0) < 1e-9
                if length_ok and std_ok and bb_20_2 is None:
                    bb_20_2 = inst
            except Exception:
                continue

    return ema_by_len, atr14, bb_20_2

# 🔸 Вычислить на t через snapshot-инстансы: close_t, ema_t_map, scale_t
async def _features_t_via_snapshot(instances_tf: list, symbol: str, df: pd.DataFrame, precision: int, tf: str):
    ema_by_len, atr14, bb_20_2 = _pick_required_instances(instances_tf, EMA_LENS)

    # close_t — последний close в df
    close_t = float(df["c"].iloc[-1])

    # ema_t_map
    ema_t_map = {}
    for L, inst in ema_by_len.items():
        vals = await compute_snapshot_values_async(inst, symbol, df, precision)
        key = f"ema{L}"
        if key in vals:
            try:
                ema_t_map[L] = float(vals[key])
            except Exception:
                pass

    # scale_t
    scale_t = None
    if tf in ("m5", "m15"):
        if atr14 is not None:
            v = await compute_snapshot_values_async(atr14, symbol, df, precision)
            vnum = None
            try:
                vnum = float(v.get("atr14")) if v and "atr14" in v else None
            except Exception:
                vnum = None
            if vnum is not None and vnum > 0.0:
                scale_t = vnum
        if scale_t is None and bb_20_2 is not None:
            v = await compute_snapshot_values_async(bb_20_2, symbol, df, precision)
            bbu = None; bbl = None
            for k, s in (v or {}).items():
                if k.endswith("_upper"):
                    bbu = float(s)
                elif k.endswith("_lower"):
                    bbl = float(s)
            if bbu is not None and bbl is not None and (bbu - bbl) > 0.0:
                scale_t = bbu - bbl
    else:
        if bb_20_2 is not None:
            v = await compute_snapshot_values_async(bb_20_2, symbol, df, precision)
            bbu = None; bbl = None
            for k, s in (v or {}).items():
                if k.endswith("_upper"):
                    bbu = float(s)
                elif k.endswith("_lower"):
                    bbl = float(s)
            if bbu is not None and bbl is not None and (bbu - bbl) > 0.0:
                scale_t = bbu - bbl

    return close_t, ema_t_map, scale_t

# 🔸 Классификация (как в indicators_ema_status.py)
def _classify(close_t: float, close_p: float,
              ema_t: float, ema_p: float,
              scale_t: float, scale_p: float,
              eps0: float, eps1: float):
    if None in (close_t, close_p, ema_t, ema_p, scale_t, scale_p):
        return None
    if scale_t <= 0.0 or scale_p <= 0.0:
        return None

    nd_t = (close_t - ema_t) / scale_t
    nd_p = (close_p - ema_p) / scale_p
    d_t = abs(nd_t)
    d_p = abs(nd_p)
    delta_d = d_t - d_p

    if d_t <= eps0:
        return 2, STATE_LABELS[2], nd_t, d_t, delta_d

    above = nd_t > 0.0
    if delta_d >= eps1:
        code = 4 if above else 0
    elif delta_d <= -eps1:
        code = 3 if above else 1
    else:
        code = 3 if above else 1  # гистерезис → towards по умолчанию

    return code, STATE_LABELS[code], nd_t, d_t, delta_d

# 🔸 Основной воркер (Этап 5: запись индивидуальных KV + композиты)
async def run_indicators_ema_status_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            tick_iso = datetime.utcnow().isoformat()
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start @ %s, symbols=%d", tick_iso, len(symbols))

            now_ms = int(datetime.utcnow().timestamp() * 1000)

            # собираем коды для композитов: {(sym, L): {"m5":code, "m15":code, "h1":code}}
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

                            # DF по t
                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                log.debug("[DF] miss %s/%s @ %s", sym, tf, datetime.utcfromtimestamp(bar_open_ms/1000).isoformat())
                                return

                            # prev готовность (TS + retry)
                            await _check_prev_ready_with_retry(redis, sym, tf, bar_open_ms)
                            step = _STEP_MS[tf]
                            prev_ms = bar_open_ms - step

                            # t через snapshot-инстансы
                            instances_tf = get_instances_by_tf(tf)
                            close_t, ema_t_map, scale_t = await _features_t_via_snapshot(instances_tf, sym, df, precision, tf)

                            # prev из TS (с fallback через snapshot на df[:prev])
                            close_prev = await _ts_get_exact(redis, _k_close(sym, tf), prev_ms)
                            if tf in ("m5", "m15"):
                                atr_prev = await _ts_get_exact(redis, _k_atr(sym, tf), prev_ms)
                                if atr_prev is not None and atr_prev > 0.0:
                                    scale_prev = atr_prev
                                else:
                                    bbu = await _ts_get_exact(redis, _k_bb(sym, tf, "upper"), prev_ms)
                                    bbl = await _ts_get_exact(redis, _k_bb(sym, tf, "lower"), prev_ms)
                                    scale_prev = (bbu - bbl) if (bbu is not None and bbl is not None) else None
                            else:
                                bbu = await _ts_get_exact(redis, _k_bb(sym, tf, "upper"), prev_ms)
                                bbl = await _ts_get_exact(redis, _k_bb(sym, tf, "lower"), prev_ms)
                                scale_prev = (bbu - bbl) if (bbu is not None and bbl is not None) else None

                            ema_prev_map = {}
                            for L in EMA_LENS:
                                ema_prev_map[L] = await _ts_get_exact(redis, _k_ema(sym, tf, L), prev_ms)

                            if (close_prev is None) or (scale_prev is None) or any(ema_prev_map[L] is None for L in EMA_LENS):
                                try:
                                    pos = df.index.get_loc(pd.to_datetime(prev_ms, unit="ms"))
                                    df_prev = df.iloc[: pos + 1]
                                except KeyError:
                                    df_prev = df.iloc[:-1] if len(df) > 1 else df

                                if close_prev is None and not df_prev.empty:
                                    close_prev = float(df_prev["c"].iloc[-1])

                                if scale_prev is None and not df_prev.empty:
                                    ema_by_len, atr14, bb_20_2 = _pick_required_instances(instances_tf, EMA_LENS)
                                    if tf in ("m5", "m15") and atr14 is not None:
                                        v = await compute_snapshot_values_async(atr14, sym, df_prev, precision)
                                        try:
                                            vnum = float(v.get("atr14")) if v and "atr14" in v else None
                                        except Exception:
                                            vnum = None
                                        if vnum is not None and vnum > 0.0:
                                            scale_prev = vnum
                                    if scale_prev is None and bb_20_2 is not None:
                                        v = await compute_snapshot_values_async(bb_20_2, sym, df_prev, precision)
                                        bbu = None; bbl = None
                                        for k, s in (v or {}).items():
                                            if k.endswith("_upper"):
                                                bbu = float(s)
                                            elif k.endswith("_lower"):
                                                bbl = float(s)
                                        if bbu is not None and bbl is not None and (bbu - bbl) > 0.0:
                                            scale_prev = bbu - bbl

                                if not df_prev.empty:
                                    ema_by_len, _, _ = _pick_required_instances(instances_tf, EMA_LENS)
                                    for L, inst in ema_by_len.items():
                                        if ema_prev_map.get(L) is None:
                                            v = await compute_snapshot_values_async(inst, sym, df_prev, precision)
                                            key = f"ema{L}"
                                            if key in v:
                                                try:
                                                    ema_prev_map[L] = float(v[key])
                                                except Exception:
                                                    pass

                            open_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

                            for L in EMA_LENS:
                                ema_t = ema_t_map.get(L)
                                ema_p = ema_prev_map.get(L)
                                if None in (close_t, close_prev, ema_t, ema_p, scale_t, scale_prev):
                                    log.debug("[STATE] miss %s/%s ema%d (insufficient features) @ %s", sym, tf, L, open_iso)
                                    continue

                                cls = _classify(close_t, close_prev, ema_t, ema_p, scale_t, scale_prev, EPS0, EPS1)
                                if cls is None:
                                    log.debug("[STATE] miss %s/%s ema%d (classify None) @ %s", sym, tf, L, open_iso)
                                    continue

                                code, label, nd, d, delta_d = cls
                                # Этап 5: запись индивидуального статуса в KV
                                key_ind = f"ind_live:{sym}:{tf}:ema{L}_status"
                                try:
                                    await redis.setex(key_ind, TTL_SEC, str(code))
                                    log.debug("[SET] %s=%s ttl=%ds", key_ind, code, TTL_SEC)
                                except Exception as e:
                                    log.error("[SETERR] %s err=%s", key_ind, e)

                                # копим для композита
                                async with cache_lock:
                                    slot = triplet_cache.setdefault((sym, L), {})
                                    slot[tf] = code

                        except Exception as e:
                            log.error("[PAIR] error %s/%s: %s", sym, tf, e, exc_info=True)

            tasks = []
            for sym in symbols:
                for tf in REQUIRED_TFS:
                    tasks.append(asyncio.create_task(handle_pair(sym, tf)))

            await asyncio.gather(*tasks)

            # 🔸 Формирование и запись композитов (m5-m15-h1) после обработки всех TF
            written_triplets = 0
            async with cache_lock:
                for (sym, L), tf_map in triplet_cache.items():
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        triplet_val = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        key_trip = f"ind_live:{sym}:ema{L}_status_triplet"
                        try:
                            await redis.setex(key_trip, TTL_SEC, triplet_val)
                            written_triplets += 1
                            log.debug("[SET] %s=%s ttl=%ds", key_trip, triplet_val, TTL_SEC)
                        except Exception as e:
                            log.error("[SETERR] %s err=%s", key_trip, e)

            log.debug("[TICK] end, triplets=%d", written_triplets)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)