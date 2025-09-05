# indicators_perminute_live.py — ежеминутный on-demand расчёт фактических значений индикаторов + корзины/триплеты (RSI/ADX/BB) с публикацией в Redis KV

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Tuple, Optional

import pandas as pd
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Логгер
log = logging.getLogger("PERMIN_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("PERMIN_LIVE_INTERVAL_SEC", "60"))
REQUIRED_BARS_DEFAULT = int(os.getenv("PERMIN_LIVE_REQUIRED_BARS", "800"))
TTL_SEC = int(os.getenv("PERMIN_LIVE_TTL_SEC", "120"))
RETRY_SEC = int(os.getenv("PERMIN_LIVE_RETRY_SEC", "15"))

MAX_CONCURRENCY = int(os.getenv("PERMIN_LIVE_MAX_CONCURRENCY", "16"))
MAX_PER_SYMBOL = int(os.getenv("PERMIN_LIVE_MAX_PER_SYMBOL", "2"))

# 🔸 Таймфреймы и поля TS
_STEP_MS: Dict[str, int] = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}
_FIELDS = ("o", "h", "l", "c", "v")
_REQUIRED_TFS = ("m5", "m15", "h1")

# 🔸 Наборы индикаторов/параметров
EMA_LENS = [9, 21, 50, 100, 200]
RSI_LEN = 14
BB_CONF = (20, 2.0, 0)  # (length, std, shift)
ADX_LEN_BY_TF: Dict[str, int] = {"m5": 14, "m15": 14, "h1": 28}

# 🔸 Флор к началу бара (UTC, мс)
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    return (ts_ms // _STEP_MS[tf]) * _STEP_MS[tf]

# 🔸 Ключи Redis TS
def _k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"

def _k_atr14(sym: str, tf: str) -> Optional[str]:
    return f"ts_ind:{sym}:{tf}:atr14" if tf in ("m5", "m15") else None

def _k_bb(sym: str, tf: str, part: str) -> str:
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

def _k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

# 🔸 Чтение ровно одной точки (TS.RANGE [t,t])
async def _ts_get_exact(redis, key: Optional[str], ts_ms: int) -> Optional[float]:
    if not key:
        return None
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Готовность t−1 (как в EMA-live): close_prev и scale_prev (ATR14 или BB width) + EMA prev для наших длин
async def _check_prev_ready_with_retry(redis, symbol: str, tf: str, bar_open_ms: int) -> bool:
    step = _STEP_MS[tf]
    prev_ms = bar_open_ms - step

    # попытка №1
    close_prev = await _ts_get_exact(redis, _k_close(symbol, tf), prev_ms)

    # scale_prev: m5/m15 — ATR14 или BB, h1 — BB
    if tf in ("m5", "m15"):
        atr_prev = await _ts_get_exact(redis, _k_atr14(symbol, tf), prev_ms)
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
        log.debug("[PREV] ok %s/%s @ %s", symbol, tf, datetime.utcfromtimestamp(prev_ms / 1000).isoformat())
        return True

    # повтор с задержкой
    log.debug("[PREV] miss %s/%s retry in %ds", symbol, tf, RETRY_SEC)
    await asyncio.sleep(RETRY_SEC)

    close_prev = await _ts_get_exact(redis, _k_close(symbol, tf), prev_ms)
    if tf in ("m5", "m15"):
        atr_prev = await _ts_get_exact(redis, _k_atr14(symbol, tf), prev_ms)
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
    log.debug(
        "[PREV] %s %s @ %s",
        "ok-after-retry" if ready else "still-miss",
        f"{symbol}/{tf}",
        datetime.utcfromtimestamp(prev_ms / 1000).isoformat(),
    )
    return ready

# 🔸 Загрузка OHLCV в df до текущего бара (включая бар t)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> Optional[pd.DataFrame]:
    start_ts = bar_open_ms - (depth - 1) * _STEP_MS[tf]

    # значимый блок: параллельные TS.RANGE по всем полям
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

# 🔸 Подбор нужных инстансов на TF
def _pick_required_instances(instances_tf: list, tf: str) -> Tuple[Dict[int, dict], Optional[dict], Optional[dict], Optional[dict]]:
    ema_by_len: Dict[int, dict] = {}
    rsi_inst: Optional[dict] = None
    bb_inst: Optional[dict] = None
    adx_inst: Optional[dict] = None

    for inst in instances_tf:
        ind = inst.get("indicator")
        p = inst.get("params", {}) or {}
        try:
            if ind == "ema":
                L = int(p.get("length", 0))
                if L in EMA_LENS and L not in ema_by_len:
                    ema_by_len[L] = inst
            elif ind == "rsi" and int(p.get("length", 0)) == RSI_LEN and rsi_inst is None:
                rsi_inst = inst
            elif ind == "bb":
                L = int(p.get("length", 0))
                std = float(p.get("std", 0.0))
                if L == BB_CONF[0] and abs(std - BB_CONF[1]) < 1e-9 and bb_inst is None:
                    bb_inst = inst
            elif ind == "adx_dmi" and int(p.get("length", 0)) == ADX_LEN_BY_TF[tf] and adx_inst is None:
                adx_inst = inst
        except Exception:
            continue

    return ema_by_len, rsi_inst, bb_inst, adx_inst

# 🔸 Биннинг RSI/ADX (шаг 5, cap=95)
def _bin_0_100_step5(x: float) -> Optional[int]:
    try:
        v = max(0.0, min(100.0, float(x)))
        b = int(v // 5) * 5
        return 95 if b == 100 else b
    except Exception:
        return None

# 🔸 Биннинг BB: 12 корзин относительно верх/низ (по текущей цене)
def _bb_bin_by_price(price: float, lower: float, upper: float) -> Optional[int]:
    try:
        width = float(upper) - float(lower)
        if width <= 0:
            return None
        bucket = width / 6.0
        if price >= upper + 2 * bucket:
            return 0
        if price >= upper + bucket:
            return 1
        if price >= upper:
            return 2
        if price >= lower:
            # 3..8 сверху вниз
            k = int((price - lower) // bucket)  # 0..5
            if k < 0:
                k = 0
            if k > 5:
                k = 5
            return 8 - k
        if price <= lower - 2 * bucket:
            return 11
        if price <= lower - bucket:
            return 10
        return 9
    except Exception:
        return None

# 🔸 Основной воркер: ежеминутная публикация фактических значений + корзин/триплетов в KV
async def run_indicators_perminute_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: Dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

            # значимые кеши для триплетов текущего тика
            rsi_bins_trip: Dict[str, Dict[str, int]] = {}   # sym -> {tf: bin}
            adx_bins_trip: Dict[str, Dict[str, int]] = {}   # sym -> {tf: bin}
            bb_bins_trip:  Dict[str, Dict[str, int]] = {}   # sym -> {tf: bin}
            cache_lock = asyncio.Lock()

            # значимый блок: обработка (symbol, tf) с ограничением параллелизма
            async def handle_pair(sym: str, tf: str) -> int:
                written = 0
                async with gate:
                    if sym not in symbol_semaphores:
                        symbol_semaphores[sym] = asyncio.Semaphore(MAX_PER_SYMBOL)
                    async with symbol_semaphores[sym]:
                        try:
                            bar_open_ms = _floor_to_bar_ms(now_ms, tf)
                            precision = get_precision(sym)

                            # готовность t-1 (как в EMA-live)
                            await _check_prev_ready_with_retry(redis, sym, tf, bar_open_ms)

                            # df до текущего бара (включая t)
                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                return 0

                            # подбор инстансов
                            instances_tf = get_instances_by_tf(tf)
                            ema_map, rsi_inst, bb_inst, adx_inst = _pick_required_instances(instances_tf, tf)

                            # ➤ EMA (фактические значения)
                            for L, inst in ema_map.items():
                                try:
                                    vals = await compute_snapshot_values_async(inst, sym, df, precision)
                                    key = f"ind_live:{sym}:{tf}:ema{L}_value"
                                    if f"ema{L}" in vals:
                                        await redis.setex(key, TTL_SEC, vals[f"ema{L}"])
                                        written += 1
                                except Exception as e:
                                    log.debug("[EMA] %s/%s ema%d err=%s", sym, tf, L, e)

                            # ➤ RSI14 (value + bin)
                            rsi_bin_this_tf: Optional[int] = None
                            if rsi_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(rsi_inst, sym, df, precision)
                                    pname = f"rsi{RSI_LEN}"
                                    if pname in vals:
                                        v = vals[pname]
                                        await redis.setex(f"ind_live:{sym}:{tf}:{pname}_value", TTL_SEC, v)
                                        written += 1
                                        try:
                                            rsi_bin_this_tf = _bin_0_100_step5(float(v))
                                        except Exception:
                                            rsi_bin_this_tf = None
                                        if rsi_bin_this_tf is not None:
                                            await redis.setex(f"ind_live:{sym}:{tf}:{pname}_bin", TTL_SEC, str(rsi_bin_this_tf))
                                            written += 1
                                            async with cache_lock:
                                                rsi_bins_trip.setdefault(sym, {})[tf] = rsi_bin_this_tf
                                except Exception as e:
                                    log.debug("[RSI] %s/%s err=%s", sym, tf, e)

                            # ➤ ADX_DMI (value + bin по ADX)
                            adx_bin_this_tf: Optional[int] = None
                            if adx_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(adx_inst, sym, df, precision)
                                    L = ADX_LEN_BY_TF[tf]
                                    adx_name = f"adx_dmi{L}_adx"
                                    if adx_name in vals:
                                        v = vals[adx_name]
                                        await redis.setex(f"ind_live:{sym}:{tf}:{adx_name}_value", TTL_SEC, v)
                                        written += 1
                                        try:
                                            adx_bin_this_tf = _bin_0_100_step5(float(v))
                                        except Exception:
                                            adx_bin_this_tf = None
                                        if adx_bin_this_tf is not None:
                                            await redis.setex(f"ind_live:{sym}:{tf}:{adx_name}_bin", TTL_SEC, str(adx_bin_this_tf))
                                            written += 1
                                            async with cache_lock:
                                                adx_bins_trip.setdefault(sym, {})[tf] = adx_bin_this_tf
                                except Exception as e:
                                    log.debug("[ADX] %s/%s err=%s", sym, tf, e)

                            # ➤ BB(20,2,0): center/upper/lower (values) + bin по mark price
                            bb_bin_this_tf: Optional[int] = None
                            if bb_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(bb_inst, sym, df, precision)
                                    # значения полос
                                    for suffix in ("center", "upper", "lower"):
                                        v = (vals.get(f"bb20_2_0_{suffix}") or vals.get(f"bb20_2_{suffix}"))
                                        if v is not None:
                                            await redis.setex(f"ind_live:{sym}:{tf}:bb20_2_0_{suffix}_value", TTL_SEC, v)
                                            written += 1

                                    # корзина по mark price
                                    try:
                                        mark_raw = await redis.get(f"price:{sym}")
                                        mark = float(mark_raw) if mark_raw is not None else None
                                    except Exception:
                                        mark = None

                                    if mark is not None:
                                        try:
                                            upper = (float(vals.get("bb20_2_0_upper"))
                                                     if vals and "bb20_2_0_upper" in vals
                                                     else float(vals.get("bb20_2_upper")) if vals and "bb20_2_upper" in vals
                                                     else None)
                                            lower = (float(vals.get("bb20_2_0_lower"))
                                                     if vals and "bb20_2_0_lower" in vals
                                                     else float(vals.get("bb20_2_lower")) if vals and "bb20_2_lower" in vals
                                                     else None)
                                        except Exception:
                                            upper = lower = None

                                        if upper is not None and lower is not None:
                                            bb_bin_this_tf = _bb_bin_by_price(mark, lower, upper)
                                            if bb_bin_this_tf is not None:
                                                await redis.setex(f"ind_live:{sym}:{tf}:bb20_2_0_bin", TTL_SEC, str(bb_bin_this_tf))
                                                written += 1
                                                async with cache_lock:
                                                    bb_bins_trip.setdefault(sym, {})[tf] = bb_bin_this_tf
                                except Exception as e:
                                    log.debug("[BB] %s/%s err=%s", sym, tf, e)

                        except Exception as e:
                            log.debug("[PAIR] %s/%s err=%s", sym, tf, e)

                return written

            # параллельная обработка всех (sym, tf)
            tasks = [asyncio.create_task(handle_pair(sym, tf)) for sym in symbols for tf in _REQUIRED_TFS]
            results = await asyncio.gather(*tasks)
            total_written = sum(results)

            # ➤ Формирование и запись триплетов (если собраны все 3 TF)
            async with cache_lock:
                for sym in symbols:
                    # RSI триплет
                    tf_map = rsi_bins_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:rsi14_bin_triplet", TTL_SEC, trip)
                        except Exception:
                            pass

                    # ADX триплет (фикс. набор: m5:14, m15:14, h1:28)
                    tf_map = adx_bins_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:adx_adx_bin_triplet", TTL_SEC, trip)
                        except Exception:
                            pass

                    # BB триплет
                    tf_map = bb_bins_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:bb20_2_0_bin_triplet", TTL_SEC, trip)
                        except Exception:
                            pass

            log.debug("[TICK] end written_keys=%d", total_written)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)