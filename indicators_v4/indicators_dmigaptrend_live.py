# indicators_dmigaptrend_live.py — ежеминутный on-demand DMI-GAP (value/bin) + TREND (по 3 точкам) и публикация в Redis KV

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Логгер
log = logging.getLogger("DMIGAP_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("DMIGAP_LIVE_INTERVAL_SEC", "30"))
REQUIRED_BARS_DEFAULT = int(os.getenv("DMIGAP_LIVE_REQUIRED_BARS", "800"))
TTL_SEC = int(os.getenv("DMIGAP_LIVE_TTL_SEC", "60"))
RETRY_SEC = int(os.getenv("DMIGAP_LIVE_RETRY_SEC", "15"))

MAX_CONCURRENCY = int(os.getenv("DMIGAP_LIVE_MAX_CONCURRENCY", "45"))
MAX_PER_SYMBOL = int(os.getenv("DMIGAP_LIVE_MAX_PER_SYMBOL", "3"))

# 🔸 Пороговые параметры тренда (см. oracle_dmigap_snapshot_aggregator.py)
S0 = float(os.getenv("DMI_GAP_S0", "2.0"))
S1 = float(os.getenv("DMI_GAP_S1", "5.0"))
JITTER = float(os.getenv("DMI_GAP_JITTER", "10.0"))

# 🔸 Таймфреймы и шаги
_REQUIRED_TFS = ("m5", "m15", "h1")
_STEP_MS: Dict[str, int] = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}
_DMI_LEN_BY_TF: Dict[str, int] = {"m5": 14, "m15": 14, "h1": 28}

# 🔸 Флор к началу бара (UTC, мс)
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    return (ts_ms // _STEP_MS[tf]) * _STEP_MS[tf]

# 🔸 Ключи Redis TS для DMI (история t-1, t-2)
def _k_plus(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{L}_plus_di"

def _k_minus(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:adx_dmi{L}_minus_di"

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

# 🔸 Загрузка OHLCV в df до текущего бара (включая бар t)
async def _load_df_for_current_bar(redis, symbol: str, tf: str, bar_open_ms: int, depth: int) -> Optional[pd.DataFrame]:
    fields = ("o", "h", "l", "c", "v")
    start_ts = bar_open_ms - (depth - 1) * _STEP_MS[tf]

    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in fields}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in fields}
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
    data = {f: [series.get(f, {}).get(ts) for ts in idx] for f in fields}
    df = pd.DataFrame(data, index=pd.to_datetime(idx, unit="ms"))
    df.index.name = "open_time"
    return df

# 🔸 Подбор нужного adx_dmi инстанса на TF (по длине)
def _pick_adx_instance(instances_tf: list, tf: str) -> Optional[dict]:
    L = _DMI_LEN_BY_TF[tf]
    for inst in instances_tf:
        if inst.get("indicator") != "adx_dmi":
            continue
        p = inst.get("params", {}) or {}
        try:
            if int(p.get("length", 0)) == L:
                return inst
        except Exception:
            continue
    return None

# 🔸 Gap-бин: клип [-100..100], шаг 5; 100 → 95, −100 остаётся −100
def _gap_bin(v: float) -> Optional[int]:
    try:
        x = max(-100.0, min(100.0, float(v)))
        b = int(x // 5) * 5
        if b == 100:
            b = 95
        return b
    except Exception:
        return None

# 🔸 Тренд по 3 точкам: slope = (gap_t − gap_{t-2})/2; ∈ {-1,0,+1} с защитой PILA(JITTER) и порогами S0/S1
def _gap_trend(gm2: float, gm1: float, gt: float) -> int:
    try:
        slope = (float(gt) - float(gm2)) / 2.0
        d1 = float(gt) - float(gm1)
        d2 = float(gm1) - float(gm2)
        if max(abs(d1), abs(d2)) > JITTER:
            return 0
        if slope >= S1:
            return +1
        if slope <= -S1:
            return -1
        if abs(slope) < S0:
            return 0
        return 0
    except Exception:
        return 0

# 🔸 Основной воркер: ежеминутная публикация DMI-GAP value/bin + TREND и триплетов в KV
async def run_indicators_dmigaptrend_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: Dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

            # значимые кеши для триплетов текущего тика
            gap_bins_trip: Dict[str, Dict[str, int]] = {}    # sym -> {tf: bin}
            trend_trip:   Dict[str, Dict[str, int]] = {}     # sym -> {tf: trend}
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
                            precision = get_precision(sym)  # не используется для gap_value (он всегда с 2 знаками), но может пригодиться позже

                            # df до текущего бара (включая t)
                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                return 0

                            # подобрать adx_dmi инстанс нужной длины
                            inst = _pick_adx_instance(get_instances_by_tf(tf), tf)
                            if inst is None:
                                return 0

                            L = _DMI_LEN_BY_TF[tf]
                            # on-demand t: берём плюс/минус DI на текущем баре
                            vals = await compute_snapshot_values_async(inst, sym, df, precision=precision)
                            plus_key = f"adx_dmi{L}_plus_di"
                            minus_key = f"adx_dmi{L}_minus_di"
                            if not vals or plus_key not in vals or minus_key not in vals:
                                return 0

                            try:
                                plus_t = float(vals[plus_key])
                                minus_t = float(vals[minus_key])
                            except Exception:
                                return 0

                            gap_t = plus_t - minus_t
                            gap_str = f"{gap_t:.2f}"

                            # записываем value и bin
                            value_key = f"ind_live:{sym}:{tf}:dmi_gap{L}_value"
                            bin_key   = f"ind_live:{sym}:{tf}:dmi_gap{L}_bin"
                            await redis.setex(value_key, TTL_SEC, gap_str)
                            written += 1

                            b = _gap_bin(gap_t)
                            if b is not None:
                                await redis.setex(bin_key, TTL_SEC, str(b))
                                written += 1
                                async with cache_lock:
                                    gap_bins_trip.setdefault(sym, {})[tf] = b

                            # тренд: нужны t-1 и t-2 (по TS), один retry
                            step = _STEP_MS[tf]
                            t1 = bar_open_ms - step
                            t2 = bar_open_ms - 2 * step

                            plus_t1  = await _ts_get_exact(redis, _k_plus(sym, tf, L), t1)
                            minus_t1 = await _ts_get_exact(redis, _k_minus(sym, tf, L), t1)
                            plus_t2  = await _ts_get_exact(redis, _k_plus(sym, tf, L), t2)
                            minus_t2 = await _ts_get_exact(redis, _k_minus(sym, tf, L), t2)

                            if any(x is None for x in (plus_t1, minus_t1, plus_t2, minus_t2)):
                                await asyncio.sleep(RETRY_SEC)
                                plus_t1  = plus_t1  if plus_t1  is not None else await _ts_get_exact(redis, _k_plus(sym, tf, L), t1)
                                minus_t1 = minus_t1 if minus_t1 is not None else await _ts_get_exact(redis, _k_minus(sym, tf, L), t1)
                                plus_t2  = plus_t2  if plus_t2  is not None else await _ts_get_exact(redis, _k_plus(sym, tf, L), t2)
                                minus_t2 = minus_t2 if minus_t2 is not None else await _ts_get_exact(redis, _k_minus(sym, tf, L), t2)

                            if None not in (plus_t1, minus_t1, plus_t2, minus_t2):
                                gap_t1 = float(plus_t1) - float(minus_t1)
                                gap_t2 = float(plus_t2) - float(minus_t2)
                                trend_code = _gap_trend(gap_t2, gap_t1, gap_t)
                                trend_key = f"ind_live:{sym}:{tf}:dmi_gap{L}_trend"
                                await redis.setex(trend_key, TTL_SEC, str(trend_code))
                                written += 1
                                async with cache_lock:
                                    trend_trip.setdefault(sym, {})[tf] = trend_code

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
                    # BIN триплет
                    tf_map = gap_bins_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:dmi_gap_bin_triplet", TTL_SEC, trip)
                        except Exception:
                            pass
                    # TREND триплет
                    tf_map = trend_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5", "m15", "h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:dmi_gap_trend_triplet", TTL_SEC, trip)
                        except Exception:
                            pass

            log.debug("[TICK] end written_keys=%d", total_written)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)