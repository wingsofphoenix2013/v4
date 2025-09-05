# indicators_perminute_live.py — ежеминутный on-demand расчёт фактических значений индикаторов и публикация в Redis KV

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, Tuple, Optional

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
    # bb20_2_{center|upper|lower}
    return f"ts_ind:{sym}:{tf}:bb20_2_{part}"


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


# 🔸 Основной воркер: ежеминутная публикация фактических значений в KV
async def run_indicators_perminute_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: Dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

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

                            # проверка готовности t-1 (как в EMA-live)
                            await _check_prev_ready_with_retry(redis, sym, tf, bar_open_ms)

                            # df до текущего бара (включая t)
                            df = await _load_df_for_current_bar(redis, sym, tf, bar_open_ms, REQUIRED_BARS_DEFAULT)
                            if df is None or df.empty:
                                return 0

                            # подбор инстансов
                            instances_tf = get_instances_by_tf(tf)
                            ema_map, rsi_inst, bb_inst, adx_inst = _pick_required_instances(instances_tf, tf)

                            # EMA (9/21/50/100/200)
                            for L, inst in ema_map.items():
                                try:
                                    vals = await compute_snapshot_values_async(inst, sym, df, precision)
                                    key = f"ind_live:{sym}:{tf}:ema{L}_value"
                                    if f"ema{L}" in vals:
                                        await redis.setex(key, TTL_SEC, vals[f"ema{L}"])
                                        written += 1
                                except Exception as e:
                                    log.debug("[EMA] %s/%s ema%d err=%s", sym, tf, L, e)

                            # RSI14
                            if rsi_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(rsi_inst, sym, df, precision)
                                    pname = f"rsi{RSI_LEN}"
                                    if pname in vals:
                                        key = f"ind_live:{sym}:{tf}:{pname}_value"
                                        await redis.setex(key, TTL_SEC, vals[pname])
                                        written += 1
                                except Exception as e:
                                    log.debug("[RSI] %s/%s err=%s", sym, tf, e)

                            # ADX_DMI (adx/plus_di/minus_di)
                            if adx_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(adx_inst, sym, df, precision)
                                    L = ADX_LEN_BY_TF[tf]
                                    for suffix in ("adx", "plus_di", "minus_di"):
                                        pname = f"adx_dmi{L}_{suffix}"
                                        if pname in vals:
                                            key = f"ind_live:{sym}:{tf}:{pname}_value"
                                            await redis.setex(key, TTL_SEC, vals[pname])
                                            written += 1
                                except Exception as e:
                                    log.debug("[ADX] %s/%s err=%s", sym, tf, e)

                            # BB(20,2,0): center/upper/lower
                            if bb_inst is not None:
                                try:
                                    vals = await compute_snapshot_values_async(bb_inst, sym, df, precision)
                                    for suffix in ("center", "upper", "lower"):
                                        pname = f"bb20_2_{suffix}"
                                        if pname in vals:
                                            key = f"ind_live:{sym}:{tf}:{pname}_value"
                                            await redis.setex(key, TTL_SEC, vals[pname])
                                            written += 1
                                except Exception as e:
                                    log.debug("[BB] %s/%s err=%s", sym, tf, e)

                        except Exception as e:
                            log.debug("[PAIR] %s/%s err=%s", sym, tf, e)

                return written

            tasks = [asyncio.create_task(handle_pair(sym, tf)) for sym in symbols for tf in _REQUIRED_TFS]
            results = await asyncio.gather(*tasks)
            total_written = sum(results)

            log.debug("[TICK] end written_keys=%d", total_written)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)