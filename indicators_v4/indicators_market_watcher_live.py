# indicators_market_watcher_live.py — regime9 v2 LIVE: on-demand по текущему бару, гистерезис в Redis, запись в KV + триплет

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, List, Tuple

import pandas as pd
from regime9_core import RegimeState, RegimeParams, decide_regime_code
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Логгер
log = logging.getLogger("MRW_LIVE")

# 🔸 Конфиг
INTERVAL_SEC = int(os.getenv("MRW_LIVE_INTERVAL_SEC", "60"))
TTL_SEC = int(os.getenv("MRW_LIVE_TTL_SEC", "120"))
RETRY_SEC = int(os.getenv("MRW_LIVE_RETRY_SEC", "15"))
MAX_CONCURRENCY = int(os.getenv("MRW_LIVE_MAX_CONCURRENCY", "16"))
MAX_PER_SYMBOL = int(os.getenv("MRW_LIVE_MAX_PER_SYMBOL", "2"))
REQUIRED_TFS = ("m5", "m15", "h1")

N_PCT = int(os.getenv("MRW_N_PCT", "200"))    # окно p30/p70
N_ACC = int(os.getenv("MRW_N_ACC", "50"))     # окно z-score ΔMACD
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5"))

HYST_TREND_BARS = int(os.getenv("MRW_R9_HYST_TREND_BARS", "2"))
HYST_SUB_BARS   = int(os.getenv("MRW_R9_HYST_SUB_BARS", "1"))

# 🔸 Таймшаги TF
_STEP_MS: Dict[str, int] = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Хранилище live-состояния (гистерезис) в Redis (Hash)
def _state_key_live(symbol: str, tf: str) -> str:
    return f"mrw_state_live:{symbol}:{tf}"

async def _load_state_live(redis, symbol: str, tf: str) -> RegimeState:
    data = await redis.hgetall(_state_key_live(symbol, tf))
    if not data:
        return RegimeState()
    try:
        return RegimeState(
            core=data.get("core", "flat"),
            core_cnt=int(data.get("core_cnt", 0)),
            sub=data.get("sub", "stable"),
            sub_cnt=int(data.get("sub_cnt", 0)),
        )
    except Exception:
        return RegimeState()

async def _save_state_live(redis, symbol: str, tf: str, st: RegimeState) -> None:
    await redis.hset(
        _state_key_live(symbol, tf),
        mapping={
            "core": st.core,
            "core_cnt": str(st.core_cnt),
            "sub": st.sub,
            "sub_cnt": str(st.sub_cnt),
        },
    )

# 🔸 Ключи TS
def _ts_key(sym: str, tf: str, name: str) -> str:
    return f"ts_ind:{sym}:{tf}:{name}"

# 🔸 Флор к началу бара
def _floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = _STEP_MS[tf]
    return (ts_ms // step) * step

# 🔸 Точка TS ровно на open_time
async def _ts_get_exact(redis, key: str, ts_ms: int) -> Optional[float]:
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Загрузка OHLCV до текущего бара t (включительно)
async def _load_df(redis, symbol: str, tf: str, bar_open_ms: int, depth: int = 800) -> Optional[pd.DataFrame]:
    fields = ("o", "h", "l", "c", "v")
    start_ts = bar_open_ms - (depth - 1) * _STEP_MS[tf]
    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in fields}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, bar_open_ms) for f in fields}
    res = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for f, r in zip(tasks.keys(), res):
        if isinstance(r, Exception):
            log.debug("[TSERR] %s err=%s", keys[f], r); continue
        if r:
            series[f] = {int(ts): float(v) for ts, v in r if v is not None}

    if "c" not in series or not series["c"]:
        return None

    idx = sorted(series["c"].keys())
    data = {f: [series.get(f, {}).get(ts) for ts in idx] for f in fields}
    df = pd.DataFrame(data, index=pd.to_datetime(idx, unit="ms"))
    df.index.name = "open_time"
    return df

# 🔸 Подбор нужных инстансов на TF
def _pick_instances(instances_tf: list, tf: str) -> Tuple[Optional[dict], Optional[dict], Optional[dict], Optional[dict], Optional[dict]]:
    ema21 = macd12 = adx = bb = atr14 = None
    for inst in instances_tf:
        ind = inst.get("indicator")
        p = inst.get("params", {}) or {}
        try:
            if ind == "ema" and int(p.get("length", 0)) == 21 and ema21 is None:
                ema21 = inst
            elif ind == "macd" and int(p.get("fast", 0)) == 12 and macd12 is None:
                macd12 = inst
            elif ind == "adx_dmi":
                need = 14 if tf in ("m5","m15") else 28
                if int(p.get("length", 0)) == need and adx is None:
                    adx = inst
            elif ind == "bb":
                if int(p.get("length", 0)) == 20 and abs(float(p.get("std", 0.0)) - 2.0) < 1e-9 and bb is None:
                    bb = inst
            elif ind == "atr" and tf in ("m5","m15") and int(p.get("length", 0)) == 14 and atr14 is None:
                atr14 = inst
        except Exception:
            continue
    return ema21, macd12, adx, bb, atr14

# 🔸 Построение windows (до t-1) + on-demand t для нужных метрик
async def _build_windows_and_current(redis, symbol: str, tf: str, bar_open_ms: int, df: pd.DataFrame,
                                     ema21, macd12, adx, bb, atr14, precision: int) -> Optional[dict]:
    step = _STEP_MS[tf]
    t_prev = bar_open_ms - step
    t_start_pct = bar_open_ms - (N_PCT - 1) * step
    t_start_acc = bar_open_ms - N_ACC * step

    # читаем окна до t-1 из TS
    ts_calls = [
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "ema21"), t_prev, t_prev),                 # ema t-1
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "macd12_macd_hist"), t_start_acc, t_prev), # macd window (до t-1)
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "adx_dmi14_adx" if tf in ("m5","m15") else "adx_dmi28_adx"), t_start_pct, t_prev),
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "bb20_2_0_upper"), t_start_pct, t_prev),
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "bb20_2_0_lower"), t_start_pct, t_prev),
        redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "bb20_2_0_center"), t_start_pct, t_prev),
    ]
    if tf in ("m5","m15"):
        ts_calls.append(redis.execute_command("TS.RANGE", _ts_key(symbol, tf, "atr14"), t_start_pct, t_prev))

    out = await asyncio.gather(*ts_calls, return_exceptions=True)

    def _vals(series): return [float(v) for _, v in series] if series and not isinstance(series, Exception) else []

    ema_t1_list  = _vals(out[0])
    macd_win_pre = _vals(out[1])
    adx_win_pre  = _vals(out[2])
    bbu_pre      = _vals(out[3])
    bbl_pre      = _vals(out[4])
    bbc_pre      = _vals(out[5])
    atr_win_pre  = _vals(out[6]) if (tf in ("m5","m15") and len(out) > 6) else None

    # on-demand t для нужных метрик
    ema_t = macd_t = adx_t = bbu_t = bbl_t = bbc_t = atr_t = None

    if ema21 is not None:
        v = await compute_snapshot_values_async(ema21, symbol, df, precision)
        try: ema_t = float(v.get("ema21")) if v and "ema21" in v else None
        except: ema_t = None

    if macd12 is not None:
        v = await compute_snapshot_values_async(macd12, symbol, df, precision)
        try: macd_t = float(v.get("macd12_macd_hist")) if v and "macd12_macd_hist" in v else None
        except: macd_t = None

    if adx is not None:
        v = await compute_snapshot_values_async(adx, symbol, df, precision)
        # имя зависит от длины
        for k in ("adx_dmi14_adx", "adx_dmi28_adx"):
            if v and k in v:
                try: adx_t = float(v[k]); break
                except: pass

    if bb is not None:
        v = await compute_snapshot_values_async(bb, symbol, df, precision)
        if v:
            try:
                bbu_t = float(v.get("bb20_2_0_upper") or v.get("bb20_2_upper")) if ("bb20_2_0_upper" in v or "bb20_2_upper" in v) else None
                bbl_t = float(v.get("bb20_2_0_lower") or v.get("bb20_2_lower")) if ("bb20_2_0_lower" in v or "bb20_2_lower" in v) else None
                bbc_t = float(v.get("bb20_2_0_center") or v.get("bb20_2_center")) if ("bb20_2_0_center" in v or "bb20_2_center" in v) else None
            except: bbu_t = bbl_t = bbc_t = None

    if tf in ("m5","m15") and atr14 is not None:
        v = await compute_snapshot_values_async(atr14, symbol, df, precision)
        try: atr_t = float(v.get("atr14")) if v and "atr14" in v else None
        except: atr_t = None

    # условия достаточности
    if None in (ema_t, macd_t, adx_t, bbu_t, bbl_t, bbc_t):  # atr_t может быть None на h1
        return None

    ema_t1 = ema_t1_list[-1] if ema_t1_list else None
    if ema_t1 is None and len(df) > 1 and ema21 is not None:
        try:
            v_prev = await compute_snapshot_values_async(ema21, symbol, df.iloc[:-1], precision)
            ema_t1 = float(v_prev.get("ema21")) if v_prev and "ema21" in v_prev else None
        except: ema_t1 = None
    if ema_t1 is None:
        return None

    # построение окон (включая t)
    macd_vals = (macd_win_pre + [macd_t])[-(N_ACC+1):]
    dhist = [macd_vals[i+1] - macd_vals[i] for i in range(len(macd_vals)-1)]

    adx_win = (adx_win_pre + [adx_t])[-N_PCT:]
    bb_u_win = (bbu_pre + [bbu_t])[-N_PCT:]
    bb_l_win = (bbl_pre + [bbl_t])[-N_PCT:]
    bb_c_win = (bbc_pre + [bbc_t])[-N_PCT:]
    atr_win = ((atr_win_pre + [atr_t]) if atr_win_pre is not None else None)
    if atr_win is not None:
        atr_win = atr_win[-N_PCT:]

    return {
        "ema_t1": ema_t1,
        "ema_t": ema_t,
        "macd_t1": macd_vals[-2],
        "macd_t": macd_vals[-1],
        "dhist_win": dhist[-N_ACC:],
        "adx_win": adx_win,
        "bb_u_win": bb_u_win,
        "bb_l_win": bb_l_win,
        "bb_c_win": bb_c_win,
        "atr_t": (atr_t if tf in ("m5","m15") else None),
        "atr_win": (atr_win if tf in ("m5","m15") else None),
    }

# 🔸 Основной воркер: LIVE regime9 → KV + triplet
async def run_indicators_market_watcher_live(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    symbol_semaphores: Dict[str, asyncio.Semaphore] = {}
    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            symbols = list(get_active_symbols() or [])
            log.debug("[TICK] start symbols=%d", len(symbols))

            # кеш для триплетов текущего тика
            codes_trip: Dict[str, Dict[str, int]] = {}
            cache_lock = asyncio.Lock()

            async def handle_pair(sym: str, tf: str) -> int:
                written = 0
                async with gate:
                    if sym not in symbol_semaphores:
                        symbol_semaphores[sym] = asyncio.Semaphore(MAX_PER_SYMBOL)
                    async with symbol_semaphores[sym]:
                        try:
                            bar_open_ms = _floor_to_bar_ms(now_ms, tf)
                            precision = get_precision(sym)

                            df = await _load_df(redis, sym, tf, bar_open_ms, 800)
                            if df is None or df.empty:
                                return 0

                            ema21, macd12, adx, bb, atr14 = _pick_instances(get_instances_by_tf(tf), tf)
                            if any(x is None for x in (ema21, macd12, adx, bb)):
                                return 0  # без этих четырёх фич решение невозможно

                            feats = await _build_windows_and_current(redis, sym, tf, bar_open_ms, df, ema21, macd12, adx, bb, atr14, precision)
                            if feats is None:
                                return 0

                            # гистерезисное состояние (LIVE)
                            state = await _load_state_live(redis, sym, tf)

                            # решение
                            code, new_state, diag = decide_regime_code(
                                tf,
                                feats,
                                state,
                                RegimeParams(hyst_trend_bars=HYST_TREND_BARS, hyst_sub_bars=HYST_SUB_BARS, eps_z=EPS_Z)
                            )

                            # сохранить state + опубликовать KV и собрать триплет
                            await _save_state_live(redis, sym, tf, new_state)
                            await redis.setex(f"ind_live:{sym}:{tf}:regime9_code", TTL_SEC, str(code))
                            written += 1
                            async with cache_lock:
                                codes_trip.setdefault(sym, {})[tf] = code

                        except Exception as e:
                            log.debug("[PAIR] %s/%s err=%s", sym, tf, e)

                return written

            tasks = [asyncio.create_task(handle_pair(sym, tf)) for sym in symbols for tf in REQUIRED_TFS]
            results = await asyncio.gather(*tasks)
            total_written = sum(results)

            # триплеты
            async with cache_lock:
                for sym in symbols:
                    tf_map = codes_trip.get(sym, {})
                    if all(t in tf_map for t in ("m5","m15","h1")):
                        trip = f"{tf_map['m5']}-{tf_map['m15']}-{tf_map['h1']}"
                        try:
                            await redis.setex(f"ind_live:{sym}:regime9_code_triplet", TTL_SEC, trip)
                        except Exception:
                            pass

            log.debug("[TICK] end written=%d", total_written)

        except Exception as e:
            log.error("loop error: %s", e, exc_info=True)

        await asyncio.sleep(INTERVAL_SEC)