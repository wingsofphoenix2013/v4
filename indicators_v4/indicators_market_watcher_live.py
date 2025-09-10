# indicators_market_watcher_live.py — live-вариант regime9 v2: «на сейчас» раз в минуту, пишет только в KV

# 🔸 Импорты
import os
import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd

from regime9_core import RegimeState, RegimeParams, decide_regime_code
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Константы/конфиг (аналогичны indicators_market_watcher.py, но независимы)
TF_LIST = ("m5", "m15", "h1")
REQUIRED_TFS = {"m5", "m15", "h1"}

N_PCT = int(os.getenv("MRW_LIVE_N_PCT", "200"))     # окно p30/p70/ADX/BB/ATR
N_ACC = int(os.getenv("MRW_LIVE_N_ACC", "50"))      # окно ΔMACD (на базе macd_hist)
EPS_Z = float(os.getenv("MRW_LIVE_EPS_Z", "0.5"))   # порог ускорения для z-score

HYST_TREND_BARS = int(os.getenv("MRW_LIVE_HYST_TREND_BARS", "2"))  # тренд↔флет
HYST_SUB_BARS   = int(os.getenv("MRW_LIVE_HYST_SUB_BARS", "1"))    # accel/stable/decel

TICK_SECONDS = int(os.getenv("MRW_LIVE_TICK_SECONDS", "60"))       # период опроса
DEBOUNCE_MS  = int(os.getenv("MRW_LIVE_DEBOUNCE_MS", "250"))       # задержка после старта бара

MAX_CONCURRENCY   = int(os.getenv("MRW_LIVE_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL    = int(os.getenv("MRW_LIVE_MAX_PER_SYMBOL", "4"))

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000     # не используется (KV only), оставлено для совместимости мыслей

# 🔸 Ключ для публикации результата
def live_kv_key(symbol: str, tf: str) -> str:
    return f"ind_live:{symbol}:{tf}:regime9_code"

# 🔸 Таймшаги TF
def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)

# 🔸 Функции состояния гистерезиса (совместимые с обычным watcher)
def _state_key(symbol: str, tf: str) -> str:
    return f"mrw_state:{symbol}:{tf}"

async def _load_state(redis, symbol: str, tf: str) -> RegimeState:
    data = await redis.hgetall(_state_key(symbol, tf))
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

async def _save_state(redis, symbol: str, tf: str, st: RegimeState) -> None:
    await redis.hset(
        _state_key(symbol, tf),
        mapping={
            "core": st.core,
            "core_cnt": str(st.core_cnt),
            "sub": st.sub,
            "sub_cnt": str(st.sub_cnt),
        },
    )

# 🔸 Вспомогательное: floor к началу бара TF
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_ms = _tf_step_ms(tf)
    return (ts_ms // step_ms) * step_ms

# 🔸 Чтение диапазона из TS → list[float] (без строгой проверки полноты)
async def _ts_range_vals(redis, key: str, start_ms: int, end_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return [float(v) for _, v in (res or [])]
    except Exception:
        return []

# 🔸 Чтение точки ровно на штампе (или None)
async def _ts_point(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res and int(res[0][0]) == ts_ms:
            return float(res[0][1])
        return None
    except Exception:
        return None

# 🔸 Загрузка OHLCV из Redis TS для сборки DataFrame
async def _load_ohlcv_df(redis, symbol: str, tf: str, end_ms: int, depth_bars: int) -> pd.DataFrame | None:
    step = _tf_step_ms(tf)
    start_ms = end_ms - (depth_bars - 1) * step
    fields = ["o", "h", "l", "c", "v"]
    keys = {f: f"ts:{symbol}:{tf}:{f}" for f in fields}
    calls = [redis.execute_command("TS.RANGE", keys[f], start_ms, end_ms) for f in fields]
    res = await asyncio.gather(*calls, return_exceptions=True)

    series = {}
    for f, r in zip(fields, res):
        if isinstance(r, Exception):
            continue
        if r:
            series[f] = {int(ts): float(val) for ts, val in r if val is not None}

    if not series or "c" not in series:
        return None

    idx = sorted(series["c"].keys())
    df = {f: [series.get(f, {}).get(ts) for ts in idx] for f in fields}
    pdf = pd.DataFrame(df, index=pd.to_datetime(idx, unit="ms"))
    pdf.index.name = "open_time"
    return pdf

# 🔸 On-demand расчёт индикаторов на текущем/предыдущем баре поверх OHLCV (локально)
async def _ondemand_values(redis, symbol: str, tf: str, bar_open_ms: int, precision: int, need_prev: bool = True) -> dict:
    # собираем достаточный DF (N_PCT баров к текущему бару включительно)
    depth = max(N_PCT, 2 + int(need_prev))
    pdf = await _load_ohlcv_df(redis, symbol, tf, bar_open_ms, depth)
    if pdf is None or len(pdf) < 2:
        return {}

    # синтетические инстансы индикаторов (чтобы имена/округления совпадали с системой)
    adx_len = 14 if tf in {"m5", "m15"} else 28
    inst_defs = [
        {"id": 90001, "indicator": "ema",     "timeframe": tf, "params": {"length": "21"}},
        {"id": 90002, "indicator": "macd",    "timeframe": tf, "params": {"fast": "12", "slow": "26", "signal": "9"}},
        {"id": 90003, "indicator": "bb",      "timeframe": tf, "params": {"length": "20", "std": "2.0"}},
        {"id": 90004, "indicator": "adx_dmi", "timeframe": tf, "params": {"length": str(adx_len)}},
    ]
    if tf in {"m5", "m15"}:
        inst_defs.append({"id": 90005, "indicator": "atr", "timeframe": tf, "params": {"length": "14"}})

    # помощник: извлечь нужный параметр
    def _get(vmap: dict, key: str) -> float | None:
        try:
            return float(vmap.get(key)) if key in vmap else None
        except Exception:
            return None

    # текущее t
    values_t = {}
    for inst in inst_defs:
        vals = await compute_snapshot_values_async(inst, symbol, pdf, precision)
        values_t.update(vals or {})

    # предыдущий t-1 (нужно только ema и macd)
    values_t1 = {}
    if need_prev:
        if len(pdf) >= 2:
            prev_pdf = pdf.iloc[:-1]
            for inst in inst_defs:
                if inst["indicator"] not in {"ema", "macd"}:
                    continue
                vals = await compute_snapshot_values_async(inst, symbol, prev_pdf, precision)
                values_t1.update(vals or {})

    # распаковка в понятные поля
    out = {
        "ema_t": _get(values_t, "ema21"),
        "macd_t": _get(values_t, "macd12_macd_hist"),
        "bb_u_t": _get(values_t, "bb20_2_0_upper"),
        "bb_l_t": _get(values_t, "bb20_2_0_lower"),
        "bb_c_t": _get(values_t, "bb20_2_0_center"),
        "adx_t":  _get(values_t, f"adx_dmi{adx_len}_adx"),
        "atr_t":  _get(values_t, "atr14") if tf in {"m5", "m15"} else None,
        "ema_t1": _get(values_t1, "ema21") if need_prev else None,
        "macd_t1": _get(values_t1, "macd12_macd_hist") if need_prev else None,
    }
    return out

# 🔸 Сбор «окон» до t_prev из ts_ind:* и склейка с on-demand t
async def _build_features_now(redis, symbol: str, tf: str, bar_open_ms: int, precision: int) -> dict | None:
    # условия достаточности
    step = _tf_step_ms(tf)
    t_prev = bar_open_ms - step
    start_pct = bar_open_ms - (N_PCT - 1) * step
    start_acc = bar_open_ms - N_ACC * step

    # ключи ts_ind
    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5", "m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key = f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bbu_key = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bbl_key = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bbc_key = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5", "m15"} else None

    # читаем «историю» ДО t_prev
    calls = [
        _ts_range_vals(redis, ema_key,  t_prev, t_prev),          # точка ema[t-1] из TS (может быть пусто)
        _ts_range_vals(redis, macd_key, t_prev, t_prev),          # точка macd[t-1] из TS (может быть пусто)
        _ts_range_vals(redis, macd_key, start_acc, t_prev),       # окно macd_hist до t_prev
        _ts_range_vals(redis, adx_key,  start_pct, t_prev),       # окно adx до t_prev
        _ts_range_vals(redis, bbu_key,  start_pct, t_prev),
        _ts_range_vals(redis, bbl_key,  start_pct, t_prev),
        _ts_range_vals(redis, bbc_key,  start_pct, t_prev),
    ]
    if atr_key:
        calls.append(_ts_range_vals(redis, atr_key, start_pct, t_prev))

    (
        ema_t1_list,
        macd_t1_list,
        macd_hist_hist,
        adx_hist,
        bbu_hist,
        bbl_hist,
        bbc_hist,
        *atr_hist_opt
    ) = await asyncio.gather(*calls, return_exceptions=False)

    atr_hist = atr_hist_opt[0] if atr_hist_opt else None

    # on-demand текущее t (+ при необходимости t-1 в качестве фоллбэка)
    ondem = await _ondemand_values(redis, symbol, tf, bar_open_ms, precision, need_prev=True)

    # проверка критично необходимых t-значений
    if ondem.get("ema_t") is None or ondem.get("macd_t") is None:
        return None
    if ondem.get("bb_u_t") is None or ondem.get("bb_l_t") is None or ondem.get("bb_c_t") is None:
        return None
    if ondem.get("adx_t") is None:
        return None
    if tf in {"m5", "m15"} and ondem.get("atr_t") is None:
        return None

    # t-1: берем из TS, если нет — берем из on-demand prev
    ema_t1  = ema_t1_list[-1] if ema_t1_list else ondem.get("ema_t1")
    macd_t1 = macd_t1_list[-1] if macd_t1_list else ondem.get("macd_t1")
    if ema_t1 is None or macd_t1 is None:
        return None

    # ΔMACD окно: до t_prev (из TS) + текущий macd_t (on-demand)
    macd_vals = list(macd_hist_hist[-(N_ACC+1):])  # максимум N_ACC+1 исторических
    macd_vals.append(ondem["macd_t"])
    if len(macd_vals) < 2:
        return None
    dhist = [macd_vals[i+1] - macd_vals[i] for i in range(len(macd_vals)-1)]

    # ADX/BB/ATR окна: до t_prev (из TS) + текущая точка (on-demand)
    adx_win = list(adx_hist[-N_PCT:])
    adx_win.append(ondem["adx_t"])

    bb_u_win = list(bbu_hist[-N_PCT:])
    bb_u_win.append(ondem["bb_u_t"])
    bb_l_win = list(bbl_hist[-N_PCT:])
    bb_l_win.append(ondem["bb_l_t"])
    bb_c_win = list(bbc_hist[-N_PCT:])
    bb_c_win.append(ondem["bb_c_t"])

    atr_win = None
    if tf in {"m5", "m15"}:
        atr_win = list((atr_hist or [])[-N_PCT:])
        atr_win.append(ondem["atr_t"])

    # финальная сборка features
    features = {
        "ema_t1": float(ema_t1),
        "ema_t":  float(ondem["ema_t"]),
        "macd_t1": float(macd_t1),
        "macd_t":  float(ondem["macd_t"]),
        "dhist_win": dhist[-N_ACC:],
        "adx_win":   adx_win[-N_PCT:],
        "bb_u_win":  bb_u_win[-N_PCT:],
        "bb_l_win":  bb_l_win[-N_PCT:],
        "bb_c_win":  bb_c_win[-N_PCT:],
        "atr_t":   (float(ondem["atr_t"]) if tf in {"m5", "m15"} else None),
        "atr_win": (atr_win[-N_PCT:] if tf in {"m5", "m15"} else None),
    }
    return features

# 🔸 Обработка одного (symbol, tf) на «текущем баре»
async def _handle_symbol_tf_now(redis, symbol: str, tf: str, bar_open_ms: int, precision: int):
    # условия достаточности
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    feats = await _build_features_now(redis, symbol, tf, bar_open_ms, precision)
    if feats is None:
        return False, None

    # гистерезис
    state = await _load_state(redis, symbol, tf)
    code, new_state, diag = decide_regime_code(
        tf, feats, state,
        RegimeParams(hyst_trend_bars=HYST_TREND_BARS, hyst_sub_bars=HYST_SUB_BARS, eps_z=EPS_Z)
    )
    await _save_state(redis, symbol, tf, new_state)

    # публикация ТОЛЬКО в KV
    kv_key = live_kv_key(symbol, tf)
    await redis.set(kv_key, str(code))

    # лог результата
    bar_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
    logging.getLogger("MRW_LIVE").info(
        f"[LIVE] {symbol}/{tf} @ {bar_iso} → code={code} "
        f"(adx={diag['adx']:.2f}/{diag['adx_low']:.2f}-{diag['adx_high']:.2f}, "
        f"bbw={diag['bb_width']:.4f}/{diag['bb_low']:.4f}-{diag['bb_high']:.4f}, "
        f"zΔ={diag['z_d_hist']:.2f})"
    )
    return True, code

# 🔸 Основной цикл воркера (раз в минуту по всем активным символам и TF)
async def run_market_watcher_live(pg, redis, get_active_symbols, get_precision):
    log = logging.getLogger("MRW_LIVE")
    log.info("market_watcher_live starting")

    # глобальные семафоры для контроля параллелизма
    task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
    symbol_semaphores: dict[str, asyncio.Semaphore] = {}

    while True:
        try:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            # проходим по актуальному списку символов (с учётом возможных включений/отключений)
            symbols = list(get_active_symbols())
            if not symbols:
                await asyncio.sleep(TICK_SECONDS)
                continue

            # задачи на текущий тик
            tasks = []
            for sym in symbols:
                if sym not in symbol_semaphores:
                    symbol_semaphores[sym] = asyncio.Semaphore(MAX_PER_SYMBOL)

                precision = get_precision(sym)
                for tf in TF_LIST:
                    bar_open_ms = floor_to_bar_ms(now_ms, tf)

                    async def runner(symbol=sym, timeframe=tf, ts_ms=bar_open_ms, prec=precision):
                        async with task_gate:
                            async with symbol_semaphores[symbol]:
                                await _handle_symbol_tf_now(redis, symbol, timeframe, ts_ms, prec)

                    tasks.append(asyncio.create_task(runner()))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            log.error(f"MRW_LIVE loop error: {e}", exc_info=True)

        await asyncio.sleep(TICK_SECONDS)