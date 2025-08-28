# 🔸 indicators_market_watcher.py — Этап 3: классификация (логируем code 0..8), без записи

import os
import asyncio
import json
import logging
from datetime import datetime
from statistics import median

# 🔸 Константы и конфиг
READY_STREAM = "indicator_stream"
GROUP_NAME = os.getenv("MRW_GROUP", "mrw_v1_group")
CONSUMER_NAME = os.getenv("MRW_CONSUMER", "mrw_1")
REQUIRED_TFS = {"m5", "m15", "h1"}

DEBOUNCE_MS = int(os.getenv("MRW_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY = int(os.getenv("MRW_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL = int(os.getenv("MRW_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS = int(os.getenv("MRW_BLOCK_MS", "1000"))
XREAD_COUNT = int(os.getenv("MRW_COUNT", "50"))

N_PCT = int(os.getenv("MRW_N_PCT", "200"))   # окно для p30/p70
N_ACC = int(os.getenv("MRW_N_ACC", "50"))    # окно для z-score Δhist
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5")) # порог ускорения

# 🔸 Глобальные структуры параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
symbol_semaphores: dict[str, asyncio.Semaphore] = {}
bucket_tasks: dict[tuple, asyncio.Task] = {}


# 🔸 Утилиты
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)

def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)

def _p30(vals: list[float]) -> float:
    if not vals:
        return float("nan")
    k = max(0, int(0.30 * (len(vals) - 1)))
    return sorted(vals)[k]

def _p70(vals: list[float]) -> float:
    if not vals:
        return float("nan")
    k = max(0, int(0.70 * (len(vals) - 1)))
    return sorted(vals)[k]

def _mad(vals: list[float]) -> float:
    if not vals:
        return 0.0
    m = median(vals)
    dev = [abs(x - m) for x in vals]
    return median(dev) or 1e-9  # защита от деления на 0

def _zscore(x: float, vals: list[float]) -> float:
    m = median(vals)
    s = _mad(vals)
    return (x - m) / s


# 🔸 Чтение фич на бар: проверяем, что всё готово на open_time, и подтягиваем окна
async def fetch_features_for_bar(redis, symbol: str, tf: str, open_time_ms: int) -> dict | None:
    log = logging.getLogger("MRW")

    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5", "m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key = f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bb_u = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bb_l = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bb_c = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5", "m15"} else None

    # Проверка наличия точки ровно на open_time
    keys_now = [adx_key, ema_key, macd_key, bb_u, bb_l, bb_c] + ([atr_key] if atr_key else [])
    now_calls = [redis.execute_command("TS.RANGE", k, open_time_ms, open_time_ms) for k in keys_now]
    now_results = await asyncio.gather(*now_calls, return_exceptions=True)
    for k, r in zip(keys_now, now_results):
        if isinstance(r, Exception) or not r or int(r[0][0]) != open_time_ms:
            logging.getLogger("MRW").debug(f"[INCOMPLETE] {symbol}/{tf} @ {open_time_ms} → missing {k}")
            return None

    # Окна
    step_ms = _tf_step_ms(tf)
    t_prev = open_time_ms - step_ms
    t_start_pct = open_time_ms - (N_PCT - 1) * step_ms
    t_start_acc = open_time_ms - N_ACC * step_ms

    window_calls = [
        redis.execute_command("TS.RANGE", ema_key, t_prev, open_time_ms),        # 2 точки
        redis.execute_command("TS.RANGE", macd_key, t_start_acc, open_time_ms),  # N_ACC+1
        redis.execute_command("TS.RANGE", adx_key, t_start_pct, open_time_ms),   # N_PCT
        redis.execute_command("TS.RANGE", bb_u,   t_start_pct, open_time_ms),
        redis.execute_command("TS.RANGE", bb_l,   t_start_pct, open_time_ms),
        redis.execute_command("TS.RANGE", bb_c,   t_start_pct, open_time_ms),
    ]
    if atr_key:
        window_calls.append(redis.execute_command("TS.RANGE", atr_key, t_start_pct, open_time_ms))

    out = await asyncio.gather(*window_calls, return_exceptions=True)

    def _vals(series):
        return [float(v) for _, v in series] if series and not isinstance(series, Exception) else []

    ema_vals  = _vals(out[0])
    macd_vals = _vals(out[1])
    adx_vals  = _vals(out[2])
    bbu_vals  = _vals(out[3])
    bbl_vals  = _vals(out[4])
    bbc_vals  = _vals(out[5])
    atr_vals  = _vals(out[6]) if (atr_key and len(out) > 6) else None

    logging.getLogger("MRW").debug(
        f"[FEATURES] {symbol}/{tf} @ {open_time_ms} → "
        f"ema={len(ema_vals)} macd={len(macd_vals)} adx={len(adx_vals)} "
        f"bb(u/l/c)={[len(bbu_vals), len(bbl_vals), len(bbc_vals)]} "
        f"atr={'-' if atr_vals is None else len(atr_vals)}"
    )

    return {
        "ema_vals": ema_vals,
        "macd_vals": macd_vals,
        "adx_vals": adx_vals,
        "bb_u": bbu_vals,
        "bb_l": bbl_vals,
        "bb_c": bbc_vals,
        "atr_vals": atr_vals,
    }


# 🔸 Классификация бара → code 0..8 и диагностические метрики
def classify_bar(tf: str, features: dict) -> tuple[int, dict]:
    ema_t1, ema_t = features["ema_vals"][-2], features["ema_vals"][-1]
    ema_slope = ema_t - ema_t1

    macd_hist_t1, macd_hist_t = features["macd_vals"][-2], features["macd_vals"][-1]
    d_hist = macd_hist_t - macd_hist_t1
    z_vals = [features["macd_vals"][i+1] - features["macd_vals"][i] for i in range(len(features["macd_vals"]) - 1)]
    z_d_hist = _zscore(d_hist, z_vals[-N_ACC:]) if z_vals else 0.0

    adx = features["adx_vals"][-1]
    adx_low_p = _p30(features["adx_vals"])
    adx_high_p = _p70(features["adx_vals"])
    adx_low = max(adx_low_p, 15.0)
    adx_high = min(adx_high_p, 30.0)

    # bb width = (upper - lower) / center
    bb_width_series = []
    for u, l, c in zip(features["bb_u"], features["bb_l"], features["bb_c"]):
        bb_width_series.append(0.0 if c == 0 else (u - l) / c)
    bb_width = bb_width_series[-1]
    bb_low = _p30(bb_width_series)
    bb_high = _p70(bb_width_series)

    atr = atr_low = atr_high = None
    if tf in {"m5", "m15"} and features.get("atr_vals"):
        atr = features["atr_vals"][-1]
        atr_low = _p30(features["atr_vals"])
        atr_high = _p70(features["atr_vals"])

    # тренд/флет
    is_trend = adx >= adx_high
    is_flat = adx <= adx_low

    if is_flat:
        if bb_width <= bb_low and (atr is None or atr <= atr_low):
            code = 0  # F_CONS
        elif bb_width >= bb_high:
            code = 1  # F_EXP
        else:
            code = 2  # F_DRIFT
    else:
        direction_up = ema_slope > 0.0
        if z_d_hist > +EPS_Z:
            accel = 0  # ACCEL
        elif abs(z_d_hist) <= EPS_Z:
            accel = 1  # STABLE
        else:
            accel = 2  # DECEL
        code = (3 + accel) if direction_up else (6 + accel)

    diag = {
        "adx": adx, "adx_low": adx_low, "adx_high": adx_high,
        "bb_width": bb_width, "bb_low": bb_low, "bb_high": bb_high,
        "atr": atr, "atr_low": atr_low, "atr_high": atr_high,
        "ema_slope": ema_slope, "macd_hist": macd_hist_t,
        "d_hist": d_hist, "z_d_hist": z_d_hist
    }
    return code, diag


# 🔸 Обработка бакета (Этап 3): debounce → фичи → классификация → лог
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis):
    log = logging.getLogger("MRW")
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    for attempt in range(2):
        feats = await fetch_features_for_bar(redis, symbol, tf, open_time_ms)
        if feats is not None:
            code, diag = classify_bar(tf, feats)
            # лаконичный вывод диага: пара ключевых полей
            log.info(f"[REGIME] {symbol}/{tf} @ {open_time_ms} → code={code} "
                     f"(adx={diag['adx']:.2f}/{diag['adx_low']:.2f}-{diag['adx_high']:.2f}, "
                     f"bbw={diag['bb_width']:.4f}/{diag['bb_low']:.4f}-{diag['bb_high']:.4f}, "
                     f"zΔ={diag['z_d_hist']:.2f})")
            return
        await asyncio.sleep(0.15)

    log.debug(f"[SKIP] features incomplete: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл компонента (XREADGROUP). Этап 1-логи переведены на debug
async def run_market_watcher(pg, redis):
    log = logging.getLogger("MRW")
    log.info("market_watcher starting: XGROUP init")

    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.debug(f"consumer group '{GROUP_NAME}' created on '{READY_STREAM}'")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"consumer group '{GROUP_NAME}' already exists")
        else:
            log.error(f"XGROUP CREATE error: {e}", exc_info=True)

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={READY_STREAM: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    try:
                        symbol = data.get("symbol")
                        tf = data.get("timeframe") or data.get("interval")
                        status = data.get("status")
                        open_time_iso = data.get("open_time")

                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_time_iso:
                            to_ack.append(msg_id)
                            continue

                        open_time_ms = _iso_to_ms(open_time_iso)
                        bucket = (symbol, tf, open_time_ms)

                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        if symbol not in symbol_semaphores:
                            symbol_semaphores[symbol] = asyncio.Semaphore(MAX_PER_SYMBOL)

                        log.debug(f"[READY] {symbol}/{tf} @ {open_time_iso} → schedule bucket")

                        async def bucket_runner():
                            async with task_gate:
                                async with symbol_semaphores[symbol]:
                                    await handle_bucket(symbol, tf, open_time_ms, redis)

                        bucket_tasks[bucket] = asyncio.create_task(bucket_runner())
                        to_ack.append(msg_id)

                    except Exception as parse_err:
                        to_ack.append(msg_id)
                        log.error(f"message parse error: {parse_err}", exc_info=True)

            if to_ack:
                await redis.xack(READY_STREAM, GROUP_NAME, *to_ack)

        except Exception as e:
            log.error(f"XREADGROUP loop error: {e}", exc_info=True)
            await asyncio.sleep(1)