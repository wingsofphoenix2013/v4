# 🔸 indicators_market_watcher.py — Этап 2: чтение фич из Redis TS и проверка комплектности

import os
import asyncio
import json
import logging
from datetime import datetime

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


# 🔸 Чтение фич на бар: проверяем, что все готово на open_time, и подтягиваем окна
async def fetch_features_for_bar(redis, symbol: str, tf: str, open_time_ms: int) -> dict | None:
    log = logging.getLogger("MRW")

    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5", "m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key = f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bb_u = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bb_l = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bb_c = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5", "m15"} else None

    # Проверка: на open_time должны быть точки у всех ключей
    keys_now = [adx_key, ema_key, macd_key, bb_u, bb_l, bb_c] + ([atr_key] if atr_key else [])
    now_calls = [redis.execute_command("TS.RANGE", k, open_time_ms, open_time_ms) for k in keys_now]
    now_results = await asyncio.gather(*now_calls, return_exceptions=True)

    for k, r in zip(keys_now, now_results):
        if isinstance(r, Exception) or not r or int(r[0][0]) != open_time_ms:
            log.info(f"[INCOMPLETE] {symbol}/{tf} @ {open_time_ms} → missing {k}")
            return None

    # Окна для дальнейших этапов (пока только читаем и логируем размеры)
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

    logging.getLogger("MRW").info(
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


# 🔸 Обработка бакета (Этап 2): debounce → 2 ретрая чтения фич → лог
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis):
    log = logging.getLogger("MRW")
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    for attempt in range(2):
        feats = await fetch_features_for_bar(redis, symbol, tf, open_time_ms)
        if feats is not None:
            log.info(f"[OK] features ready: {symbol}/{tf} @ {open_time_ms}")
            return
        await asyncio.sleep(0.15)

    log.info(f"[SKIP] features incomplete: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл компонента (переписан под XREADGROUP) — Этап 1 логи переведены в debug
async def run_market_watcher(pg, redis):
    log = logging.getLogger("MRW")
    log.info(f"market_watcher starting: XGROUP init")

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