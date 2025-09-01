# 🔸 indicators_ema_status.py — Этап 2: сбор фич из TS (diagnostics), без расчёта и записи

import os
import asyncio
import json
import logging
from datetime import datetime

log = logging.getLogger("EMA_STATUS")

# 🔸 Конфиг
READY_STREAM   = "indicator_stream"
GROUP_NAME     = os.getenv("EMA_STATUS_GROUP", "ema_status_v1")
CONSUMER_NAME  = os.getenv("EMA_STATUS_CONSUMER", "ema_status_1")
REQUIRED_TFS   = {"m5", "m15", "h1"}

DEBOUNCE_MS       = int(os.getenv("EMA_STATUS_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY   = int(os.getenv("EMA_STATUS_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL    = int(os.getenv("EMA_STATUS_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS    = int(os.getenv("EMA_STATUS_BLOCK_MS", "1000"))
XREAD_COUNT       = int(os.getenv("EMA_STATUS_COUNT", "50"))

# EMA длины
def _parse_ema_lens(raw: str) -> list[int]:
    out = []
    for part in raw.split(","):
        s = part.strip()
        if not s: continue
        try:
            out.append(int(s))
        except:
            pass
    return out or [9,21,50,100,200]

EMA_LENS = _parse_ema_lens(os.getenv("EMA_STATUS_EMA_LENS", "9,21,50,100,200"))

# окна для порогов (Stage 3), пока только счётчик
N_PCT = int(os.getenv("EMA_STATUS_N_PCT", "200"))

# 🔸 Пул параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
symbol_semaphores: dict[str, asyncio.Semaphore] = {}
bucket_tasks: dict[tuple, asyncio.Task] = {}

# 🔸 Утилиты
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)

def _tf_step_ms(tf: str) -> int:
    return 300_000 if tf == "m5" else (900_000 if tf == "m15" else 3_600_000)

# 🔸 Ключи TS
def k_close(sym: str, tf: str) -> str:
    return f"ts:{sym}:{tf}:c"  # OHLCV close

def k_ema(sym: str, tf: str, L: int) -> str:
    return f"ts_ind:{sym}:{tf}:ema{L}"

def k_atr(sym: str, tf: str) -> str:
    return f"ts_ind:{sym}:{tf}:atr14"

def k_bb(sym: str, tf: str, part: str) -> str:
    # part in {'upper','center','lower'}
    return f"ts_ind:{sym}:{tf}:bb20_2_0_{part}"

# 🔸 Чтение одного сэмпла ровно на open_time (TS.RANGE open open)
async def _get_point(redis, key: str, ts_ms: int):
    try:
        r = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if r and int(r[0][0]) == ts_ms:
            return float(r[0][1])
    except Exception as e:
        log.debug("[TSERR] key=%s err=%s", key, e)
    return None

# 🔸 Чтение окна (N_PCT) для диагностик/Stage3 (сейчас — счётчик)
async def _get_window_len(redis, key: str, start_ms: int, end_ms: int) -> int:
    try:
        r = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return len(r or [])
    except Exception as e:
        log.debug("[TSERR] win key=%s err=%s", key, e)
        return 0

# 🔸 Сбор фич для одного (symbol, tf, open_time)
async def collect_features(redis, symbol: str, tf: str, open_ms: int) -> dict:
    step = _tf_step_ms(tf)
    prev_ms = open_ms - step
    # масштаб: m5/m15 = ATR14; h1 = BB width
    need_atr = (tf in {"m5","m15"})

    # обязательные текущие точки:
    # close_t, emaL_t (все L), emaL_prev, scale_t (atr14_t или bb width), bb parts (для h1 масштаба и fallback)
    # Параллельные запросы текущих точек
    calls = []
    calls.append(_get_point(redis, k_close(symbol, tf), open_ms))
    for L in EMA_LENS:
        calls.append(_get_point(redis, k_ema(symbol, tf, L), open_ms))
        calls.append(_get_point(redis, k_ema(symbol, tf, L), prev_ms))
    # scale
    if need_atr:
        calls.append(_get_point(redis, k_atr(symbol, tf), open_ms))
        # на fallback держим bb тоже (вдруг atr≈0)
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "center"), open_ms))
    else:
        # h1: сразу bb
        calls.append(_get_point(redis, k_bb(symbol, tf, "upper"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "lower"), open_ms))
        calls.append(_get_point(redis, k_bb(symbol, tf, "center"), open_ms))

    res = await asyncio.gather(*calls, return_exceptions=True)
    idx = 0

    close_t = res[idx]; idx += 1

    ema_t = {}
    ema_prev = {}
    for L in EMA_LENS:
        ema_t[L] = res[idx]; idx += 1
        ema_prev[L] = res[idx]; idx += 1

    if need_atr:
        atr_t = res[idx]; idx += 1
        bb_up = res[idx]; idx += 1
        bb_lo = res[idx]; idx += 1
        bb_ce = res[idx]; idx += 1
    else:
        atr_t = None
        bb_up = res[idx]; idx += 1
        bb_lo = res[idx]; idx += 1
        bb_ce = res[idx]; idx += 1

    # Окна для диагностики — по одному ключу (например ema21/atr/bb), просто длины
    start_ms = open_ms - (N_PCT - 1) * step
    win_calls = []
    # ema21 для окна
    win_calls.append(_get_window_len(redis, k_ema(symbol, tf, 21), start_ms, open_ms))
    # scale-окно
    if need_atr:
        win_calls.append(_get_window_len(redis, k_atr(symbol, tf), start_ms, open_ms))
    else:
        win_calls.append(_get_window_len(redis, k_bb(symbol, tf, "upper"), start_ms, open_ms))
        win_calls.append(_get_window_len(redis, k_bb(symbol, tf, "lower"), start_ms, open_ms))
        win_calls.append(_get_window_len(redis, k_bb(symbol, tf, "center"), start_ms, open_ms))

    wins = await asyncio.gather(*win_calls, return_exceptions=True)

    features = {
        "close_t": close_t,
        "ema_t": ema_t,
        "ema_prev": ema_prev,
        "atr_t": atr_t,
        "bb_up": bb_up, "bb_lo": bb_lo, "bb_ce": bb_ce,
        "ema_win_len": wins[0] if wins else 0,
        "scale_win_len": sum(wins[1:]) if (not need_atr and len(wins) >= 4) else (wins[1] if len(wins) >= 2 else 0),
        "need_atr": need_atr
    }
    return features

# 🔸 Заглушка обработки «бакета» (Stage 2): debounce → сбор фич → диагностический лог
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis, pg):
    await asyncio.sleep(DEBOUNCE_MS / 1000)

    feats = await collect_features(redis, symbol, tf, open_time_ms)

    # сколько EMA длин готовы полностью (ema_t и ema_prev на месте)?
    ready_ema = 0
    for L in EMA_LENS:
        if feats["ema_t"].get(L) is not None and feats["ema_prev"].get(L) is not None:
            ready_ema += 1

    # масштаб готов?
    if feats["need_atr"]:
        scale_ok = (feats["atr_t"] is not None) or (
            feats["bb_up"] is not None and feats["bb_lo"] is not None
        )
        scale_type = "ATR14" if feats["atr_t"] is not None else ("BBwidth" if scale_ok else "NA")
    else:
        scale_ok = (feats["bb_up"] is not None and feats["bb_lo"] is not None)
        scale_type = "BBwidth" if scale_ok else "NA"

    close_ok = (feats["close_t"] is not None)

    # краткий INFO лог
    log.info(
        "[FEATURES] %s/%s @ %d → close=%s, ema_ready=%d/%d, scale=%s, win_ema=%s, win_scale=%s",
        symbol, tf, open_time_ms,
        "ok" if close_ok else "NA",
        ready_ema, len(EMA_LENS),
        scale_type,
        feats.get("ema_win_len"),
        feats.get("scale_win_len")
    )

# 🔸 Основной цикл: XREADGROUP по indicator_stream
async def run_indicators_ema_status(pg, redis):
    log.info("EMA Status: init consumer-group")
    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ consumer-group '%s' создана на '%s'", GROUP_NAME, READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ consumer-group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ XGROUP CREATE error: %s", e)
            raise

    log.info("🚀 Этап 2: слушаем '%s' (group=%s, consumer=%s)", READY_STREAM, GROUP_NAME, CONSUMER_NAME)

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
                        tf      = data.get("timeframe") or data.get("interval")
                        status  = data.get("status")
                        open_iso= data.get("open_time")

                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_iso:
                            to_ack.append(msg_id)
                            continue

                        open_ms = _iso_to_ms(open_iso)
                        bucket  = (symbol, tf, open_ms)

                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        if symbol not in symbol_semaphores:
                            symbol_semaphores[symbol] = asyncio.Semaphore(MAX_PER_SYMBOL)

                        log.info("[READY] %s/%s @ %s → schedule EMA-status", symbol, tf, open_iso)

                        async def bucket_runner():
                            async with task_gate:
                                async with symbol_semaphores[symbol]:
                                    await handle_bucket(symbol, tf, open_ms, redis, pg)

                        bucket_tasks[bucket] = asyncio.create_task(bucket_runner())
                        to_ack.append(msg_id)

                    except Exception as parse_err:
                        to_ack.append(msg_id)
                        log.exception("❌ message parse error: %s", parse_err)

            if to_ack:
                await redis.xack(READY_STREAM, GROUP_NAME, *to_ack)

        except Exception as e:
            log.exception("❌ XREADGROUP loop error: %s", e)
            await asyncio.sleep(1)