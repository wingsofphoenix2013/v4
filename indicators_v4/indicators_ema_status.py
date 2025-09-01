# 🔸 indicators_ema_status.py — Этап 1: каркас воркера EMA Status (подписка на indicator_stream)

import os
import asyncio
import json
import logging
from datetime import datetime

# 🔸 Конфиг
READY_STREAM   = "indicator_stream"                             # слушаем этот стрим
GROUP_NAME     = os.getenv("EMA_STATUS_GROUP", "ema_status_v1") # наша consumer-group
CONSUMER_NAME  = os.getenv("EMA_STATUS_CONSUMER", "ema_status_1")
REQUIRED_TFS   = {"m5", "m15", "h1"}

DEBOUNCE_MS       = int(os.getenv("EMA_STATUS_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY   = int(os.getenv("EMA_STATUS_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL    = int(os.getenv("EMA_STATUS_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS    = int(os.getenv("EMA_STATUS_BLOCK_MS", "1000"))
XREAD_COUNT       = int(os.getenv("EMA_STATUS_COUNT", "50"))

# 🔸 Пул параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)
symbol_semaphores: dict[str, asyncio.Semaphore] = {}
bucket_tasks: dict[tuple, asyncio.Task] = {}

log = logging.getLogger("EMA_STATUS")


# 🔸 Утилиты
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


# 🔸 Заглушка обработки «бакета» (Этап 1: только debounce и лог)
async def handle_bucket(symbol: str, tf: str, open_time_ms: int, redis, pg):
    # debounce: ждём дозапись базовых индикаторов/цены
    await asyncio.sleep(DEBOUNCE_MS / 1000)
    # здесь позже появится: сбор фич из TS → расчёт статуса → запись в Redis/PG
    log.info(f"[STAGE1] bucket done: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл: XREADGROUP по indicator_stream, планирование бакетов
async def run_indicators_ema_status(pg, redis):
    log.info("EMA Status: init consumer-group")
    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.info("✅ consumer-group '%s' создана на '%s'", GROUP_NAME, READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ consumer-group '%s' уже существует", GROUP_NAME)
        else:
            log.exception("❌ XGROUP CREATE error: %s", e)
            raise

    log.info("🚀 Этап 1: слушаем '%s' (group=%s, consumer=%s)", READY_STREAM, GROUP_NAME, CONSUMER_NAME)

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

                        # базовая фильтрация
                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_iso:
                            to_ack.append(msg_id)
                            continue

                        open_ms = _iso_to_ms(open_iso)
                        bucket  = (symbol, tf, open_ms)

                        # не дублируем задачи
                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        # лимит per-symbol
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