# 🔸 indicators_market_watcher.py — Этап 1 (stream/consumer group): каркас компонента

import os
import asyncio
import json
import logging
from datetime import datetime

# 🔸 Константы и конфиг
READY_STREAM = "indicator_stream"                      # читаем события готовности индикаторов
GROUP_NAME = os.getenv("MRW_GROUP", "mrw_v1_group")    # наша consumer group (независима от других)
CONSUMER_NAME = os.getenv("MRW_CONSUMER", "mrw_1")     # имя consumer'а в группе
REQUIRED_TFS = {"m5", "m15", "h1"}                     # поддерживаемые ТФ

DEBOUNCE_MS = int(os.getenv("MRW_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY = int(os.getenv("MRW_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL = int(os.getenv("MRW_MAX_PER_SYMBOL", "4"))
XREAD_BLOCK_MS = int(os.getenv("MRW_BLOCK_MS", "1000"))
XREAD_COUNT = int(os.getenv("MRW_COUNT", "50"))

# 🔸 Глобальные структуры параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)         # общий лимит задач
symbol_semaphores: dict[str, asyncio.Semaphore] = {}   # лимит задач на один символ
bucket_tasks: dict[tuple, asyncio.Task] = {}           # (symbol, tf, open_time_ms) -> task


# 🔸 Вспомогательные функции
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


# 🔸 Обработка одного «бакета» (пока без бизнес-логики)
async def handle_bucket(log: logging.Logger, symbol: str, tf: str, open_time_ms: int):
    await asyncio.sleep(DEBOUNCE_MS / 1000)  # debounce: ждём дозапись всех базовых индикаторов
    # следующими этапами сюда добавим: чтение фич → классификация → запись (TS/KV/PG)
    log.info(f"[BUCKET] done: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл компонента: XREADGROUP indicator_stream и планирование бакетов
async def run_market_watcher(pg, redis):
    log = logging.getLogger("MRW")
    log.info(f"market_watcher starting: XGROUP={GROUP_NAME}, CONSUMER={CONSUMER_NAME}")

    # создаём consumer group (если уже есть — игнорируем BUSYGROUP)
    try:
        await redis.xgroup_create(READY_STREAM, GROUP_NAME, id="$", mkstream=True)
        log.info(f"consumer group '{GROUP_NAME}' created on '{READY_STREAM}'")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"consumer group '{GROUP_NAME}' already exists")
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

                        # базовая валидация входа
                        if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_time_iso:
                            to_ack.append(msg_id)
                            continue

                        open_time_ms = _iso_to_ms(open_time_iso)
                        bucket = (symbol, tf, open_time_ms)

                        # не планируем дубликаты
                        if bucket in bucket_tasks and not bucket_tasks[bucket].done():
                            to_ack.append(msg_id)
                            continue

                        # инициализация лимита на символ
                        if symbol not in symbol_semaphores:
                            symbol_semaphores[symbol] = asyncio.Semaphore(MAX_PER_SYMBOL)

                        log.info(f"[READY] {symbol}/{tf} @ {open_time_iso} → schedule bucket")

                        async def bucket_runner():
                            async with task_gate:
                                async with symbol_semaphores[symbol]:
                                    await handle_bucket(log, symbol, tf, open_time_ms)

                        bucket_tasks[bucket] = asyncio.create_task(bucket_runner())
                        to_ack.append(msg_id)

                    except Exception as parse_err:
                        # если сломались на конкретном сообщении — ack, чтобы не зациклиться, и логируем
                        to_ack.append(msg_id)
                        log.error(f"message parse error: {parse_err}", exc_info=True)

            # подтверждаем обработку прочитанных сообщений в нашей группе
            if to_ack:
                await redis.xack(READY_STREAM, GROUP_NAME, *to_ack)

        except Exception as e:
            log.error(f"XREADGROUP loop error: {e}", exc_info=True)
            await asyncio.sleep(1)