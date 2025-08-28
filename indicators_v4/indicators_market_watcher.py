# 🔸 indicators_market_watcher.py — Этап 1: каркас компонента и подписка на indicator_stream

import os
import asyncio
import json
import logging
from datetime import datetime

# 🔸 Константы и конфиг
READY_STREAM = "indicator_stream"                   # слушаем только этот стрим
REQUIRED_TFS = {"m5", "m15", "h1"}                  # поддерживаемые ТФ

DEBOUNCE_MS = int(os.getenv("MRW_DEBOUNCE_MS", "250"))
MAX_CONCURRENCY = int(os.getenv("MRW_MAX_CONCURRENCY", "64"))
MAX_PER_SYMBOL = int(os.getenv("MRW_MAX_PER_SYMBOL", "4"))

# 🔸 Глобальные структуры параллелизма
task_gate = asyncio.Semaphore(MAX_CONCURRENCY)      # общий лимит задач
symbol_semaphores: dict[str, asyncio.Semaphore] = {}  # лимит задач на один символ
bucket_tasks: dict[tuple, asyncio.Task] = {}          # (symbol, tf, open_time_ms) -> task


# 🔸 Вспомогательные функции
def _iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    return int(dt.timestamp() * 1000)


# 🔸 Обработка одного «бакета» (пока без бизнес-логики)
async def handle_bucket(log: logging.Logger, symbol: str, tf: str, open_time_ms: int):
    await asyncio.sleep(DEBOUNCE_MS / 1000)  # debounce: ждём дозапись всех базовых индикаторов
    # здесь позже появится: чтение фич → классификация → запись (TS/KV/PG)
    log.info(f"[BUCKET] done: {symbol}/{tf} @ {open_time_ms}")


# 🔸 Основной цикл компонента: подписка на indicator_stream и планирование бакетов
async def run_market_watcher(pg, redis):
    log = logging.getLogger("MRW")
    log.info("market_watcher starting: subscribe to indicator_stream")

    pubsub = redis.pubsub()
    await pubsub.subscribe(READY_STREAM)

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data.get("symbol")
            tf = data.get("timeframe") or data.get("interval")
            status = data.get("status")
            open_time_iso = data.get("open_time")

            # базовая валидация входа
            if not symbol or tf not in REQUIRED_TFS or status != "ready" or not open_time_iso:
                continue

            open_time_ms = _iso_to_ms(open_time_iso)
            bucket = (symbol, tf, open_time_ms)

            # не планируем дубликаты
            if bucket in bucket_tasks and not bucket_tasks[bucket].done():
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

        except Exception as e:
            log.error(f"stream parse error: {e}", exc_info=True)