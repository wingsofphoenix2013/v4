# redis_io.py

import asyncio
import logging

from infra import redis_client

# 🔸 Логгер для Redis операций
log = logging.getLogger("REDIS_IO")


# 🔸 Основной воркер Redis
async def redis_task():
    log.info("🔁 [redis_task] стартует")

    while True:
        try:
            # Здесь будет логика: очистка ключей, аудит, метрики и т.п.
            log.info("⏳ redis_task: имитация работы с Redis")
            await asyncio.sleep(600)

        except Exception:
            log.exception("❌ Ошибка в redis_task — продолжаем выполнение")
            await asyncio.sleep(5)  # обязательная пауза при ошибке