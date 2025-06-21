# core_io.py

import asyncio
import logging

from infra import pg_pool

# 🔸 Логгер для PostgreSQL операций
log = logging.getLogger("CORE_IO")


# 🔸 Основной воркер PostgreSQL
async def pg_task(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            # Здесь будет логика: чистка, агрегация, аудит и т.п.
            log.info("⏳ pg_task: имитация работы с PostgreSQL")
            await asyncio.sleep(10)

        except Exception:
            log.exception("❌ Ошибка в pg_task — продолжаем выполнение")
            await asyncio.sleep(5)  # предотвратить лавинообразный перезапуск