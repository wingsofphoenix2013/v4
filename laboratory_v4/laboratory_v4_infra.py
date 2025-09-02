# laboratory_v4_infra.py — базовая инфраструктура Laboratory v4 (PG/Redis, логирование, safe-loop)

import os
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis

# 🔸 Конфиг из ENV
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL    = os.getenv("REDIS_URL")
LAB_DEBUG    = os.getenv("LAB_DEBUG", "false").lower() == "true"

# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if LAB_DEBUG else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Подключение к PostgreSQL (pool)
async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis (async) с короткими ретраями
async def init_redis_client():
    client = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
        socket_connect_timeout=3,
        socket_keepalive=True,
    )
    for attempt in range(3):
        try:
            await client.ping()
            return client
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(1 + attempt)

# 🔸 Безопасный запуск асинхронного воркера в цикле
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger(name)
    while True:
        try:
            log.info("🚀 Запуск задачи")
            await coro_fn()
        except asyncio.CancelledError:
            log.info("⏹️ Остановка задачи")
            raise
        except Exception as e:
            log.error(f"❌ Ошибка: {e}", exc_info=True)
            log.info(f"⏳ Перезапуск через {retry_delay} с...")
            await asyncio.sleep(retry_delay)