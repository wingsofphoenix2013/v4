# infra.py — инфраструктурный модуль системы
import os
import logging
import asyncpg
import redis.asyncio as aioredis
import asyncio

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Подключение к PostgreSQL
async def init_pg_pool():
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis (async + health-check)
async def init_redis_client():
    client = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        encoding="utf-8",
        socket_connect_timeout=3,
        socket_keepalive=True,
    )
    # health-check с короткими ретраями
    for attempt in range(3):
        try:
            await client.ping()
            return client
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(1 + attempt)

# 🔸 Безопасный запуск фонового воркера
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger("INFRA_PY")
    while True:
        try:
            log.info("Запуск воркера")
            await coro_fn()
        except Exception as e:
            log.error(f"Ошибка: {e}", exc_info=True)
            log.info(f"Перезапуск через {retry_delay} секунд...")
            await asyncio.sleep(retry_delay)

# 🔸 Настройка централизованного логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )