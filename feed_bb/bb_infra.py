# bb_infra.py — инфраструктура feed_bb (Bybit): логирование, PG/Redis клиенты, безопасный запуск

# 🔸 Импорты и зависимости
import os
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis

# 🔸 Переменные окружения (изолированы от v4)
BB_DATABASE_URL = os.getenv("BB_DATABASE_URL")
BB_REDIS_URL = os.getenv("BB_REDIS_URL")
BB_DEBUG = os.getenv("BB_DEBUG", "false").lower() == "true"

# 🔸 Подключение к PostgreSQL (пул 10..30)
async def init_pg_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        BB_DATABASE_URL,
        min_size=10,
        max_size=30
    )

# 🔸 Подключение к Redis
def init_redis_client() -> aioredis.Redis:
    return aioredis.from_url(
        BB_REDIS_URL,
        decode_responses=True,
        encoding="utf-8"
    )

# 🔸 Безопасный запуск фонового воркера
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger(name)
    while True:
        try:
            log.info("Запуск воркера")
            await coro_fn()
        except asyncio.CancelledError:
            log.info("Воркер остановлен")
            raise
        except Exception as e:
            log.error(f"Ошибка: {e}", exc_info=True)
            log.info(f"Перезапуск через {retry_delay} секунд...")
            await asyncio.sleep(retry_delay)

# 🔸 Централизованное логирование
def setup_logging():
    level = logging.DEBUG if BB_DEBUG else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )