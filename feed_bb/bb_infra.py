# bb_infra.py — инфраструктура feed_bb (Bybit): логирование, PG/Redis клиенты, безопасный запуск

# 🔸 Импорты и зависимости
import os
import logging
import asyncio
import psycopg
from psycopg_pool import AsyncConnectionPool
import redis.asyncio as aioredis

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Подключение к PostgreSQL (пул 10..30, явное open)
async def init_pg_pool() -> AsyncConnectionPool:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    pool = AsyncConnectionPool(
        conninfo=DATABASE_URL,
        min_size=10,
        max_size=30,
        open=False,
        kwargs={"autocommit": False}
    )
    await pool.open()
    async with pool.connection() as _:
        pass
    return pool

# 🔸 Подключение к Redis
def init_redis_client() -> aioredis.Redis:
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL not set")
    return aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        encoding="utf-8"
    )

# 🔸 Безопасный запуск фонового воркера
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger(name)
    while True:
        try:
            log.debug("Запуск воркера")
            await coro_fn()
        except asyncio.CancelledError:
            log.debug("Воркер остановлен")
            raise
        except Exception as e:
            log.error(f"Ошибка: {e}", exc_info=True)
            log.debug(f"Перезапуск через {retry_delay} секунд...")
            await asyncio.sleep(retry_delay)

# 🔸 Централизованное логирование
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )