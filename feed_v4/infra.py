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
    return await asyncpg.create_pool(
        DATABASE_URL,
        min_size=10,
        max_size=20  # Соответствует максимальному числу параллельных вставок в run_core_io
    )

# 🔸 Подключение к Redis
def init_redis_client():
    return aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        encoding="utf-8"
    )

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
    """
    Централизованная настройка логирования для всех компонентов системы.
    Если DEBUG_MODE=True — показываются debug/info/warning/error,
    если DEBUG_MODE=False — только info/warning/error.
    """
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )