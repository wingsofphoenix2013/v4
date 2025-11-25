# backtester_infra.py — инфраструктурный модуль для backtester_v1

import os
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis

# 🔸 Переменные окружения
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"


# 🔸 Подключение к PostgreSQL (async pool)
async def init_pg_pool():
    log = logging.getLogger("BT_INFRA")
    if not DATABASE_URL:
        log.error("DATABASE_URL не задан")
        raise RuntimeError("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(DATABASE_URL)
    log.debug("BT_INFRA: пул соединений PostgreSQL инициализирован")
    return pool


# 🔸 Подключение к Redis (async + health-check)
async def init_redis_client():
    log = logging.getLogger("BT_INFRA")
    if not REDIS_URL:
        log.error("REDIS_URL не задан")
        raise RuntimeError("REDIS_URL is not set")

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
            log.debug("BT_INFRA: Redis клиент успешно инициализирован")
            return client
        except Exception as e:
            # последняя попытка — пробрасываем ошибку
            if attempt == 2:
                log.error(f"BT_INFRA: не удалось подключиться к Redis: {e}")
                raise
            wait_sec = 1 + attempt
            log.warning(f"BT_INFRA: ошибка Redis ping (попытка {attempt + 1}), повтор через {wait_sec} сек")
            await asyncio.sleep(wait_sec)


# 🔸 Обёртка для безопасного запуска воркеров (перезапуск при ошибках)
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    log = logging.getLogger("BT_INFRA")
    while True:
        try:
            log.debug(f"[{name}] запуск воркера")
            await coro_fn()
        except Exception as e:
            log.error(f"[{name}] ошибка: {e}", exc_info=True)
            log.debug(f"[{name}] перезапуск через {retry_delay} секунд...")
            await asyncio.sleep(retry_delay)


# 🔸 Централизованная настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("BT_INFRA")
    log.debug(f"BT_INFRA: логирование настроено, уровень = {'DEBUG' if DEBUG_MODE else 'INFO'}")