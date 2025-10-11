# infra.py — инфраструктурный модуль системы

# 🔸 Импорты
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

# 🔸 Безопасный запуск фонового воркера (c именованием и перезапуском)
async def run_safe_loop(coro_fn, name: str, retry_delay: int = 5):
    """
    Бесконечно запускает корутину coro_fn().
    Логирует имя воркера, номер попытки и причину перезапуска.
    На любой ошибке перезапускает через retry_delay секунд.
    Если воркер «нормально завершился» (вернул управление) — также перезапускает.
    """
    base_log = logging.getLogger("INFRA_PY")
    attempt = 0
    while True:
        attempt += 1
        try:
            base_log.info(f"[{name}] запуск воркера (attempt={attempt})")
            await coro_fn()
            # если корутина вернулась без исключения — это нетипично для вечных воркеров
            base_log.warning(f"[{name}] завершился (вернул управление); перезапуск через {retry_delay} c.")
        except asyncio.CancelledError:
            # корректная остановка процесса
            base_log.info(f"[{name}] остановлен (CancelledError)")
            raise
        except Exception as e:
            base_log.error(f"[{name}] ошибка: {e}", exc_info=True)
            base_log.info(f"[{name}] перезапуск через {retry_delay} c.")
        # пауза перед перезапуском
        await asyncio.sleep(retry_delay)

# 🔸 Настройка централизованного логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )