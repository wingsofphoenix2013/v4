# laboratory_infra.py — инфраструктура лабораторного сервиса (идентична infra.py из v4: ENV-паттерн, логи, PG/Redis, safe-loop)

# 🔸 Импорты
import os
import logging
import asyncpg
import redis.asyncio as aioredis
import asyncio

# 🔸 Локальные дефолты (если ENV не заданы)
# Заполните эти значения только если в окружении нет переменных:
LAB_DB_URL_DEFAULT = None            # например: "postgresql://user:pass@host:5432/db?sslmode=require"
LAB_REDIS_URL_DEFAULT = None         # например: "rediss://:password@host:6379/0"
LAB_DEBUG_MODE_DEFAULT = "false"     # "true" / "false"

# 🔸 Переменные окружения (совместимо с infra.py из indicators_v4)
DATABASE_URL = os.getenv("DATABASE_URL", LAB_DB_URL_DEFAULT or "")
REDIS_URL    = os.getenv("REDIS_URL",    LAB_REDIS_URL_DEFAULT or "")
DEBUG_MODE   = (os.getenv("DEBUG_MODE", LAB_DEBUG_MODE_DEFAULT).lower() == "true")

# 🔸 Подключение к PostgreSQL
async def init_pg_pool():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL не задан: укажите ENV или LAB_DB_URL_DEFAULT в laboratory_infra.py")
    return await asyncpg.create_pool(DATABASE_URL)

# 🔸 Подключение к Redis (async + health-check)
async def init_redis_client():
    if not REDIS_URL:
        raise RuntimeError("REDIS_URL не задан: укажите ENV или LAB_REDIS_URL_DEFAULT в laboratory_infra.py")
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
    log = logging.getLogger(name)
    while True:
        try:
            log.info("Запуск воркера")
            await coro_fn()
        except asyncio.CancelledError:
            log.info("Воркер остановлен (cancelled)")
            raise
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
    logging.getLogger("LAB_INFRA").info("Логирование инициализировано (level=%s)", "DEBUG" if DEBUG_MODE else "INFO")