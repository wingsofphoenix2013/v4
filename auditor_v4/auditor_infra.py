# 🔸 auditor_infra.py — инфраструктура auditor_v4: логирование, подключения к PostgreSQL/Redis, глобальные кэши

# 🔸 Импорты
import os
import logging
import asyncpg
import redis.asyncio as aioredis

from typing import Optional

# 🔸 Глобальные подключения
pg_pool: Optional[asyncpg.pool.Pool] = None
redis_client: Optional[aioredis.Redis] = None

# 🔸 Переменные окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Логгер
log = logging.getLogger("AUD_INFRA")


# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log.debug("Логирование настроено (DEBUG_MODE=%s)", DEBUG_MODE)


# 🔸 Инициализация подключения к PostgreSQL
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    pool = await asyncpg.create_pool(
        dsn=db_url,
        min_size=2,
        max_size=10,
        timeout=30.0,
    )

    # health-check
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")

    globals()["pg_pool"] = pool
    log.debug("🛢️ Подключение к PostgreSQL установлено")


# 🔸 Инициализация подключения к Redis
async def setup_redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

    protocol = "rediss" if use_tls else "redis"
    redis_url = f"{protocol}://{host}:{port}"

    client = aioredis.from_url(
        redis_url,
        password=password,
        decode_responses=True,  # строки на вход/выход
    )

    # health-check
    await client.ping()

    globals()["redis_client"] = client
    log.debug("📡 Подключение к Redis установлено")