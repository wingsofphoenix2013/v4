# infra.py

import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальные переменные
pg_pool = None
redis_client = None

# 🔸 Переменные окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Логгер для инфраструктуры
log = logging.getLogger("AUDITOR_INFRA")


# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
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
        timeout=30.0
    )
    await pool.execute("SELECT 1")
    globals()["pg_pool"] = pool
    log.info("🛢️ Подключение к PostgreSQL установлено")


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
        decode_responses=True
    )

    await client.ping()
    globals()["redis_client"] = client
    log.info("📡 Подключение к Redis установлено")
    
# 🔸 Глобальные конфигурации
enabled_tickers = {}
enabled_strategies = {}
enabled_indicators = {}


# 🔸 Обновление кэша тикеров
def set_enabled_tickers(new_dict: dict):
    global enabled_tickers
    enabled_tickers = new_dict
    log.debug("Кэш тикеров обновлён (%d)", len(new_dict))


# 🔸 Обновление кэша стратегий
def set_enabled_strategies(new_dict: dict):
    global enabled_strategies
    enabled_strategies = new_dict
    log.debug("Кэш стратегий обновлён (%d)", len(new_dict))


# 🔸 Обновление кэша индикаторов
def set_enabled_indicators(new_dict: dict):
    global enabled_indicators
    enabled_indicators = new_dict
    log.debug("Кэш индикаторов обновлён (%d)", len(new_dict))