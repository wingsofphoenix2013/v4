import logging
import os
import asyncpg
import redis.asyncio as redis

# 🔸 Режим отладки через переменные окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# 🔸 Настройка централизованного логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Redis и PostgreSQL клиенты
REDIS = None
PG_POOL = None

# 🔸 In-memory справочники
ENABLED_TICKERS = {}
ENABLED_SIGNALS = {}
ENABLED_STRATEGIES = {}

# 🔸 Инициализация Redis
async def init_redis_client():
    global REDIS
    REDIS = redis.from_url(REDIS_URL, decode_responses=True)

# 🔸 Инициализация PostgreSQL пула
async def init_pg_pool():
    global PG_POOL
    log = logging.getLogger("PG_INIT")
    log.info("Попытка подключения к PostgreSQL")
    PG_POOL = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=10,
        max_size=20
    )
    log.info("PG_POOL успешно инициализирован")