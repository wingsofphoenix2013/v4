import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальные переменные
pg_pool: asyncpg.Pool = None
redis_client: aioredis.Redis = None

# 🔸 Константы Redis Stream
SIGNAL_STREAM = "signals_stream"
EVENT_STREAM = "strategy_events"

# 🔸 DEBUG режим
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Настройка логов
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Инициализация PostgreSQL
async def setup_pg():
    global pg_pool
    pg_pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"))

# 🔸 Инициализация Redis
def setup_redis_client():
    global redis_client
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

    protocol = "rediss" if use_tls else "redis"
    redis_client = aioredis.from_url(
        f"{protocol}://{host}:{port}",
        password=password,
        decode_responses=True
    )