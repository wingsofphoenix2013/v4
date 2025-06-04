import os
import json
import logging
import asyncio
import asyncpg
import redis.asyncio as aioredis

# 🔸 Контейнер глобального состояния
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

infra = Infra()

# 🔸 Константы Redis
SIGNAL_STREAM = "signals_stream"
EVENT_STREAM = "strategy_events"

# 🔸 DEBUG режим
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Настройка логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Подключение к PostgreSQL
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    infra.pg_pool = await asyncpg.create_pool(db_url)

# 🔸 Подключение к Redis
def setup_redis_client():
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", 6379))
    password = os.getenv("REDIS_PASSWORD")
    use_tls = os.getenv("REDIS_USE_TLS", "false").lower() == "true"

    protocol = "rediss" if use_tls else "redis"
    redis_url = f"{protocol}://{host}:{port}"

    infra.redis_client = aioredis.from_url(
        redis_url,
        password=password,
        decode_responses=True
    )
# 🔸 Загрузка значений индикаторов из Redis
async def load_indicators(symbol: str, params: list[str], timeframe: str) -> dict:
    redis = infra.redis_client
    result = {}

    if not redis:
        raise RuntimeError("❌ Redis клиент не инициализирован")

    keys = [f"ind:{symbol}:{timeframe}:{param}" for param in params]
    values = await redis.mget(*keys)

    for param, value in zip(params, values):
        result[param] = float(value) if value is not None else None

    return result
