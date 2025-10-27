# trader_infra.py — инфраструктурный компонент (подключения к PostgreSQL и Redis)

# 🔸 Импорты
import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Логгер инфраструктуры
log = logging.getLogger("TRADER_INFRA")

# 🔸 Глобальное состояние
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

infra = Infra()

# 🔸 Константы (жёстко в коде по требованиям)
DEBUG_MODE = False  # без вынесения в ENV

# 🔸 Логирование
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 PostgreSQL: инициализация пула
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    pool = await asyncpg.create_pool(db_url)
    # проверка соединения
    await pool.execute("SELECT 1")
    infra.pg_pool = pool
    log.info("🛢️ Подключение к PostgreSQL установлено")

# 🔸 Redis: инициализация клиента
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

    # проверка соединения
    await client.ping()
    infra.redis_client = client
    log.info("📡 Подключение к Redis установлено")