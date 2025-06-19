# infra.py

import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальное состояние
class Infra:
    pg_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

infra = Infra()

# 🔸 Константы
SIGNAL_STREAM = "signals_stream"
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

ENABLED_TICKERS = {}
SIGNAL_CONFIGS = []
RULE_DEFINITIONS = {}

# 🔸 Настройка централизованного логирования
def setup_logging():
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

log = logging.getLogger("GEN")

# 🔸 Инициализация подключения к PostgreSQL
async def setup_pg():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("❌ DATABASE_URL не задан")

    pool = await asyncpg.create_pool(db_url)
    await pool.execute("SELECT 1")
    infra.pg_pool = pool
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
    infra.redis_client = client
    log.info("📡 Подключение к Redis установлено")

# 🔸 Загрузка тикеров из tickers_v4
async def load_enabled_tickers():
    query = """
        SELECT symbol, precision_price, precision_qty
        FROM tickers_v4
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        ENABLED_TICKERS.clear()
        ENABLED_TICKERS.update({
            row["symbol"]: {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"]
            }
            for row in rows
        })
    log.info(f"[INIT] Загружено тикеров: {len(ENABLED_TICKERS)}")

# 🔸 Загрузка сигналов из signals_v4
async def load_signal_configs():
    query = """
        SELECT id, name, long_phrase, short_phrase, timeframe, rule
        FROM signals_v4
        WHERE enabled = true AND source = 'generator'
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        SIGNAL_CONFIGS.clear()
        SIGNAL_CONFIGS.extend(rows)
    log.info(f"[INIT] Загружено сигналов генератора: {len(SIGNAL_CONFIGS)}")

# 🔸 Загрузка правил из signal_rules_v4
async def load_rule_definitions():
    query = "SELECT name, class_name, module_name FROM signal_rules_v4"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        RULE_DEFINITIONS.clear()
        RULE_DEFINITIONS.update({row["name"]: row for row in rows})
    log.info(f"[INIT] Загружено правил: {len(RULE_DEFINITIONS)}")

# 🔸 Комплексная загрузка всех конфигураций
async def load_configs():
    await load_enabled_tickers()
    await load_signal_configs()
    await load_rule_definitions()