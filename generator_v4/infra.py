# infra.py

import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальные объекты и кэши
PG_POOL = None
REDIS = None
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

ENABLED_TICKERS = {}       # symbol → {precision_price, precision_qty}
SIGNAL_CONFIGS = []        # Список записей из signals_v4
RULE_DEFINITIONS = {}      # rule → {class_name, module_name}

log = logging.getLogger("GEN")

# 🔸 Настройка централизованного логирования
def setup_logging():
    """
    Централизованная настройка логирования для всех компонентов системы.
    Если DEBUG_MODE=True — показываются debug/info/warning/error,
    если DEBUG_MODE=False — только info/warning/error.
    """
    level = logging.DEBUG if DEBUG_MODE else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# 🔸 Инициализация подключения к PostgreSQL
async def init_pg():
    global PG_POOL
    dsn = os.environ["POSTGRES_DSN"]
    PG_POOL = await asyncpg.create_pool(dsn)
    log.info("[INIT] Подключение к PostgreSQL установлено")

# 🔸 Инициализация подключения к Redis
async def init_redis():
    global REDIS
    REDIS = aioredis.from_url(os.environ["REDIS_URL"])
    log.info("[INIT] Подключение к Redis установлено")

# 🔸 Загрузка активных тикеров из tickers_v4
async def load_enabled_tickers():
    global ENABLED_TICKERS
    query = """
        SELECT symbol, precision_price, precision_qty
        FROM tickers_v4
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """
    async with PG_POOL.acquire() as conn:
        rows = await conn.fetch(query)
        ENABLED_TICKERS = {
            row["symbol"]: {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"]
            }
            for row in rows
        }
    log.info(f"[INIT] Загружено тикеров: {len(ENABLED_TICKERS)}")

# 🔸 Загрузка сигналов генератора из signals_v4
async def load_signal_configs():
    global SIGNAL_CONFIGS
    query = """
        SELECT id, name, long_phrase, short_phrase, timeframe, rule
        FROM signals_v4
        WHERE enabled = true AND source = 'generator'
    """
    async with PG_POOL.acquire() as conn:
        SIGNAL_CONFIGS = await conn.fetch(query)
    log.info(f"[INIT] Загружено сигналов генератора: {len(SIGNAL_CONFIGS)}")

# 🔸 Загрузка определений правил из signal_rules_v4
async def load_rule_definitions():
    global RULE_DEFINITIONS
    query = "SELECT name, class_name, module_name FROM signal_rules_v4"
    async with PG_POOL.acquire() as conn:
        rows = await conn.fetch(query)
        RULE_DEFINITIONS = {row["name"]: row for row in rows}
    log.info(f"[INIT] Загружено правил: {len(RULE_DEFINITIONS)}")

# 🔸 Комплексная загрузка всех конфигураций
async def load_configs():
    await load_enabled_tickers()
    await load_signal_configs()
    await load_rule_definitions()