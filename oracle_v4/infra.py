# 🔸 infra.py — инфраструктура oracle_v4: логирование, PG/Redis, кэши тикеров и стратегий

import os
import logging
import asyncpg
import redis.asyncio as aioredis

# 🔸 Глобальные переменные
pg_pool = None
redis_client = None
enabled_tickers: dict[str, dict] = {}
market_watcher_strategies: set[int] = set()
king_watcher_strategies: set[int] = set()  # NEW

# 🔸 Переменные окружения
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 🔸 Логгер
log = logging.getLogger("ORACLE_INFRA")


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
    # быстрый health-check
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")

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
        decode_responses=True,  # строки на вход/выход
    )

    # health-check
    await client.ping()

    globals()["redis_client"] = client
    log.info("📡 Подключение к Redis установлено")


# 🔸 Обновление кэша тикеров
def set_enabled_tickers(new_dict: dict):
    global enabled_tickers
    enabled_tickers = new_dict or {}
    log.debug("Кэш тикеров обновлён (%d)", len(enabled_tickers))


# 🔸 Обновление кэша стратегий (king_watcher=true)
def set_king_watcher_strategies(id_set: set[int]):
    global king_watcher_strategies
    king_watcher_strategies = set(int(x) for x in (id_set or set()))
    log.info("🧠 Кэш стратегий king_watcher обновлён (%d)", len(king_watcher_strategies))

def add_king_watcher_strategy(sid: int):
    king_watcher_strategies.add(int(sid))
    log.debug("Добавлена стратегия в кэш king_watcher: %s (итого %d)", sid, len(king_watcher_strategies))

def remove_king_watcher_strategy(sid: int):
    king_watcher_strategies.discard(int(sid))
    log.debug("Удалена стратегия из кэша king_watcher: %s (итого %d)", sid, len(king_watcher_strategies))


# 🔸 Точечное добавление стратегии в кэш
def add_market_watcher_strategy(sid: int):
    market_watcher_strategies.add(int(sid))
    log.debug("Добавлена стратегия в кэш market_watcher: %s (итого %d)", sid, len(market_watcher_strategies))


# 🔸 Точечное удаление стратегии из кэша
def remove_market_watcher_strategy(sid: int):
    market_watcher_strategies.discard(int(sid))
    log.debug("Удалена стратегия из кэша market_watcher: %s (итого %d)", sid, len(market_watcher_strategies))

