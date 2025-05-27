import asyncio
import logging
import infra  # <— используем как модуль

from infra import (
    setup_logging,
    init_pg_pool,
    init_redis_client,
    ENABLED_TICKERS,
    ENABLED_SIGNALS,
    ENABLED_STRATEGIES
)

log = logging.getLogger("SIGNALS_COORDINATOR")

# 🔸 Обёртка безопасного запуска задач
async def run_safe_loop(coro_factory, name: str):
    logger = logging.getLogger(name)
    while True:
        try:
            logger.info(f"Запуск задачи: {name}")
            await coro_factory()
        except Exception as e:
            logger.exception(f"Ошибка в задаче {name}: {e}")
        await asyncio.sleep(1)

# 🔸 Загрузка разрешённых тикеров из БД
async def load_enabled_tickers():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_TICKERS.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        ENABLED_TICKERS.update(row["symbol"] for row in rows)
    log.info(f"Загружено {len(ENABLED_TICKERS)} активных тикеров")

# 🔸 Загрузка активных сигналов из БД
async def load_enabled_signals():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_SIGNALS.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, long_phrase, short_phrase
            FROM signals_v4
            WHERE enabled = true
        """)
        for row in rows:
            ENABLED_SIGNALS[row["id"]] = {
                "long": row["long_phrase"],
                "short": row["short_phrase"]
            }
    log.info(f"Загружено {len(ENABLED_SIGNALS)} сигналов")

# 🔸 Загрузка активных стратегий из БД
async def load_enabled_strategies():
    log = logging.getLogger("STATE_LOADER")
    ENABLED_STRATEGIES.clear()
    async with infra.PG_POOL.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, signal_id, allow_open, reverse
            FROM strategies_v4
            WHERE enabled = true AND archived = false
        """)
        for row in rows:
            ENABLED_STRATEGIES[row["id"]] = {
                "signal_id": row["signal_id"],
                "allow_open": row["allow_open"],
                "reverse": row["reverse"]
            }
    log.info(f"Загружено {len(ENABLED_STRATEGIES)} стратегий")

# 🔸 Загрузка начальных справочников из БД
async def load_initial_state():
    log = logging.getLogger("STATE_LOADER")
    log.info("Загрузка активных тикеров, сигналов и стратегий...")

    await load_enabled_tickers()
    await load_enabled_signals()
    await load_enabled_strategies()

    await asyncio.sleep(999999)

# 🔸 Подписка на Pub/Sub обновления
async def subscribe_and_watch_pubsub():
    log = logging.getLogger("PUBSUB_WATCHER")
    log.info("Подписка на каналы Pub/Sub...")
    await asyncio.sleep(999999)

# 🔸 Чтение из Redis Stream
async def read_and_process_signals():
    log = logging.getLogger("SIGNAL_STREAM_READER")
    log.info("Чтение сигналов из Redis Stream...")
    await asyncio.sleep(999999)

# 🔸 Основной запуск
async def main():
    setup_logging()
    log.info("Инициализация signals_v4")

    await init_pg_pool()
    await init_redis_client()
    log.info("Подключения Redis и PostgreSQL установлены")

    await load_initial_state()

    await asyncio.gather(
        run_safe_loop(subscribe_and_watch_pubsub, "PUBSUB_WATCHER"),
        run_safe_loop(read_and_process_signals, "SIGNAL_STREAM_READER")
    )

if __name__ == "__main__":
    asyncio.run(main())