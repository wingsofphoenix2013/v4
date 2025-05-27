import asyncio
import logging

from infra import setup_logging, init_pg_pool, init_redis_client

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

# 🔸 Основной запуск
async def main():
    setup_logging()
    log.info("Инициализация signals_v4")

    # Инициализация подключений
    await init_pg_pool()
    await init_redis_client()
    log.info("Подключения Redis и PostgreSQL установлены")

    # Параллельный запуск всех воркеров
    await asyncio.gather(
        run_safe_loop(load_initial_state, "STATE_LOADER"),
        run_safe_loop(subscribe_and_watch_pubsub, "PUBSUB_WATCHER"),
        run_safe_loop(read_and_process_signals, "SIGNAL_STREAM_READER")
    )

# 🔸 Загрузка справочников
async def load_initial_state():
    log = logging.getLogger("STATE_LOADER")
    log.info("Загрузка активных тикеров, сигналов и стратегий...")
    await asyncio.sleep(999999)  # временная заглушка

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

if __name__ == "__main__":
    asyncio.run(main())