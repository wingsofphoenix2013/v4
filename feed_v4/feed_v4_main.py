# feed_v4_main.py — управляющий модуль системы v4

import asyncio
from infra import init_pg_pool, init_redis_client, run_safe_loop, setup_logging
from feed_and_aggregate import run_feed_and_aggregator

# 🔸 Главная точка запуска
async def main():
    # Настройка логирования
    setup_logging()

    # Инициализация подключений
    pg = await init_pg_pool()
    redis = init_redis_client()

    # Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: run_feed_and_aggregator(pg, redis), "FEED+AGGREGATOR"),
        run_safe_loop(lambda: asyncio.sleep(1), "INDICATORS"),
        run_safe_loop(lambda: asyncio.sleep(1), "SNAPSHOT")
    )

# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())
