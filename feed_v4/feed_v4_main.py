# feed_v4_main.py — управляющий модуль системы v4

import asyncio
from infra import init_pg_pool, init_redis_client, run_safe_loop

# 🔸 Заглушки для воркеров (будут реализованы позже)
async def run_feed_and_aggregator(pg, redis):
    while True:
        await asyncio.sleep(1)  # заглушка цикла

async def run_indicator_worker(pg, redis):
    while True:
        await asyncio.sleep(1)  # заглушка цикла

async def run_snapshot_loop(pg, redis):
    while True:
        await asyncio.sleep(1)  # заглушка цикла

# 🔸 Главная точка запуска
async def main():
    # Инициализация подключений
    pg = await init_pg_pool()
    redis = init_redis_client()

    # Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: run_feed_and_aggregator(pg, redis), "FEED+AGGREGATOR"),
        run_safe_loop(lambda: run_indicator_worker(pg, redis), "INDICATORS"),
        run_safe_loop(lambda: run_snapshot_loop(pg, redis), "SNAPSHOT")
    )

# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())
