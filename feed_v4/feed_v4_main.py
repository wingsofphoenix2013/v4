# feed_v4_main.py — управляющий модуль системы v4
import asyncio
import logging

from infra import init_pg_pool, init_redis_client, run_safe_loop, setup_logging
from feed_and_aggregate import run_feed_and_aggregator, load_all_tickers, handle_ticker_events
from core_io import run_core_io
from markprice_watcher import run_markprice_watcher

# 🔸 Главная точка запуска
async def main():
    # Настройка логирования
    setup_logging()

    # Инициализация подключений
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # Загрузка тикеров и состояния
    tickers, active = await load_all_tickers(pg)
    state = {
        "tickers": tickers,
        "active": active,
        "activated_at": {},
        "markprice_tasks": {},
    }
    refresh_queue = asyncio.Queue()

    # Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: handle_ticker_events(redis, state, pg, refresh_queue), "TICKER_EVENTS"),
        run_safe_loop(lambda: run_feed_and_aggregator(state, redis, pg, refresh_queue), "FEED+AGGREGATOR"),
        run_safe_loop(lambda: run_core_io(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_markprice_watcher(state, redis), "MARKPRICE")
    )

# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())
