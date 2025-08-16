# feed_v4_main.py — управляющий модуль системы v4

import asyncio
import logging

from infra import init_pg_pool, init_redis_client, run_safe_loop, setup_logging
from core_io import run_core_io, listen_ticker_activations
from markprice_watcher import run_markprice_watcher
from feed_and_aggregate import (
    run_feed_and_aggregator_m5,
    run_feed_and_aggregator_m15,
    run_feed_and_aggregator_h1,
    load_all_tickers,
    handle_ticker_events
)
from feed_auditor import run_feed_auditor
from feed_healer import run_feed_healer
from feed_ts_filler import run_feed_ts_filler
from feed_stream_maintenance import run_stream_maintenance

# 🔸 Главная точка запуска
async def main():
    setup_logging()

    pg = await init_pg_pool()
    redis = await init_redis_client()

    tickers, active, activated_at = await load_all_tickers(pg)

    state = {
        "tickers": tickers,
        "active": active,
        "activated_at": activated_at,
        "markprice_tasks": {},
        "m5_tasks": {},
        "m15_tasks": {},
        "h1_tasks": {},
    }

    # 🔸 Независимые очереди для каждого интервала
    refresh_queue_m5 = asyncio.Queue()
    refresh_queue_m15 = asyncio.Queue()
    refresh_queue_h1 = asyncio.Queue()

    # 🔸 Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: handle_ticker_events(
            redis, state, pg,
            refresh_queue_m5,
            refresh_queue_m15, refresh_queue_h1
        ), "TICKER_EVENTS"),

        run_safe_loop(lambda: run_feed_and_aggregator_m5(
            state, redis, pg, refresh_queue_m5
        ), "FEED+AGGREGATOR:M5"),

        run_safe_loop(lambda: run_feed_and_aggregator_m15(
            state, redis, pg, refresh_queue_m15
        ), "FEED+AGGREGATOR:M15"),

        run_safe_loop(lambda: run_feed_and_aggregator_h1(
            state, redis, pg, refresh_queue_h1
        ), "FEED+AGGREGATOR:H1"),

        run_safe_loop(lambda: run_core_io(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_markprice_watcher(state, redis), "MARKPRICE"),
        run_safe_loop(lambda: run_feed_auditor(pg, redis), "FEED_AUDITOR"),
        run_safe_loop(lambda: run_feed_healer(pg, redis), "FEED_HEALER"),
        run_safe_loop(lambda: run_feed_ts_filler(pg, redis), "TS_FILLER"),
        run_safe_loop(lambda: run_stream_maintenance(redis), "STREAM_MAINT"),
        run_safe_loop(lambda: listen_ticker_activations(pg, redis), "TICKER_ACTIVATIONS")
    )

if __name__ == "__main__":
    asyncio.run(main())