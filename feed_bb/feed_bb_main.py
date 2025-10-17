# feed_bb_main.py — управляющий модуль feed_bb: запуск воркеров, 90с пауза для аггрегаторов и markprice

# 🔸 Импорты и зависимости
import os
import asyncio
import logging

from bb_infra import setup_logging, init_pg_pool, init_redis_client, run_safe_loop
from bb_stream_maintenance import run_stream_maintenance_bb
from bb_core_io import run_core_io_bb
from bb_feed_auditor import run_feed_auditor_bb
from bb_feed_ts_filler import run_feed_ts_filler_bb
from bb_feed_healer import run_feed_healer_bb
from bb_tickers_precision_updater import run_tickers_precision_updater_bb
from bb_feed_and_aggregate import (
    run_feed_and_aggregator_m5_bb,
    run_feed_and_aggregator_m15_bb,
    run_feed_and_aggregator_h1_bb,
)
from bb_markprice_watcher import run_markprice_watcher_bb
from feed_cleaner import run_feed_cleaner_bb

log = logging.getLogger("FEED_BB_MAIN")

# 🔸 Главная точка запуска
async def main():
    setup_logging()

    # подключение к инфраструктуре
    pg_pool = await init_pg_pool()
    redis = init_redis_client()
    log.info("PG/Redis подключены (feed_bb)")

    # 🔸 Пауза перед стартом WS-воркеров (аггрегаторы + markprice)
    aggr_delay_sec = int(os.getenv("BB_AGGR_START_DELAY_SEC", "90"))

    async def delayed_ws_workers():
        if aggr_delay_sec > 0:
            log.info(f"⏸ Пауза {aggr_delay_sec}s перед запуском агрегаторов и markprice (bb)")
            await asyncio.sleep(aggr_delay_sec)
        await asyncio.gather(
            run_safe_loop(lambda: run_feed_and_aggregator_m5_bb(pg_pool, redis),  "BB_FEED_AGGR:M5"),
            run_safe_loop(lambda: run_feed_and_aggregator_m15_bb(pg_pool, redis), "BB_FEED_AGGR:M15"),
            run_safe_loop(lambda: run_feed_and_aggregator_h1_bb(pg_pool, redis),  "BB_FEED_AGGR:H1"),
            run_safe_loop(lambda: run_markprice_watcher_bb(pg_pool, redis),       "BB_MARKPRICE"),
        )

    # 🔸 Запуск остальных воркеров сразу (сервер считается «живым»)
    await asyncio.gather(
        run_safe_loop(lambda: run_stream_maintenance_bb(redis),            "BB_STREAM_MAINT"),
        run_safe_loop(lambda: run_core_io_bb(pg_pool, redis),              "BB_CORE_IO"),
        run_safe_loop(lambda: run_feed_auditor_bb(pg_pool, redis),         "BB_FEED_AUDITOR"),
        run_safe_loop(lambda: run_feed_ts_filler_bb(pg_pool, redis),       "BB_TS_FILLER"),
        run_safe_loop(lambda: run_feed_healer_bb(pg_pool, redis),          "BB_FEED_HEALER"),
        run_safe_loop(lambda: run_tickers_precision_updater_bb(pg_pool),   "BB_PRECISION_UPDATER"),
        run_safe_loop(lambda: run_feed_cleaner_bb(pg_pool, redis),         "BB_FEED_CLEANER"),
        delayed_ws_workers(),  # аггрегаторы и markprice стартуют спустя задержку
    )

if __name__ == "__main__":
    asyncio.run(main())