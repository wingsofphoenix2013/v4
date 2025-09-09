# feed_bb_main.py — управляющий модуль feed_bb: минимальный запуск (dry), PG/Redis и heartbeat

# 🔸 Импорты и зависимости
import asyncio
import logging
from bb_infra import setup_logging, init_pg_pool, init_redis_client, run_safe_loop
from bb_stream_maintenance import run_stream_maintenance_bb
from bb_core_io import run_core_io_bb
from bb_feed_auditor import run_feed_auditor_bb

log = logging.getLogger("FEED_BB_MAIN")

# 🔸 Heartbeat-воркер (держит процесс живым, показывает, что main работает)
async def heartbeat():
    while True:
        log.info("feed_bb main up (dry) — heartbeat")
        await asyncio.sleep(60)

# 🔸 Главная точка запуска
async def main():
    setup_logging()
    pg_pool = await init_pg_pool()   # <-- теперь psycopg pool
    redis = init_redis_client()
    log.info("PG/Redis подключены (feed_bb)")

    await asyncio.gather(
        run_safe_loop(heartbeat, "HEARTBEAT"),
        run_safe_loop(lambda: run_stream_maintenance_bb(redis), "BB_STREAM_MAINT"),
        run_safe_loop(lambda: run_core_io_bb(pg_pool, redis), "BB_CORE_IO"),
        run_safe_loop(lambda: run_feed_auditor_bb(pg_pool, redis), "BB_FEED_AUDITOR"),
    )

if __name__ == "__main__":
    asyncio.run(main())