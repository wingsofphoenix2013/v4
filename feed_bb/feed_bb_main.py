# feed_bb_main.py — управляющий модуль feed_bb: минимальный запуск (dry), PG/Redis и heartbeat

# 🔸 Импорты и зависимости
import asyncio
import logging
from bb_infra import setup_logging, init_pg_pool, init_redis_client, run_safe_loop

log = logging.getLogger("FEED_BB_MAIN")

# 🔸 Heartbeat-воркер (держит процесс живым, показывает, что main работает)
async def heartbeat():
    while True:
        log.info("feed_bb main up (dry) — heartbeat")
        await asyncio.sleep(10)

# 🔸 Главная точка запуска
async def main():
    setup_logging()

    # подключение к инфраструктуре
    pg = await init_pg_pool()
    redis = init_redis_client()
    log.info("PG/Redis подключены (feed_bb)")

    # здесь позже добавим остальные воркеры через run_safe_loop(...)
    await asyncio.gather(
        run_safe_loop(heartbeat, "HEARTBEAT")
    )

if __name__ == "__main__":
    asyncio.run(main())