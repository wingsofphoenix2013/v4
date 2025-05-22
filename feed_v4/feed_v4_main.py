# feed_v4_main.py — управляющий модуль системы v4
import uuid
import uuid
import asyncio
import logging
from infra import init_pg_pool, init_redis_client, run_safe_loop, setup_logging
from feed_and_aggregate import run_feed_and_aggregator
from core_io import run_core_writer
from feed_v4_auditor import run_auditor
# from indicators_v4 import run_indicators_v4


# 🔸 Попытка захватить лидерство через Redis Lock
async def try_acquire_team_lock(redis, lock_key="team_leader_lock", ttl=60):
    
    instance_id = str(uuid.uuid4())
    log = logging.getLogger("TEAM_LOCK")

    got = await redis.set(lock_key, instance_id, nx=True, ex=ttl)
    if not got:
        log.info("Лидер уже выбран — текущий инстанс завершает работу")
        return False

    log.info(f"Инстанс получил lock (instance_id={instance_id})")

    # 🔸 Обновление TTL lock-а в фоне
    async def refresh():
        try:
            while True:
                await asyncio.sleep(ttl / 2)
                current = await redis.get(lock_key)
                if current == instance_id:
                    await redis.expire(lock_key, ttl)
                    log.debug("Lock обновлён")
                else:
                    log.warning("Lock утерян, остановка TTL обновления")
                    break
        except asyncio.CancelledError:
            log.info("Обновление lock остановлено")

    asyncio.create_task(refresh())
    return True
# 🔸 Главная точка запуска
async def main():
    # Настройка логирования
    setup_logging()

    # Инициализация подключений
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # Попытка стать ведущим инстансом
    if not await try_acquire_team_lock(redis):
        return  # Завершаем, если это не лидер    

    # Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: run_feed_and_aggregator(pg, redis), "FEED+AGGREGATOR"),
        run_safe_loop(lambda: run_core_writer(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_auditor(pg, redis), "AUDITOR"),
#         run_safe_loop(lambda: run_indicators_v4(pg, redis), "INDICATORS_V4")
    )
# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())
