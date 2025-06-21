# auditor_v4_main.py

import asyncio
import signal
import logging

from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from core_io import pg_task
from redis_io import redis_task

# 🔸 Логгер для главного процесса
log = logging.getLogger("AUDITOR_MAIN")


# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro_factory, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro_factory()
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)


# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса auditor_v4")

    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения PG и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("🚀 Запуск фоновых воркеров")

    await asyncio.gather(
        run_safe_loop(lambda: pg_task(stop_event), "CORE_IO"),
        run_safe_loop(lambda: redis_task(stop_event), "REDIS_IO"),
        stop_event.wait()
    )


if __name__ == "__main__":
    asyncio.run(main())