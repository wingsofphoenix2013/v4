import asyncio
import logging

from infra import (
    setup_logging,
    setup_redis_client,
)
from redis_io import redis_task

# 🔸 Логгер для главного процесса
log = logging.getLogger("AUDITOR_MAIN")


# 🔸 Обёртка с автоперезапуском
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro()
            break  # Выполнить один раз и выйти
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)


# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск одноразового Redis TS обновления")

    try:
        await setup_redis_client()
        log.info("🔌 Подключение к Redis установлено")
    except Exception:
        log.exception("❌ Ошибка подключения к Redis")
        return

    await run_safe_loop(redis_task, "REDIS_RETENTION_UPDATER")


if __name__ == "__main__":
    asyncio.run(main())