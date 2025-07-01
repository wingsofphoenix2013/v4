# auditor_v4_main.py

import asyncio
import logging

from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from config_loader import (
    load_enabled_tickers,
    load_enabled_strategies,
    load_enabled_indicators,
    config_event_listener,
)
from core_io import pg_task, finmonitor_task, treasury_task
from redis_io import redis_task

# 🔸 Логгер для главного процесса
log = logging.getLogger("AUDITOR_MAIN")


# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro()
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
        
    try:
        await load_enabled_tickers()
        await load_enabled_strategies()
        await load_enabled_indicators()
        log.info("📦 Конфигурации тикеров, стратегий и индикаторов загружены")
    except Exception:
        log.exception("❌ Ошибка загрузки начальной конфигурации")
        return

    log.info("🚀 Запуск фоновых воркеров")

    await asyncio.gather(
        run_safe_loop(pg_task, "CORE_IO"),
        run_safe_loop(redis_task, "REDIS_IO"),
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(finmonitor_task, "FINMONITOR"),
        run_safe_loop(treasury_task, "TREASURY")
    )


if __name__ == "__main__":
    asyncio.run(main())