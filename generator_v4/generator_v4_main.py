# generator_v4_main.py

import asyncio
import logging

# 🔸 Импорт базовых компонентов
from infra import setup_logging, setup_pg, setup_redis_client, load_configs

from rule_processor import run_rule_processor
from core_io import run_core_io
from events_listener import run_event_listener

log = logging.getLogger("GEN")

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
    log.info("🚀 Запуск generator_v4")

    try:
        await setup_pg()
        await setup_redis_client()
        await load_configs()
    except Exception:
        log.exception("❌ Ошибка инициализации сервисов")
        return

    log.info("✅ Инициализация завершена, запуск воркеров")

    await asyncio.gather(
        run_safe_loop(run_rule_processor, "RULE_PROCESSOR"),
        run_safe_loop(run_core_io, "CORE_IO"),
        run_safe_loop(run_event_listener, "PUBSUB_WATCHER")
    )

# 🔸 Точка запуска
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка при запуске generator_v4")