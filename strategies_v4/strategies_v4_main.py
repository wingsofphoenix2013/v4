# strategies_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client
from config_loader import init_config_state, config_event_listener
from signal_processor import run_signal_loop
from position_handler import run_position_loop
from config_loader import config_event_listener

# 🔸 Настройка логгера для главного воркера
log = logging.getLogger("STRATEGY_MAIN")

# 🔸 Обёртка для безопасного запуска задач с авто-перезапуском
async def run_safe_loop(coro_factory, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro_factory()
        except Exception as e:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)

# 🔸 Основная точка входа
async def main():
    setup_logging()
    await setup_pg()
    setup_redis_client()
    await init_config_state()

    log.info("🚀 Воркеры стратегий v4 запущены")

    await asyncio.gather(
        run_safe_loop(lambda: run_signal_loop(), "SIGNAL_PROCESSOR"),
        run_safe_loop(lambda: run_position_loop(), "POSITION_HANDLER"),
        run_safe_loop(lambda: config_event_listener(), "CONFIG_LOADER"),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("👋 Остановка по Ctrl+C")
    except Exception as e:
        log.exception("💥 Фатальная ошибка")