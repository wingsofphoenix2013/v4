# strategies_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client
from config_loader import init_config_state, config_event_listener
from strategy_loader import load_strategies

# 🔸 Логгер для главного процесса
log = logging.getLogger("STRATEGY_MAIN")

# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro_factory, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro_factory()
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)

# 🔸 Заглушки (временно, до полной реализации)
async def stub_signal_processor(): await asyncio.sleep(3600)
async def stub_core_io_signal_log_writer(): await asyncio.sleep(3600)
async def stub_position_opener(): await asyncio.sleep(3600)
async def stub_position_writer(): await asyncio.sleep(3600)
async def stub_position_handler(): await asyncio.sleep(3600)
async def stub_position_update_writer(): await asyncio.sleep(3600)
async def stub_reverse_trigger(): await asyncio.sleep(3600)

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск системы стратегий v4")

    try:
        await setup_pg()
        await setup_redis_client()
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    try:
        await init_config_state()
        log.info("🧩 Конфигурация стратегий загружена")
    except Exception:
        log.exception("❌ Ошибка инициализации конфигурации")
        return

    try:
        strategy_registry = load_strategies()
        log.info("🧠 Регистр стратегий загружен")
    except Exception:
        log.exception("❌ Ошибка при загрузке стратегий")
        return

    log.info("🚀 Запуск asyncio-воркеров")

    await asyncio.gather(
        run_safe_loop(stub_signal_processor, "SIGNAL_PROCESSOR"),
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(stub_core_io_signal_log_writer, "CORE_IO"),
        run_safe_loop(stub_position_opener, "POSITION_OPENER"),
        run_safe_loop(stub_position_writer, "POSITION_WRITER"),
        run_safe_loop(stub_position_handler, "POSITION_HANDLER"),
        run_safe_loop(stub_position_update_writer, "POSITION_UPDATE_WRITER"),
        run_safe_loop(stub_reverse_trigger, "REVERSE_TRIGGER")
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")