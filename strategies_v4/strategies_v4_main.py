# strategies_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client, listen_indicator_stream, init_indicator_cache_via_redis
from config_loader import init_config_state, config_event_listener
from strategy_loader import load_strategies
from position_state_loader import load_position_state
from signal_processor import run_signal_loop, set_strategy_registry
from position_opener import run_position_opener_loop
from position_handler import run_position_handler
from core_io import run_signal_log_writer, run_position_open_writer, run_position_update_writer

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
async def stub_reverse_trigger(): await asyncio.sleep(3600)

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск системы стратегий v4")

    try:
        await setup_pg()
        await setup_redis_client()
        await init_indicator_cache_via_redis()
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
        await load_position_state()
        log.info("📦 Позиции восстановлены из БД")
    except Exception:
        log.exception("❌ Ошибка при восстановлении позиций")
        return

    try:
        strategy_registry = load_strategies()
        set_strategy_registry(strategy_registry)
        log.info("🧠 Регистр стратегий загружен")
    except Exception:
        log.exception("❌ Ошибка при загрузке стратегий")
        return

    log.info("🚀 Запуск asyncio-воркеров")

    await asyncio.gather(
        run_safe_loop(run_signal_loop, "SIGNAL_PROCESSOR"),
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(run_signal_log_writer, "CORE_IO"),
        run_safe_loop(listen_indicator_stream, "INDICATOR_CACHE"),
        run_safe_loop(run_position_opener_loop, "POSITION_OPENER"),
        run_safe_loop(run_position_open_writer, "POSITION_DB_WRITER"),
        run_safe_loop(run_position_handler, "POSITION_HANDLER"),
        run_safe_loop(run_position_update_writer, "POSITION_UPDATE_WRITER"),
        run_safe_loop(stub_reverse_trigger, "REVERSE_TRIGGER")
    )
    
# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")