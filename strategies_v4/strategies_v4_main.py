# strategies_v4_main.py — главный оркестратор подсистемы стратегий v4 (+ воркер TRADER_INFORMER без тумблера)

# 🔸 Импорты
import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client
from config_loader import init_config_state, config_event_listener, listen_strategy_update_stream
from strategy_loader import load_strategies
from position_state_loader import load_position_state
from signal_processor import run_signal_loop, set_strategy_registry
from position_opener import run_position_opener_loop
from position_handler import run_position_handler
from core_io import run_signal_log_writer, run_position_open_writer, run_position_update_writer
from strategies_v4_cleaner import run_strategies_v4_cleaner
from lab_response_router import run_lab_response_router
from trader_informer import run_trader_informer  # новый воркер мгновенных уведомлений (positions_bybit_status)

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

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск системы стратегий v4")

    # инициализация внешних сервисов
    try:
        await setup_pg()
        await setup_redis_client()
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # инициализация конфигурации
    try:
        await init_config_state()
        log.info("🧩 Конфигурация стратегий загружена")
    except Exception:
        log.exception("❌ Ошибка инициализации конфигурации")
        return

    # восстановление открытых позиций
    try:
        await load_position_state()
        log.info("📦 Позиции восстановлены из БД")
    except Exception:
        log.exception("❌ Ошибка при восстановлении позиций")
        return

    # загрузка регистра стратегий
    try:
        strategy_registry = load_strategies()
        set_strategy_registry(strategy_registry)
        log.info("🧠 Регистр стратегий загружен")
    except Exception:
        log.exception("❌ Ошибка при загрузке стратегий")
        return

    log.info("🚀 Запуск asyncio-воркеров")
    log.info("📢 TRADER_INFORMER включен (stream=positions_bybit_status)")

    await asyncio.gather(
        # основной конвейер
        run_safe_loop(run_signal_loop, "SIGNAL_PROCESSOR"),
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(run_signal_log_writer, "CORE_IO"),
        run_safe_loop(run_position_opener_loop, "POSITION_OPENER"),
        run_safe_loop(run_position_open_writer, "POSITION_DB_WRITER"),
        run_safe_loop(run_position_handler, "POSITION_HANDLER"),
        run_safe_loop(run_position_update_writer, "POSITION_UPDATE_WRITER"),
        run_safe_loop(listen_strategy_update_stream, "STRATEGY_STREAM"),
        run_safe_loop(run_strategies_v4_cleaner, "STRATEGY_CLEANER"),
        run_safe_loop(run_lab_response_router, "LAB_RESP_ROUTER"),
        run_safe_loop(run_trader_informer, "TRADER_INFORMER"),
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")