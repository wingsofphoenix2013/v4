# strategies_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client
from config_loader import init_config_state, config_event_listener
from signal_processor import run_signal_loop
from position_handler import run_position_monitor_loop  # 🔹 актуальный обработчик
from strategy_loader import load_strategies
from position_state_loader import load_position_state, position_registry
from core_io import run_signal_log_writer, run_position_writer
from position_opener import run_position_opener_loop

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
    pool = await setup_pg()
    setup_redis_client()
    await init_config_state()
    await load_position_state()

    strategy_registry = load_strategies()
    log.info("🚀 Воркеры стратегий v4 запущены")

    await asyncio.gather(
        run_safe_loop(lambda: run_signal_loop(strategy_registry), "SIGNAL_PROCESSOR"),
        run_safe_loop(lambda: config_event_listener(), "CONFIG_LOADER"),
        run_safe_loop(lambda: run_signal_log_writer(), "CORE_IO"),
        run_safe_loop(lambda: run_position_opener_loop(), "POSITION_OPENER"),
        run_safe_loop(lambda: run_position_writer(), "POSITION_WRITER"),
        run_safe_loop(run_position_monitor_loop, "POSITION_HANDLER"),
        run_safe_loop(run_position_update_writer, "POSITION_UPDATE_WRITER")
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("👋 Остановка по Ctrl+C")
    except Exception:
        log.exception("💥 Фатальная ошибка")