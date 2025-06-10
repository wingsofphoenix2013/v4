# strategies_v4_main.py

import asyncio
import logging

from infra import setup_logging, setup_pg, setup_redis_client

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


# 🔸 Заглушки (временно, до новой реализации)
async def stub_signal_processor():
    while True:
        await asyncio.sleep(3600)

async def stub_config_loader():
    while True:
        await asyncio.sleep(3600)

async def stub_core_io_signal_log_writer():
    while True:
        await asyncio.sleep(3600)

async def stub_position_opener():
    while True:
        await asyncio.sleep(3600)

async def stub_position_writer():
    while True:
        await asyncio.sleep(3600)

async def stub_position_handler():
    while True:
        await asyncio.sleep(3600)

async def stub_position_update_writer():
    while True:
        await asyncio.sleep(3600)

async def stub_reverse_trigger():
    while True:
        await asyncio.sleep(3600)


# 🔸 Главная точка входа
async def main():
    # Инициализация логирования
    setup_logging()
    log.info("🛠️ Логирование настроено")

    # Инициализация внешних сервисов
    await setup_pg()
    log.info("🛢️ Подключение к PostgreSQL установлено")

    setup_redis_client()
    log.info("📡 Подключение к Redis установлено")

    # TODO: init_config_state()  — загрузка конфигурации стратегий
    # TODO: load_position_state() — восстановление позиций из БД
    # TODO: load_strategies()     — регистрация стратегий

    # Временная заглушка для strategy_registry
    strategy_registry = {}

    log.info("🚀 Запуск asyncio-воркеров")

    # Запуск воркеров
    await asyncio.gather(
        run_safe_loop(stub_signal_processor, "SIGNAL_PROCESSOR"),
        run_safe_loop(stub_config_loader, "CONFIG_LOADER"),
        run_safe_loop(stub_core_io_signal_log_writer, "CORE_IO"),
        run_safe_loop(stub_position_opener, "POSITION_OPENER"),
        run_safe_loop(stub_position_writer, "POSITION_WRITER"),
        run_safe_loop(stub_position_handler, "POSITION_HANDLER"),
        run_safe_loop(stub_position_update_writer, "POSITION_UPDATE_WRITER"),
        run_safe_loop(stub_reverse_trigger, "REVERSE_TRIGGER")
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")