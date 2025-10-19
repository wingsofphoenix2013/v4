# trader_v4_main.py — оркестратор фонового воркера Trader v4

# 🔸 Импорты
import asyncio
import logging

from trader_infra import setup_logging, setup_pg, setup_redis_client
from trader_config import init_trader_config_state, config_event_listener
from trader_position_filler import run_trader_position_filler_loop
from trader_position_closer import run_trader_position_closer_loop

# 🔸 Логгер для главного процесса
log = logging.getLogger("TRADER_MAIN")

# 🔸 Настройки отложенного старта (жёстко в коде)
CONFIG_LISTENER_START_DELAY_SEC = 1.0

# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro_factory, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro_factory()
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)

# 🔸 Обёртка: запуск долгоживущего воркера с отложенным стартом
async def run_with_delay(coro_factory, label: str, start_delay: float = 0.0):
    if start_delay and start_delay > 0:
        log.info(f"[{label}] Отложенный старт на {start_delay:.1f} сек")
        await asyncio.sleep(start_delay)
    await run_safe_loop(coro_factory, label)

# 🔸 Обёртка: периодическая «тик-задача»
async def run_periodic(coro_factory, label: str, start_delay: float = 0.0, interval: float = 60.0):
    if start_delay and start_delay > 0:
        log.info(f"[{label}] Отложенный старт тик-задачи на {start_delay:.1f} сек")
        await asyncio.sleep(start_delay)
    while True:
        try:
            log.info(f"[{label}] Запуск периодической задачи")
            await coro_factory()
            log.info(f"[{label}] Завершено успешно — следующий запуск через {interval:.1f} сек")
        except Exception:
            log.exception(f"[{label}] ❌ Ошибка — повтор через {interval:.1f} сек")
        await asyncio.sleep(interval)

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск воркера trader v4")

    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🧩 Инфраструктура инициализирована")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    try:
        await init_trader_config_state()
        log.info("✅ Конфигурация трейдера загружена")
    except Exception:
        log.exception("❌ Ошибка инициализации конфигурации")
        return

    log.info("🚀 Запуск воркеров")

    await asyncio.gather(
        # слушатель Pub/Sub апдейтов конфигурации
        run_with_delay(config_event_listener, "TRADER_CONFIG", start_delay=CONFIG_LISTENER_START_DELAY_SEC),

        # последовательный слушатель открытий (signal_log_queue: status='opened')
        run_with_delay(run_trader_position_filler_loop, "TRADER_FILLER", start_delay=65.0),

        # последовательный слушатель закрытий (signal_log_queue: status='closed')
        run_with_delay(run_trader_position_closer_loop, "TRADER_CLOSER", start_delay=65.0),
        
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")