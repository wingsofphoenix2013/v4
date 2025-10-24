# trader_v4_main.py — оркестратор Trader v4 (МИНИМАЛЬНЫЙ режим: инфраструктура + конфиг + TRADER_FILLER)

# 🔸 Импорты
import asyncio
import logging

from trader_infra import setup_logging, setup_pg, setup_redis_client
from trader_config import init_trader_config_state, config_event_listener
from trader_position_filler import run_trader_position_filler_loop
from trader_position_closer import run_trader_position_closer_loop

# 🔸 Логгер для главного процесса
log = logging.getLogger("TRADER_MAIN")

# 🔸 Настройки отложенного старта
CONFIG_LISTENER_START_DELAY_SEC = 1.0
FILLER_START_DELAY_SEC = 60.0
CLOSER_START_DELAY_SEC = 60.0

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

# 🔸 Главная точка входа (минимальный режим)
async def main():
    setup_logging()
    log.info("📦 Запуск Trader v4 (минимальный режим)")

    # инициализация инфраструктуры
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🧩 Инфраструктура инициализирована (PostgreSQL + Redis)")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # загрузка конфигурации (тикеры/стратегии/политики)
    try:
        await init_trader_config_state()
        log.info("✅ Конфигурация трейдера загружена")
    except Exception:
        log.exception("❌ Ошибка инициализации конфигурации")
        return

    log.info("🚀 Старт воркеров: CONFIG_LISTENER + TRADER_FILLER + TRADER_CLOSER")
    await asyncio.gather(
        # слушатель Pub/Sub апдейтов конфигурации
        run_with_delay(config_event_listener, "TRADER_CONFIG", start_delay=CONFIG_LISTENER_START_DELAY_SEC),

        # подписчик открытий (positions_bybit_status: event='opened' v2) → якорение + «толстая» заявка
        run_with_delay(run_trader_position_filler_loop, "TRADER_FILLER", start_delay=FILLER_START_DELAY_SEC),
        
        # слушатель закрытий (positions_bybit_status: event='closed.*') → ensure_closed + апдейт агрегата
        run_with_delay(run_trader_position_closer_loop, "TRADER_CLOSER", start_delay=CLOSER_START_DELAY_SEC),
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")