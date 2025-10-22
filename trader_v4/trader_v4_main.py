# trader_v4_main.py — оркестратор воркеров Trader v4 (конфиг, синк, filler v2, processor v2, closer, maintainer v2 + аудит)

# 🔸 Импорты
import asyncio
import logging

from trader_infra import setup_logging, setup_pg, setup_redis_client
from trader_config import init_trader_config_state, config_event_listener, config
from trader_position_filler import run_trader_position_filler_loop          # listener positions_bybit_status → якорь + заявка
from trader_position_closer import run_trader_position_closer_loop          # слушатель закрытий (positions_bybit_status: closed.*)
from bybit_sync import run_bybit_private_ws_sync_loop, run_bybit_rest_resync_job
from bybit_processor import run_bybit_processor_loop                        # v2: entry → stable fill → TP/SL (priced) + virtuals
from trader_maintainer import run_trader_maintainer_loop, run_trader_maintainer_audit_loop
from trader_sl_handler import run_trader_sl_handler_loop

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

    # инициализация инфраструктуры
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🧩 Инфраструктура инициализирована")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # загрузка конфигурации
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

        # периодическое обновление кэша trader_winner (старт через 10с, затем каждые 5 минут)
        run_periodic(config.refresh_trader_winners_state, "TRADER_WINNERS", start_delay=10.0, interval=300.0),

        # приватный WS-синк Bybit (read-only + статусы ордеров)
        run_with_delay(run_bybit_private_ws_sync_loop, "BYBIT_SYNC", start_delay=60.0),

        # периодический REST-ресинк Bybit (баланс и позиции, каждые 10 минут)
        run_periodic(run_bybit_rest_resync_job, "BYBIT_RESYNC", start_delay=20.0, interval=600.0),

        # подписчик открытий (positions_bybit_status: event='opened') → якорение позиции + публикация «толстой» заявки
        run_with_delay(run_trader_position_filler_loop, "TRADER_FILLER", start_delay=60.0),

        # слушатель закрытий (positions_bybit_status: closed.*) → финализация pnl и статус в портфеле
        run_with_delay(run_trader_position_closer_loop, "TRADER_CLOSER", start_delay=60.0),

        # воркер: план/submit ордеров Bybit (читает trader_order_requests)
        run_with_delay(run_bybit_processor_loop, "BYBIT_PROCESSOR", start_delay=60.0),

        # синхронизатор Система-Биржа
        run_with_delay(run_trader_maintainer_loop, "TRADER_MAINTAINER", start_delay=60.0),

        # обработчик SL-protect (positions_bybit_status: sl_replaced / tp_hit; двойной гейт внутри)
        run_with_delay(run_trader_sl_handler_loop, "TRADER_SL_HANDLER", start_delay=60.0),

        # аудит «гигиены» (форсируем включение; независим от ENV)
        run_with_delay(lambda: run_trader_maintainer_audit_loop(force=True), "TRADER_AUDIT", start_delay=120.0),
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")