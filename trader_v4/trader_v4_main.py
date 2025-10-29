# trader_v4_main.py — оркестратор фонового воркера Trader v4 (конфиг, политики TP/SL winners, Bybit-синк) - 28 октября

# 🔸 Импорты
import asyncio
import logging

from trader_infra import setup_logging, setup_pg, setup_redis_client
from trader_config import (
    init_trader_config_state,
    config_event_listener,
    strategy_state_listener,
    config,
)
from bybit_sync import run_bybit_private_ws_sync_loop, run_bybit_rest_resync_job
from trader_position_opener import run_trader_position_opener
from bybit_processor import run_bybit_processor
from bybit_activator import run_bybit_activator
from bybit_auditor import run_bybit_auditor

# 🔸 Логгер для главного процесса
log = logging.getLogger("TRADER_MAIN")

# 🔸 Настройки отложенного старта (жёстко в коде)
CONFIG_LISTENER_START_DELAY_SEC = 1.0
STRATEGY_STATE_START_DELAY_SEC = 1.0
BYBIT_WS_START_DELAY_SEC = 10.0
BYBIT_RESYNC_START_DELAY_SEC = 20.0
BYBIT_RESYNC_INTERVAL_SEC = 600.0
POS_OPENER_START_DELAY_SEC = 30.0
BYBIT_PROC_START_DELAY_SEC = 30.0
BYBIT_ACTIVATOR_START_DELAY_SEC = 45.0
BYBIT_AUDITOR_START_DELAY_SEC = 45.0

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
        # базовая конфигурация: тикеры, стратегии, связи, кэш winners (+meta)
        await init_trader_config_state()
        log.info("✅ Конфигурация трейдера загружена")

        # стартовая загрузка полной политики TP/SL для текущих winners
        await config.reload_all_policies_for_winners()
        log.info(
            "🏷️ Стартовые политики загружены: winners=%d, policies=%d, min_dep=%s",
            len(config.trader_winners),
            len(config.strategy_policies),
            config.trader_winners_min_deposit,
        )
    except Exception:
        log.exception("❌ Ошибка инициализации конфигурации/политик")
        return

    log.info("🚀 Запуск воркеров")

    await asyncio.gather(
        # слушатель Pub/Sub апдейтов конфигурации (тикеры/стратегии)
        run_with_delay(
            config_event_listener,
            "TRADER_CONFIG_PUBSUB",
            start_delay=CONFIG_LISTENER_START_DELAY_SEC,
        ),
        # слушатель стрима состояния стратегий
        run_with_delay(
            strategy_state_listener,
            "TRADER_STRATEGY_STATE",
            start_delay=STRATEGY_STATE_START_DELAY_SEC,
        ),
        # планирование ордеров по событиям opened
        run_with_delay(
            run_trader_position_opener,
            "TRADER_POS_OPENER",
            start_delay=POS_OPENER_START_DELAY_SEC,
        ),
        # обработка очереди ордеров для биржи
        run_with_delay(
            run_bybit_processor,
            "BYBIT_PROCESSOR",
            start_delay=BYBIT_PROC_START_DELAY_SEC,
        ),
        # приватный WS-синк Bybit (read-only)
        run_with_delay(
            run_bybit_private_ws_sync_loop,
            "BYBIT_SYNC",
            start_delay=BYBIT_WS_START_DELAY_SEC,
        ),
        # активатор офчейн-уровней (SL on TP, SL protect)
        run_with_delay(
            run_bybit_activator,
            "BYBIT_ACTIVATOR",
            start_delay=BYBIT_ACTIVATOR_START_DELAY_SEC,
        ),
        # аудитор конвергенции по SL (order/position)
        run_with_delay(
            run_bybit_auditor,
            "BYBIT_AUDITOR",
            start_delay=BYBIT_AUDITOR_START_DELAY_SEC,
        ),
        # периодический REST-ресинк Bybit (баланс и позиции)
        run_periodic(
            run_bybit_rest_resync_job,
            "BYBIT_RESYNC",
            start_delay=BYBIT_RESYNC_START_DELAY_SEC,
            interval=BYBIT_RESYNC_INTERVAL_SEC,
        ),
    )

# 🔸 Запуск через CLI
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("💥 Фатальная ошибка")