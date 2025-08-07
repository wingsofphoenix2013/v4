# oracle_v4_main.py

import asyncio
import logging

from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from config_loader import (
    load_enabled_tickers,
    config_event_listener,
)

from trend_worker import run_trend_worker
from volatility_worker import run_volatility_worker
from volume_worker import run_volume_worker
from ema_position_worker import run_ema_position_worker
from ema_snapshot_worker import run_ema_snapshot_worker
from strategy_confidence_worker import run_strategy_confidence_worker
from voting_engine import run_voting_engine
from voter_analyzer import run_voter_analyzer

log = logging.getLogger("ORACLE_MAIN")

# 🔸 Обёртка с автоперезапуском воркера
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro()
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)

# 🔸 Обёртка для периодического запуска
async def run_periodic(coro_func, interval_sec: int, label: str, initial_delay: int = 0):
    if initial_delay > 0:
        log.info(f"[{label}] ⏳ Ожидание {initial_delay} сек перед первым запуском")
        await asyncio.sleep(initial_delay)
    while True:
        try:
            log.info(f"[{label}] 🔁 Запуск периодической задачи")
            await coro_func()
        except Exception:
            log.exception(f"[{label}] ❌ Ошибка при выполнении периодической задачи")
        await asyncio.sleep(interval_sec)

# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса oracle_v4")

    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения к PostgreSQL и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    try:
        await load_enabled_tickers()
        log.info("📦 Конфигурация тикеров загружена")
    except Exception:
        log.exception("❌ Ошибка загрузки конфигурации")
        return

    log.info("🚀 Запуск фоновых воркеров")

    await asyncio.gather(
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(run_trend_worker, "TREND_WORKER"),
        run_safe_loop(run_volatility_worker, "VOLATILITY_WORKER"),
        run_safe_loop(run_volume_worker, "VOLUME_WORKER"),
        run_safe_loop(run_ema_position_worker, "EMA_POSITION_WORKER"),
        run_safe_loop(run_ema_snapshot_worker, "EMA_SNAPSHOT_WORKER"),
        run_safe_loop(run_strategy_confidence_worker, "STRATEGY_CONFIDENCE_WORKER"),
        run_safe_loop(run_voting_engine, "VOTING_ENGINE"),
        run_safe_loop(lambda: run_periodic(run_voter_analyzer, 300, "VOTER_ANALYZER", initial_delay=90), "VOTER_ANALYZER"),
    )

if __name__ == "__main__":
    asyncio.run(main())