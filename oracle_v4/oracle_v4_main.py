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
from repair_snapshot_worker import run_snapshot_repair

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
        run_snapshot_repair(),
    )


if __name__ == "__main__":
    asyncio.run(main())