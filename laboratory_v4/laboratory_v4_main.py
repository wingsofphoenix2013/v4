# laboratory_v4_main.py — entrypoint laboratory_v4: инициализация, загрузка конфигов, запуск слушателей

import asyncio
import logging

import infra
from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from config import (
    load_enabled_tickers,
    load_enabled_strategies,
    load_pack_whitelist,
    load_mw_whitelist,
    config_event_listener,
    whitelist_stream_listener,
)

log = logging.getLogger("LAB_MAIN")


# 🔸 Обёртка с автоперезапуском фоновой задачи
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] 🚀 Запуск задачи")
            await coro()
        except asyncio.CancelledError:
            log.info(f"[{label}] ⏹️ Остановлено по сигналу")
            raise
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)


# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса laboratory_v4")

    # Подключения к внешним сервисам
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения к PostgreSQL и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # Первичная загрузка конфигурации
    try:
        await load_enabled_tickers()
        await load_enabled_strategies()
        await load_pack_whitelist()
        await load_mw_whitelist()
        log.info("📦 Конфигурация загружена: тикеры, стратегии, whitelist")
    except Exception:
        log.exception("❌ Ошибка первичной загрузки конфигурации")
        return

    log.info("🚀 Запуск фоновых слушателей")

    await asyncio.gather(
        run_safe_loop(config_event_listener, "CONFIG_EVENT_LISTENER"),
        run_safe_loop(whitelist_stream_listener, "WL_STREAM_LISTENER"),
    )


if __name__ == "__main__":
    asyncio.run(main())