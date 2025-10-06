# laboratory_v4_main.py — entrypoint laboratory_v4: инициализация, загрузка конфигов, запуск слушателей (WL, CONFIG, DECISION)

import asyncio
import logging

# 🔸 Инфраструктура
import laboratory_infra as infra
from laboratory_infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)

# 🔸 Загрузка конфигов и слушатели WL/CONFIG
from laboratory_config import (
    load_enabled_tickers,
    load_enabled_strategies,
    load_pack_whitelist,
    load_mw_whitelist,
    config_event_listener,
    whitelist_stream_listener,
)

# 🔸 Обработчик решений (allow/deny)
from laboratory_decision_maker import run_laboratory_decision_maker

# 🔸 Пост-allow писатель статистики (filler)
from laboratory_decision_filler import (
    run_laboratory_decision_filler,      # seed → первичное наполнение
    run_position_close_updater,          # signal_log_queue → допись pnl/result/closed_at
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
        # Слушатель изменений конфигов (тикеры/стратегии) — Pub/Sub
        run_safe_loop(config_event_listener, "CONFIG_EVENT_LISTENER"),
        # Слушатель обновлений whitelist (PACK + MW) — Streams
        run_safe_loop(whitelist_stream_listener, "WL_STREAM_LISTENER"),
        # Обработчик решений (allow/deny)
        run_safe_loop(run_laboratory_decision_maker, "LAB_DECISION"),
        # Пост-allow наполнитель статистики — Streams: laboratory_decision_filler → laboratoty_position_stat
        run_safe_loop(run_laboratory_decision_filler, "LAB_DECISION_FILLER"),
        run_safe_loop(run_position_close_updater, "LAB_POS_CLOSE_FILLER"),
    )


if __name__ == "__main__":
    asyncio.run(main())