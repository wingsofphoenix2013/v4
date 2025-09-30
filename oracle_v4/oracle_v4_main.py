# 🔸 oracle_v4_main.py — entrypoint oracle_v4: инициализация, загрузка конфигов, запуск фоновых воркеров

import asyncio
import logging

from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from config_loader import (
    load_enabled_tickers,
    load_market_watcher_strategies,
    config_event_listener,
)

# 🔸 импорт воркера MW-отчётов
# from oracle_mw_snapshot import run_oracle_mw_snapshot, INITIAL_DELAY_SEC, INTERVAL_SEC
# 🔸 импорт воркера PACK-отчётов
from oracle_pack_snapshot import run_oracle_pack_snapshot as run_pack, INITIAL_DELAY_SEC as PACK_INIT_DELAY, INTERVAL_SEC as PACK_INTERVAL
# 🔸 импорт воркера confidence
# from oracle_mw_confidence import run_oracle_confidence
from oracle_pack_confidence import run_oracle_pack_confidence
# 🔸 импорт воркера ночной автокалибровки confidence
# from oracle_mw_confidence_night import run_oracle_confidence_night, INITIAL_DELAY_H, INTERVAL_H
# 🔸 импорт воркера проверки sense
# from oracle_mw_sense_stat import run_oracle_sense_stat

log = logging.getLogger("ORACLE_MAIN")


# 🔸 Обёртка с автоперезапуском воркера
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


# 🔸 Обёртка для периодического запуска (заготовка)
async def run_periodic(coro_func, interval_sec: int, label: str, initial_delay: int = 0):
    if initial_delay > 0:
        log.info(f"[{label}] ⏳ Ожидание {initial_delay} сек перед первым запуском")
        await asyncio.sleep(initial_delay)
    while True:
        try:
            log.info(f"[{label}] 🔁 Периодический запуск")
            await coro_func()
        except asyncio.CancelledError:
            log.info(f"[{label}] ⏹️ Периодическая задача остановлена")
            raise
        except Exception:
            log.exception(f"[{label}] ❌ Ошибка при периодическом выполнении")
        await asyncio.sleep(interval_sec)


# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса oracle_v4")

    # Подключения к внешним сервисам
    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения к PostgreSQL и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return

    # Первичная загрузка конфигов: тикеры + стратегии с market_watcher=true
    try:
        await load_enabled_tickers()
        await load_market_watcher_strategies()
        log.info("📦 Конфигурация загружена: тикеры и кэш стратегий market_watcher")
    except Exception:
        log.exception("❌ Ошибка первичной загрузки конфигурации")
        return

    log.info("🚀 Запуск фоновых воркеров")

    # Слушатель конфигурационных событий (тикеры + стратегии)
    await asyncio.gather(
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
#         run_periodic(run_oracle_mw_snapshot, INTERVAL_SEC, "ORACLE_MW_SNAPSHOT", initial_delay=INITIAL_DELAY_SEC),
        run_periodic(run_pack, PACK_INTERVAL, "ORACLE_PACK_SNAPSHOT", initial_delay=PACK_INIT_DELAY),
        run_safe_loop(run_oracle_pack_confidence, "ORACLE_PACK_CONFIDENCE"),
#         run_safe_loop(run_oracle_confidence, "ORACLE_CONFIDENCE"),
#         run_periodic(run_oracle_confidence_night, INTERVAL_H * 60 * 60, "ORACLE_CONFIDENCE_NIGHT", initial_delay=INITIAL_DELAY_H * 60 * 60),
#         run_safe_loop(run_oracle_sense_stat, "ORACLE_SENSE_STAT"),
    )

if __name__ == "__main__":
    asyncio.run(main())