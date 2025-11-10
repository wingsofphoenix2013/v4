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

# 🔸 импорт воркера анализа позиций (каталоги + фиксация состояний)
from oracle_positions_analyzer import (
    run_oracle_positions_analyzer,
    INITIAL_DELAY_SEC as POS_INITIAL_DELAY_SEC,
    INTERVAL_SEC as POS_INTERVAL_SEC,
)
# 🔸 импорт воркера отчётов
from oracle_mw_snapshot import run_oracle_mw_snapshot
from oracle_pack_snapshot import run_oracle_pack_snapshot
# 🔸 импорт воркера confidence
# from oracle_mw_confidence import run_oracle_confidence
# from oracle_pack_confidence import run_oracle_pack_confidence
# 🔸 импорт воркера проверки sense
# from oracle_mw_sense_stat import run_oracle_sense_stat
# from oracle_pack_sense_stat import run_oracle_pack_sense
# from oracle_pack_lists import run_oracle_pack_lists
# 🔸 импорт воркера backtest v3/v4/v5
# from oracle_mw_backtest_v3 import run_oracle_mw_backtest_v3
# from oracle_mw_backtest_v4 import run_oracle_mw_backtest
from oracle_mw_backtest_v5 import run_oracle_mw_backtest_v5
# 🔸 импорт воркеров PACK backtest v3/v4/v5
# from oracle_pack_backtest_v3 import run_oracle_pack_backtest_v3
# from oracle_pack_backtest_v4 import run_oracle_pack_backtest_v4
from oracle_pack_backtest_v5 import run_oracle_pack_backtest_v5
# 🔸 импорт анализаторов
from oracle_mw_bl_analyzer import run_oracle_mw_bl_analyzer
from oracle_pack_bl_analyzer import run_oracle_pack_bl_analyzer
# 🔸 импорт воркера уборщика
from oracle_cleaner import run_oracle_cleaner

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
        
        run_periodic(run_oracle_positions_analyzer, POS_INTERVAL_SEC, "ORACLE_POSITIONS_ANALYZER", initial_delay=POS_INITIAL_DELAY_SEC),
        
#         run_safe_loop(run_oracle_pack_confidence, "ORACLE_PACK_CONFIDENCE"),
#         run_safe_loop(run_oracle_pack_sense, "ORACLE_PACK_SENSE"),
#         run_safe_loop(run_oracle_pack_lists, "ORACLE_PACK_LISTS"),
#         run_safe_loop(run_oracle_confidence, "ORACLE_CONFIDENCE"),
#         run_safe_loop(run_oracle_sense_stat, "ORACLE_SENSE_STAT"),
#         run_safe_loop(run_oracle_mw_backtest_v3, "ORACLE_BACKTEST_V3"),
#         run_safe_loop(run_oracle_mw_backtest, "ORACLE_BACKTEST_V4"),

        run_safe_loop(run_oracle_mw_snapshot, "ORACLE_MW_SNAPSHOT_EVENT"),
        run_safe_loop(run_oracle_mw_backtest_v5,"ORACLE_BACKTEST_V5"),
        run_safe_loop(run_oracle_mw_bl_analyzer, "ORACLE_MW_BL_ANALYZER"),
        
#         run_safe_loop(run_oracle_pack_backtest_v3, "PACK_BACKTEST_V3"),
#         run_safe_loop(run_oracle_pack_backtest_v4, "PACK_BACKTEST_V4"),
        run_safe_loop(run_oracle_pack_snapshot, "ORACLE_PACK_SNAPSHOT_EVENT"),
        run_safe_loop(run_oracle_pack_backtest_v5, "PACK_BACKTEST_V5"),
        run_safe_loop(run_oracle_pack_bl_analyzer, "ORACLE_PACK_BL_ANALYZER"),

        run_safe_loop(run_oracle_cleaner, "ORACLE_CLEANER"),
    )

if __name__ == "__main__":
    asyncio.run(main())