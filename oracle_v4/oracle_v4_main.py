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

from oracle_rsibins_snapshot_aggregator import run_oracle_rsibins_snapshot_aggregator
from oracle_rsibins_snapshot_backfill import run_oracle_rsibins_snapshot_backfill
from oracle_bbbins_snapshot_aggregator import run_oracle_bbbins_snapshot_aggregator
from oracle_bbbins_snapshot_backfill import run_oracle_bbbins_snapshot_backfill
from oracle_adxbins_snapshot_aggregator import run_oracle_adxbins_snapshot_aggregator
from oracle_adxbins_snapshot_backfill import run_oracle_adxbins_snapshot_backfill
from oracle_dmigap_snapshot_aggregator import run_oracle_dmigap_snapshot_aggregator
from oracle_dmigap_snapshot_backfill import run_oracle_dmigap_snapshot_backfill
from oracle_emastatus_snapshot_aggregator import run_oracle_emastatus_snapshot_aggregator
from oracle_emastatus_snapshot_backfill import run_oracle_emastatus_snapshot_backfill
from oracle_emapattern_snapshot_aggregator import run_oracle_emapattern_snapshot_aggregator
from oracle_emapattern_snapshot_backfill import run_oracle_emapattern_snapshot_backfill
from oracle_mw_aggregator import run_oracle_mw_aggregator
from oracle_mw_backfill import run_oracle_mw_backfill
from oracle_mw_rsi_quartet_aggregator import run_oracle_mw_rsi_quartet_aggregator

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
        run_safe_loop(run_oracle_rsibins_snapshot_aggregator, "RSIBINS_SNAP"),
        run_safe_loop(run_oracle_rsibins_snapshot_backfill, "RSI_BINS_BF"),
        run_safe_loop(run_oracle_bbbins_snapshot_aggregator, "BB_BINS_SNAP"),
        run_safe_loop(run_oracle_bbbins_snapshot_backfill, "BB_BINS_BF"),
        run_safe_loop(run_oracle_adxbins_snapshot_aggregator, "ADX_BINS_SNAP"),
        run_safe_loop(run_oracle_adxbins_snapshot_backfill, "ADX_BINS_BF"),
        run_safe_loop(run_oracle_dmigap_snapshot_aggregator, "DMI_GAP_SNAP"),
        run_safe_loop(run_oracle_dmigap_snapshot_backfill, "DMI_GAP_BF"),
        run_safe_loop(run_oracle_emastatus_snapshot_aggregator, "EMA_STATUS_SNAP"),
        run_safe_loop(run_oracle_emastatus_snapshot_backfill, "EMA_STATUS_BF"),
        run_safe_loop(run_oracle_emapattern_snapshot_aggregator, "EMAPATTERN_SNAP"),
        run_safe_loop(run_oracle_emapattern_snapshot_backfill, "EMAPATTERN_BF"),
        run_safe_loop(run_oracle_mw_aggregator, "MW_AGG"),
        run_safe_loop(run_oracle_mw_backfill, "MW_BF"),
        run_safe_loop(run_oracle_mw_rsi_quartet_aggregator, "MW_RSI_Q"),
    )

if __name__ == "__main__":
    asyncio.run(main())