import asyncio
import logging

from infra import (
    setup_logging,
    setup_pg,
    setup_redis_client,
)
from config_loader import (
    load_enabled_tickers,
    load_enabled_strategies,
    load_enabled_indicators,
    config_event_listener,
)
from core_io import pg_task, finmonitor_task, treasury_task
from ohlcv_auditor import run_audit_all_symbols, fix_missing_candles
from redis_io import run_audit_all_symbols_ts, fix_missing_ts_points
from redis_compare import compare_redis_vs_db_once
# from strategy_rating_worker import run_strategy_rating_worker
# from king_marker_worker import run_king_marker_worker

# 🔸 Логгер для главного процесса
log = logging.getLogger("AUDITOR_MAIN")


# 🔸 Обёртка с автоперезапуском для воркеров
async def run_safe_loop(coro, label: str):
    while True:
        try:
            log.info(f"[{label}] Запуск задачи")
            await coro()
        except Exception:
            log.exception(f"[{label}] ❌ Упал с ошибкой — перезапуск через 5 секунд")
            await asyncio.sleep(5)

# 🔸 Периодическая обёртка с задержкой между циклами
async def loop_with_interval(coro_func, label: str, interval_sec: int, initial_delay: int = 0):
    if initial_delay > 0:
        log.info(f"[{label}] ⏳ Первая задержка {initial_delay} сек перед запуском")
        await asyncio.sleep(initial_delay)

    while True:
        try:
            log.info(f"[{label}] ⏳ Запуск задачи")
            await coro_func()
            log.info(f"[{label}] ⏸ Следующий запуск через {interval_sec} сек")
            await asyncio.sleep(interval_sec)
        except Exception:
            log.exception(f"[{label}] ❌ Ошибка — перезапуск через 5 секунд")
            await asyncio.sleep(5)
            
# 🔸 Главная точка входа
async def main():
    setup_logging()
    log.info("📦 Запуск сервиса auditor_v4")

    try:
        await setup_pg()
        await setup_redis_client()
        log.info("🔌 Подключения PG и Redis инициализированы")
    except Exception:
        log.exception("❌ Ошибка инициализации внешних сервисов")
        return
        
    try:
        await load_enabled_tickers()
        await load_enabled_strategies()
        await load_enabled_indicators()
        log.info("📦 Конфигурации тикеров, стратегий и индикаторов загружены")
    except Exception:
        log.exception("❌ Ошибка загрузки начальной конфигурации")
        return

    log.info("🚀 Запуск фоновых воркеров")

    await asyncio.gather(
#         run_safe_loop(redis_task, "REDIS_RETENTION_UPDATER")
        run_safe_loop(pg_task, "CORE_IO"),
        run_safe_loop(config_event_listener, "CONFIG_LOADER"),
        run_safe_loop(finmonitor_task, "FINMONITOR"),
        run_safe_loop(treasury_task, "TREASURY"),
        loop_with_interval(run_audit_all_symbols, "OHLCV_AUDITOR", 3600),
        loop_with_interval(fix_missing_candles, "OHLCV_FIXER", 300, initial_delay=180),
        loop_with_interval(run_audit_all_symbols_ts, "REDIS_TS_AUDITOR", 3600, initial_delay=300),
        loop_with_interval(fix_missing_ts_points, "REDIS_TS_FIXER", 3600, initial_delay=420),
        loop_with_interval(compare_redis_vs_db_once, "REDIS_DB_COMPARE", 3600, initial_delay=90),
#         loop_with_interval(run_strategy_rating_worker, "STRATEGY_RATER", 300),
#         loop_with_interval(run_king_marker_worker, "KING_MARKER", 300, initial_delay=120)
    )

if __name__ == "__main__":
    asyncio.run(main())