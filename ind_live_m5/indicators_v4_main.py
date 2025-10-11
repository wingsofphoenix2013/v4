# indicators_v4_main.py — управляющий модуль: конфиг через Pub/Sub + live m5 publisher (ind_live:*), без стримов/PG-записей

# 🔸 Импорты
import asyncio
import logging

from infra import init_pg_pool, init_redis_client, setup_logging, run_safe_loop
from ind_live_config import IndLiveConfig
from live_indicators_m5 import run_live_indicators_m5


# 🔸 Точка входа
async def main():
    setup_logging()
    log = logging.getLogger("MAIN")
    log.debug("ind_live_v4: старт процесса")

    # инициализация соединений
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # конфигурация: активные тикеры/инстансы + L1-кэш
    config = IndLiveConfig(pg, redis)
    await config.initialize()

    # запуск фоновых задач:
    #  - подписки на Pub/Sub (тикеры/индикаторы) обновляют конфигурацию в памяти
    #  - live-паблишер m5 читает конфиг и публикует ind_live:* (RAW значения)
    await asyncio.gather(
        run_safe_loop(config.run_ticker_events, "CFG_TICKERS"),
        run_safe_loop(config.run_indicator_events, "CFG_INDICATORS"),
        run_safe_loop(
            lambda: run_live_indicators_m5(
                pg,
                redis,
                config.get_instances_by_tf,
                config.get_precision,
                config.get_active_symbols,
                live_cache=config.live_cache,
            ),
            "LIVE_M5",
        ),
    )


# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())