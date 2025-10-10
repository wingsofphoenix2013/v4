# laboratory_v4_main.py — оркестратор фонового сервиса laboratory_v4 (инициализация, кеши, подписчики Pub/Sub, IND/MW-live воркеры)

# 🔸 Импорты
import asyncio
import logging

from laboratory_infra import (
    setup_logging,
    init_pg_pool,
    init_redis_client,
    run_safe_loop,
)
from laboratory_config import (
    bootstrap_caches,             # стартовая загрузка кешей (тикеры + индикаторы)
    get_cache_stats,              # метрики кешей для стартового лога
    get_instances_by_tf,          # геттеры кешей для IND-воркера
    get_precision,
    get_active_symbols,
    get_last_bar,
    run_watch_tickers_events,     # Pub/Sub: tickers_v4_events
    run_watch_indicators_events,  # Pub/Sub: indicators_v4_events
    run_watch_ohlcv_ready_channel # Pub/Sub: bb:ohlcv_channel → обновляет last_bar
)
from laboratory_ind_live import run_lab_ind_live        # IND-live публикация в lab_live:ind:*
from laboratory_mw_live import run_lab_mw_live          # MW-live публикация в lab_live:mw:*

# 🔸 Параметры сервиса (локально, без ENV)
LAB_SETTINGS = {
    # работаем только с m5/m15/h1
    "TF_SET": ("m5", "m15", "h1"),

    # Redis Pub/Sub каналы
    "CHANNEL_TICKERS": "tickers_v4_events",
    "CHANNEL_INDICATORS": "indicators_v4_events",
    "CHANNEL_OHLCV_READY": "bb:ohlcv_channel",

    # Паузы старта воркеров (сек)
    "DELAY_TICKERS": 0.5,
    "DELAY_INDICATORS": 0.5,
    "DELAY_OHLCV_CHANNEL": 2.0,
}

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")


# 🔸 Основной запуск: инициализация, стартовая загрузка, запуск подписчиков и воркеров
async def main():
    setup_logging()
    log.info("LAB: запуск инициализации")

    # подключение к БД/Redis
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # стартовая загрузка кешей (тикеры + индикаторы)
    await bootstrap_caches(
        pg=pg,
        redis=redis,
        tf_set=LAB_SETTINGS["TF_SET"],
    )

    # лог успешного старта
    stats = get_cache_stats()
    log.info(
        "LAB INIT: tickers=%d indicators=%d",
        stats.get("symbols", 0),
        stats.get("indicators", 0),
    )

    # запуск фоновых подписчиков и live-воркеров
    await asyncio.gather(
        # Pub/Sub: тикеры
        run_safe_loop(
            lambda: run_watch_tickers_events(
                pg=pg,
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_TICKERS"],
                initial_delay=LAB_SETTINGS["DELAY_TICKERS"],
            ),
            "LAB_TICKERS",
        ),
        # Pub/Sub: индикаторы
        run_safe_loop(
            lambda: run_watch_indicators_events(
                pg=pg,
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_INDICATORS"],
                initial_delay=LAB_SETTINGS["DELAY_INDICATORS"],
                tf_set=LAB_SETTINGS["TF_SET"],
            ),
            "LAB_INDICATORS",
        ),
        # Pub/Sub: готовность свечей (обновляет кеш last_bar)
        run_safe_loop(
            lambda: run_watch_ohlcv_ready_channel(
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_OHLCV_READY"],
                initial_delay=LAB_SETTINGS["DELAY_OHLCV_CHANNEL"],
            ),
            "LAB_OHLCV_READY",
        ),
        # IND-live публикация (каждые 30 секунд публикует lab_live:ind:* с TTL 45s)
        run_safe_loop(
            lambda: run_lab_ind_live(
                pg=pg,
                redis=redis,
                get_instances_by_tf=get_instances_by_tf,
                get_precision=get_precision,
                get_active_symbols=get_active_symbols,
                get_last_bar=get_last_bar,
                tf_set=LAB_SETTINGS["TF_SET"],
            ),
            "LAB_IND_LIVE",
        ),
        # MW-live публикация (каждые 30 секунд публикует lab_live:mw:* с TTL 45s)
        run_safe_loop(
            lambda: run_lab_mw_live(
                pg=pg,
                redis=redis,
                get_active_symbols=get_active_symbols,
                get_precision=get_precision,
                get_last_bar=get_last_bar,
                tf_set=LAB_SETTINGS["TF_SET"],
            ),
            "LAB_MW_LIVE",
        ),
    )


# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())