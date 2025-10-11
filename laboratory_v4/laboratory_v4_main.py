# laboratory_v4_main.py — оркестратор фонового сервиса laboratory_v4 (инициализация, кеши, Pub/Sub, эксперимент: только IND m5)

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
    get_instances_by_tf,          # геттеры кешей
    get_precision,
    get_active_symbols,
    get_last_bar,
    run_watch_tickers_events,     # Pub/Sub: tickers_v4_events
    run_watch_indicators_events,  # Pub/Sub: indicators_v4_events
    run_watch_ohlcv_ready_channel # Pub/Sub: bb:ohlcv_channel → обновляет last_bar
)
from laboratory_ind_live import run_lab_ind_live  # IND-live публикация lab_live:ind:*

# 🔸 Параметры сервиса (локально, без ENV)
LAB_SETTINGS = {
    # Pub/Sub каналы
    "CHANNEL_TICKERS": "tickers_v4_events",
    "CHANNEL_INDICATORS": "indicators_v4_events",
    "CHANNEL_OHLCV_READY": "bb:ohlcv_channel",

    # Задержки старта слушателей (сек)
    "DELAY_TICKERS": 0.5,
    "DELAY_INDICATORS": 0.5,
    "DELAY_OHLCV_CHANNEL": 2.0,

    # Эксперимент: запуск только IND по m5
    "IND_ONLY_TF_SET": ("m5",),  # полностью отключаем m15/h1
    "IND_START_DELAY": 60,       # старт через 60 секунд после загрузки
    "IND_TICK_SEC": 3,           # пауза между циклами 3 секунды
}

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")

# 🔸 Вспомогательное: запуск корутины с начальной задержкой
async def _run_with_delay(coro_factory, delay: float):
    if delay and delay > 0:
        await asyncio.sleep(delay)
    await coro_factory()

# 🔸 Основной запуск: инициализация, стартовая загрузка, запуск подписчиков и IND-only воркера
async def main():
    setup_logging()
    log.info("LAB: запуск инициализации (experiment: IND-only m5)")

    # подключение к БД/Redis
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # стартовая загрузка кешей (тикеры + индикаторы)
    await bootstrap_caches(pg=pg, redis=redis, tf_set=("m5", "m15", "h1"))

    # лог успешного старта
    stats = get_cache_stats()
    log.info("LAB INIT: tickers=%d indicators=%d", stats.get("symbols", 0), stats.get("indicators", 0))

    # запуск слушателей Pub/Sub и единственного IND-live воркера по m5
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
                tf_set=("m5", "m15", "h1"),
            ),
            "LAB_INDICATORS",
        ),
        # Pub/Sub: готовность свечей
        run_safe_loop(
            lambda: run_watch_ohlcv_ready_channel(
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_OHLCV_READY"],
                initial_delay=LAB_SETTINGS["DELAY_OHLCV_CHANNEL"],
            ),
            "LAB_OHLCV_READY",
        ),
        # IND-only m5: старт через 60 секунд, период 3 секунды
        run_safe_loop(
            lambda: _run_with_delay(
                lambda: run_lab_ind_live(
                    pg=pg,
                    redis=redis,
                    get_instances_by_tf=get_instances_by_tf,
                    get_precision=get_precision,
                    get_active_symbols=get_active_symbols,
                    get_last_bar=get_last_bar,
                    tf_set=LAB_SETTINGS["IND_ONLY_TF_SET"],       # ("m5",)
                    tick_interval_sec=LAB_SETTINGS["IND_TICK_SEC"] # 3 секунды
                ),
                LAB_SETTINGS["IND_START_DELAY"],
            ),
            "LAB_IND_LIVE_m5",
        ),
    )

# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())