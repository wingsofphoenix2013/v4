# laboratory_v4_main.py — оркестратор фонового сервиса laboratory_v4 (инициализация, кеши, воркеры-подписчики)

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timezone

from laboratory_infra import (
    setup_logging,
    init_pg_pool,
    init_redis_client,
    run_safe_loop,
)
from laboratory_config import (
    bootstrap_caches,            # стартовая загрузка кешей (тикеры + индикаторы) и лог итогов
    get_cache_stats,             # метрики кешей для heartbeat
    run_watch_tickers_events,    # подписчик Pub/Sub: tickers_v4_events
    run_watch_indicators_events, # подписчик Pub/Sub: indicators_v4_events
    run_watch_signals_events,    # подписчик Pub/Sub: signals_v4_events (лог)
    run_watch_strategies_events, # подписчик Pub/Sub: strategies_v4_events (лог)
    run_watch_signals_stream,    # потребитель Stream: signals_stream (лог)
    run_watch_ohlcv_ready_stream # потребитель Stream: готовность свечей → обновляет last_bar
)

# 🔸 Параметры сервиса (вместо ENV)
LAB_SETTINGS = {
    # TF фиксированы: работаем только с m5/m15/h1
    "TF_SET": ("m5", "m15", "h1"),

    # Redis каналы/стримы
    "CHANNEL_TICKERS": "tickers_v4_events",
    "CHANNEL_INDICATORS": "indicators_v4_events",
    "CHANNEL_SIGNALS": "signals_v4_events",
    "CHANNEL_STRATEGIES": "strategies_v4_events",
    "STREAM_SIGNALS": "signals_stream",
    "STREAM_OHLCV_READY": "bb:ohlcv_channel",  # используем как Stream готовности свечей

    # Имена consumer-групп/консьюмеров (лабораторные, не конфликтуют с v4)
    "GROUP_SIGNALS": "labv4_signals_group",
    "CONSUMER_SIGNALS": "labv4_signals_1",
    "GROUP_OHLCV": "labv4_ohlcv_group",
    "CONSUMER_OHLCV": "labv4_ohlcv_1",

    # Стартовые паузы на воркеры (сек)
    "DELAY_TICKERS": 0.5,
    "DELAY_INDICATORS": 0.5,
    "DELAY_SIGNALS_PUBSUB": 2.0,
    "DELAY_STRATEGIES_PUBSUB": 2.0,
    "DELAY_SIGNALS_STREAM": 2.0,
    "DELAY_OHLCV_STREAM": 4.0,

    # Каденс heartbeat (сек)
    "HEARTBEAT_SEC": 30,
}

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")


# 🔸 Воркер heartbeat (метрики кешей и “живость”)
async def run_heartbeat(pg, redis, every_sec: int):
    # простая периодическая метрика по кешам
    while True:
        try:
            stats = get_cache_stats()
            # формируем человекочитаемую отметку
            now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
            # лог финального вида
            log.info(
                "LAB HEARTBEAT: symbols=%d indicators=%d last_bars=%d at=%s",
                stats.get("symbols", 0),
                stats.get("indicators", 0),
                stats.get("last_bars", 0),
                now_iso,
            )
        except Exception as e:
            log.error(f"HEARTBEAT error: {e}", exc_info=True)
        await asyncio.sleep(every_sec)


# 🔸 Основной запуск: инициализация, стартовая загрузка, запуск подписчиков
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

    # запуск фоновых подписчиков/воркеров с паузами старта
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
        # Pub/Sub: сигналы (логируем)
        run_safe_loop(
            lambda: run_watch_signals_events(
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_SIGNALS"],
                initial_delay=LAB_SETTINGS["DELAY_SIGNALS_PUBSUB"],
            ),
            "LAB_SIGNALS_PUBSUB",
        ),
        # Pub/Sub: стратегии (логируем)
        run_safe_loop(
            lambda: run_watch_strategies_events(
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_STRATEGIES"],
                initial_delay=LAB_SETTINGS["DELAY_STRATEGIES_PUBSUB"],
            ),
            "LAB_STRATEGIES_PUBSUB",
        ),
        # Stream: входящие сигналы (логируем пакетами)
        run_safe_loop(
            lambda: run_watch_signals_stream(
                redis=redis,
                stream=LAB_SETTINGS["STREAM_SIGNALS"],
                group=LAB_SETTINGS["GROUP_SIGNALS"],
                consumer=LAB_SETTINGS["CONSUMER_SIGNALS"],
                initial_delay=LAB_SETTINGS["DELAY_SIGNALS_STREAM"],
            ),
            "LAB_SIGNALS_STREAM",
        ),
        # Stream: готовность свечей (обновляет кеш last_bar)
        run_safe_loop(
            lambda: run_watch_ohlcv_ready_stream(
                redis=redis,
                stream=LAB_SETTINGS["STREAM_OHLCV_READY"],
                group=LAB_SETTINGS["GROUP_OHLCV"],
                consumer=LAB_SETTINGS["CONSUMER_OHLCV"],
                initial_delay=LAB_SETTINGS["DELAY_OHLCV_STREAM"],
            ),
            "LAB_OHLCV_READY",
        ),
        # Heartbeat
        run_safe_loop(
            lambda: run_heartbeat(
                pg=pg, redis=redis, every_sec=LAB_SETTINGS["HEARTBEAT_SEC"]
            ),
            "LAB_HEARTBEAT",
        ),
    )


# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())