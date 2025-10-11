# laboratory_v4_main.py — оркестратор фонового сервиса laboratory_v4 (инициализация, кеши, Pub/Sub, IND/MW/PACK per-TF с разными интервалами)

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
from laboratory_ind_live import run_lab_ind_live        # IND-live публикация lab_live:ind:*
from laboratory_mw_live import run_lab_mw_live          # MW-live публикация lab_live:mw:*
from laboratory_pack_live import run_lab_pack_live      # PACK-live публикация lab_live:pack:*

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
}

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")

# 🔸 Вспомогательное: запуск корутины с начальной задержкой
async def _run_with_delay(coro_factory, delay: float):
    if delay and delay > 0:
        await asyncio.sleep(delay)
    await coro_factory()

# 🔸 Основной запуск: инициализация, стартовая загрузка, запуск подписчиков и per-TF воркеров
async def main():
    setup_logging()
    log.info("LAB: запуск инициализации")

    # подключение к БД/Redis
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # стартовая загрузка кешей (тикеры + индикаторы)
    await bootstrap_caches(pg=pg, redis=redis, tf_set=("m5", "m15", "h1"))

    # лог успешного старта
    stats = get_cache_stats()
    log.info("LAB INIT: tickers=%d indicators=%d", stats.get("symbols", 0), stats.get("indicators", 0))

    # 🔸 Расписание per-TF (инициирующая задержка и период тика)
    #    Требование: m5 → 20s (старт 60–65s), m15 → 60s (старт 75–80s), h1 → 90s (старт 90–95s)
    IND_SCHEDULE = [
        (("m5",),  20,  62),   # (tf_set, interval_sec, initial_delay_sec)
        (("m15",), 60,  78),
        (("h1",),  90,  92),
    ]
    MW_SCHEDULE = [
        (("m5",),  20,  63),
        (("m15",), 60,  79),
        (("h1",),  90,  94),
    ]
    PACK_SCHEDULE = [
        (("m5",),  20,  65),
        (("m15",), 60,  80),
        (("h1",),  90,  95),
    ]

    # 🔸 Запуск слушателей и per-TF воркеров
    tasks = [
        # слушатели Pub/Sub
        run_safe_loop(lambda: run_watch_tickers_events(pg=pg, redis=redis,
                          channel=LAB_SETTINGS["CHANNEL_TICKERS"],
                          initial_delay=LAB_SETTINGS["DELAY_TICKERS"]), "LAB_TICKERS"),
        run_safe_loop(lambda: run_watch_indicators_events(pg=pg, redis=redis,
                          channel=LAB_SETTINGS["CHANNEL_INDICATORS"],
                          initial_delay=LAB_SETTINGS["DELAY_INDICATORS"],
                          tf_set=("m5", "m15", "h1")), "LAB_INDICATORS"),
        run_safe_loop(lambda: run_watch_ohlcv_ready_channel(redis=redis,
                          channel=LAB_SETTINGS["CHANNEL_OHLCV_READY"],
                          initial_delay=LAB_SETTINGS["DELAY_OHLCV_CHANNEL"]), "LAB_OHLCV_READY"),
    ]

    # IND per-TF
    for tf_set, interval, delay in IND_SCHEDULE:
        tasks.append(
            run_safe_loop(
                lambda tf_set=tf_set, interval=interval, delay=delay: _run_with_delay(
                    lambda: run_lab_ind_live(
                        pg=pg, redis=redis,
                        get_instances_by_tf=get_instances_by_tf,
                        get_precision=get_precision,
                        get_active_symbols=get_active_symbols,
                        get_last_bar=get_last_bar,
                        tf_set=tf_set,
                        tick_interval_sec=interval,
                    ),
                    delay,
                ),
                f"LAB_IND_LIVE_{'_'.join(tf_set)}",
            )
        )

    # MW per-TF
    for tf_set, interval, delay in MW_SCHEDULE:
        tasks.append(
            run_safe_loop(
                lambda tf_set=tf_set, interval=interval, delay=delay: _run_with_delay(
                    lambda: run_lab_mw_live(
                        pg=pg, redis=redis,
                        get_active_symbols=get_active_symbols,
                        get_precision=get_precision,
                        get_last_bar=get_last_bar,
                        tf_set=tf_set,
                        tick_interval_sec=interval,
                    ),
                    delay,
                ),
                f"LAB_MW_LIVE_{'_'.join(tf_set)}",
            )
        )

    # PACK per-TF
    for tf_set, interval, delay in PACK_SCHEDULE:
        tasks.append(
            run_safe_loop(
                lambda tf_set=tf_set, interval=interval, delay=delay: _run_with_delay(
                    lambda: run_lab_pack_live(
                        pg=pg, redis=redis,
                        get_active_symbols=get_active_symbols,
                        get_precision=get_precision,
                        get_instances_by_tf=get_instances_by_tf,
                        get_last_bar=get_last_bar,
                        tf_set=tf_set,
                        tick_interval_sec=interval,
                    ),
                    delay,
                ),
                f"LAB_PACK_LIVE_{'_'.join(tf_set)}",
            )
        )

    await asyncio.gather(*tasks)

# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())