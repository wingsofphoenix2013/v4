# laboratory_v4_main.py — оркестратор laboratory_v4 (инициализация, кеши, Pub/Sub, эксперимент: m5 цикл IND → MW → PACK с паузами)

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
    run_watch_ohlcv_ready_channel # Pub/Sub: bb:ohlcv_channel → обновляет lab_last_bar
)
from laboratory_ind_live import tick_ind          # одиночный тик IND (без цикла)
from laboratory_mw_live import tick_mw            # одиночный тик MW  (без цикла)
from laboratory_pack_live import tick_pack        # одиночный тик PACK (без цикла)

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

    # Экспериментальный цикл (m5): IND → пауза → MW → пауза → PACK → пауза → …
    "CYCLE_TF_SET": ("m5",),  # считаем только m5
    "CYCLE_START_DELAY": 60,  # старт через 60 секунд после загрузки
    "CYCLE_PAUSE_SEC": 3,     # пауза между IND/MW/PACK и между итерациями
}

# 🔸 Логгер
log = logging.getLogger("LAB_MAIN")


# 🔸 Вспомогательное: запуск корутины с начальной задержкой
async def _run_with_delay(coro_factory, delay: float):
    if delay and delay > 0:
        await asyncio.sleep(delay)
    await coro_factory()


# 🔸 Координатор m5-цикла: IND → пауза → MW → пауза → PACK → пауза → …
async def run_lab_cycle_m5(pg, redis):
    tf_set = LAB_SETTINGS["CYCLE_TF_SET"]
    pause = LAB_SETTINGS["CYCLE_PAUSE_SEC"]

    while True:
        # IND тик (обновляет кеш и пишет lab_live:ind:*)
        pairs, published, skipped, elapsed_ms = await tick_ind(
            pg=pg,
            redis=redis,
            get_instances_by_tf=get_instances_by_tf,
            get_precision=get_precision,
            get_active_symbols=get_active_symbols,
            get_last_bar=get_last_bar,
            tf_set=tf_set,
        )
        log.info(
            "LAB CYCLE: IND tick tf=%s pairs=%d params=%d skipped=%d elapsed_ms=%d",
            ",".join(tf_set), pairs, published, skipped, elapsed_ms
        )

        await asyncio.sleep(pause)

        # MW тик (берёт live из кеша, пишет lab_live:mw:*)
        pairs, published, skipped, elapsed_ms = await tick_mw(
            pg=pg,
            redis=redis,
            get_active_symbols=get_active_symbols,
            get_precision=get_precision,
            get_last_bar=get_last_bar,
            tf_set=tf_set,
        )
        log.info(
            "LAB CYCLE: MW tick tf=%s pairs=%d states=%d skipped=%d elapsed_ms=%d",
            ",".join(tf_set), pairs, published, skipped, elapsed_ms
        )

        await asyncio.sleep(pause)

        # PACK тик (строит rsi/mfi/ema/atr/lr/adx_dmi/macd/bb, пишет lab_live:pack:*)
        pairs, published, skipped, elapsed_ms = await tick_pack(
            pg=pg,
            redis=redis,
            get_active_symbols=get_active_symbols,
            get_precision=get_precision,
            get_instances_by_tf=get_instances_by_tf,
            get_last_bar=get_last_bar,
            tf_set=tf_set,
        )
        log.info(
            "LAB CYCLE: PACK tick tf=%s pairs=%d packs=%d skipped=%d elapsed_ms=%d",
            ",".join(tf_set), pairs, published, skipped, elapsed_ms
        )

        await asyncio.sleep(pause)


# 🔸 Основной запуск: инициализация, стартовая загрузка, запуск подписчиков и экспериментального цикла m5
async def main():
    setup_logging()
    log.info("LAB: запуск инициализации (experiment: m5 IND → MW → PACK alternation)")

    # подключение к БД/Redis
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # стартовая загрузка кешей (тикеры + индикаторы)
    await bootstrap_caches(pg=pg, redis=redis, tf_set=("m5", "m15", "h1"))

    # лог успешного старта
    stats = get_cache_stats()
    log.info("LAB INIT: tickers=%d indicators=%d", stats.get("symbols", 0), stats.get("indicators", 0))

    # запуск слушателей Pub/Sub и экспериментального цикла m5
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
        # Pub/Sub: готовность свечей (обновляет lab_last_bar)
        run_safe_loop(
            lambda: run_watch_ohlcv_ready_channel(
                redis=redis,
                channel=LAB_SETTINGS["CHANNEL_OHLCV_READY"],
                initial_delay=LAB_SETTINGS["DELAY_OHLCV_CHANNEL"],
            ),
            "LAB_OHLCV_READY",
        ),
        # Эксперимент: координатор IND→MW→PACK для m5 (старт через 60 секунд)
        run_safe_loop(
            lambda: _run_with_delay(
                lambda: run_lab_cycle_m5(pg=pg, redis=redis),
                LAB_SETTINGS["CYCLE_START_DELAY"],
            ),
            "LAB_CYCLE_M5",
        ),
    )


# 🔸 Точка входа
if __name__ == "__main__":
    asyncio.run(main())