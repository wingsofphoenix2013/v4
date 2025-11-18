# backtester_v1_main.py — управляющий модуль backtester_v1

import asyncio
import logging

# 🔸 Инфраструктура backtester_v1
from backtester_infra import init_pg_pool, init_redis_client, setup_logging, run_safe_loop

# 🔸 Конфигурация и кеш метаданных backtester_v1
from backtester_config import (
    load_initial_tickers,
    load_initial_indicators,
    load_initial_signals,
)

# 🔸 Таймфреймы, которые используем в backtester_v1 для индикаторов/сигналов
BT_TIMEFRAMES = ["m5", "m15", "h1"]


# 🔸 Воркеры backtester_v1 (заглушки для базовой проверки инфраструктуры)
async def run_backtester_supervisor(pg, redis):
    log = logging.getLogger("BT_SUPERVISOR")
    log.info("BT_SUPERVISOR: воркер запущен, backtester_v1 в режиме ожидания (без доменной логики)")

    # здесь мы сознательно ничего не делаем с PG/Redis, только держим процесс живым
    while True:
        # периодический heartbeat, чтобы было видно, что модуль жив
        log.debug("BT_SUPERVISOR: heartbeat — соединения активны")
        await asyncio.sleep(60)


# 🔸 Точка входа
async def main():
    setup_logging()
    log = logging.getLogger("BT_MAIN")

    log.info("BT_MAIN: старт инициализации backtester_v1")

    # подключение к PostgreSQL
    pg = await init_pg_pool()

    # подключение к Redis
    redis = await init_redis_client()

    log.info("BT_MAIN: подключения к PostgreSQL и Redis успешно установлены для backtester_v1")

    # 🔸 Загрузка кешей: тикеры, инстансы индикаторов и инстансы псевдо-сигналов
    tickers_count = await load_initial_tickers(pg)
    indicators_count = await load_initial_indicators(pg, timeframes=BT_TIMEFRAMES)
    signals_count = await load_initial_signals(pg, timeframes=BT_TIMEFRAMES, only_enabled=True)

    log.info(
        f"BT_MAIN: инициализация конфигурации завершена — "
        f"тикеров={tickers_count}, инстансов индикаторов={indicators_count}, "
        f"инстансов псевдо-сигналов={signals_count}, TF={BT_TIMEFRAMES}"
    )

    # запуск базового воркера-наблюдателя в безопасном цикле
    await asyncio.gather(
        run_safe_loop(lambda: run_backtester_supervisor(pg, redis), "BT_SUPERVISOR"),
    )


if __name__ == "__main__":
    asyncio.run(main())