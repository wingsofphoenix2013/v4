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
    load_initial_scenarios,
    load_initial_scenario_signals,
)

# 🔸 Оркестратор псевдо-сигналов
from bt_signals_main import run_bt_signals_orchestrator

# 🔸 Оркестратор сценариев
from bt_scenarios_main import run_bt_scenarios_orchestrator

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

    # 🔸 Загрузка сценариев и связок сценарий ↔ сигнал
    scenarios_count = await load_initial_scenarios(pg)
    scenario_links_count = await load_initial_scenario_signals(pg, only_enabled=True)

    log.info(
        f"BT_MAIN: инициализация конфигурации завершена — "
        f"тикеров={tickers_count}, инстансов индикаторов={indicators_count}, "
        f"инстансов псевдо-сигналов={signals_count}, сценариев={scenarios_count}, "
        f"связок_сценарий_сигнал={scenario_links_count}, TF={BT_TIMEFRAMES}"
    )

    # запуск воркеров в безопасных циклах
    await asyncio.gather(
        run_safe_loop(lambda: run_backtester_supervisor(pg, redis), "BT_SUPERVISOR"),
        run_safe_loop(lambda: run_bt_signals_orchestrator(pg, redis), "BT_SIGNALS"),
        run_safe_loop(lambda: run_bt_scenarios_orchestrator(pg, redis), "BT_SCENARIOS"),
    )


if __name__ == "__main__":
    asyncio.run(main())