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
    load_initial_analysis_instances,
    load_initial_analysis_connections,
)

# 🔸 Оркестратор псевдо-сигналов
from bt_signals_main import run_bt_signals_orchestrator
# 🔸 Управление загрузки в кеш таблицы labels (v2)
from bt_signals_cache_config_v2 import run_bt_signals_cache_watcher_v2

# 🔸 Оркестратор сценариев
from bt_scenarios_main import run_bt_scenarios_orchestrator
# 🔸 Постпроцессор сценариев v2 (raw_stat + daily/stat)
from bt_scenarios_postproc_v2 import run_bt_scenarios_postproc_v2

# 🔸 Оркестратор анализаторов
from bt_analysis_main import run_bt_analysis_orchestrator
# 🔸 Оркестратор препроцессинга анализов v2 (stability + kept_bins)
from bt_analysis_preproc_v2 import run_bt_analysis_preproc_v2_orchestrator
# 🔸 Оркестратор постпроцессинга v2 (финальный score)
from bt_analysis_postproc_v2 import run_bt_analysis_postproc_v2_orchestrator
# 🔸 Оркестратор stabproc v2 (consensus по укороченным окнам)
from bt_analysis_stabproc_v2 import run_bt_analysis_stabproc_v2_orchestrator

# 🔸 Таймфреймы, которые используем в backtester_v1 для индикаторов/сигналов
BT_TIMEFRAMES = ["m5", "m15", "h1"]

# 🔸 Точка входа
async def main():
    setup_logging()
    log = logging.getLogger("BT_MAIN")

    log.debug("BT_MAIN: старт инициализации backtester_v1")

    # подключение к PostgreSQL
    pg = await init_pg_pool()

    # подключение к Redis
    redis = await init_redis_client()

    log.debug("BT_MAIN: подключения к PostgreSQL и Redis успешно установлены для backtester_v1")

    # 🔸 Загрузка кешей: тикеры, инстансы индикаторов и инстансы псевдо-сигналов
    tickers_count = await load_initial_tickers(pg)
    indicators_count = await load_initial_indicators(pg, timeframes=BT_TIMEFRAMES)
    signals_count = await load_initial_signals(pg, timeframes=BT_TIMEFRAMES, only_enabled=True)

    # 🔸 Загрузка сценариев и связок сценарий ↔ сигнал
    scenarios_count = await load_initial_scenarios(pg)
    scenario_links_count = await load_initial_scenario_signals(pg, only_enabled=True)

    # 🔸 Загрузка анализаторов и связок сценарий ↔ сигнал ↔ анализатор
    analysis_instances_count = await load_initial_analysis_instances(pg, only_enabled=True)
    analysis_links_count = await load_initial_analysis_connections(pg, only_enabled=True)

    log.debug(
        f"BT_MAIN: инициализация конфигурации завершена — "
        f"тикеров={tickers_count}, инстансов индикаторов={indicators_count}, "
        f"инстансов псевдо-сигналов={signals_count}, сценариев={scenarios_count}, "
        f"связок_сценарий_сигнал={scenario_links_count}, "
        f"анализаторов={analysis_instances_count}, "
        f"связок_сценарий_сигнал_анализатор={analysis_links_count}, "
        f"TF={BT_TIMEFRAMES}"
    )

    # запуск воркеров в безопасных циклах
    await asyncio.gather(
        run_safe_loop(lambda: run_bt_signals_orchestrator(pg, redis), "BT_SIGNALS"),
        run_safe_loop(lambda: run_bt_signals_cache_watcher_v2(pg, redis), "BT_SIGNALS_CACHE_V2"),
        run_safe_loop(lambda: run_bt_scenarios_orchestrator(pg, redis), "BT_SCENARIOS"),
        run_safe_loop(lambda: run_bt_scenarios_postproc_v2(pg, redis), "BT_SCENARIOS_POSTPROC_V2"),
        run_safe_loop(lambda: run_bt_analysis_orchestrator(pg, redis), "BT_ANALYSIS"),
        run_safe_loop(lambda: run_bt_analysis_preproc_v2_orchestrator(pg, redis), "BT_ANALYSIS_PREPROC_V2"),
        run_safe_loop(lambda: run_bt_analysis_postproc_v2_orchestrator(pg, redis), "BT_ANALYSIS_POSTPROC_V2"),
        run_safe_loop(lambda: run_bt_analysis_stabproc_v2_orchestrator(pg, redis), "BT_ANALYSIS_STABPROC_V2"),
    )


if __name__ == "__main__":
    asyncio.run(main())