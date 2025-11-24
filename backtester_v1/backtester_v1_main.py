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
# 🔸 Оркестратор сценариев
from bt_scenarios_main import run_bt_scenarios_orchestrator
# 🔸 Постпроцессор сценариев
from bt_scenarios_postproc import run_bt_scenarios_postproc

# 🔸 Оркестратор аналитики сценариев (анализ фич по bt:postproc:ready) — v1
from bt_analysis_main import run_bt_analysis_orchestrator
# 🔸 Пост-процессор аналитики (оценка силы анализаторов по bt:analysis:ready)
from bt_analysis_postproc import run_bt_analysis_postproc
# 🔸 Калибровочный воркер (сырые значения фич по bt:analysis:ready)
from bt_analysis_calibration_raw import run_bt_analysis_calibration_raw
# 🔸 Постпроцессор калибровки (бин-конфиги по bt:analysis:calibration:ready)
from bt_analysis_calibration_processor import run_bt_analysis_calibration_processor
# 🔸 Адаптивный анализатор RSI (слушает bt:analysis:adaptive:ready, пишет v2)
from bt_analysis_rsi_adaptive import run_bt_analysis_rsi_adaptive_worker
# 🔸 Суточная аналитика анализаторов (слушает bt:analysis:postproc:ready)
from bt_analysis_daily import run_bt_analysis_daily
# 🔸 Воркер стабильности анализаторов (слушает bt:analysis:daily:ready)
from bt_analysis_stability import run_bt_analysis_stability

# 🔸 Таймфреймы, которые используем в backtester_v1 для индикаторов/сигналов
BT_TIMEFRAMES = ["m5", "m15", "h1"]


# 🔸 Воркеры backtester_v1 (заглушки для базовой проверки инфраструктуры)
async def run_backtester_supervisor(pg, redis):
    log = logging.getLogger("BT_SUPERVISOR")
    log.debug("BT_SUPERVISOR: воркер запущен, backtester_v1 в режиме ожидания (без доменной логики)")

    # здесь мы сознательно ничего не делаем с PG/Redis, только держим процесс живым
    while True:
        # периодический heartbeat, чтобы было видно, что модуль жив
        log.debug("BT_SUPERVISOR: heartbeat — соединения активны")
        await asyncio.sleep(60)


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

    # 🔸 Загрузка инстансов анализаторов и их связок со сценариями/сигналами
    analysis_instances_count = await load_initial_analysis_instances(pg)
    analysis_connections_count = await load_initial_analysis_connections(pg, only_enabled=True)

    log.info(
        f"BT_MAIN: инициализация конфигурации завершена — "
        f"тикеров={tickers_count}, инстансов индикаторов={indicators_count}, "
        f"инстансов псевдо-сигналов={signals_count}, сценариев={scenarios_count}, "
        f"связок_сценарий_сигнал={scenario_links_count}, "
        f"инстансов_анализаторов={analysis_instances_count}, "
        f"связок_анализатор_сценарий_сигнал={analysis_connections_count}, "
        f"TF={BT_TIMEFRAMES}"
    )

    # запуск воркеров в безопасных циклах
    await asyncio.gather(
        run_safe_loop(lambda: run_backtester_supervisor(pg, redis), "BT_SUPERVISOR"),
        run_safe_loop(lambda: run_bt_signals_orchestrator(pg, redis), "BT_SIGNALS"),
        run_safe_loop(lambda: run_bt_scenarios_orchestrator(pg, redis), "BT_SCENARIOS"),
        run_safe_loop(lambda: run_bt_scenarios_postproc(pg, redis), "BT_SCENARIOS_POSTPROC"),
        run_safe_loop(lambda: run_bt_analysis_orchestrator(pg, redis), "BT_ANALYSIS"),              # v1 бины
        run_safe_loop(lambda: run_bt_analysis_postproc(pg, redis), "BT_ANALYSIS_POSTPROC"),         # v1/v2 postproc
        run_safe_loop(lambda: run_bt_analysis_calibration_raw(pg, redis), "BT_ANALYSIS_CALIB_RAW"), # сырые фичи
        run_safe_loop(lambda: run_bt_analysis_calibration_processor(pg, redis), "BT_ANALYSIS_CALIB_PROC"),  # бин-конфиги
        run_safe_loop(lambda: run_bt_analysis_rsi_adaptive_worker(pg, redis), "BT_ANALYSIS_RSI_ADAPTIVE"),  # v2 бины
        run_safe_loop(lambda: run_bt_analysis_daily(pg, redis), "BT_ANALYSIS_DAILY"),               # суточная аналитика
        run_safe_loop(lambda: run_bt_analysis_stability(pg, redis), "BT_ANALYSIS_STABILITY"),       # индекс стабильности
    )


if __name__ == "__main__":
    asyncio.run(main())