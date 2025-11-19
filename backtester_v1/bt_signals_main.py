# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1

import asyncio
import logging
from typing import Dict, Any, List

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры семейств псевдо-сигналов
from bt_signals_emacross import run_emacross_backfill

# 🔸 Константы расписания для известных сигналов
EMA_CROSS_PLAIN_START_DELAY_SEC = 60      # старт через минуту после запуска backtester_v1
EMA_CROSS_PLAIN_INTERVAL_SEC = 3600       # повторный запуск раз в час


# 🔸 Оркестратор ps-сигналов: поднимает планировщики для всех включённых инстансов
async def run_bt_signals_orchestrator(pg, redis):
    log = logging.getLogger("BT_SIGNALS_MAIN")
    log.info("BT_SIGNALS_MAIN: оркестратор псевдо-сигналов запущен")

    # получаем все включённые инстансы псевдо-сигналов из кеша
    signals: List[Dict[str, Any]] = get_enabled_signals()
    if not signals:
        log.info("BT_SIGNALS_MAIN: включённых псевдо-сигналов не найдено, оркестратор в режиме ожидания")
        # держим процесс живым, чтобы run_safe_loop не перезапускал без необходимости
        while True:
            await asyncio.sleep(60)

    tasks = []

    for signal in signals:
        key = signal.get("key")
        sid = signal.get("id")
        name = signal.get("name")
        mode = signal.get("mode")

        # логируем обнаруженный сигнал
        log.info(f"BT_SIGNALS_MAIN: найден сигнал id={sid}, key={key}, name={name}, mode={mode}")

        # пока обрабатываем только ema_cross_plain и только режимы, включающие backfill
        if key == "ema_cross_plain" and mode in ("backfill", "both"):
            # запускаем отдельный планировщик для backfill EMA-cross
            task = asyncio.create_task(
                _schedule_ema_cross_backfill(signal, pg, redis),
                name=f"BT_SIG_EMA_CROSS_{sid}",
            )
            tasks.append(task)
            log.info(
                f"BT_SIGNALS_MAIN: для сигнала id={sid} (ema_cross_plain) "
                f"поднят планировщик backfill: старт через {EMA_CROSS_PLAIN_START_DELAY_SEC} сек, "
                f"интервал {EMA_CROSS_PLAIN_INTERVAL_SEC} сек"
            )
        else:
            # остальные типы сигналов пока не реализованы
            log.info(f"BT_SIGNALS_MAIN: сигнал id={sid} с key={key} пока не поддерживается оркестратором")

    if not tasks:
        log.info("BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков, оркестратор в режиме ожидания")
        while True:
            await asyncio.sleep(60)

    # ждём завершения всех планировщиков (они, по идее, живут бесконечно)
    await asyncio.gather(*tasks)


# 🔸 Планировщик backfill для ema_cross_plain: старт с задержкой, затем периодический запуск
async def _schedule_ema_cross_backfill(signal: Dict[str, Any], pg, redis):
    log = logging.getLogger("BT_SIG_EMA_CROSS")
    sid = signal.get("id")
    name = signal.get("name")
    backfill_days = signal.get("backfill_days")

    # начальная задержка перед первым запуском
    if EMA_CROSS_PLAIN_START_DELAY_SEC > 0:
        log.info(
            f"BT_SIG_EMA_CROSS: сигнал id={sid} ('{name}') — ожидание перед стартом "
            f"{EMA_CROSS_PLAIN_START_DELAY_SEC} секунд"
        )
        await asyncio.sleep(EMA_CROSS_PLAIN_START_DELAY_SEC)

    # цикл периодического запуска backfill
    while True:
        try:
            log.info(
                f"BT_SIG_EMA_CROSS: запуск backfill для сигнала id={sid} ('{name}'), "
                f"окно={backfill_days} дней"
            )
            # один прогон backfill по истории для данного сигнала
            await run_emacross_backfill(signal, pg, redis)
            log.info(
                f"BT_SIG_EMA_CROSS: backfill для сигнала id={sid} ('{name}') завершён, "
                f"следующий запуск через {EMA_CROSS_PLAIN_INTERVAL_SEC} секунд"
            )
        except Exception as e:
            # защищаемся от падений конкретного воркера, чтобы планировщик не умер
            log.error(
                f"BT_SIG_EMA_CROSS: ошибка при выполнении backfill сигнала id={sid} ('{name}'): {e}",
                exc_info=True,
            )

        # ожидание до следующего запуска
        await asyncio.sleep(EMA_CROSS_PLAIN_INTERVAL_SEC)