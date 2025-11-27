# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1

import asyncio
import logging
from typing import Dict, Any, List

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры семейств псевдо-сигналов
from bt_signals_emacross import run_emacross_backfill

# 🔸 Дефолтные настройки расписания для ema_cross_plain (используются, если нет параметров)
EMA_CROSS_PLAIN_DEFAULT_START_DELAY_SEC = 60     # старт через минуту после запуска backtester_v1
EMA_CROSS_PLAIN_DEFAULT_INTERVAL_SEC = 3600      # повторный запуск раз в час


# 🔸 Оркестратор псевдо-сигналов: поднимает планировщики для всех включённых инстансов
async def run_bt_signals_orchestrator(pg, redis):
    log = logging.getLogger("BT_SIGNALS_MAIN")
    log.debug("BT_SIGNALS_MAIN: оркестратор псевдо-сигналов запущен")

    # получаем все включённые инстансы псевдо-сигналов из кеша
    signals: List[Dict[str, Any]] = get_enabled_signals()
    if not signals:
        log.debug("BT_SIGNALS_MAIN: включённых псевдо-сигналов не найдено, оркестратор в режиме ожидания")
        # держим процесс живым, чтобы run_safe_loop не перезапускал без необходимости
        while True:
            await asyncio.sleep(60)

    tasks = []

    for signal in signals:
        key = signal.get("key")
        sid = signal.get("id")
        name = signal.get("name")
        mode = signal.get("mode")
        params = signal.get("params") or {}

        # логируем обнаруженный сигнал
        log.debug(f"BT_SIGNALS_MAIN: найден сигнал id={sid}, key={key}, name={name}, mode={mode}")

        # schedule_type по умолчанию считаем "timer", если не задан
        schedule_type_cfg = params.get("schedule_type")
        if schedule_type_cfg is not None:
            schedule_type_raw = schedule_type_cfg.get("value")
            schedule_type = str(schedule_type_raw).strip().lower()
        else:
            schedule_type = "timer"

        # пока обрабатываем только ema_cross_plain и только режимы, включающие backfill
        if key == "ema_cross_plain" and mode in ("backfill", "both"):
            # поддерживаем только расписание по таймеру для текущего семейства
            if schedule_type != "timer":
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет schedule_type=%s, "
                    "который пока не поддерживается для ema_cross_plain",
                    sid,
                    key,
                    name,
                    schedule_type,
                )
                continue

            # определяем параметры таймера из конфигурации (или дефолты)
            start_delay_sec = _get_int_param(params, "start_delay_sec", EMA_CROSS_PLAIN_DEFAULT_START_DELAY_SEC)
            interval_sec = _get_int_param(params, "interval_sec", EMA_CROSS_PLAIN_DEFAULT_INTERVAL_SEC)

            if interval_sec <= 0:
                log.error(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет interval_sec=%s (<=0), "
                    "планировщик запущен не будет",
                    sid,
                    key,
                    name,
                    interval_sec,
                )
                continue

            # запускаем отдельный планировщик для backfill EMA-cross
            task = asyncio.create_task(
                _schedule_ema_cross_backfill(signal, pg, redis, start_delay_sec, interval_sec),
                name=f"BT_SIG_EMA_CROSS_{sid}",
            )
            tasks.append(task)
            log.debug(
                "BT_SIGNALS_MAIN: для сигнала id=%s (ema_cross_plain) поднят планировщик backfill: "
                "schedule_type=%s, старт через %s сек, интервал %s сек",
                sid,
                schedule_type,
                start_delay_sec,
                interval_sec,
            )
        else:
            # остальные типы сигналов пока не реализованы
            log.debug(
                "BT_SIGNALS_MAIN: сигнал id=%s с key=%s (name=%s) пока не поддерживается оркестратором",
                sid,
                key,
                name,
            )

    if not tasks:
        log.debug(
            "BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков, "
            "оркестратор в режиме ожидания"
        )
        while True:
            await asyncio.sleep(60)

    # ждём завершения всех планировщиков (они, по идее, живут бесконечно)
    await asyncio.gather(*tasks)


# 🔸 Планировщик backfill для ema_cross_plain: старт с задержкой, затем периодический запуск
async def _schedule_ema_cross_backfill(
    signal: Dict[str, Any],
    pg,
    redis,
    start_delay_sec: int,
    interval_sec: int,
):
    log = logging.getLogger("BT_SIG_EMA_CROSS")
    sid = signal.get("id")
    name = signal.get("name")
    backfill_days = signal.get("backfill_days")

    # начальная задержка перед первым запуском
    if start_delay_sec > 0:
        log.debug(
            "BT_SIG_EMA_CROSS: сигнал id=%s ('%s') — ожидание перед стартом %s секунд",
            sid,
            name,
            start_delay_sec,
        )
        await asyncio.sleep(start_delay_sec)

    # цикл периодического запуска backfill
    while True:
        try:
            log.debug(
                "BT_SIG_EMA_CROSS: запуск backfill для сигнала id=%s ('%s'), окно=%s дней",
                sid,
                name,
                backfill_days,
            )
            # один прогон backfill по истории для данного сигнала
            await run_emacross_backfill(signal, pg, redis)
            log.info(
                "BT_SIG_EMA_CROSS: backfill для сигнала id=%s ('%s') завершён, следующий запуск через %s секунд",
                sid,
                name,
                interval_sec,
            )
        except Exception as e:
            # защищаемся от падений конкретного воркера, чтобы планировщик не умер
            log.error(
                "BT_SIG_EMA_CROSS: ошибка при выполнении backfill сигнала id=%s ('%s'): %s",
                sid,
                name,
                e,
                exc_info=True,
            )

        # ожидание до следующего запуска
        await asyncio.sleep(interval_sec)


# 🔸 Вспомогательная функция: безопасное чтение int-параметров сигнала
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        # параметр может быть строкой или числом, приводим к int
        return int(str(raw))
    except Exception:
        return default