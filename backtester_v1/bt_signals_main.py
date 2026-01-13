# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1 (backfill: timer + stream, live: stream)

import asyncio
import logging
import inspect
from datetime import datetime, timedelta
from typing import Dict, Any, List, Callable, Awaitable, Optional


# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры timer-backfill сигналов
from signals.bt_signals_lr_universal import run_lr_universal_backfill
from signals.bt_signals_emacross import run_emacross_backfill

# 🔸 Воркеры stream-backfill сигналов
from signals.bt_signals_lr_universal_level2 import run_lr_universal_level2_stream_backfill

# 🔸 Live-воркеры сигналов
from signals.bt_signals_lr_universal_live_v2 import (
    init_lr_universal_live_v2,
    handle_lr_universal_indicator_ready_v2,
)


# 🔸 Глобальные настройки расписания для всех timer-backfill сигналов
BT_TIMER_BACKFILL_START_DELAY_SEC = 60       # старт через минуту после запуска backtester_v1
BT_TIMER_BACKFILL_INTERVAL_SEC = 14400       # повторный запуск полного цикла раз в Х секунд

# 🔸 Настройки стримовых backfill-сигналов (по умолчанию)
BT_STREAM_BACKFILL_BATCH_SIZE = 10
BT_STREAM_BACKFILL_BLOCK_MS = 5000

# 🔸 Настройки live-сигналов (по умолчанию)
BT_LIVE_STREAM_BATCH_SIZE = 100
BT_LIVE_STREAM_BLOCK_MS = 5000

# 🔸 Ограничение параллельной обработки live-сообщений
BT_LIVE_MAX_CONCURRENCY = 50

# 🔸 Таблица прогонов backfill (run)
BT_BACKFILL_RUNS_TABLE = "bt_signal_backfill_runs"

# 🔸 Таймшаги TF (в минутах) для decision_time (live helpers)
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Длительность таймфрейма в виде timedelta
def _get_timeframe_timedelta(timeframe: str) -> timedelta:
    tf = (timeframe or "").strip().lower()
    step_min = TF_STEP_MINUTES.get(tf)
    if not step_min:
        return timedelta(0)
    return timedelta(minutes=step_min)


# 🔸 Типы обработчиков сигналов
TimerBackfillHandler = Callable[..., Awaitable[None]]
StreamBackfillHandler = Callable[[Dict[str, Any], Dict[str, Any], Any, Any], Awaitable[None]]
LiveInitHandler = Callable[[List[Dict[str, Any]], Any, Any], Awaitable[Any]]
LiveHandleHandler = Callable[[Any, Dict[str, str], Any, Any], Awaitable[List[Dict[str, Any]]]]


class LiveSignalHandler:
    # простой контейнер для live-обработчиков (init + handle)
    def __init__(self, init: LiveInitHandler, handle: LiveHandleHandler):
        self.init = init
        self.handle = handle


# 🔸 Реестр таймерных backfill-сигналов: key → handler(...)
TIMER_BACKFILL_HANDLERS: Dict[str, TimerBackfillHandler] = {
    "lr_universal": run_lr_universal_backfill,
    "ema_cross": run_emacross_backfill,
}

# 🔸 Реестр стримовых backfill-сигналов: key → handler(signal, msg_ctx, pg, redis)
STREAM_BACKFILL_HANDLERS: Dict[str, StreamBackfillHandler] = {
    "lr_universal_level2": run_lr_universal_level2_stream_backfill,
}

# 🔸 Реестр live-сигналов: key → LiveSignalHandler(init, handle)
LIVE_SIGNAL_HANDLERS: Dict[str, LiveSignalHandler] = {
    "lr_universal_v2": LiveSignalHandler(init_lr_universal_live_v2, handle_lr_universal_indicator_ready_v2),
}


# 🔸 Оркестратор псевдо-сигналов: поднимает backfill и live-воркеры для всех включённых инстансов
async def run_bt_signals_orchestrator(pg, redis):
    log = logging.getLogger("BT_SIGNALS_MAIN")
    log.debug("BT_SIGNALS_MAIN: оркестратор псевдо-сигналов запущен")

    # получаем все включённые инстансы псевдо-сигналов из кеша
    signals: List[Dict[str, Any]] = get_enabled_signals()
    if not signals:
        log.debug("BT_SIGNALS_MAIN: включённых псевдо-сигналов не найдено, оркестратор в режиме ожидания")
        while True:
            await asyncio.sleep(60)

    tasks: List[asyncio.Task] = []

    # 🔸 Коллекции сигналов по типам обработки
    timer_signals: List[Dict[str, Any]] = []
    stream_backfill_by_stream_key: Dict[str, List[Dict[str, Any]]] = {}
    live_signals_by_stream_key: Dict[str, List[Dict[str, Any]]] = {}

    for signal in signals:
        key_raw = signal.get("key")
        key = str(key_raw or "").strip().lower()
        sid = signal.get("id")
        name = signal.get("name")
        mode_raw = signal.get("mode")
        mode = str(mode_raw or "").strip().lower()
        params = signal.get("params") or {}

        log.debug(
            "BT_SIGNALS_MAIN: найден сигнал id=%s, key=%s, name=%s, mode=%s",
            sid,
            key,
            name,
            mode,
        )

        # допустимые режимы: backfill или live
        if mode not in ("backfill", "live"):
            log.error(
                "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет неподдерживаемый mode=%s, сигнал игнорируется",
                sid,
                key,
                name,
                mode,
            )
            continue

        is_backfill = mode == "backfill"
        is_live = mode == "live"

        # schedule_type по умолчанию считаем "timer", если не задан
        schedule_type_cfg = params.get("schedule_type")
        if schedule_type_cfg is not None:
            schedule_type_raw = schedule_type_cfg.get("value")
            schedule_type = str(schedule_type_raw or "").strip().lower()
        else:
            schedule_type = "timer"

        # 🔸 1) Таймерные backfill-сигналы — общий планировщик
        if is_backfill and schedule_type == "timer":
            if key in TIMER_BACKFILL_HANDLERS:
                timer_signals.append(signal)
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как timer-backfill сигнал",
                    sid,
                    key,
                    name,
                )
            else:
                log.debug(
                    "BT_SIGNALS_MAIN: для timer-backfill сигнала id=%s (key=%s, name=%s) нет handler в TIMER_BACKFILL_HANDLERS",
                    sid,
                    key,
                    name,
                )

        # 🔸 2) Стримовые backfill-сигналы — стартуют от сообщений в стриме
        if is_backfill and schedule_type == "stream":
            # stream_key: backfill_stream_key или stream_key
            stream_key_cfg = (
                params.get("backfill_stream_key")
                or params.get("stream_key")
            )
            stream_key_raw = stream_key_cfg.get("value") if stream_key_cfg else None
            stream_key = str(stream_key_raw or "").strip()

            if not stream_key:
                log.error(
                    "BT_SIGNALS_MAIN: stream-backfill сигнал id=%s (key=%s, name=%s) имеет пустой stream_key, сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            elif key not in STREAM_BACKFILL_HANDLERS:
                log.debug(
                    "BT_SIGNALS_MAIN: stream-backfill сигнал id=%s (key=%s, name=%s) не имеет handler в STREAM_BACKFILL_HANDLERS, сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            else:
                stream_signals = stream_backfill_by_stream_key.setdefault(stream_key, [])
                stream_signals.append(signal)
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как stream-backfill сигнал, stream_key=%s",
                    sid,
                    key,
                    name,
                    stream_key,
                )

        # 🔸 3) Live-сигналы — работают по сообщениям в стриме "на сейчас"
        if is_live:
            live_stream_key_cfg = (
                params.get("live_stream_key")
                or params.get("stream_key")
            )
            live_stream_key_raw = live_stream_key_cfg.get("value") if live_stream_key_cfg else None
            live_stream_key = str(live_stream_key_raw or "").strip()

            if not live_stream_key:
                log.error(
                    "BT_SIGNALS_MAIN: live сигнал id=%s (key=%s, name=%s) имеет пустой live_stream_key, сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            elif key not in LIVE_SIGNAL_HANDLERS:
                log.debug(
                    "BT_SIGNALS_MAIN: live сигнал id=%s (key=%s, name=%s) не имеет handler в LIVE_SIGNAL_HANDLERS, сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            else:
                live_signals = live_signals_by_stream_key.setdefault(live_stream_key, [])
                live_signals.append(signal)
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как live-сигнал, stream_key=%s",
                    sid,
                    key,
                    name,
                    live_stream_key,
                )

    # 🔸 Поднимаем общий таймерный планировщик backfill для всех timer-сигналов
    if timer_signals:
        timer_signals_sorted = sorted(timer_signals, key=lambda s: s.get("id") or 0)
        task = asyncio.create_task(
            _run_timer_backfill_scheduler(timer_signals_sorted, pg, redis),
            name="BT_SIG_TIMER_BACKFILL",
        )
        tasks.append(task)
        log.debug(
            "BT_SIGNALS_MAIN: поднят общий таймерный планировщик backfill, сигналов=%s",
            len(timer_signals_sorted),
        )

    # 🔸 Поднимаем воркеры для стримовых backfill-сигналов (по каждому stream_key)
    for stream_key, signals_for_stream in stream_backfill_by_stream_key.items():
        signals_for_stream_sorted = sorted(
            signals_for_stream,
            key=lambda s: s.get("id") or 0,
        )
        task = asyncio.create_task(
            _run_stream_backfill_dispatcher(stream_key, signals_for_stream_sorted, pg, redis),
            name=f"BT_SIG_STREAM_BACKFILL_{stream_key}",
        )
        tasks.append(task)
        log.debug(
            "BT_SIGNALS_MAIN: поднят stream-backfill диспетчер для stream_key='%s', сигналов=%s",
            stream_key,
            len(signals_for_stream_sorted),
        )

    # 🔸 Поднимаем воркеры для live-сигналов (по каждому stream_key)
    for stream_key, signals_for_stream in live_signals_by_stream_key.items():
        signals_for_stream_sorted = sorted(
            signals_for_stream,
            key=lambda s: s.get("id") or 0,
        )
        task = asyncio.create_task(
            _run_live_stream_dispatcher(stream_key, signals_for_stream_sorted, pg, redis),
            name=f"BT_SIG_LIVE_{stream_key}",
        )
        tasks.append(task)
        log.debug(
            "BT_SIGNALS_MAIN: поднят live-диспетчер для stream_key='%s', сигналов=%s",
            stream_key,
            len(signals_for_stream_sorted),
        )

    if not tasks:
        log.debug(
            "BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков/стримов/live-воркеров, оркестратор в режиме ожидания",
        )
        while True:
            await asyncio.sleep(60)

    log.info(
        "BT_SIGNALS_MAIN: оркестратор готов — timer_signals=%s, stream_backfill_groups=%s, live_stream_groups=%s",
        len(timer_signals),
        len(stream_backfill_by_stream_key),
        len(live_signals_by_stream_key),
    )

    await asyncio.gather(*tasks)


# 🔸 Таймерный планировщик backfill для всех timer-сигналов (последовательный)
async def _run_timer_backfill_scheduler(
    timer_signals: List[Dict[str, Any]],
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_TIMER")
    log.debug(
        "BT_SIGNALS_TIMER: таймерный планировщик backfill запущен, сигналов=%s",
        len(timer_signals),
    )

    # начальная задержка перед первым циклом
    if BT_TIMER_BACKFILL_START_DELAY_SEC > 0:
        log.debug(
            "BT_SIGNALS_TIMER: ожидание перед первым циклом backfill %s секунд",
            BT_TIMER_BACKFILL_START_DELAY_SEC,
        )
        await asyncio.sleep(BT_TIMER_BACKFILL_START_DELAY_SEC)

    # основной цикл последовательного запуска всех timer-сигналов
    while True:
        cycle_started_at = datetime.utcnow()

        total_signals = len(timer_signals)
        processed_signals = 0

        runs_started = 0
        runs_success = 0
        runs_error = 0
        runs_skipped = 0

        for signal in timer_signals:
            sid_raw = signal.get("id")
            key = str(signal.get("key") or "").strip().lower()
            name = signal.get("name")
            timeframe = signal.get("timeframe")
            mode = signal.get("mode")
            backfill_days_raw = signal.get("backfill_days") or 0

            try:
                signal_id = int(sid_raw)
            except Exception:
                signal_id = 0

            log.debug(
                "BT_SIGNALS_TIMER: старт backfill для timer-сигнала id=%s, key=%s, name=%s, timeframe=%s, mode=%s",
                signal_id,
                key,
                name,
                timeframe,
                mode,
            )

            handler = TIMER_BACKFILL_HANDLERS.get(key)
            if handler is None:
                log.debug(
                    "BT_SIGNALS_TIMER: timer-backfill для сигнала id=%s с key=%s (name=%s) не поддерживается",
                    signal_id,
                    key,
                    name,
                )
                processed_signals += 1
                continue

            # окно прогона (для сущности run)
            try:
                backfill_days = int(backfill_days_raw)
            except Exception:
                backfill_days = 0

            if backfill_days <= 0:
                # backfill_days некорректен — запускаем handler, но run не создаём
                runs_skipped += 1
                try:
                    await _call_timer_backfill_handler(handler, signal, pg, redis, None, None, None)
                except Exception as e:
                    log.error(
                        "BT_SIGNALS_TIMER: ошибка при выполнении backfill для timer-сигнала id=%s (key=%s, name=%s): %s",
                        signal_id,
                        key,
                        name,
                        e,
                        exc_info=True,
                    )
                processed_signals += 1
                continue

            now = datetime.utcnow()
            from_time = now - timedelta(days=backfill_days)
            to_time = now

            # создаём сущность прогона в БД
            run_id: Optional[int] = None
            try:
                run_id = await _create_backfill_run(pg, signal_id, from_time, to_time, origin_msg_id=None)
                runs_started += 1
                log.info(
                    "BT_SIGNALS_TIMER: backfill run создан — run_id=%s, signal_id=%s, key=%s, TF=%s, window=[%s..%s]",
                    run_id,
                    signal_id,
                    key,
                    timeframe,
                    from_time,
                    to_time,
                )
            except Exception as e:
                # run не создан — всё равно пробуем выполнить backfill
                runs_error += 1
                run_id = None
                log.error(
                    "BT_SIGNALS_TIMER: не удалось создать backfill run для signal_id=%s (key=%s, name=%s): %s",
                    signal_id,
                    key,
                    name,
                    e,
                    exc_info=True,
                )

            # запускаем backfill (handler сам пишет events + membership)
            try:
                await _call_timer_backfill_handler(handler, signal, pg, redis, run_id, from_time, to_time)

                if run_id is not None:
                    await _finish_backfill_run(pg, run_id, status="success", error=None)
                    runs_success += 1
                    log.info(
                        "BT_SIGNALS_TIMER: backfill run завершён успешно — run_id=%s, signal_id=%s, key=%s",
                        run_id,
                        signal_id,
                        key,
                    )

            except Exception as e:
                log.error(
                    "BT_SIGNALS_TIMER: ошибка при выполнении backfill для timer-сигнала id=%s (key=%s, name=%s): %s",
                    signal_id,
                    key,
                    name,
                    e,
                    exc_info=True,
                )

                if run_id is not None:
                    try:
                        await _finish_backfill_run(pg, run_id, status="error", error=str(e))
                    except Exception:
                        pass
                    runs_error += 1

            processed_signals += 1

        cycle_finished_at = datetime.utcnow()
        duration_sec = (cycle_finished_at - cycle_started_at).total_seconds()

        log.info(
            "BT_SIGNALS_TIMER: цикл timer-backfill завершён: сигналов=%s, обработано=%s, длительность=%.2f сек, "
            "runs_started=%s, runs_success=%s, runs_error=%s, runs_skipped=%s, следующий запуск через %s сек",
            total_signals,
            processed_signals,
            duration_sec,
            runs_started,
            runs_success,
            runs_error,
            runs_skipped,
            BT_TIMER_BACKFILL_INTERVAL_SEC,
        )

        # ожидание до следующего цикла backfill
        if BT_TIMER_BACKFILL_INTERVAL_SEC > 0:
            await asyncio.sleep(BT_TIMER_BACKFILL_INTERVAL_SEC)
        else:
            await asyncio.sleep(1)


# 🔸 Универсальный диспетчер стримовых backfill-сигналов по stream_key
async def _run_stream_backfill_dispatcher(
    stream_key: str,
    signals_for_stream: List[Dict[str, Any]],
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_STREAM")
    log.debug(
        "BT_SIGNALS_STREAM: диспетчер для стрима '%s' (backfill) запущен, сигналов=%s",
        stream_key,
        len(signals_for_stream),
    )

    group_name = f"bt_signals_stream_{stream_key}"
    consumer_name = f"{group_name}_main"

    await _ensure_stream_consumer_group(stream_key, group_name, log, redis)

    # основной цикл чтения стрима и маршрутизации по сигналам
    while True:
        try:
            try:
                entries = await redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=BT_STREAM_BACKFILL_BATCH_SIZE,
                    block=BT_STREAM_BACKFILL_BLOCK_MS,
                )
            except Exception as e:
                msg = str(e)
                if "NOGROUP" in msg:
                    log.warning(
                        "BT_SIGNALS_STREAM: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем (stream=%s, group=%s)",
                        stream_key,
                        group_name,
                    )
                    await _ensure_stream_consumer_group(stream_key, group_name, log, redis)
                    continue
                raise

            if not entries:
                continue

            total_msgs = 0
            total_triggers = 0

            for raw_stream_key, messages in entries:
                if isinstance(raw_stream_key, bytes):
                    raw_stream_key = raw_stream_key.decode("utf-8")

                if raw_stream_key != stream_key:
                    continue

                for msg_id, fields in messages:
                    if isinstance(msg_id, bytes):
                        msg_id = msg_id.decode("utf-8")

                    total_msgs += 1

                    # нормализуем поля в str-словарь
                    str_fields: Dict[str, str] = {}
                    for k, v in fields.items():
                        key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                        val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                        str_fields[key_str] = val_str

                    msg_ctx: Dict[str, Any] = {
                        "stream_key": stream_key,
                        "msg_id": msg_id,
                        "fields": str_fields,
                    }

                    triggers_for_msg = 0

                    # проверяем, какие сигналы для этого стрима нужно триггерить
                    for signal in signals_for_stream:
                        key = str(signal.get("key") or "").strip().lower()
                        handler = STREAM_BACKFILL_HANDLERS.get(key)
                        if handler is None:
                            continue

                        sid = signal.get("id")
                        name = signal.get("name")

                        log.debug(
                            "BT_SIGNALS_STREAM: запуск stream-backfill для сигнала id=%s (key=%s, name=%s) по сообщению stream_id=%s",
                            sid,
                            key,
                            name,
                            msg_id,
                        )

                        try:
                            await handler(signal, msg_ctx, pg, redis)
                            triggers_for_msg += 1
                            total_triggers += 1
                        except Exception as e:
                            log.error(
                                "BT_SIGNALS_STREAM: ошибка при выполнении stream-backfill для сигнала id=%s (key=%s, name=%s) "
                                "по сообщению stream_id=%s: %s",
                                sid,
                                key,
                                name,
                                msg_id,
                                e,
                                exc_info=True,
                            )

                    # помечаем сообщение как обработанное
                    await redis.xack(stream_key, group_name, msg_id)

                    log.debug(
                        "BT_SIGNALS_STREAM: сообщение stream_id=%s обработано, триггеров_по_сигналам=%s",
                        msg_id,
                        triggers_for_msg,
                    )

            log.debug(
                "BT_SIGNALS_STREAM: пакет сообщений обработан — сообщений=%s, триггеров_по_сигналам=%s",
                total_msgs,
                total_triggers,
            )

            if total_triggers > 0:
                log.info(
                    "BT_SIGNALS_STREAM: обработан пакет stream-backfill: сообщений=%s, запущено backfill-сигналов=%s",
                    total_msgs,
                    total_triggers,
                )

        except Exception as e:
            log.error(
                "BT_SIGNALS_STREAM: ошибка в основном цикле диспетчера стрима '%s': %s",
                stream_key,
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Универсальный live-диспетчер по stream_key (параллельная обработка сообщений)
async def _run_live_stream_dispatcher(
    stream_key: str,
    signals_for_stream: List[Dict[str, Any]],
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_LIVE")
    log.debug(
        "BT_SIGNALS_LIVE: live-диспетчер для стрима '%s' запущен, сигналов=%s",
        stream_key,
        len(signals_for_stream),
    )

    group_name = f"bt_signals_live_{stream_key}"
    consumer_name = f"{group_name}_main"

    await _ensure_stream_consumer_group(stream_key, group_name, log, redis)

    # инициализируем live-контексты по ключам сигналов
    ctx_by_key: Dict[str, Any] = {}
    for signal in signals_for_stream:
        key = str(signal.get("key") or "").strip().lower()
        handler_cfg = LIVE_SIGNAL_HANDLERS.get(key)
        if handler_cfg is None:
            continue

        if key in ctx_by_key:
            continue

        # один ctx на ключ, init получает список сигналов с этим key (например long+short)
        try:
            ctx = await handler_cfg.init(
                [s for s in signals_for_stream if str(s.get("key") or "").strip().lower() == key],
                pg,
                redis,
            )
            ctx_by_key[key] = ctx
            log.debug("BT_SIGNALS_LIVE: инициализирован live-контекст для key=%s", key)
        except Exception as e:
            log.error(
                "BT_SIGNALS_LIVE: ошибка инициализации live-контекста для key=%s: %s",
                key,
                e,
                exc_info=True,
            )

    # если после инициализации нет ни одного контекста — выходим в ожидание
    if not ctx_by_key:
        log.debug(
            "BT_SIGNALS_LIVE: для стрима '%s' нет активных live-обработчиков, диспетчер в режиме ожидания",
            stream_key,
        )
        while True:
            await asyncio.sleep(60)

    # ограничение параллелизма по сообщениям
    sema = asyncio.Semaphore(BT_LIVE_MAX_CONCURRENCY)

    async def _process_one_live_message(msg_id: str, fields: Dict[str, Any]) -> int:
        async with sema:
            # нормализуем поля в str-словарь
            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            produced = 0

            try:
                # вызываем бизнес-логику live по всем ключам
                for signal in signals_for_stream:
                    key = str(signal.get("key") or "").strip().lower()
                    handler_cfg = LIVE_SIGNAL_HANDLERS.get(key)
                    if handler_cfg is None:
                        continue

                    ctx = ctx_by_key.get(key)
                    if ctx is None:
                        continue

                    try:
                        live_signals = await handler_cfg.handle(
                            ctx,
                            str_fields,
                            pg,
                            redis,
                        )
                    except Exception as e:
                        log.error(
                            "BT_SIGNALS_LIVE: ошибка обработки live-сообщения stream_id=%s для key=%s: %s, fields=%s",
                            msg_id,
                            key,
                            e,
                            str_fields,
                            exc_info=True,
                        )
                        live_signals = []

                    for live_sig in live_signals:
                        await _publish_live_signal(live_sig, redis)
                        produced += 1

            finally:
                # помечаем сообщение как обработанное (включая ошибки)
                try:
                    await redis.xack(stream_key, group_name, msg_id)
                except Exception as e:
                    log.error(
                        "BT_SIGNALS_LIVE: не удалось xack stream_id=%s (stream=%s, group=%s): %s",
                        msg_id,
                        stream_key,
                        group_name,
                        e,
                        exc_info=True,
                    )

            return produced

    # основной цикл чтения стрима и параллельной маршрутизации
    while True:
        try:
            try:
                entries = await redis.xreadgroup(
                    groupname=group_name,
                    consumername=consumer_name,
                    streams={stream_key: ">"},
                    count=BT_LIVE_STREAM_BATCH_SIZE,
                    block=BT_LIVE_STREAM_BLOCK_MS,
                )
            except Exception as e:
                msg = str(e)
                if "NOGROUP" in msg:
                    log.warning(
                        "BT_SIGNALS_LIVE: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем (stream=%s, group=%s)",
                        stream_key,
                        group_name,
                    )
                    await _ensure_stream_consumer_group(stream_key, group_name, log, redis)
                    continue
                raise

            if not entries:
                continue

            batch_started_at = datetime.utcnow()
            total_msgs = 0
            total_signals = 0

            msg_tasks: List[asyncio.Task] = []

            for raw_stream_key, messages in entries:
                if isinstance(raw_stream_key, bytes):
                    raw_stream_key = raw_stream_key.decode("utf-8")

                if raw_stream_key != stream_key:
                    continue

                for msg_id, fields in messages:
                    if isinstance(msg_id, bytes):
                        msg_id = msg_id.decode("utf-8")

                    total_msgs += 1
                    msg_tasks.append(
                        asyncio.create_task(
                            _process_one_live_message(msg_id, fields),
                            name=f"BT_SIG_LIVE_MSG_{stream_key}_{msg_id}",
                        )
                    )

            if msg_tasks:
                results = await asyncio.gather(*msg_tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        continue
                    total_signals += int(r)

            duration_ms = int((datetime.utcnow() - batch_started_at).total_seconds() * 1000)

            if total_msgs > 0 and total_signals > 0:
                log.info(
                    "BT_SIGNALS_LIVE: обработан пакет live: сообщений=%s, сгенерировано_live_сигналов=%s, duration_ms=%s",
                    total_msgs,
                    total_signals,
                    duration_ms,
                )

        except Exception as e:
            log.error(
                "BT_SIGNALS_LIVE: ошибка в основном цикле live-диспетчера стрима '%s': %s",
                stream_key,
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Создание consumer group для произвольного стрима
async def _ensure_stream_consumer_group(
    stream_key: str,
    group_name: str,
    log: logging.Logger,
    redis,
) -> None:
    try:
        await redis.xgroup_create(
            name=stream_key,
            groupname=group_name,
            id="$",
            mkstream=True,
        )
        log.debug("BT_SIGNALS_STREAM: создана consumer group '%s' для стрима '%s'", group_name, stream_key)
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_SIGNALS_STREAM: consumer group '%s' для стрима '%s' уже существует — SETID='$' (игнор истории до старта)",
                group_name,
                stream_key,
            )
            await redis.execute_command("XGROUP", "SETID", stream_key, group_name, "$")
            log.debug("BT_SIGNALS_STREAM: consumer group '%s' SETID='$' для стрима '%s' выполнен", group_name, stream_key)
        else:
            log.error(
                "BT_SIGNALS_STREAM: ошибка при создании consumer group '%s' для стрима '%s': %s",
                group_name,
                stream_key,
                e,
                exc_info=True,
            )
            raise


# 🔸 Вызов timer-backfill handler с обратной совместимостью по сигнатурам
async def _call_timer_backfill_handler(
    handler: TimerBackfillHandler,
    signal: Dict[str, Any],
    pg,
    redis,
    run_id: Optional[int],
    from_time: Optional[datetime],
    to_time: Optional[datetime],
) -> None:
    # handler может быть старого формата: (signal, pg, redis)
    # или нового: (signal, pg, redis, run_id) / (signal, pg, redis, run_id, from_time, to_time)
    try:
        sig = inspect.signature(handler)
        argc = len(sig.parameters)
    except Exception:
        argc = 3

    # условия достаточности
    if argc >= 6 and run_id is not None and from_time is not None and to_time is not None:
        await handler(signal, pg, redis, int(run_id), from_time, to_time)
        return

    if argc >= 4 and run_id is not None:
        await handler(signal, pg, redis, int(run_id))
        return

    await handler(signal, pg, redis)


# 🔸 Создание сущности прогона backfill в БД (origin_msg_id используется для stream-идемпотентности)
async def _create_backfill_run(
    pg,
    signal_id: int,
    from_time: datetime,
    to_time: datetime,
    origin_msg_id: Optional[str],
) -> int:
    async with pg.acquire() as conn:
        run_id = await conn.fetchval(
            f"""
            INSERT INTO {BT_BACKFILL_RUNS_TABLE}
                (signal_id, from_time, to_time, started_at, status, origin_msg_id)
            VALUES ($1, $2, $3, NOW(), 'running', $4)
            RETURNING id
            """,
            int(signal_id),
            from_time,
            to_time,
            origin_msg_id,
        )

    return int(run_id)


# 🔸 Завершение сущности прогона backfill в БД
async def _finish_backfill_run(
    pg,
    run_id: int,
    status: str,
    error: Optional[str],
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE {BT_BACKFILL_RUNS_TABLE}
               SET finished_at = NOW(),
                   status = $2,
                   error = $3
             WHERE id = $1
            """,
            int(run_id),
            str(status),
            error,
        )


# 🔸 Публикация live-сигнала (на текущем этапе: только в Redis signals_stream)
async def _publish_live_signal(
    live_signal: Dict[str, Any],
    redis,
) -> None:
    log = logging.getLogger("BT_SIGNALS_LIVE")

    try:
        symbol = str(live_signal.get("symbol") or "")
        direction = str(live_signal.get("direction") or "")
        open_time = live_signal.get("open_time")
        timeframe = str(live_signal.get("timeframe") or "m5")
        message = str(live_signal.get("message") or "")
        if not symbol or not direction or not open_time or not message:
            return

        # decision_time = close_time бара (open_time + TF), для трассировки
        tf_delta = _get_timeframe_timedelta(timeframe)
        decision_time = open_time + tf_delta if tf_delta > timedelta(0) else None

        bar_time_iso = open_time.isoformat()
        now_iso = datetime.utcnow().isoformat()

        payload = {
            "message": message,
            "symbol": symbol,
            "bar_time": bar_time_iso,
            "sent_at": now_iso,
            "received_at": now_iso,
            "source": "backtester_v1",
        }

        if decision_time is not None:
            payload["decision_time"] = decision_time.isoformat()
        if "raw_message" in live_signal and isinstance(live_signal["raw_message"], dict):
            payload["raw_message"] = str(live_signal["raw_message"])

        await redis.xadd("signals_stream", payload)

        log.debug(
            "BT_SIGNALS_LIVE: опубликован live-сигнал symbol=%s direction=%s bar_time=%s",
            symbol,
            direction,
            bar_time_iso,
        )
    except Exception as e:
        log.error(
            "BT_SIGNALS_LIVE: не удалось опубликовать live-сигнал в signals_stream: %s, live_signal=%s",
            e,
            live_signal,
            exc_info=True,
        )