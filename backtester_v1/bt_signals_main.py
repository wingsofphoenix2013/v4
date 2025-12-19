# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1 (backfill + live)

import asyncio
import logging
import uuid
import json
from datetime import datetime
from typing import Dict, Any, List, Callable, Awaitable

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры таймерных backfill-сигналов
from signals.bt_signals_lr_universal import run_lr_universal_backfill
from signals.bt_signals_emacross import run_emacross_backfill

# 🔸 Live-воркеры сигналов
from signals.bt_signals_lr_universal_live import init_lr_universal_live, handle_lr_universal_indicator_ready
from signals.bt_signals_emacross_live import init_emacross_live, handle_emacross_indicator_ready

# 🔸 Глобальные настройки расписания для всех timer-backfill сигналов
BT_TIMER_BACKFILL_START_DELAY_SEC = 60      # старт через минуту после запуска backtester_v1
BT_TIMER_BACKFILL_INTERVAL_SEC = 7200       # повторный запуск полного цикла раз в Х секунд

# 🔸 Настройки стримовых backfill-сигналов (по умолчанию)
BT_STREAM_BACKFILL_BATCH_SIZE = 10
BT_STREAM_BACKFILL_BLOCK_MS = 5000

# 🔸 Настройки live-сигналов (по умолчанию)
BT_LIVE_STREAM_BATCH_SIZE = 100
BT_LIVE_STREAM_BLOCK_MS = 5000

# 🔸 Ограничение параллельной обработки live-сообщений (важно для скорости)
BT_LIVE_MAX_CONCURRENCY = 50


# 🔸 Типы обработчиков сигналов
TimerBackfillHandler = Callable[[Dict[str, Any], Any, Any], Awaitable[None]]
StreamBackfillHandler = Callable[[Dict[str, Any], Dict[str, Any], Any, Any], Awaitable[None]]
LiveInitHandler = Callable[[List[Dict[str, Any]], Any, Any], Awaitable[Any]]
LiveHandleHandler = Callable[[Any, Dict[str, str], Any, Any], Awaitable[List[Dict[str, Any]]]]


class LiveSignalHandler:
    # простой контейнер для live-обработчиков (init + handle)
    def __init__(self, init: LiveInitHandler, handle: LiveHandleHandler):
        self.init = init
        self.handle = handle


# 🔸 Реестр таймерных backfill-сигналов: key → handler(signal, pg, redis)
TIMER_BACKFILL_HANDLERS: Dict[str, TimerBackfillHandler] = {
    "lr_universal": run_lr_universal_backfill,
    "ema_cross": run_emacross_backfill,
}

# 🔸 Реестр стримовых backfill-сигналов: key → handler(signal, msg_ctx, pg, redis)
STREAM_BACKFILL_HANDLERS: Dict[str, StreamBackfillHandler] = {
    # пример для будущего:
    # "ema_cross_rsislope": run_emacross_rsislope_backfill,
}

# 🔸 Реестр live-сигналов: key → LiveSignalHandler(init, handle)
LIVE_SIGNAL_HANDLERS: Dict[str, LiveSignalHandler] = {
    "lr_universal": LiveSignalHandler(init_lr_universal_live, handle_lr_universal_indicator_ready),
    "emacross": LiveSignalHandler(init_emacross_live, handle_emacross_indicator_ready),
}

# 🔸 Оркестратор псевдо-сигналов: поднимает backfill и live-воркеры для всех включённых инстансов
async def run_bt_signals_orchestrator(pg, redis):
    log = logging.getLogger("BT_SIGNALS_MAIN")
    log.debug("BT_SIGNALS_MAIN: оркестратор псевдо-сигналов запущен")

    # получаем все включённые инстансы псевдо-сигналов из кеша
    signals: List[Dict[str, Any]] = get_enabled_signals()
    if not signals:
        log.debug(
            "BT_SIGNALS_MAIN: включённых псевдо-сигналов не найдено, оркестратор в режиме ожидания"
        )
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
                    "BT_SIGNALS_MAIN: stream-backfill сигнал id=%s (key=%s, name=%s) имеет пустой stream_key, "
                    "сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            elif key not in STREAM_BACKFILL_HANDLERS:
                log.debug(
                    "BT_SIGNALS_MAIN: stream-backfill сигнал id=%s (key=%s, name=%s) не имеет handler в STREAM_BACKFILL_HANDLERS, "
                    "сигнал игнорируется",
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
                    "BT_SIGNALS_MAIN: live сигнал id=%s (key=%s, name=%s) имеет пустой live_stream_key, "
                    "сигнал игнорируется",
                    sid,
                    key,
                    name,
                )
            elif key not in LIVE_SIGNAL_HANDLERS:
                log.debug(
                    "BT_SIGNALS_MAIN: live сигнал id=%s (key=%s, name=%s) не имеет handler в LIVE_SIGNAL_HANDLERS, "
                    "сигнал игнорируется",
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
            "BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков/стримов/live-воркеров, "
            "оркестратор в режиме ожидания",
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
        total_deleted_rows = 0

        for signal in timer_signals:
            sid = signal.get("id")
            key = str(signal.get("key") or "").strip().lower()
            name = signal.get("name")
            timeframe = signal.get("timeframe")
            mode = signal.get("mode")

            # очистка истории по конкретному сигналу перед backfill
            deleted_rows = 0
            if sid is not None:
                try:
                    deleted_rows = await _delete_signal_values(pg, int(sid))
                    total_deleted_rows += deleted_rows
                    log.info(
                        "BT_SIGNALS_TIMER: очистка bt_signals_values перед backfill: signal_id=%s, deleted_rows=%s",
                        sid,
                        deleted_rows,
                    )
                except Exception as e:
                    log.error(
                        "BT_SIGNALS_TIMER: ошибка очистки bt_signals_values перед backfill для signal_id=%s: %s",
                        sid,
                        e,
                        exc_info=True,
                    )

            log.debug(
                "BT_SIGNALS_TIMER: старт backfill для timer-сигнала id=%s, key=%s, name=%s, timeframe=%s, mode=%s",
                sid,
                key,
                name,
                timeframe,
                mode,
            )

            handler = TIMER_BACKFILL_HANDLERS.get(key)
            if handler is None:
                log.debug(
                    "BT_SIGNALS_TIMER: timer-backfill для сигнала id=%s с key=%s (name=%s) не поддерживается",
                    sid,
                    key,
                    name,
                )
            else:
                try:
                    await handler(signal, pg, redis)
                except Exception as e:
                    log.error(
                        "BT_SIGNALS_TIMER: ошибка при выполнении backfill для timer-сигнала id=%s (key=%s, name=%s): %s",
                        sid,
                        key,
                        name,
                        e,
                        exc_info=True,
                    )

            processed_signals += 1

        cycle_finished_at = datetime.utcnow()
        duration_sec = (cycle_finished_at - cycle_started_at).total_seconds()

        log.info(
            "BT_SIGNALS_TIMER: цикл timer-backfill завершён: сигналов=%s, обработано=%s, длительность=%.2f сек, "
            "deleted_rows_total=%s, следующий запуск через %s сек",
            total_signals,
            processed_signals,
            duration_sec,
            total_deleted_rows,
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

    await _ensure_stream_consumer_group(stream_key, group_name, consumer_name, log, redis)

    # основной цикл чтения стрима и маршрутизации по сигналам
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_key: ">"},
                count=BT_STREAM_BACKFILL_BATCH_SIZE,
                block=BT_STREAM_BACKFILL_BLOCK_MS,
            )

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
                            "BT_SIGNALS_STREAM: запуск stream-backfill для сигнала id=%s (key=%s, name=%s) "
                            "по сообщению stream_id=%s",
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
                                "BT_SIGNALS_STREAM: ошибка при выполнении stream-backfill для сигнала id=%s "
                                "(key=%s, name=%s) по сообщению stream_id=%s: %s",
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

    await _ensure_stream_consumer_group(stream_key, group_name, consumer_name, log, redis)

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
            log.debug(
                "BT_SIGNALS_LIVE: инициализирован live-контекст для key=%s",
                key,
            )
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

    # ограничение параллелизма по сообщениям (важно для скорости и стабильности)
    sema = asyncio.Semaphore(BT_LIVE_MAX_CONCURRENCY)

    async def _process_one_live_message(msg_id: str, fields: Dict[str, Any]) -> int:
        # обработка одного сообщения + ack (старые/повторные события не догоняем, поэтому ack всегда)
        async with sema:
            # нормализуем поля в str-словарь
            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            produced = 0

            try:
                # вызываем бизнес-логику live по всем ключам, для которых есть контекст
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
                        await _publish_live_signal(live_sig, pg, redis)
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
            entries = await redis.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_key: ">"},
                count=BT_LIVE_STREAM_BATCH_SIZE,
                block=BT_LIVE_STREAM_BLOCK_MS,
            )

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

            log.debug(
                "BT_SIGNALS_LIVE: пакет сообщений обработан — сообщений=%s, сгенерировано_live_сигналов=%s, duration_ms=%s",
                total_msgs,
                total_signals,
                duration_ms,
            )

            if total_msgs > 0:
                log.debug(
                    "BT_SIGNALS_LIVE: обработан live-пакет (stream=%s): сообщений=%s, live-сигналов=%s, duration_ms=%s",
                    stream_key,
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
    consumer_name: str,
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
        log.debug(
            "BT_SIGNALS_STREAM: создана consumer group '%s' для стрима '%s'",
            group_name,
            stream_key,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_SIGNALS_STREAM: consumer group '%s' для стрима '%s' уже существует",
                group_name,
                stream_key,
            )
        else:
            log.error(
                "BT_SIGNALS_STREAM: ошибка при создании consumer group '%s' для стрима '%s': %s",
                group_name,
                stream_key,
                e,
                exc_info=True,
            )
            raise


# 🔸 Публикация live-сигнала в signals_stream и bt_signals_values
async def _publish_live_signal(
    live_signal: Dict[str, Any],
    pg,
    redis,
) -> None:
    log = logging.getLogger("BT_SIGNALS_LIVE")

    try:
        signal_cfg = live_signal.get("signal") or {}
        signal_id = live_signal.get("signal_id") or signal_cfg.get("id")
        symbol = live_signal["symbol"]
        timeframe = live_signal.get("timeframe") or "m5"
        direction = live_signal["direction"]
        open_time: datetime = live_signal["open_time"]
        message = live_signal["message"]
        raw_message = live_signal.get("raw_message") or {}

        # формируем времена
        bar_time_iso = open_time.isoformat()
        now_iso = datetime.utcnow().isoformat()

        # публикация в signals_stream (signals_v4 consumer)
        await redis.xadd(
            "signals_stream",
            {
                "message": message,
                "symbol": symbol,
                "bar_time": bar_time_iso,
                "sent_at": now_iso,
                "received_at": now_iso,
                "source": "backtester_v1",
            },
        )

        log.debug(
            "BT_SIGNALS_LIVE: опубликован live-сигнал signal_id=%s, symbol=%s, direction=%s, bar_time=%s",
            signal_id,
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
        return

    # попытка логирования в bt_signals_values (не критично, но желательно)
    try:
        signal_uuid = str(uuid.uuid4())

        # помечаем, что сигнал live
        if "mode" not in raw_message:
            raw_message["mode"] = "live"
        raw_message.setdefault("source", "backtester_v1")

        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bt_signals_values
                    (signal_uuid, signal_id, symbol, timeframe, open_time, direction, message, raw_message)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                signal_uuid,
                signal_id,
                symbol,
                timeframe,
                open_time,
                direction,
                message,
                json.dumps(raw_message),
            )

        log.debug(
            "BT_SIGNALS_LIVE: live-сигнал залогирован в bt_signals_values signal_id=%s, symbol=%s, time=%s, direction=%s",
            signal_id,
            symbol,
            bar_time_iso,
            direction,
        )
    except Exception as e:
        log.error(
            "BT_SIGNALS_LIVE: не удалось залогировать live-сигнал в bt_signals_values: %s, live_signal=%s",
            e,
            live_signal,
            exc_info=True,
        )


# 🔸 Удаление всех значений конкретного сигнала из bt_signals_values
async def _delete_signal_values(pg, signal_id: int) -> int:
    log = logging.getLogger("BT_SIGNALS_TIMER")
    if not signal_id:
        return 0

    async with pg.acquire() as conn:
        res = await conn.execute(
            "DELETE FROM bt_signals_values WHERE signal_id = $1",
            signal_id,
        )

    # res обычно вида "DELETE 123"
    try:
        deleted_rows = int(str(res).split()[-1])
    except Exception:
        deleted_rows = 0
        log.debug(
            "BT_SIGNALS_TIMER: не удалось распарсить результат DELETE для signal_id=%s, res=%s",
            signal_id,
            res,
        )

    return deleted_rows