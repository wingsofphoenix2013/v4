# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1

import asyncio
import logging
import uuid
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Awaitable

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры семейств псевдо-сигналов (backfill, timer/stream)
from bt_signals_emacross import run_emacross_backfill
from bt_signals_emacross_rsislope import run_emacross_rsislope_backfill
from bt_signals_bbrsi import run_bbrsi_backfill
from bt_signals_bbadx import run_bbadx_backfill
from bt_signals_rsimfi import run_rsimfi_backfill

# 🔸 Live-бизнес-логика EMA-cross + RSI-slope (online)
from bt_signals_emacross_rsislope_online import (
    init_emacross_rsislope_live,
    handle_emacross_rsislope_indicator_event,
)

# 🔸 Глобальные настройки расписания для всех timer-backfill сигналов
BT_TIMER_BACKFILL_START_DELAY_SEC = 60   # старт через минуту после запуска backtester_v1
BT_TIMER_BACKFILL_INTERVAL_SEC = 28800    # повторный запуск полного цикла раз в Х часов

# 🔸 Константы стримов для стримовых backfill-сигналов
ANALYSIS_POSTPROC_STREAM_KEY = "bt:analysis:postproc:ready"
ANALYSIS_POSTPROC_STREAM_GROUP = "bt_signals_stream_analysis_postproc"
ANALYSIS_POSTPROC_STREAM_CONSUMER = "bt_signals_stream_analysis_postproc_main"
ANALYSIS_POSTPROC_STREAM_BATCH_SIZE = 10
ANALYSIS_POSTPROC_STREAM_BLOCK_MS = 5000

# 🔸 Константы стрима индикаторов для live-сигналов
INDICATOR_STREAM_KEY = "indicator_stream"
INDICATOR_STREAM_GROUP = "bt_signals_live_indicator"
INDICATOR_STREAM_CONSUMER = "bt_signals_live_indicator_main"
INDICATOR_STREAM_BATCH_SIZE = 100
INDICATOR_STREAM_BLOCK_MS = 5000


# 🔸 Реестр обработчиков таймерных backfill-сигналов (key → async handler(signal, pg, redis))
TimerBackfillHandler = Callable[[Dict[str, Any], Any, Any], Awaitable[None]]

TIMER_BACKFILL_HANDLERS: Dict[str, TimerBackfillHandler] = {
    "ema_cross_plain": run_emacross_backfill,
    "bb_rsi_reversion": run_bbrsi_backfill,
    "bb_adx_breakout": run_bbadx_backfill,
    "rsi_mfi_range": run_rsimfi_backfill,
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
        # держим процесс живым, чтобы run_safe_loop не перезапускал без необходимости
        while True:
            await asyncio.sleep(60)

    tasks: List[asyncio.Task] = []

    # 🔸 Коллекции сигналов по типам расписания / режимам
    timer_signals: List[Dict[str, Any]] = []
    stream_to_signals: Dict[str, List[Dict[str, Any]]] = {}
    live_rsislope_signals: List[Dict[str, Any]] = []

    for signal in signals:
        key = signal.get("key")
        sid = signal.get("id")
        name = signal.get("name")
        mode_raw = signal.get("mode")
        mode = str(mode_raw or "").strip().lower()
        params = signal.get("params") or {}

        # логируем обнаруженный сигнал
        log.debug(
            "BT_SIGNALS_MAIN: найден сигнал id=%s, key=%s, name=%s, mode=%s",
            sid,
            key,
            name,
            mode,
        )

        # флаги режимов
        is_backfill_enabled = mode in ("backfill", "both")
        is_live_enabled = mode in ("live", "both")

        # schedule_type по умолчанию считаем "timer", если не задан
        schedule_type_cfg = params.get("schedule_type")
        if schedule_type_cfg is not None:
            schedule_type_raw = schedule_type_cfg.get("value")
            schedule_type = str(schedule_type_raw).strip().lower()
        else:
            schedule_type = "timer"

        # backfill-режимы (timer / stream)
        if is_backfill_enabled:
            # таймерные backfill-сигналы — будут запускаться в одном общем планировщике
            if schedule_type == "timer":
                timer_signals.append(signal)
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как timer-backfill сигнал",
                    sid,
                    key,
                    name,
                )

            # сигналы с расписанием по стриму (backfill по анализу)
            elif schedule_type == "stream":
                # пока обрабатываем только ema_cross_rsislope по стриму bt:analysis:postproc:ready
                if key == "ema_cross_rsislope":
                    stream_key_cfg = params.get("stream_key")
                    if stream_key_cfg is None:
                        log.error(
                            "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет schedule_type=stream, "
                            "но stream_key не задан, backfill-сигнал игнорируется",
                            sid,
                            key,
                            name,
                        )
                    else:
                        stream_key = str(stream_key_cfg.get("value") or "").strip()
                        if not stream_key:
                            log.error(
                                "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет пустой stream_key, "
                                "backfill-сигнал игнорируется",
                                sid,
                                key,
                                name,
                            )
                        else:
                            stream_signals = stream_to_signals.setdefault(stream_key, [])
                            stream_signals.append(signal)
                            log.debug(
                                "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как stream-сигнал "
                                "schedule_type=stream, stream_key=%s (backfill)",
                                sid,
                                key,
                                name,
                                stream_key,
                            )
                else:
                    log.debug(
                        "BT_SIGNALS_MAIN: stream-backfill сигнал id=%s с key=%s (name=%s) пока не поддерживается",
                        sid,
                        key,
                        name,
                    )

            # неизвестный тип расписания для backfill
            else:
                log.error(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет неизвестный schedule_type=%s "
                    "в режиме backfill, backfill-сигнал игнорируется",
                    sid,
                    key,
                    name,
                    schedule_type,
                )
        else:
            # сигнал не участвует в backfill — это не ошибка, просто фиксируем
            log.debug(
                "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) в режиме '%s' не использует backfill",
                sid,
                key,
                name,
                mode,
            )

        # live-режим (online EMA-cross + RSI-slope)
        if is_live_enabled and key == "ema_cross_rsislope":
            live_rsislope_signals.append(signal)
            log.debug(
                "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как live-сигнал EMA+RSI-slope (mode=%s)",
                sid,
                key,
                name,
                mode,
            )

    # 🔸 Поднимаем общий таймерный планировщик backfill для всех timer-сигналов
    if timer_signals:
        # упорядочиваем timer-сигналы по id, чтобы обеспечить детерминированную последовательность
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

    # 🔸 Поднимаем воркеры для стримов backfill, если есть stream-сигналы
    for stream_key, signals_for_stream in stream_to_signals.items():
        if stream_key == ANALYSIS_POSTPROC_STREAM_KEY:
            # сортируем stream-сигналы по id для детерминированного порядка
            signals_for_stream_sorted = sorted(
                signals_for_stream,
                key=lambda s: s.get("id") or 0,
            )
            task = asyncio.create_task(
                _run_analysis_postproc_stream_dispatcher(signals_for_stream_sorted, pg, redis),
                name="BT_SIG_STREAM_ANALYSIS_POSTPROC",
            )
            tasks.append(task)
            log.debug(
                "BT_SIGNALS_MAIN: поднят stream-диспетчер backfill для '%s', сигналов=%s",
                stream_key,
                len(signals_for_stream_sorted),
            )
        else:
            log.debug(
                "BT_SIGNALS_MAIN: stream_key='%s' пока не поддерживается для backfill, сигналы для него игнорируются",
                stream_key,
            )

    # 🔸 Поднимаем live-диспетчер EMA-cross+RSI-slope, если есть live-сигналы
    live_rsislope_ctx: Optional[Any] = None
    if live_rsislope_signals:
        try:
            live_rsislope_ctx = await init_emacross_rsislope_live(
                live_rsislope_signals,
                pg,
                redis,
            )
            task = asyncio.create_task(
                _run_indicator_stream_live_dispatcher(live_rsislope_ctx, pg, redis),
                name="BT_SIG_EMA_CROSS_RSISLOPE_LIVE",
            )
            tasks.append(task)
            log.debug(
                "BT_SIGNALS_MAIN: поднят live-диспетчер EMA-cross+RSI-slope, сигналов=%s",
                len(live_rsislope_signals),
            )
        except Exception as e:
            log.error(
                "BT_SIGNALS_MAIN: не удалось инициализировать live-контекст EMA-cross+RSI-slope: %s",
                e,
                exc_info=True,
            )

    if not tasks:
        log.debug(
            "BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков/стримов/live-воркеров, "
            "оркестратор в режиме ожидания",
        )
        while True:
            await asyncio.sleep(60)

    log.info(
        "BT_SIGNALS_MAIN: оркестратор готов — timer_signals=%s, stream_groups=%s, live_rsislope_signals=%s",
        len(timer_signals),
        len(stream_to_signals),
        len(live_rsislope_signals),
    )

    # ждём завершения всех планировщиков и воркеров (они, по идее, живут бесконечно)
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

    # глобальная начальная задержка перед первым циклом
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

        for signal in timer_signals:
            sid = signal.get("id")
            key = signal.get("key")
            name = signal.get("name")
            timeframe = signal.get("timeframe")
            mode = signal.get("mode")

            log.debug(
                "BT_SIGNALS_TIMER: старт backfill для timer-сигнала id=%s, key=%s, name=%s, timeframe=%s, mode=%s",
                sid,
                key,
                name,
                timeframe,
                mode,
            )

            handler = TIMER_BACKFILL_HANDLERS.get(str(key or "").strip().lower())
            if handler is None:
                log.debug(
                    "BT_SIGNALS_TIMER: timer-backfill для сигнала id=%s с key=%s (name=%s) пока не поддерживается",
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
            "следующий запуск через %s сек",
            total_signals,
            processed_signals,
            duration_sec,
            BT_TIMER_BACKFILL_INTERVAL_SEC,
        )

        # ожидание до следующего цикла backfill
        if BT_TIMER_BACKFILL_INTERVAL_SEC > 0:
            await asyncio.sleep(BT_TIMER_BACKFILL_INTERVAL_SEC)
        else:
            # защита от нулевого интервала
            await asyncio.sleep(1)


# 🔸 Воркёр-диспетчер по стриму bt:analysis:postproc:ready для stream-сигналов backfill
async def _run_analysis_postproc_stream_dispatcher(
    signals_for_stream: List[Dict[str, Any]],
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_STREAM")
    log.debug(
        "BT_SIGNALS_STREAM: диспетчер для стрима '%s' (backfill) запущен, сигналов=%s",
        ANALYSIS_POSTPROC_STREAM_KEY,
        len(signals_for_stream),
    )

    # создаём consumer group для стрима bt:analysis:postproc:ready (если ещё не создана)
    await _ensure_analysis_postproc_consumer_group(redis)

    # основной цикл чтения стрима и маршрутизации по сигналам
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=ANALYSIS_POSTPROC_STREAM_GROUP,
                consumername=ANALYSIS_POSTPROC_STREAM_CONSUMER,
                streams={ANALYSIS_POSTPROC_STREAM_KEY: ">"},
                count=ANALYSIS_POSTPROC_STREAM_BATCH_SIZE,
                block=ANALYSIS_POSTPROC_STREAM_BLOCK_MS,
            )

            if not entries:
                continue

            total_msgs = 0
            total_triggers = 0

            for stream_key, messages in entries:
                if isinstance(stream_key, bytes):
                    stream_key = stream_key.decode("utf-8")

                if stream_key != ANALYSIS_POSTPROC_STREAM_KEY:
                    # защищаемся от чужих стримов
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

                    ctx = _parse_analysis_postproc_message(str_fields)
                    if not ctx:
                        # не удалось корректно распарсить сообщение — ACK и пропускаем
                        await redis.xack(
                            ANALYSIS_POSTPROC_STREAM_KEY,
                            ANALYSIS_POSTPROC_STREAM_GROUP,
                            msg_id,
                        )
                        continue

                    scenario_id = ctx["scenario_id"]
                    base_signal_id = ctx["signal_id"]
                    family_key = ctx["family_key"]
                    version = ctx["version"]
                    analysis_ids = ctx["analysis_ids"]

                    log.debug(
                        "BT_SIGNALS_STREAM: сообщение stream_id=%s, scenario_id=%s, signal_id=%s, "
                        "family_key=%s, version=%s, analysis_ids=%s",
                        msg_id,
                        scenario_id,
                        base_signal_id,
                        family_key,
                        version,
                        analysis_ids,
                    )

                    triggers_for_msg = 0

                    # проверяем, какие сигналы для этого стрима нужно триггерить
                    for signal in signals_for_stream:
                        if _should_trigger_rsislope_signal(
                            signal,
                            scenario_id,
                            base_signal_id,
                            family_key,
                            version,
                            analysis_ids,
                        ):
                            sid = signal.get("id")
                            name = signal.get("name")
                            key = signal.get("key")

                            log.debug(
                                "BT_SIGNALS_STREAM: сработал триггер для stream-сигнала id=%s (key=%s, name=%s) "
                                "по сообщению stream_id=%s (scenario_id=%s, base_signal_id=%s, analysis_ids=%s, version=%s)",
                                sid,
                                key,
                                name,
                                msg_id,
                                scenario_id,
                                base_signal_id,
                                analysis_ids,
                                version,
                            )

                            # запускаем зависимый воркер EMA+RSI-slope (backfill) последовательно
                            try:
                                await run_emacross_rsislope_backfill(signal, pg, redis, ctx)
                                triggers_for_msg += 1
                                total_triggers += 1
                            except Exception as e:
                                log.error(
                                    "BT_SIGNALS_STREAM: ошибка при выполнении backfill для stream-сигнала id=%s "
                                    "(key=%s, name=%s) по сообщению stream_id=%s: %s",
                                    sid,
                                    key,
                                    name,
                                    msg_id,
                                    e,
                                    exc_info=True,
                                )

                    # помечаем сообщение как обработанное
                    await redis.xack(
                        ANALYSIS_POSTPROC_STREAM_KEY,
                        ANALYSIS_POSTPROC_STREAM_GROUP,
                        msg_id,
                    )

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
                ANALYSIS_POSTPROC_STREAM_KEY,
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой
            await asyncio.sleep(2)


# 🔸 Live-диспетчер по стриму indicator_stream для EMA-cross + RSI-slope
async def _run_indicator_stream_live_dispatcher(
    live_ctx: Any,
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_LIVE")
    log.debug(
        "BT_SIGNALS_LIVE: live-диспетчер по стриму '%s' запущен",
        INDICATOR_STREAM_KEY,
    )

    # создаём consumer group для стрима indicator_stream (если ещё не создана)
    await _ensure_indicator_stream_consumer_group(redis)

    # основной цикл чтения стрима индикаторов и маршрутизации в бизнес-логику
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=INDICATOR_STREAM_GROUP,
                consumername=INDICATOR_STREAM_CONSUMER,
                streams={INDICATOR_STREAM_KEY: ">"},
                count=INDICATOR_STREAM_BATCH_SIZE,
                block=INDICATOR_STREAM_BLOCK_MS,
            )

            if not entries:
                continue

            total_msgs = 0
            total_signals = 0

            for stream_key, messages in entries:
                if isinstance(stream_key, bytes):
                    stream_key = stream_key.decode("utf-8")

                if stream_key != INDICATOR_STREAM_KEY:
                    # защищаемся от чужих стримов
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

                    try:
                        # вызываем бизнес-логику live EMA+RSI-slope
                        live_signals = await handle_emacross_rsislope_indicator_event(
                            live_ctx,
                            str_fields,
                            pg,
                            redis,
                        )
                    except Exception as e:
                        log.error(
                            "BT_SIGNALS_LIVE: ошибка обработки сообщения stream_id=%s: %s, fields=%s",
                            msg_id,
                            e,
                            str_fields,
                            exc_info=True,
                        )
                        live_signals = []

                    # публикация live-сигналов в signals_stream и bt_signals_values
                    for live_sig in live_signals:
                        await _publish_live_signal(live_sig, pg, redis)
                        total_signals += 1

                    # помечаем сообщение как обработанное
                    await redis.xack(
                        INDICATOR_STREAM_KEY,
                        INDICATOR_STREAM_GROUP,
                        msg_id,
                    )

            log.debug(
                "BT_SIGNALS_LIVE: пакет сообщений обработан — сообщений=%s, сгенерировано_live_сигналов=%s",
                total_msgs,
                total_signals,
            )

            if total_signals > 0:
                log.info(
                    "BT_SIGNALS_LIVE: обработан пакет live-сообщений: сообщений=%s, live-сигналов=%s",
                    total_msgs,
                    total_signals,
                )

        except Exception as e:
            log.error(
                "BT_SIGNALS_LIVE: ошибка в основном цикле live-диспетчера стрима '%s': %s",
                INDICATOR_STREAM_KEY,
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой
            await asyncio.sleep(2)


# 🔸 Создание consumer group для bt:analysis:postproc:ready
async def _ensure_analysis_postproc_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_POSTPROC_STREAM_KEY,
            groupname=ANALYSIS_POSTPROC_STREAM_GROUP,
            id="$",
            mkstream=True,
        )
        logging.getLogger("BT_SIGNALS_STREAM").debug(
            "BT_SIGNALS_STREAM: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_POSTPROC_STREAM_GROUP,
            ANALYSIS_POSTPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            logging.getLogger("BT_SIGNALS_STREAM").info(
                "BT_SIGNALS_STREAM: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_POSTPROC_STREAM_GROUP,
                ANALYSIS_POSTPROC_STREAM_KEY,
            )
        else:
            logging.getLogger("BT_SIGNALS_STREAM").error(
                "BT_SIGNALS_STREAM: ошибка при создании consumer group '%s': %s",
                ANALYSIS_POSTPROC_STREAM_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Создание consumer group для indicator_stream
async def _ensure_indicator_stream_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=INDICATOR_STREAM_KEY,
            groupname=INDICATOR_STREAM_GROUP,
            id="$",
            mkstream=True,
        )
        logging.getLogger("BT_SIGNALS_LIVE").debug(
            "BT_SIGNALS_LIVE: создана consumer group '%s' для стрима '%s'",
            INDICATOR_STREAM_GROUP,
            INDICATOR_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            logging.getLogger("BT_SIGNALS_LIVE").info(
                "BT_SIGNALS_LIVE: consumer group '%s' для стрима '%s' уже существует",
                INDICATOR_STREAM_GROUP,
                INDICATOR_STREAM_KEY,
            )
        else:
            logging.getLogger("BT_SIGNALS_LIVE").error(
                "BT_SIGNALS_LIVE: ошибка при создании consumer group '%s': %s",
                INDICATOR_STREAM_GROUP,
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

        # публикация в signals_stream
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
        # без записи в stream логировать в БД не имеет смысла
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


# 🔸 Разбор сообщения стрима bt:analysis:postproc:ready
def _parse_analysis_postproc_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        family_key = fields.get("family_key")
        analysis_ids_str = fields.get("analysis_ids") or ""
        finished_at_str = fields.get("finished_at")
        version = fields.get("version") or "v1"

        if not (scenario_id_str and signal_id_str and family_key and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        raw_ids = [s.strip() for s in analysis_ids_str.split(",") if s.strip()]
        analysis_ids: List[int] = []
        for s in raw_ids:
            try:
                analysis_ids.append(int(s))
            except Exception:
                continue

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "family_key": family_key,
            "version": version,
            "analysis_ids": analysis_ids,
            "finished_at": finished_at,
        }
    except Exception as e:
        logging.getLogger("BT_SIGNALS_STREAM").error(
            "BT_SIGNALS_STREAM: ошибка разбора сообщения стрима '%s': %s, fields=%s",
            ANALYSIS_POSTPROC_STREAM_KEY,
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Проверка: нужно ли триггерить stream-сигнал ema_cross_rsislope по сообщению bt:analysis:postproc:ready
def _should_trigger_rsislope_signal(
    signal: Dict[str, Any],
    scenario_id: int,
    base_signal_id: int,
    family_key: str,
    version: str,
    analysis_ids: List[int],
) -> bool:
    params = signal.get("params") or {}

    # триггер по scenario_id
    trigger_scenario_cfg = params.get("trigger_scenario_id")
    if trigger_scenario_cfg is not None:
        try:
            trigger_scenario_id = int(str(trigger_scenario_cfg.get("value")))
        except Exception:
            trigger_scenario_id = None
        if trigger_scenario_id is not None and trigger_scenario_id != scenario_id:
            return False

    # триггер по ведущему signal_id
    trigger_base_signal_cfg = params.get("trigger_base_signal_id")
    if trigger_base_signal_cfg is not None:
        try:
            trigger_base_signal_id = int(str(trigger_base_signal_cfg.get("value")))
        except Exception:
            trigger_base_signal_id = None
        if trigger_base_signal_id is not None and trigger_base_signal_id != base_signal_id:
            return False

    # триггер по семейству анализатора
    trigger_family_cfg = params.get("trigger_family_key")
    if trigger_family_cfg is not None:
        trigger_family_val = str(trigger_family_cfg.get("value") or "").strip().lower()
        if trigger_family_val and trigger_family_val != str(family_key).strip().lower():
            return False

    # триггер по версии анализа
    trigger_version_cfg = params.get("trigger_version")
    if trigger_version_cfg is not None:
        trigger_version_val = str(trigger_version_cfg.get("value") or "").strip().lower()
        if trigger_version_val and trigger_version_val != str(version).strip().lower():
            return False

    # триггер по конкретному analysis_id
    trigger_analysis_cfg = params.get("trigger_analysis_id")
    if trigger_analysis_cfg is not None:
        try:
            trigger_analysis_id = int(str(trigger_analysis_cfg.get("value")))
        except Exception:
            trigger_analysis_id = None
        if trigger_analysis_id is not None and trigger_analysis_id not in analysis_ids:
            return False

    return True