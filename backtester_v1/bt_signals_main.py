# bt_signals_main.py — оркестратор псевдо-сигналов backtester_v1

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import get_enabled_signals

# 🔸 Воркеры семейств псевдо-сигналов
from bt_signals_emacross import run_emacross_backfill
from bt_signals_emacross_rsislope import run_emacross_rsislope_backfill

# 🔸 Дефолтные настройки расписания для ema_cross_plain (используются, если нет параметров)
EMA_CROSS_PLAIN_DEFAULT_START_DELAY_SEC = 60     # старт через минуту после запуска backtester_v1
EMA_CROSS_PLAIN_DEFAULT_INTERVAL_SEC = 3600      # повторный запуск раз в час

# 🔸 Константы стримов для стримовых сигналов
ANALYSIS_POSTPROC_STREAM_KEY = "bt:analysis:postproc:ready"
ANALYSIS_POSTPROC_STREAM_GROUP = "bt_signals_stream_analysis_postproc"
ANALYSIS_POSTPROC_STREAM_CONSUMER = "bt_signals_stream_analysis_postproc_main"
ANALYSIS_POSTPROC_STREAM_BATCH_SIZE = 10
ANALYSIS_POSTPROC_STREAM_BLOCK_MS = 5000


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

    tasks: List[asyncio.Task] = []
    stream_to_signals: Dict[str, List[Dict[str, Any]]] = {}

    for signal in signals:
        key = signal.get("key")
        sid = signal.get("id")
        name = signal.get("name")
        mode = signal.get("mode")
        params = signal.get("params") or {}

        # логируем обнаруженный сигнал
        log.debug("BT_SIGNALS_MAIN: найден сигнал id=%s, key=%s, name=%s, mode=%s", sid, key, name, mode)

        # schedule_type по умолчанию считаем "timer", если не задан
        schedule_type_cfg = params.get("schedule_type")
        if schedule_type_cfg is not None:
            schedule_type_raw = schedule_type_cfg.get("value")
            schedule_type = str(schedule_type_raw).strip().lower()
        else:
            schedule_type = "timer"

        # сигналы с таймерным расписанием
        if schedule_type == "timer":
            # пока обрабатываем только ema_cross_plain и только режимы, включающие backfill
            if key == "ema_cross_plain" and mode in ("backfill", "both"):
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
                # остальные типы таймерных сигналов пока не реализованы
                log.debug(
                    "BT_SIGNALS_MAIN: таймерный сигнал id=%s с key=%s (name=%s) пока не поддерживается",
                    sid,
                    key,
                    name,
                )
            continue

        # сигналы с расписанием по стриму
        if schedule_type == "stream":
            # пока обрабатываем только ema_cross_rsislope по стриму bt:analysis:postproc:ready
            if key == "ema_cross_rsislope" and mode in ("backfill", "both"):
                stream_key_cfg = params.get("stream_key")
                if stream_key_cfg is None:
                    log.error(
                        "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет schedule_type=stream, "
                        "но stream_key не задан, сигнал игнорируется",
                        sid,
                        key,
                        name,
                    )
                    continue

                stream_key = str(stream_key_cfg.get("value") or "").strip()
                if not stream_key:
                    log.error(
                        "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет пустой stream_key, "
                        "сигнал игнорируется",
                        sid,
                        key,
                        name,
                    )
                    continue

                stream_signals = stream_to_signals.setdefault(stream_key, [])
                stream_signals.append(signal)
                log.debug(
                    "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) зарегистрирован как stream-сигнал "
                    "schedule_type=stream, stream_key=%s",
                    sid,
                    key,
                    name,
                    stream_key,
                )
            else:
                log.debug(
                    "BT_SIGNALS_MAIN: stream-сигнал id=%s с key=%s (name=%s) пока не поддерживается",
                    sid,
                    key,
                    name,
                )
            continue

        # неизвестный тип расписания
        log.error(
            "BT_SIGNALS_MAIN: сигнал id=%s (key=%s, name=%s) имеет неизвестный schedule_type=%s, "
            "сигнал игнорируется",
            sid,
            key,
            name,
            schedule_type,
        )

    # поднимаем воркеры для стримов, если есть stream-сигналы
    for stream_key, signals_for_stream in stream_to_signals.items():
        if stream_key == ANALYSIS_POSTPROC_STREAM_KEY:
            task = asyncio.create_task(
                _run_analysis_postproc_stream_dispatcher(signals_for_stream, pg, redis),
                name="BT_SIG_STREAM_ANALYSIS_POSTPROC",
            )
            tasks.append(task)
            logging.getLogger("BT_SIGNALS_MAIN").debug(
                "BT_SIGNALS_MAIN: поднят stream-диспетчер для '%s', сигналов=%s",
                stream_key,
                len(signals_for_stream),
            )
        else:
            logging.getLogger("BT_SIGNALS_MAIN").debug(
                "BT_SIGNALS_MAIN: stream_key='%s' пока не поддерживается, сигналы для него игнорируются",
                stream_key,
            )

    if not tasks:
        log.debug(
            "BT_SIGNALS_MAIN: нет поддерживаемых сигналов для запуска планировщиков/стримов, "
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


# 🔸 Воркёр-диспетчер по стриму bt:analysis:postproc:ready для stream-сигналов
async def _run_analysis_postproc_stream_dispatcher(
    signals_for_stream: List[Dict[str, Any]],
    pg,
    redis,
):
    log = logging.getLogger("BT_SIGNALS_STREAM")
    log.info(
        "BT_SIGNALS_STREAM: диспетчер для стрима '%s' запущен, сигналов=%s",
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

                            log.info(
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

                            # запускаем зависимый воркер EMA+RSI-slope
                            asyncio.create_task(
                                run_emacross_rsislope_backfill(signal, pg, redis, ctx),
                                name=f"BT_SIG_EMA_CROSS_RSISLOPE_{sid}",
                            )

                            triggers_for_msg += 1
                            total_triggers += 1

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

            log.info(
                "BT_SIGNALS_STREAM: пакет сообщений обработан — сообщений=%s, триггеров_по_сигналам=%s",
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


# 🔸 Вспомогательная функция: безопасное чтение int-параметров сигнала
def _get_int_param(params: Dict[str, Any], name: str, default: int) -> int:
    cfg = params.get(name)
    if cfg is None:
        return default

    raw = cfg.get("value")
    try:
        return int(str(raw))
    except Exception:
        return default
