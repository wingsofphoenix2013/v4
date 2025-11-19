# bt_scenarios_main.py — оркестратор сценариев backtester_v1

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import (
    get_scenario_signal_links_for_signal,
    get_scenario_instance,
)

# 🔸 Воркеры сценариев
from bt_scenario_basic_straight_mono import run_basic_straight_mono_backfill

# 🔸 Константы стрима сценариев
SCENARIO_STREAM_KEY = "bt:signals:ready"
SCENARIO_CONSUMER_GROUP = "bt_scenarios"
SCENARIO_CONSUMER_NAME = "bt_scenarios_main"

# 🔸 Настройки чтения стрима
SCENARIO_STREAM_BATCH_SIZE = 10      # сколько сообщений читаем за один заход
SCENARIO_STREAM_BLOCK_MS = 5000      # блокировка чтения (мс)

log = logging.getLogger("BT_SCENARIOS_MAIN")


# 🔸 Публичная точка входа: оркестратор сценариев
async def run_bt_scenarios_orchestrator(pg, redis):
    log.info("BT_SCENARIOS_MAIN: оркестратор сценариев запущен")

    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и запуска сценариев
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_tasks_started = 0
            total_signals = 0
            total_scenarios = 0

            for stream_key, entries in messages:
                if stream_key != SCENARIO_STREAM_KEY:
                    # на всякий случай игнорируем чужие стримы
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    signal_ctx = _parse_signal_message(fields)
                    if not signal_ctx:
                        # если не удалось корректно распарсить поля — ACK и пропускаем
                        await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)
                        continue

                    signal_id = signal_ctx["signal_id"]
                    total_signals += 1

                    # получаем все связки сценарий ↔ сигнал
                    links = get_scenario_signal_links_for_signal(signal_id)
                    if not links:
                        log.info(
                            f"BT_SCENARIOS_MAIN: для signal_id={signal_id} нет активных связок сценариев, "
                            f"сообщение {entry_id} будет помечено как обработанное"
                        )
                        await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)
                        continue

                    started_for_message = 0

                    for link in links:
                        scenario_id = link.get("scenario_id")
                        scenario = get_scenario_instance(scenario_id)
                        if not scenario:
                            log.warning(
                                f"BT_SCENARIOS_MAIN: сценарий id={scenario_id} не найден в кеше, "
                                f"signal_id={signal_id}, сообщение {entry_id}"
                            )
                            continue

                        if not scenario.get("enabled"):
                            log.info(
                                f"BT_SCENARIOS_MAIN: сценарий id={scenario_id} отключён, "
                                f"signal_id={signal_id}, сообщение {entry_id}"
                            )
                            continue

                        # запускаем обработку окна сигнала сценарием в отдельной задаче
                        asyncio.create_task(
                            _run_scenario_worker(
                                scenario=scenario,
                                signal_ctx=signal_ctx,
                                pg=pg,
                                redis=redis,
                            ),
                            name=f"BT_SCENARIO_{scenario_id}_SIG_{signal_id}",
                        )
                        started_for_message += 1
                        total_tasks_started += 1
                        total_scenarios += 1

                    # помечаем сообщение как обработанное
                    await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)

                    log.info(
                        f"BT_SCENARIOS_MAIN: сообщение stream_id={entry_id} для signal_id={signal_id} "
                        f"обработано, сценариев запущено={started_for_message}"
                    )

            log.info(
                f"BT_SCENARIOS_MAIN: пакет обработан — сообщений={total_msgs}, сигналов={total_signals}, "
                f"сценариев-запусков={total_scenarios}, задач создано={total_tasks_started}"
            )

        except Exception as e:
            log.error(
                f"BT_SCENARIOS_MAIN: ошибка в основном цикле оркестратора: {e}",
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима сценариев
async def _ensure_consumer_group(redis) -> None:
    try:
        # пытаемся создать группу; MKSTREAM создаст стрим, если его ещё нет
        await redis.xgroup_create(
            name=SCENARIO_STREAM_KEY,
            groupname=SCENARIO_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.info(
            f"BT_SCENARIOS_MAIN: создана consumer group '{SCENARIO_CONSUMER_GROUP}' "
            f"для стрима '{SCENARIO_STREAM_KEY}'"
        )
    except Exception as e:
        # если группа уже существует — Redis вернёт ошибку BUSYGROUP, её игнорируем
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                f"BT_SCENARIOS_MAIN: consumer group '{SCENARIO_CONSUMER_GROUP}' "
                f"для стрима '{SCENARIO_STREAM_KEY}' уже существует"
            )
        else:
            log.error(
                f"BT_SCENARIOS_MAIN: ошибка при создании consumer group '{SCENARIO_CONSUMER_GROUP}': {e}",
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима сценариев
async def _read_from_stream(redis) -> List[Any]:
    # XREADGROUP GROUP <group> <consumer> COUNT N BLOCK M STREAMS key >
    entries = await redis.xreadgroup(
        groupname=SCENARIO_CONSUMER_GROUP,
        consumername=SCENARIO_CONSUMER_NAME,
        streams={SCENARIO_STREAM_KEY: ">"},
        count=SCENARIO_STREAM_BATCH_SIZE,
        block=SCENARIO_STREAM_BLOCK_MS,
    )

    if not entries:
        return []

    # entries: List[Tuple[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]
    parsed: List[Any] = []
    for stream_key, messages in entries:
        # redis-py может возвращать bytes, приводим к str
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            # поля приводим к str
            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:signals:ready
def _parse_signal_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        signal_id_str = fields.get("signal_id")
        from_time_str = fields.get("from_time")
        to_time_str = fields.get("to_time")
        finished_at_str = fields.get("finished_at")

        if not (signal_id_str and from_time_str and to_time_str and finished_at_str):
            # не хватает обязательных полей
            return None

        signal_id = int(signal_id_str)
        from_time = datetime.fromisoformat(from_time_str)
        to_time = datetime.fromisoformat(to_time_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "signal_id": signal_id,
            "from_time": from_time,
            "to_time": to_time,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            f"BT_SCENARIOS_MAIN: ошибка разбора сообщения стрима bt:signals:ready: {e}, fields={fields}",
            exc_info=True,
        )
        return None


# 🔸 Диспетчер воркеров сценариев по типу/ключу
async def _run_scenario_worker(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    scenario_id = scenario.get("id")
    scenario_key = scenario.get("key")
    scenario_type = scenario.get("type")

    signal_id = signal_ctx.get("signal_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    log.info(
        f"BT_SCENARIOS_MAIN: запуск сценарного воркера для scenario_id={scenario_id}, "
        f"key={scenario_key}, type={scenario_type}, signal_id={signal_id}, "
        f"окно=[{from_time} .. {to_time}]"
    )

    # маршрутизация по ключу/типу сценария
    try:
        if scenario_key == "basic_straight_mono" and scenario_type == "straight":
            await run_basic_straight_mono_backfill(scenario, signal_ctx, pg, redis)
            log.info(
                f"BT_SCENARIOS_MAIN: сценарий id={scenario_id} (basic_straight_mono) успешно отработал "
                f"для signal_id={signal_id}, окно=[{from_time} .. {to_time}]"
            )
            return

        log.info(
            f"BT_SCENARIOS_MAIN: сценарий id={scenario_id} (key={scenario_key}, type={scenario_type}) "
            f"пока не поддерживается воркером сценариев"
        )
    except Exception as e:
        log.error(
            f"BT_SCENARIOS_MAIN: ошибка при выполнении сценария id={scenario_id} "
            f"(key={scenario_key}, type={scenario_type}, signal_id={signal_id}): {e}",
            exc_info=True,
        )