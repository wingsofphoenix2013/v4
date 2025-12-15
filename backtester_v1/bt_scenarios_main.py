# bt_scenarios_main.py — оркестратор сценариев backtester_v1

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Awaitable, Tuple

# 🔸 Конфиг и кеши backtester_v1
from backtester_config import (
    get_scenario_signal_links_for_signal,
    get_scenario_instance,
)

# 🔸 Тип обработчика сценария:
#    (scenario, signal_ctx, pg_pool, redis_client) -> None
ScenarioHandler = Callable[[Dict[str, Any], Dict[str, Any], Any, Any], Awaitable[None]]

# 🔸 Воркеры сценариев (из пакета scenarios/)
from scenarios.bt_scenario_basic_straight_mono import run_basic_straight_mono_backfill
from scenarios.bt_scenario_double_straight_mono import run_double_straight_mono_backfill

# 🔸 Реестр сценарных воркеров: (key, type) → handler
SCENARIO_HANDLERS: Dict[Tuple[str, str], ScenarioHandler] = {
    ("basic_straight_mono", "straight"): run_basic_straight_mono_backfill,
    ("double_straight_mono", "straight"): run_double_straight_mono_backfill,
}

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
    log.debug("BT_SCENARIOS_MAIN: оркестратор сценариев запущен")

    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и запуска сценариев
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
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
                        log.debug(
                            "BT_SCENARIOS_MAIN: для signal_id=%s нет активных связок сценариев, "
                            "сообщение %s будет помечено как обработанное",
                            signal_id,
                            entry_id,
                        )
                        await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)
                        continue

                    started_for_message = 0

                    # последовательный запуск всех сценариев для данного сообщения
                    for link in links:
                        scenario_id = link.get("scenario_id")
                        scenario = get_scenario_instance(scenario_id)
                        if not scenario:
                            log.warning(
                                "BT_SCENARIOS_MAIN: сценарий id=%s не найден в кеше, "
                                "signal_id=%s, сообщение %s",
                                scenario_id,
                                signal_id,
                                entry_id,
                            )
                            continue

                        if not scenario.get("enabled"):
                            log.debug(
                                "BT_SCENARIOS_MAIN: сценарий id=%s отключён, "
                                "signal_id=%s, сообщение %s",
                                scenario_id,
                                signal_id,
                                entry_id,
                            )
                            continue

                        # выполняем сценарий синхронно (последовательно)
                        await _run_scenario_worker(
                            scenario=scenario,
                            signal_ctx=signal_ctx,
                            pg=pg,
                            redis=redis,
                        )
                        started_for_message += 1
                        total_scenarios += 1

                    # помечаем сообщение как обработанное после выполнения всех сценариев
                    await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_SCENARIOS_MAIN: сообщение stream_id=%s для signal_id=%s "
                        "обработано, сценариев запущено=%s",
                        entry_id,
                        signal_id,
                        started_for_message,
                    )

            log.debug(
                "BT_SCENARIOS_MAIN: пакет обработан — сообщений=%s, сигналов=%s, "
                "сценариев-запусков=%s",
                total_msgs,
                total_signals,
                total_scenarios,
            )
            log.info(
                "BT_SCENARIOS_MAIN: итог по пакету — сообщений=%s, сигналов=%s, "
                "запусков сценариев=%s (последовательный режим)",
                total_msgs,
                total_signals,
                total_scenarios,
            )

        except Exception as e:
            log.error(
                "BT_SCENARIOS_MAIN: ошибка в основном цикле оркестратора: %s",
                e,
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
        log.debug(
            "BT_SCENARIOS_MAIN: создана consumer group '%s' для стрима '%s'",
            SCENARIO_CONSUMER_GROUP,
            SCENARIO_STREAM_KEY,
        )
    except Exception as e:
        # если группа уже существует — Redis вернёт ошибку BUSYGROUP, её игнорируем
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SCENARIOS_MAIN: consumer group '%s' для стрима '%s' уже существует",
                SCENARIO_CONSUMER_GROUP,
                SCENARIO_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SCENARIOS_MAIN: ошибка при создании consumer group '%s': %s",
                SCENARIO_CONSUMER_GROUP,
                e,
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
            "BT_SCENARIOS_MAIN: ошибка разбора сообщения стрима bt:signals:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Диспетчер воркеров сценариев по key/type через реестр
async def _run_scenario_worker(
    scenario: Dict[str, Any],
    signal_ctx: Dict[str, Any],
    pg,
    redis,
) -> None:
    scenario_id = scenario.get("id")
    scenario_key = str(scenario.get("key") or "").strip()
    scenario_type = str(scenario.get("type") or "").strip()

    signal_id = signal_ctx.get("signal_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    log.debug(
        "BT_SCENARIOS_MAIN: запуск сценарного воркера для scenario_id=%s, "
        "key=%s, type=%s, signal_id=%s, окно=[%s .. %s]",
        scenario_id,
        scenario_key,
        scenario_type,
        signal_id,
        from_time,
        to_time,
    )

    handler = SCENARIO_HANDLERS.get((scenario_key, scenario_type))
    if handler is None:
        log.debug(
            "BT_SCENARIOS_MAIN: сценарий id=%s (key=%s, type=%s) пока не поддерживается реестром сценариев",
            scenario_id,
            scenario_key,
            scenario_type,
        )
        return

    # очистка результатов сценария перед прогоном "с чистого листа"
    try:
        cleanup = await _cleanup_scenario_tables(pg, int(scenario_id), int(signal_id))
        log.info(
            "BT_SCENARIOS_MAIN: cleanup перед сценарием scenario_id=%s, signal_id=%s — "
            "deleted_positions=%s, deleted_logs=%s, deleted_daily=%s, deleted_stat=%s, deleted_total=%s",
            scenario_id,
            signal_id,
            cleanup["positions"],
            cleanup["logs"],
            cleanup["daily"],
            cleanup["stat"],
            cleanup["total"],
        )
    except Exception as e:
        log.error(
            "BT_SCENARIOS_MAIN: ошибка cleanup перед сценарием scenario_id=%s, signal_id=%s: %s",
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )

    try:
        await handler(scenario, signal_ctx, pg, redis)
        log.debug(
            "BT_SCENARIOS_MAIN: сценарий id=%s (key=%s, type=%s) успешно отработал для signal_id=%s, "
            "окно=[%s .. %s]",
            scenario_id,
            scenario_key,
            scenario_type,
            signal_id,
            from_time,
            to_time,
        )
    except Exception as e:
        log.error(
            "BT_SCENARIOS_MAIN: ошибка при выполнении сценария id=%s (key=%s, type=%s, signal_id=%s): %s",
            scenario_id,
            scenario_key,
            scenario_type,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Очистка таблиц сценария перед прогоном (scenario_id + signal_id)
async def _cleanup_scenario_tables(pg, scenario_id: int, signal_id: int) -> Dict[str, int]:
    deleted_positions = 0
    deleted_logs = 0
    deleted_daily = 0
    deleted_stat = 0

    async with pg.acquire() as conn:
        # транзакция очистки
        async with conn.transaction():
            res_logs = await conn.execute(
                """
                DELETE FROM bt_signals_log l
                USING bt_signals_values v
                WHERE l.signal_uuid = v.signal_uuid
                  AND l.scenario_id = $1
                  AND v.signal_id = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_logs = _parse_pg_execute_count(res_logs)

            res_pos = await conn.execute(
                """
                DELETE FROM bt_scenario_positions
                WHERE scenario_id = $1
                  AND signal_id = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_positions = _parse_pg_execute_count(res_pos)

            res_daily = await conn.execute(
                """
                DELETE FROM bt_scenario_daily
                WHERE scenario_id = $1
                  AND signal_id = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_daily = _parse_pg_execute_count(res_daily)

            res_stat = await conn.execute(
                """
                DELETE FROM bt_scenario_stat
                WHERE scenario_id = $1
                  AND signal_id = $2
                """,
                scenario_id,
                signal_id,
            )
            deleted_stat = _parse_pg_execute_count(res_stat)

    return {
        "positions": deleted_positions,
        "logs": deleted_logs,
        "daily": deleted_daily,
        "stat": deleted_stat,
        "total": deleted_positions + deleted_logs + deleted_daily + deleted_stat,
    }


# 🔸 Парсинг результата asyncpg conn.execute вида "DELETE 123"
def _parse_pg_execute_count(res: Any) -> int:
    try:
        return int(str(res).split()[-1])
    except Exception:
        return 0