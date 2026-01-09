# bt_scenarios_main.py — оркестратор сценариев backtester_v1 (consumer bt:signals:ready_v2 → запуск сценариев)

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
from scenarios.bt_scenario_raw_straight_mono import run_raw_straight_mono_backfill
from scenarios.bt_scenario_basic_straight_mono import run_basic_straight_mono_backfill
from scenarios.bt_scenario_double_straight_mono import run_double_straight_mono_backfill

# 🔸 Воркеры сценариев v2
from scenarios.bt_scenario_raw_straight_mono_v2 import run_raw_straight_mono_backfill_v2

# 🔸 Реестр сценарных воркеров: (key, type) → handler
#    (остальные сценарии временно не включаем, чтобы не запускать старую логику)
SCENARIO_HANDLERS: Dict[Tuple[str, str], ScenarioHandler] = {
    ("raw_straight_mono", "straight"): run_raw_straight_mono_backfill,
    ("basic_straight_mono", "straight"): run_basic_straight_mono_backfill,
    ("double_straight_mono", "straight"): run_double_straight_mono_backfill,
    ("raw_straight_mono_v2", "straight"): run_raw_straight_mono_backfill_v2,
}

# 🔸 Константы стрима сценариев (потребитель ready датасета сигналов)
SCENARIO_STREAM_KEY = "bt:signals:ready_v2"
SCENARIO_CONSUMER_GROUP = "bt_scenarios"
SCENARIO_CONSUMER_NAME = "bt_scenarios_main"

# 🔸 Настройки чтения стрима
SCENARIO_STREAM_BATCH_SIZE = 10
SCENARIO_STREAM_BLOCK_MS = 5000

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
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    signal_ctx = _parse_signal_message(fields)
                    if not signal_ctx:
                        await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)
                        continue

                    signal_id = signal_ctx["signal_id"]
                    run_id = signal_ctx["run_id"]
                    total_signals += 1

                    # получаем все связки сценарий ↔ сигнал
                    links = get_scenario_signal_links_for_signal(signal_id)
                    if not links:
                        log.debug(
                            "BT_SCENARIOS_MAIN: для signal_id=%s нет активных связок сценариев, run_id=%s, stream_id=%s — ACK",
                            signal_id,
                            run_id,
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
                                "BT_SCENARIOS_MAIN: сценарий id=%s не найден в кеше, signal_id=%s, run_id=%s, stream_id=%s",
                                scenario_id,
                                signal_id,
                                run_id,
                                entry_id,
                            )
                            continue

                        if not scenario.get("enabled"):
                            log.debug(
                                "BT_SCENARIOS_MAIN: сценарий id=%s отключён, signal_id=%s, run_id=%s, stream_id=%s",
                                scenario_id,
                                signal_id,
                                run_id,
                                entry_id,
                            )
                            continue

                        await _run_scenario_worker(
                            scenario=scenario,
                            signal_ctx=signal_ctx,
                            pg=pg,
                            redis=redis,
                        )

                        started_for_message += 1
                        total_scenarios += 1

                    await redis.xack(SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_SCENARIOS_MAIN: сообщение stream_id=%s обработано — signal_id=%s run_id=%s сценариев=%s",
                        entry_id,
                        signal_id,
                        run_id,
                        started_for_message,
                    )

            log.info(
                "BT_SCENARIOS_MAIN: пакет обработан — сообщений=%s, сигналов=%s, запусков сценариев=%s",
                total_msgs,
                total_signals,
                total_scenarios,
            )

        except Exception as e:
            log.error("BT_SCENARIOS_MAIN: ошибка в основном цикле оркестратора: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима сценариев
async def _ensure_consumer_group(redis) -> None:
    try:
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
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SCENARIOS_MAIN: consumer group '%s' уже существует — SETID '$' (игнор истории до старта)",
                SCENARIO_CONSUMER_GROUP,
            )
            await redis.execute_command("XGROUP", "SETID", SCENARIO_STREAM_KEY, SCENARIO_CONSUMER_GROUP, "$")
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
    try:
        entries = await redis.xreadgroup(
            groupname=SCENARIO_CONSUMER_GROUP,
            consumername=SCENARIO_CONSUMER_NAME,
            streams={SCENARIO_STREAM_KEY: ">"},
            count=SCENARIO_STREAM_BATCH_SIZE,
            block=SCENARIO_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning("BT_SCENARIOS_MAIN: NOGROUP при XREADGROUP — переинициализация группы")
            await _ensure_consumer_group(redis)
            return []
        raise

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор одного сообщения из стрима bt:signals:ready_v2 (membership-датасет)
def _parse_signal_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        from_time_str = fields.get("from_time")
        to_time_str = fields.get("to_time")
        finished_at_str = fields.get("finished_at")
        dataset_kind = str(fields.get("dataset_kind") or "").strip().lower()

        # условия достаточности
        if not (signal_id_str and run_id_str and from_time_str and to_time_str and finished_at_str):
            return None
        if dataset_kind and dataset_kind != "membership":
            return None

        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        from_time = datetime.fromisoformat(from_time_str)
        to_time = datetime.fromisoformat(to_time_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "signal_id": signal_id,
            "run_id": run_id,
            "from_time": from_time,
            "to_time": to_time,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_SCENARIOS_MAIN: ошибка разбора сообщения bt:signals:ready_v2: %s, fields=%s",
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
    run_id = signal_ctx.get("run_id")
    from_time = signal_ctx.get("from_time")
    to_time = signal_ctx.get("to_time")

    log.debug(
        "BT_SCENARIOS_MAIN: запуск сценария scenario_id=%s key=%s type=%s signal_id=%s run_id=%s window=[%s..%s]",
        scenario_id,
        scenario_key,
        scenario_type,
        signal_id,
        run_id,
        from_time,
        to_time,
    )

    handler = SCENARIO_HANDLERS.get((scenario_key, scenario_type))
    if handler is None:
        log.debug(
            "BT_SCENARIOS_MAIN: сценарий id=%s (key=%s, type=%s) не поддерживается реестром",
            scenario_id,
            scenario_key,
            scenario_type,
        )
        return

    try:
        await handler(scenario, signal_ctx, pg, redis)
    except Exception as e:
        log.error(
            "BT_SCENARIOS_MAIN: ошибка сценария id=%s (key=%s, type=%s, signal_id=%s, run_id=%s): %s",
            scenario_id,
            scenario_key,
            scenario_type,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )