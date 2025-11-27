# bt_analysis_main.py — оркестратор анализаторов (feature bins) для backtester_v1

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

# 🔸 Конфиг и кеши backtester_v1 (анализаторы и связки)
from backtester_config import (
    get_analysis_instances_for_scenario_signal,
)

# 🔸 Воркеры семей анализаторов
from bt_analysis_rsi import run_analysis_rsi
from bt_analysis_adx import run_analysis_adx
from bt_analysis_ema import run_analysis_ema
from bt_analysis_atr import run_analysis_atr
from bt_analysis_supertrend import run_analysis_supertrend

# 🔸 Реестр обработчиков семейств анализаторов
FAMILY_ANALYSIS_HANDLERS = {
    "rsi": run_analysis_rsi,
    "adx": run_analysis_adx,
    "ema": run_analysis_ema,
    "atr": run_analysis_atr,
    "supertrend": run_analysis_supertrend,
}

# 🔸 Константы стримов анализа
ANALYSIS_STREAM_KEY = "bt:postproc:ready"
ANALYSIS_CONSUMER_GROUP = "bt_analysis"
ANALYSIS_CONSUMER_NAME = "bt_analysis_main"

# 🔸 Стрим готовности анализа (после всех анализаторов семьи)
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"

# 🔸 Настройки чтения стрима bt:postproc:ready
ANALYSIS_STREAM_BATCH_SIZE = 10      # сколько сообщений читаем за один заход
ANALYSIS_STREAM_BLOCK_MS = 5000      # блокировка чтения (мс)

log = logging.getLogger("BT_ANALYSIS_MAIN")


# 🔸 Публичная точка входа: оркестратор анализа фич
async def run_bt_analysis_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_MAIN: оркестратор анализа запущен")

    # подготавливаем consumer group для стрима bt:postproc:ready
    await _ensure_consumer_group(redis)

    # основной цикл чтения стрима и запуска анализаторов
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_tasks_started = 0
            total_pairs = 0

            for stream_key, entries in messages:
                if stream_key != ANALYSIS_STREAM_KEY:
                    # защищаемся от чужих стримов на всякий случай
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_analysis_message(fields)
                    if not ctx:
                        # не удалось корректно распарсить сообщение — ACK и пропускаем
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]

                    log.debug(
                        "BT_ANALYSIS_MAIN: получено сообщение о готовности постпроцессинга "
                        "scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        ctx["finished_at"],
                        entry_id,
                    )

                    # получаем все включённые инстансы анализаторов для этой пары
                    analysis_instances = get_analysis_instances_for_scenario_signal(
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                    )

                    if not analysis_instances:
                        log.debug(
                            "BT_ANALYSIS_MAIN: для scenario_id=%s, signal_id=%s нет включённых анализаторов, "
                            "сообщение %s помечено как обработанное",
                            scenario_id,
                            signal_id,
                            entry_id,
                        )
                        await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)
                        continue

                    # группируем инстансы по семье (family_key)
                    instances_by_family: Dict[str, List[Dict[str, Any]]] = {}
                    for inst in analysis_instances:
                        family_key = inst.get("family_key")
                        if not family_key:
                            continue
                        instances_by_family.setdefault(family_key, []).append(inst)

                    started_for_message = 0

                    for family_key, family_instances in instances_by_family.items():
                        asyncio.create_task(
                            _run_family_worker(
                                family_key=family_key,
                                instances=family_instances,
                                scenario_id=scenario_id,
                                signal_id=signal_id,
                                pg=pg,
                                redis=redis,
                            ),
                            name=f"BT_ANALYSIS_{family_key.upper()}_SC{scenario_id}_SIG{signal_id}",
                        )
                        started_for_message += 1
                        total_tasks_started += 1
                        total_pairs += len(family_instances)

                    # помечаем сообщение как обработанное
                    await redis.xack(ANALYSIS_STREAM_KEY, ANALYSIS_CONSUMER_GROUP, entry_id)

                    log.debug(
                        "BT_ANALYSIS_MAIN: сообщение stream_id=%s для scenario_id=%s, signal_id=%s "
                        "обработано, семей анализаторов запущено=%s",
                        entry_id,
                        scenario_id,
                        signal_id,
                        started_for_message,
                    )

            log.debug(
                "BT_ANALYSIS_MAIN: пакет сообщений обработан — сообщений=%s, семей_запусков=%s, "
                "инстансов_анализаторов=%s",
                total_msgs,
                total_tasks_started,
                total_pairs,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_MAIN: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима анализа
async def _ensure_consumer_group(redis) -> None:
    try:
        # MKSTREAM создаст стрим, если его ещё нет
        await redis.xgroup_create(
            name=ANALYSIS_STREAM_KEY,
            groupname=ANALYSIS_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_MAIN: создана consumer group '%s' для стрима '%s'",
            ANALYSIS_CONSUMER_GROUP,
            ANALYSIS_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_MAIN: consumer group '%s' для стрима '%s' уже существует",
                ANALYSIS_CONSUMER_GROUP,
                ANALYSIS_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_MAIN: ошибка при создании consumer group '%s': %s",
                ANALYSIS_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:postproc:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ANALYSIS_CONSUMER_GROUP,
        consumername=ANALYSIS_CONSUMER_NAME,
        streams={ANALYSIS_STREAM_KEY: ">"},
        count=ANALYSIS_STREAM_BATCH_SIZE,
        block=ANALYSIS_STREAM_BLOCK_MS,
    )

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


# 🔸 Разбор одного сообщения из стрима bt:postproc:ready
def _parse_analysis_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        processed_str = fields.get("processed") or "0"
        skipped_str = fields.get("skipped") or "0"
        errors_str = fields.get("errors") or "0"

        try:
            processed = int(processed_str)
        except Exception:
            processed = 0

        try:
            skipped = int(skipped_str)
        except Exception:
            skipped = 0

        try:
            errors = int(errors_str)
        except Exception:
            errors = 0

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
            "processed": processed,
            "skipped": skipped,
            "errors": errors,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_MAIN: ошибка разбора сообщения стрима bt:postproc:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None

# 🔸 Диспетчер воркеров семей анализаторов по family_key
async def _run_family_worker(
    family_key: str,
    instances: List[Dict[str, Any]],
    scenario_id: int,
    signal_id: int,
    pg,
    redis,
) -> None:
    log.debug(
        "BT_ANALYSIS_MAIN: запуск семейного воркера для family_key=%s, "
        "scenario_id=%s, signal_id=%s, инстансов=%s",
        family_key,
        scenario_id,
        signal_id,
        len(instances),
    )

    try:
        handler = FAMILY_ANALYSIS_HANDLERS.get(family_key)
        if not handler:
            log.debug(
                "BT_ANALYSIS_MAIN: family_key=%s пока не поддерживается воркером анализа "
                "(scenario_id=%s, signal_id=%s)",
                family_key,
                scenario_id,
                signal_id,
            )
            return

        # запускаем семейный анализатор
        await handler(
            scenario_id=scenario_id,
            signal_id=signal_id,
            analysis_instances=instances,
            pg=pg,
        )

        # после успешного завершения семейства публикуем событие в bt:analysis:ready
        finished_at = datetime.utcnow()
        analysis_ids = [str(inst.get("id")) for inst in instances if inst.get("id") is not None]

        try:
            await redis.xadd(
                ANALYSIS_READY_STREAM_KEY,
                {
                    "scenario_id": str(scenario_id),
                    "signal_id": str(signal_id),
                    "family_key": str(family_key),
                    "analysis_ids": ",".join(analysis_ids),
                    "finished_at": finished_at.isoformat(),
                },
            )
            log.debug(
                "BT_ANALYSIS_MAIN: опубликовано событие готовности анализа в стрим '%s' "
                "для scenario_id=%s, signal_id=%s, family=%s, analysis_ids=%s",
                ANALYSIS_READY_STREAM_KEY,
                scenario_id,
                signal_id,
                family_key,
                analysis_ids,
            )
        except Exception as e:
            log.error(
                "BT_ANALYSIS_MAIN: не удалось опубликовать событие в стрим '%s' "
                "для scenario_id=%s, signal_id=%s, family=%s: %s",
                ANALYSIS_READY_STREAM_KEY,
                scenario_id,
                signal_id,
                family_key,
                e,
                exc_info=True,
            )

        log.debug(
            "BT_ANALYSIS_MAIN: family_key=%s успешно отработал для scenario_id=%s, signal_id=%s",
            family_key,
            scenario_id,
            signal_id,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_MAIN: ошибка при выполнении семейного анализатора family_key=%s "
            "(scenario_id=%s, signal_id=%s): %s",
            family_key,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )