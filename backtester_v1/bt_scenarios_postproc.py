# bt_scenarios_postproc.py — постпроцессинг позиций сценариев (сбор снапшотов индикаторов, run-aware)

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import json

# 🔸 Кеши backtester_v1 (инстансы индикаторов)
from backtester_config import get_all_indicator_instances

log = logging.getLogger("BT_SCENARIOS_POSTPROC")

# 🔸 Константы стрима постпроцессора
POSTPROC_STREAM_KEY = "bt:scenarios:ready"
POSTPROC_CONSUMER_GROUP = "bt_scenarios_postproc"
POSTPROC_CONSUMER_NAME = "bt_scenarios_postproc_main"

# 🔸 Стрим готовности постпроцессинга
POSTPROC_READY_STREAM_KEY = "bt:postproc:ready"

# 🔸 Настройки чтения стрима
POSTPROC_STREAM_BATCH_SIZE = 10
POSTPROC_STREAM_BLOCK_MS = 5000

# 🔸 Настройки обработки позиций
POSTPROC_BATCH_SIZE = 200
POSTPROC_MAX_CONCURRENCY = 10

# 🔸 Поддерживаемые таймфреймы и их шаг в минутах
TF_STEP_MINUTES = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}


# 🔸 Публичная точка входа: оркестратор постпроцессинга сценариев
async def run_bt_scenarios_postproc(pg, redis) -> None:
    log.debug("BT_SCENARIOS_POSTPROC: воркер постпроцессинга сценариев запущен")

    # подготавливаем consumer group для стрима
    await _ensure_consumer_group(redis)

    # загружаем инстансы индикаторов и раскладываем по TF
    indicator_by_tf, instances_by_id = _build_indicator_instances_cache()
    log.debug(
        "BT_SCENARIOS_POSTPROC: кеш индикаторов загружен — TF=%s",
        {tf: len(instances) for tf, instances in indicator_by_tf.items()},
    )

    # основной цикл чтения стрима и обработки сценариев
    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            total_scenarios_processed = 0
            total_positions_processed = 0
            total_positions_skipped = 0
            total_positions_errors = 0

            for stream_key, entries in messages:
                if stream_key != POSTPROC_STREAM_KEY:
                    # защищаемся от чужих стримов на всякий случай
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_ready_message(fields)
                    if not ctx:
                        # не удалось корректно распарсить сообщение — ACK и пропускаем
                        await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    log.debug(
                        "BT_SCENARIOS_POSTPROC: получено сообщение о готовности сценария "
                        "scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s, stream_id=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        entry_id,
                    )

                    # обработка всех позиций данного сценария
                    processed, skipped, errors = await _process_scenario_positions(
                        pg=pg,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        indicator_by_tf=indicator_by_tf,
                        instances_by_id=instances_by_id,
                    )
                    total_scenarios_processed += 1
                    total_positions_processed += processed
                    total_positions_skipped += skipped
                    total_positions_errors += errors

                    log.debug(
                        "BT_SCENARIOS_POSTPROC: summary для scenario_id=%s, signal_id=%s, run_id=%s — processed=%s, skipped=%s, errors=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        processed,
                        skipped,
                        errors,
                    )

                    # публикуем событие о завершении постпроцессинга в bt:postproc:ready (run-aware)
                    finished_at_postproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            POSTPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "run_id": str(run_id),
                                "processed": str(processed),
                                "skipped": str(skipped),
                                "errors": str(errors),
                                "finished_at": finished_at_postproc.isoformat(),
                            },
                        )
                        log.debug(
                            "BT_SCENARIOS_POSTPROC: опубликовано событие готовности постпроцессинга "
                            "в стрим '%s' для scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s",
                            POSTPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            run_id,
                            finished_at_postproc,
                        )
                    except Exception as e:
                        # проблемы стрима не должны ломать основной воркер
                        log.error(
                            "BT_SCENARIOS_POSTPROC: не удалось опубликовать событие в стрим '%s' "
                            "для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            POSTPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )

                    # помечаем сообщение как обработанное
                    await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_CONSUMER_GROUP, entry_id)

            log.debug(
                "BT_SCENARIOS_POSTPROC: итог по пакету — сообщений=%s, сценариев=%s, positions processed=%s, skipped=%s, errors=%s",
                total_msgs,
                total_scenarios_processed,
                total_positions_processed,
                total_positions_skipped,
                total_positions_errors,
            )

        except Exception as e:
            log.error(
                "BT_SCENARIOS_POSTPROC: ошибка в основном цикле воркера: %s",
                e,
                exc_info=True,
            )
            # небольшая пауза перед повторной попыткой, чтобы не крутить CPU при постоянной ошибке
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима постпроцессинга
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_STREAM_KEY,
            groupname=POSTPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_SCENARIOS_POSTPROC: создана consumer group '%s' для стрима '%s'",
            POSTPROC_CONSUMER_GROUP,
            POSTPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_SCENARIOS_POSTPROC: consumer group '%s' уже существует — сдвигаем курсор группы на '$' (SETID) для игнора истории до старта",
                POSTPROC_CONSUMER_GROUP,
            )

            await redis.execute_command(
                "XGROUP",
                "SETID",
                POSTPROC_STREAM_KEY,
                POSTPROC_CONSUMER_GROUP,
                "$",
            )

            log.debug(
                "BT_SCENARIOS_POSTPROC: consumer group '%s' SETID='$' для стрима '%s' выполнен",
                POSTPROC_CONSUMER_GROUP,
                POSTPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_SCENARIOS_POSTPROC: ошибка при создании consumer group '%s': %s",
                POSTPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise

# 🔸 Чтение сообщений из стрима bt:scenarios:ready
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=POSTPROC_CONSUMER_GROUP,
            consumername=POSTPROC_CONSUMER_NAME,
            streams={POSTPROC_STREAM_KEY: ">"},
            count=POSTPROC_STREAM_BATCH_SIZE,
            block=POSTPROC_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning(
                "BT_SCENARIOS_POSTPROC: NOGROUP при XREADGROUP — пересоздаём/переинициализируем группу и продолжаем",
            )
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


# 🔸 Разбор одного сообщения из стрима bt:scenarios:ready (run-aware)
def _parse_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_SCENARIOS_POSTPROC: ошибка разбора сообщения стрима bt:scenarios:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Построение кеша инстансов индикаторов по TF
def _build_indicator_instances_cache() -> Tuple[Dict[str, Dict[int, Dict[str, Any]]], Dict[int, Dict[str, Any]]]:
    all_instances = get_all_indicator_instances()  # instance_id -> {indicator, timeframe, params, ...}

    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]] = {"m5": {}, "m15": {}, "h1": {}}
    instances_by_id: Dict[int, Dict[str, Any]] = {}

    for instance_id, inst in all_instances.items():
        tf = inst.get("timeframe")

        if tf not in indicator_by_tf:
            # игнорируем TF, с которыми пока не работаем в postproc
            continue

        indicator_by_tf[tf][instance_id] = inst
        instances_by_id[instance_id] = inst

    return indicator_by_tf, instances_by_id


# 🔸 Обработка всех позиций одного сценария (по одному сообщению из стрима)
async def _process_scenario_positions(
    pg,
    scenario_id: int,
    signal_id: int,
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
) -> Tuple[int, int, int]:
    processed = 0
    skipped = 0
    errors = 0

    # загружаем позиции этого сценария/сигнала, которые ещё не прошли постпроцессинг
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                symbol,
                timeframe,
                entry_time
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id = $2
              AND postproc = false
            ORDER BY entry_time
            """,
            scenario_id,
            signal_id,
        )

    positions: List[Dict[str, Any]] = [
        {
            "id": r["id"],
            "symbol": r["symbol"],
            "timeframe": r["timeframe"],
            "entry_time": r["entry_time"],
        }
        for r in rows
    ]

    if not positions:
        log.debug(
            "BT_SCENARIOS_POSTPROC: для scenario_id=%s, signal_id=%s нет позиций с postproc=false",
            scenario_id,
            signal_id,
        )
        return processed, skipped, errors

    total_positions = len(positions)
    log.debug(
        "BT_SCENARIOS_POSTPROC: сценарий scenario_id=%s, signal_id=%s — позиций для постпроцессинга=%s",
        scenario_id,
        signal_id,
        total_positions,
    )

    # обрабатываем позиции батчами, внутри батча до POSTPROC_MAX_CONCURRENCY задач параллельно
    for i in range(0, total_positions, POSTPROC_BATCH_SIZE):
        batch = positions[i : i + POSTPROC_BATCH_SIZE]
        sema = asyncio.Semaphore(POSTPROC_MAX_CONCURRENCY)
        tasks = []

        for pos in batch:
            tasks.append(
                _process_position_with_semaphore(
                    pg=pg,
                    position=pos,
                    indicator_by_tf=indicator_by_tf,
                    instances_by_id=instances_by_id,
                    sema=sema,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                errors += 1
                continue
            if res == "processed":
                processed += 1
            elif res == "skipped":
                skipped += 1
            elif res == "error":
                errors += 1

    return processed, skipped, errors


# 🔸 Обёртка для ограничения параллелизма по позициям
async def _process_position_with_semaphore(
    pg,
    position: Dict[str, Any],
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
    sema: asyncio.Semaphore,
) -> str:
    async with sema:
        try:
            return await _process_single_position(pg, position, indicator_by_tf, instances_by_id)
        except Exception as e:
            log.error(
                "BT_SCENARIOS_POSTPROC: ошибка постпроцессинга позиции id=%s: %s",
                position.get("id"),
                e,
                exc_info=True,
            )
            return "error"


# 🔸 Постпроцессинг одной позиции: сбор индикаторов по трём TF и запись raw_stat
async def _process_single_position(
    pg,
    position: Dict[str, Any],
    indicator_by_tf: Dict[str, Dict[int, Dict[str, Any]]],
    instances_by_id: Dict[int, Dict[str, Any]],
) -> str:
    pos_id = position["id"]
    symbol = position["symbol"]
    base_tf = position["timeframe"]
    entry_time: datetime = position["entry_time"]

    # вычисляем опорные open_time для всех TF по единому правилу "что известно к моменту решения"
    open_times = await _resolve_open_times_for_position(pg, symbol, base_tf, entry_time)
    if not open_times:
        log.debug(
            "BT_SCENARIOS_POSTPROC: позиция id=%s, symbol=%s — не удалось определить open_time для всех TF, позиция пропущена",
            pos_id,
            symbol,
        )
        return "skipped"

    tf_payload: Dict[str, Dict[str, Any]] = {}

    async with pg.acquire() as conn:
        for tf in ("m5", "m15", "h1"):
            tf_instances = indicator_by_tf.get(tf) or {}
            if not tf_instances:
                continue

            tf_open_time = open_times.get(tf)
            if tf_open_time is None:
                log.debug(
                    "BT_SCENARIOS_POSTPROC: позиция id=%s, symbol=%s — нет open_time для TF=%s, позиция пропущена",
                    pos_id,
                    symbol,
                    tf,
                )
                return "skipped"

            instance_ids = list(tf_instances.keys())

            rows = await conn.fetch(
                """
                SELECT instance_id, param_name, value
                FROM indicator_values_v4
                WHERE symbol = $1
                  AND open_time = $2
                  AND instance_id = ANY($3::int[])
                """,
                symbol,
                tf_open_time,
                instance_ids,
            )

            by_instance: Dict[int, List[Dict[str, Any]]] = {}
            for r in rows:
                iid = r["instance_id"]
                by_instance.setdefault(iid, []).append(
                    {
                        "param_name": r["param_name"],
                        "value": float(r["value"]),
                    }
                )

            tf_families: Dict[str, Dict[str, float]] = {}

            for iid, records in by_instance.items():
                inst = instances_by_id.get(iid)
                if not inst:
                    continue

                family = inst.get("indicator") or "unknown"
                family_dict = tf_families.setdefault(family, {})

                for rec in records:
                    param_name = rec["param_name"]
                    value = rec["value"]
                    family_dict[param_name] = value

            if not tf_families:
                continue

            tf_payload[tf] = {
                "open_time": tf_open_time.isoformat(),
                "indicators": tf_families,
            }

    if not tf_payload:
        log.debug(
            "BT_SCENARIOS_POSTPROC: позиция id=%s, symbol=%s — нет данных индикаторов для записи, позиция пропущена",
            pos_id,
            symbol,
        )
        return "skipped"

    raw_stat = {
        "version": "v1",
        "tf": tf_payload,
    }

    raw_stat_json = json.dumps(raw_stat)

    # записываем raw_stat и помечаем позицию как обработанную
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE bt_scenario_positions
            SET raw_stat = $1::jsonb,
                postproc = true
            WHERE id = $2
            """,
            raw_stat_json,
            pos_id,
        )

    return "processed"


# 🔸 Определение open_time для позиции по всем TF (единая логика доступности данных)
async def _resolve_open_times_for_position(
    pg,
    symbol: str,
    base_tf: str,
    entry_time: datetime,
) -> Optional[Dict[str, datetime]]:
    open_times: Dict[str, datetime] = {}

    base_tf_lower = (base_tf or "").lower()
    base_step_min = TF_STEP_MINUTES.get(base_tf_lower)

    if not base_step_min:
        log.warning(
            "BT_SCENARIOS_POSTPROC: неизвестный базовый TF '%s' для позиции symbol=%s",
            base_tf,
            symbol,
        )
        return None

    # момент принятия решения по сделке: закрытие бара позиции
    decision_time = entry_time + timedelta(minutes=base_step_min)

    async with pg.acquire() as conn:
        # для всех TF используем единое правило:
        # open_time_TF + Δ_TF <= decision_time (бар полностью закрыт к моменту решения)
        for tf in ("m5", "m15", "h1"):
            table_name = _ohlcv_table_for_timeframe(tf)
            if not table_name:
                continue

            step_minutes = TF_STEP_MINUTES.get(tf)
            if step_minutes is None:
                continue

            interval_str = f"{step_minutes} minutes"

            tf_row = await conn.fetchrow(
                f"""
                SELECT max(open_time) AS open_time
                FROM {table_name}
                WHERE symbol = $1
                  AND open_time + interval '{interval_str}' <= $2
                """,
                symbol,
                decision_time,
            )

            if not tf_row or tf_row["open_time"] is None:
                return None

            open_times[tf] = tf_row["open_time"]

    required_tfs = {"m5", "m15", "h1"}
    if not required_tfs.issubset(open_times.keys()):
        return None

    return open_times


# 🔸 Определение таблицы OHLCV по TF
def _ohlcv_table_for_timeframe(timeframe: str) -> Optional[str]:
    if timeframe == "m5":
        return "ohlcv_bb_m5"
    if timeframe == "m15":
        return "ohlcv_bb_m15"
    if timeframe == "h1":
        return "ohlcv_bb_h1"
    return None