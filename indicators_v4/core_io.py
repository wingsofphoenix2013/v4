# core_io.py — воркер для чтения indicator_stream_core и быстрой записи индикаторов в PostgreSQL (multi-consumer + bulk upsert)

# 🔸 Импорты и зависимости
import os
import json
import logging
import asyncio
import socket
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP


# 🔸 Конфиг (env)
CORE_STREAM = "indicator_stream_core"
CORE_GROUP = os.getenv("IV4_CORE_IO_GROUP", "group_core_io")
CONSUMER_PREFIX = os.getenv("IV4_CORE_IO_CONSUMER_PREFIX", "core_io")

CORE_IO_CONSUMERS = int(os.getenv("IV4_CORE_IO_CONSUMERS", "3"))
CORE_IO_BATCH = int(os.getenv("IV4_CORE_IO_BATCH", "2000"))
CORE_IO_BLOCK_MS = int(os.getenv("IV4_CORE_IO_BLOCK_MS", "500"))


# 🔸 Квантизация значения как в старом core_io (с учётом angle=5)
def _quantize_value(str_value: str, precision: int, is_angle: bool) -> float:
    p_prec = 5 if is_angle else int(precision)
    quantize_str = "1." + "0" * p_prec
    val = Decimal(str(str_value)).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
    return float(val)


# 🔸 Bulk upsert в indicator_values_v4 (одним запросом)
async def _bulk_upsert_indicator_values(conn, rows: list[tuple[int, str, datetime, str, float]]):
    if not rows:
        return

    instance_ids = [r[0] for r in rows]
    symbols = [r[1] for r in rows]
    open_times = [r[2] for r in rows]
    param_names = [r[3] for r in rows]
    values = [r[4] for r in rows]

    await conn.execute(
        """
        INSERT INTO indicator_values_v4 (instance_id, symbol, open_time, param_name, value)
        SELECT *
        FROM UNNEST(
            $1::int4[],
            $2::text[],
            $3::timestamp[],
            $4::text[],
            $5::float8[]
        )
        ON CONFLICT (instance_id, symbol, open_time, param_name)
        DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        instance_ids,
        symbols,
        open_times,
        param_names,
        values,
    )


# 🔸 Обработка одного батча сообщений stream → rows + iv4_inserted keys
def _parse_core_messages(messages, log: logging.Logger):
    records: list[tuple[int, str, datetime, str, float]] = []
    to_ack: list[str] = []

    # для iv4_inserted агрегируем по (symbol, interval, open_time, instance_id)
    event_counts: dict[tuple[str, str, datetime, int], int] = {}

    msg_total = 0
    msg_bad = 0
    params_total = 0

    for msg_id, data in messages:
        msg_total += 1
        to_ack.append(msg_id)

        try:
            symbol = data["symbol"]
            interval = data["interval"]
            instance_id = int(data["instance_id"])
            open_time = datetime.fromisoformat(data["open_time"])
            precision = int(data.get("precision", 8))

            # новый формат: values_json = {param_name: value_str, ...}
            if "values_json" in data:
                values = json.loads(data.get("values_json") or "{}")
                if not isinstance(values, dict) or not values:
                    msg_bad += 1
                    continue

                added = 0
                for param_name, str_value in values.items():
                    try:
                        is_angle = "angle" in str(param_name)
                        value = _quantize_value(str(str_value), precision, is_angle)
                        records.append((instance_id, symbol, open_time, str(param_name), value))
                        added += 1
                        params_total += 1
                    except Exception as e:
                        log.error(f"Ошибка param (values_json): {e}")

                if added:
                    key = (symbol, interval, open_time, instance_id)
                    event_counts[key] = event_counts.get(key, 0) + added

            # старый формат: param_name/value (на всякий случай)
            else:
                param_name = data["param_name"]
                str_value = data["value"]

                is_angle = "angle" in str(param_name)
                value = _quantize_value(str(str_value), precision, is_angle)
                records.append((instance_id, symbol, open_time, str(param_name), value))
                params_total += 1

                key = (symbol, interval, open_time, instance_id)
                event_counts[key] = event_counts.get(key, 0) + 1

        except Exception as e:
            msg_bad += 1
            log.error(f"Ошибка при разборе сообщения core stream: {e}", exc_info=True)

    return records, to_ack, event_counts, msg_total, msg_bad, params_total


# 🔸 Один consumer-loop (читает stream и пишет в PG)
async def _core_io_consumer_loop(pg, redis, consumer: str):
    log = logging.getLogger(f"CORE_IO:{consumer}")

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=CORE_GROUP,
                consumername=consumer,
                streams={CORE_STREAM: ">"},
                count=CORE_IO_BATCH,
                block=CORE_IO_BLOCK_MS,
            )
            if not resp:
                continue

            # распакуем сообщения (обычно одна запись stream → список messages)
            all_messages = []
            for _, messages in resp:
                all_messages.extend(messages)

            records, to_ack, event_counts, msg_total, msg_bad, params_total = _parse_core_messages(all_messages, log)

            # пишем в PG
            if records:
                async with pg.acquire() as conn:
                    async with conn.transaction():
                        await _bulk_upsert_indicator_values(conn, records)

                # суммирующий лог (info)
                log.debug(
                    "batch stored (msgs=%s bad=%s params=%s records=%s keys=%s)",
                    msg_total,
                    msg_bad,
                    params_total,
                    len(records),
                    len(event_counts),
                )

                # публикация факта вставки (триггер для аудитора индикаторов)
                for (symbol, interval, open_time, instance_id), cnt in event_counts.items():
                    try:
                        await redis.xadd("iv4_inserted", {
                            "symbol": symbol,
                            "interval": interval,
                            "open_time": open_time.isoformat(),
                            "instance_id": str(instance_id),
                            "param_count": str(cnt),
                        })
                    except Exception as e:
                        log.warning(f"Не удалось отправить событие в iv4_inserted: {e}")

            # подтверждение обработки сообщений
            if to_ack:
                await redis.xack(CORE_STREAM, CORE_GROUP, *to_ack)

        except Exception as e:
            log.error(f"CORE_IO loop error: {e}", exc_info=True)
            await asyncio.sleep(1)


# 🔸 Точка входа: стартует N consumers в одной группе
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")

    # создать consumer-group (если уже существует — игнорировать)
    try:
        await redis.xgroup_create(CORE_STREAM, CORE_GROUP, id="$", mkstream=True)
        log.debug(f"Consumer group '{CORE_GROUP}' создана")
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.error(f"Ошибка при создании группы: {e}")

    host = os.getenv("HOSTNAME") or socket.gethostname()
    pid = os.getpid()

    # стартуем несколько consumers
    tasks = []
    for i in range(1, max(1, CORE_IO_CONSUMERS) + 1):
        consumer = f"{CONSUMER_PREFIX}:{host}:{pid}:{i}"
        tasks.append(asyncio.create_task(_core_io_consumer_loop(pg, redis, consumer)))

    log.debug(
        "CORE_IO: started (group=%s consumers=%s batch=%s block_ms=%s)",
        CORE_GROUP,
        len(tasks),
        CORE_IO_BATCH,
        CORE_IO_BLOCK_MS,
    )

    await asyncio.gather(*tasks)