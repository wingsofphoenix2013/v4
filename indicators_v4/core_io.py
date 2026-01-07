# core_io.py — воркер для чтения indicator_stream_core (batched per instance) и записи индикаторов в PostgreSQL

# 🔸 Импорты и зависимости
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import asyncio


# 🔸 Асинхронный воркер для чтения из Redis Stream и записи в PG
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")

    stream = "indicator_stream_core"
    group = "group_core_io"
    consumer = "core_io_1"

    # попытка создать группу (если уже существует — игнорировать)
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.debug(f"Consumer group '{group}' создана")
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.error(f"Ошибка при создании группы: {e}")

    while True:
        try:
            response = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=500,
                block=500
            )
            if not response:
                continue

            records = []
            to_ack = []

            # для публикации в iv4_inserted агрегируем по (symbol, interval, open_time, instance_id)
            event_counts = {}  # key -> param_count

            msg_total = 0
            msg_bad = 0

            for _, messages in response:
                for msg_id, data in messages:
                    msg_total += 1
                    to_ack.append(msg_id)

                    try:
                        symbol = data["symbol"]
                        interval = data["interval"]
                        instance_id = int(data["instance_id"])
                        open_time = datetime.fromisoformat(data["open_time"])

                        # precision в сообщении — общий (для angle применим 5)
                        precision = int(data.get("precision", 8))
                        values_json = data.get("values_json") or "{}"

                        # values_json: {param_name: value_str, ...}
                        values = json.loads(values_json)
                        if not isinstance(values, dict) or not values:
                            msg_bad += 1
                            continue

                    except Exception as e:
                        msg_bad += 1
                        log.error(f"Ошибка при разборе сообщения core stream: {e}")
                        continue

                    # разворачиваем параметры в строки для PG
                    added_params = 0
                    for param_name, str_value in values.items():
                        try:
                            # angle — всегда 5 знаков, остальное — precision тикера
                            p_prec = 5 if "angle" in str(param_name) else precision
                            quantize_str = "1." + "0" * int(p_prec)
                            value = Decimal(str(str_value)).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)

                            records.append((instance_id, symbol, open_time, str(param_name), value))
                            added_params += 1

                        except Exception as e:
                            log.error(f"Ошибка при обработке param из core stream: {e}")

                    # учёт для iv4_inserted
                    if added_params:
                        key = (symbol, interval, open_time, instance_id)
                        event_counts[key] = event_counts.get(key, 0) + added_params

            # запись в PG
            if records:
                async with pg.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany("""
                            INSERT INTO indicator_values_v4
                            (instance_id, symbol, open_time, param_name, value)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (instance_id, symbol, open_time, param_name)
                            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                        """, records)

                # суммирующий лог по батчу
                log.info(
                    "CORE_IO: batch stored (msgs=%s bad=%s records=%s keys=%s)",
                    msg_total,
                    msg_bad,
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
                await redis.xack(stream, group, *to_ack)

        except Exception as stream_err:
            log.error(f"Ошибка чтения из Redis Stream: {stream_err}", exc_info=True)
            await asyncio.sleep(1)