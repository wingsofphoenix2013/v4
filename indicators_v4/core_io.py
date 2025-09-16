# core_io.py — воркер для записи индикаторов в PostgreSQL

import logging
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

# 🔸 Асинхронный воркер для чтения из Redis Stream и записи в PG
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")

    stream = "indicator_stream_core"
    group = "group_core_io"
    consumer = "core_io_1"

    # Попытка создать группу (если уже существует — игнорировать)
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
                count=50,
                block=1000
            )
            if not response:
                continue

            records = []
            to_ack = []

            # для публикации в iv4_inserted агрегируем по (symbol, interval, open_time, instance_id)
            event_counts = {}  # key -> count

            for _, messages in response:
                for msg_id, data in messages:
                    try:
                        symbol = data["symbol"]
                        interval = data["interval"]  # передаётся из compute_and_store
                        instance_id = int(data["instance_id"])
                        open_time = datetime.fromisoformat(data["open_time"])
                        param_name = data["param_name"]
                        precision = int(data.get("precision", 8))
                        quantize_str = "1." + "0" * precision
                        value = Decimal(data["value"]).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)

                        records.append((instance_id, symbol, open_time, param_name, value))
                        to_ack.append(msg_id)

                        key = (symbol, interval, open_time, instance_id)
                        event_counts[key] = event_counts.get(key, 0) + 1

                    except Exception as e:
                        log.error(f"Ошибка при обработке записи из Stream: {e}")

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

                log.debug(f"PG ← записано {len(records)} параметров")

                # 🔸 публикация факта вставки (триггер для аудитора индикаторов)
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

            # Подтверждение обработки сообщений
            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as stream_err:
            log.error(f"Ошибка чтения из Redis Stream: {stream_err}")
            await asyncio.sleep(1)