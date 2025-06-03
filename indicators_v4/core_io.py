# core_io.py — воркер для записи индикаторов в PostgreSQL

import logging
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

# 🔸 Асинхронный воркер для чтения из Redis Stream и записи в PG
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")

    last_id = "$"  # читать только новые записи
    stream = "indicator_stream_core"

    while True:
        try:
            response = await redis.xread({stream: last_id}, count=50, block=1000)
            if not response:
                continue

            records = []

            for _, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id
                    try:
                        symbol = data["symbol"]
                        interval = data["interval"]
                        instance_id = int(data["instance_id"])
                        open_time = datetime.fromisoformat(data["open_time"])
                        param_name = data["param_name"]
                        precision = int(data.get("precision", 8))
                        quantize_str = "1." + "0" * precision
                        value = Decimal(data["value"]).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)

                        records.append((instance_id, symbol, open_time, param_name, value))
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

        except Exception as stream_err:
            log.error(f"Ошибка чтения из Redis Stream: {stream_err}")
            await asyncio.sleep(1)