# core_io.py — воркер для записи индикаторов в PostgreSQL

import logging

# 🔸 Асинхронный воркер для чтения из Redis Stream и записи в PG
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")

    last_id = "$"  # читать только новые записи
    stream = "indicator_stream_core"

    while True:
        try:
            response = await redis.xread({stream: last_id}, block=5000, count=1)
            if not response:
                continue

            for _, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id
                    try:
                        symbol = data["symbol"]
                        interval = data["interval"]
                        instance_id = int(data["instance_id"])
                        open_time = data["open_time"]
                        param_name = data["param_name"]
                        value = float(data["value"])

                        async with pg.acquire() as conn:
                            await conn.execute("""
                                INSERT INTO indicator_values_v4
                                (instance_id, symbol, open_time, param_name, value)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (instance_id, symbol, open_time, param_name)
                                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                            """, instance_id, symbol, open_time, param_name, value)

                        log.info(f"PG ← {symbol}/{interval} {param_name}={value} @ {open_time}")

                    except Exception as e:
                        log.error(f"Ошибка при записи: {e}")

        except Exception as stream_err:
            log.error(f"Ошибка чтения из Redis Stream: {stream_err}")
            await asyncio.sleep(1)