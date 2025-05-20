# core_io.py — запись OHLCV-свечей из Redis Stream в PostgreSQL с ограничением в 14 дней

import asyncio
import json
import logging
from datetime import datetime

STREAM_NAME = "ohlcv_stream"
GROUP_NAME = "core_writer"
CONSUMER_NAME = "writer_1"

# Асинхронный запуск компонента
async def run_core_writer(pg, redis):
    log = logging.getLogger("CORE_IO")

    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        try:
            messages = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=50,
                block=1000,
            )

            for stream_name, entries in messages:
                for msg_id, data in entries:
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")
                        timestamp = int(data.get("timestamp"))

                        if not all([symbol, interval, timestamp]):
                            log.warning(f"Некорректное сообщение: {data}")
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        key = f"ohlcv:{symbol.lower()}:{interval}:{timestamp}"
                        raw = await redis.execute_command("JSON.GET", key, "$")

                        if not raw:
                            log.warning(f"Пустой ключ в Redis: {key}")
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        candle = json.loads(raw)[0]
                        await insert_candle(pg, symbol, interval, timestamp, candle)

                        log.info(f"[{symbol}] {interval.upper()} записана в PG: {timestamp}")
                        await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

                    except Exception as e:
                        log.exception(f"Ошибка обработки записи: {e}")
                        # не xack — пусть повторится позже

        except Exception as e:
            log.error(f"Ошибка чтения из потока: {e}", exc_info=True)
            await asyncio.sleep(3)

# Вставка свечи в нужную таблицу + удаление старых (> 14 дней)
async def insert_candle(pg, symbol, interval, ts, candle):
    open_time = datetime.utcfromtimestamp(ts / 1000)
    table = f"ohlcv4_{interval}"

    async with pg.acquire() as conn:
        # Вставка новой свечи
        await conn.execute(f"""
            INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source, inserted_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT DO NOTHING
        """, symbol, open_time, candle["o"], candle["h"], candle["l"], candle["c"],
             candle["v"], candle.get("fixed") and "api" or "stream")

        # Удаление устаревших записей
        await conn.execute(f"""
            DELETE FROM {table}
            WHERE symbol = $1 AND open_time < NOW() - INTERVAL '14 days'
        """, symbol)