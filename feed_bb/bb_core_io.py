# bb_core_io.py — чтение bb:ohlcv_stream и запись в ohlcv_bb_* (PG), генерация событий для аудитора

# 🔸 Импорты и зависимости
import asyncio
import logging
from decimal import Decimal

log = logging.getLogger("BB_CORE_IO")

# 🔸 Сопоставление интервалов с таблицами
TABLE_MAP = {
    "m5": "ohlcv_bb_m5",
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}

# 🔸 Основной воркер
async def run_core_io_bb(pg_pool, redis):
    stream_key = "bb:ohlcv_stream"
    last_id = "$"  # только новые сообщения
    semaphore = asyncio.Semaphore(20)  # ограничим конкуренцию

    log.debug("BB_CORE_IO запущен: слушаю bb:ohlcv_stream и пишу в PG")

    async def process_message(data):
        async with semaphore:
            interval = data.get("interval")
            table = TABLE_MAP.get(interval)
            if not table:
                return

            symbol = data["symbol"]

            try:
                ts_int = int(data["timestamp"]) // 1000
                open_time = ts_int
            except Exception as e:
                log.warning(f"Ошибка преобразования timestamp: {data.get('timestamp')} → {e}")
                return

            try:
                o = Decimal(data["o"])
                h = Decimal(data["h"])
                l = Decimal(data["l"])
                c = Decimal(data["c"])
                v = Decimal(data["v"])

                async with pg_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            f"""
                            INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source)
                            VALUES (%s, to_timestamp(%s), %s, %s, %s, %s, %s, 'stream')
                            ON CONFLICT (symbol, open_time) DO NOTHING
                            """,
                            (symbol, open_time, o, h, l, c, v)
                        )
                        status = cur.statusmessage

                log.debug(f"Вставка {symbol} {interval} @ {open_time}: {status}")

                # если реально вставили — уведомляем аудитора
                if status == "INSERT 0 1":
                    try:
                        await redis.xadd("bb:pg_candle_inserted", {
                            "symbol": symbol,
                            "interval": interval,
                            "timestamp": data["timestamp"]
                        })
                    except Exception as e:
                        log.warning(f"Не удалось xadd в bb:pg_candle_inserted: {e}")

            except Exception as e:
                log.exception(f"Ошибка вставки в PG для {symbol}: {e}")

    while True:
        try:
            response = await redis.xread({stream_key: last_id}, count=10, block=5000)
            if not response:
                continue

            for _stream, messages in response:
                last_id = messages[-1][0]
                tasks = [process_message(data) for _, data in messages]
                await asyncio.gather(*tasks)

        except Exception as e:
            log.error(f"BB_CORE_IO ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)