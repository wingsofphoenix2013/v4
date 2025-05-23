# 🔸 Импорты и зависимости
import asyncio
from decimal import Decimal
from datetime import datetime, timezone
import logging

# 🔸 Сопоставление интервалов с таблицами
TABLE_MAP = {
    "m1": "ohlcv4_m1",
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
}

# 🔸 Основной воркер: чтение из Redis и запись в PostgreSQL
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")
    stream_key = "ohlcv_stream"
    last_id = "0"  # можно заменить на "$" для чтения только новых

    while True:
        try:
            # 🔸 Чтение одного сообщения из Redis Stream
            response = await redis.xread({stream_key: last_id}, count=10, block=5000)

            if not response:
                continue  # таймаут

            for stream, messages in response:
                for msg_id, data in messages:
                    last_id = msg_id

                    # 🔸 Распаковка и преобразование данных
                    interval = data["interval"]
                    table = TABLE_MAP.get(interval)
                    if not table:
                        continue

                    symbol = data["symbol"]

                    try:
                        ts_int = int(data["timestamp"]) // 1000
                        open_time = datetime.utcfromtimestamp(ts_int).replace(tzinfo=timezone.utc)
                    except Exception as e:
                        log.warning(f"Ошибка преобразования timestamp: {data.get('timestamp')} → {e}")
                        continue

                    o = Decimal(data["o"])
                    h = Decimal(data["h"])
                    l = Decimal(data["l"])
                    c = Decimal(data["c"])
                    v = Decimal(data["v"])

                    # 🔸 Вставка записи в PostgreSQL и удаление старых
                    async with pg.acquire() as conn:
                        async with conn.transaction():
                            inserted = await conn.execute(f"""
                                INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, 'stream')
                                ON CONFLICT (symbol, open_time) DO NOTHING
                            """, symbol, open_time, o, h, l, c, v)

                            log.info(f"Вставлена запись в {table}: {symbol} @ {open_time.isoformat()} [{interval.upper()}]")

                            deleted = await conn.execute(f"""
                                DELETE FROM {table}
                                WHERE open_time < (NOW() - INTERVAL '30 days')
                            """)

                            log.info(f"Удалено старых записей из {table}: {deleted}")
        except Exception as e:
            # 🔸 Обработка исключений и логирование
            log.error(f"Ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)