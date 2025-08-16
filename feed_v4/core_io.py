# 🔸 Импорты и зависимости
import asyncio
from decimal import Decimal
from datetime import datetime, timezone
import logging
import json

# 🔸 Сопоставление интервалов с таблицами
TABLE_MAP = {
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}
# 🔸 Подписка на Redis PubSub: обновление activated_at при активации тикера
async def listen_ticker_activations(pg, redis):
    log = logging.getLogger("CORE_IO")

    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")
    log.info("Подписка на Redis PubSub: tickers_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        try:
            data = json.loads(msg["data"])
            if data.get("type") == "status" and data.get("action") == "enabled":
                symbol = data.get("symbol")
                if symbol:
                    async with pg.acquire() as conn:
                        await conn.execute("""
                            UPDATE tickers_v4
                            SET activated_at = NOW()
                            WHERE symbol = $1
                        """, symbol)
                        log.info(f"[{symbol}] activated_at обновлён → NOW()")

        except Exception as e:
            log.warning(f"Ошибка в обработке tickers_v4_events: {e}", exc_info=True)
            
# 🔸 Основной воркер: чтение из Redis и запись в PostgreSQL
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")
    stream_key = "ohlcv_stream"
    last_id = "$"

    semaphore = asyncio.Semaphore(20)

    async def process_message(data):
        async with semaphore:
            interval = data["interval"]
            table = TABLE_MAP.get(interval)
            if not table:
                return

            symbol = data["symbol"]

            try:
                ts_int = int(data["timestamp"]) // 1000
                open_time = datetime.utcfromtimestamp(ts_int)
            except Exception as e:
                log.warning(f"Ошибка преобразования timestamp: {data.get('timestamp')} → {e}")
                return

            try:
                o = Decimal(data["o"])
                h = Decimal(data["h"])
                l = Decimal(data["l"])
                c = Decimal(data["c"])
                v = Decimal(data["v"])

                async with pg.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute(f"""
                            INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, 'stream')
                            ON CONFLICT (symbol, open_time) DO NOTHING
                        """, symbol, open_time, o, h, l, c, v)

                        log.debug(
                            f"Вставлена запись в {table}: {symbol} @ {open_time.isoformat()} "
                            f"[{interval.upper()}] вставлено={