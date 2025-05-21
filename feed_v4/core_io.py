import asyncio
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

from infra import info_log

STREAM_NAME = "ohlcv_stream"
GROUP_NAME = "core_writer"
CONSUMER_NAME = "writer_1"
TICKER_CHANNEL = "tickers_v4_events"

state = {"tickers": {}}  # symbol.upper() -> precision_price


def r(val, precision):
    return Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)


async def preload_tickers(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, precision_price FROM tickers_v4 WHERE status = 'enabled'")
        for row in rows:
            state["tickers"][row["symbol"].upper()] = row["precision_price"]
        info_log("CORE_IO", f"📥 Загружено тикеров при старте: {len(state['tickers'])}")


async def listen_ticker_pubsub(redis, pg):
    pubsub = redis.pubsub()
    await pubsub.subscribe(TICKER_CHANNEL)
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data.get("symbol")
            if data.get("type") == "tradepermission" and data.get("action") == "enabled" and symbol:
                async with pg.acquire() as conn:
                    row = await conn.fetchrow("SELECT precision_price FROM tickers_v4 WHERE symbol = $1", symbol)
                    if row:
                        state["tickers"][symbol.upper()] = row["precision_price"]
                        info_log("CORE_IO", f"[TICKER] {symbol} enabled → precision={row['precision_price']}")
        except Exception as e:
            info_log("CORE_IO", f"[TICKER] Ошибка PubSub: {e}")


async def insert_candle(pg, symbol, interval, ts, candle):
    open_time = datetime.utcfromtimestamp(ts / 1000)
    table = f"ohlcv4_{interval}"
    precision = state["tickers"].get(symbol.upper())
    if precision is None:
        return

    try:
        async with pg.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source, inserted_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                ON CONFLICT DO NOTHING
            """,
                symbol,
                open_time,
                r(candle["o"], precision),
                r(candle["h"], precision),
                r(candle["l"], precision),
                r(candle["c"], precision),
                r(candle["v"], 2),
                candle.get("fixed") and "api" or "stream",
            )

            await conn.execute(f"""
                DELETE FROM {table} WHERE symbol = $1 AND open_time < NOW() - INTERVAL '14 days'
            """, symbol)

    except Exception as e:
        info_log("CORE_IO", f"❌ Ошибка insert_candle: {e}")


async def m5_integrity_loop(pg, redis):
    while True:
        now = datetime.utcnow().replace(second=0, microsecond=0)
        end_time = now - timedelta(minutes=5)
        start_time = now - timedelta(minutes=15)

        async with pg.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT symbol FROM ohlcv4_m1")
        symbols = [r["symbol"] for r in rows]

        for symbol in symbols:
            precision = state["tickers"].get(symbol.upper())
            if precision is None:
                continue

            t = start_time
            while t <= end_time:
                if t.minute % 5 != 0:
                    t += timedelta(minutes=1)
                    continue

                async with pg.acquire() as conn:
                    exists = await conn.fetchval("SELECT 1 FROM ohlcv4_m5 WHERE symbol = $1 AND open_time = $2", symbol, t)
                    if exists:
                        t += timedelta(minutes=5)
                        continue

                    m1_rows = await conn.fetch("""
                        SELECT open, high, low, close, volume FROM ohlcv4_m1
                        WHERE symbol = $1 AND open_time >= $2 AND open_time < $3
                        ORDER BY open_time ASC
                    """, symbol, t, t + timedelta(minutes=5))

                    if len(m1_rows) < 5:
                        t += timedelta(minutes=5)
                        continue

                    o = r(m1_rows[0]["open"], precision)
                    h = r(max(rw["high"] for rw in m1_rows), precision)
                    l = r(min(rw["low"] for rw in m1_rows), precision)
                    c = r(m1_rows[-1]["close"], precision)
                    v = r(sum(rw["volume"] for rw in m1_rows), 2)
                    m5_ts = int(t.timestamp() * 1000)
                    redis_key = f"ohlcv:{symbol.lower()}:m5:{m5_ts}"
                    candle = {"o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v), "ts": m5_ts}
                    await conn.execute("""
                        INSERT INTO ohlcv4_m5 (symbol, open_time, open, high, low, close, volume, source, inserted_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 'recovery', NOW())
                        ON CONFLICT DO NOTHING
                    """, symbol, t, o, h, l, c, v)
                    await redis.execute_command("JSON.SET", redis_key, "$", json.dumps(candle))
                    info_log("CORE_IO", f"🔁 Восстановлена M5: {symbol} @ {t}")

                t += timedelta(minutes=5)

        await asyncio.sleep(300)


async def run_core_writer(pg, redis):
    info_log("CORE_IO", "▶ Старт обработчика core_writer")

    try:
        await redis.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
    except Exception:
        pass  # Группа уже существует

    await preload_tickers(pg)
    asyncio.create_task(listen_ticker_pubsub(redis, pg))
    asyncio.create_task(m5_integrity_loop(pg, redis))

    while True:
        try:
            messages = await redis.xreadgroup(GROUP_NAME, CONSUMER_NAME, streams={STREAM_NAME: ">"}, count=50, block=1000)
            for _, entries in messages:
                for msg_id, data in entries:
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")
                        timestamp = int(data.get("timestamp"))
                        if not all([symbol, interval, timestamp]):
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        key = f"ohlcv:{symbol.lower()}:{interval}:{timestamp}"
                        raw = await redis.execute_command("JSON.GET", key, "$")
                        if not raw:
                            await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)
                            continue

                        candle = json.loads(raw)[0]
                        await insert_candle(pg, symbol, interval, timestamp, candle)
                        await redis.xack(STREAM_NAME, GROUP_NAME, msg_id)

                    except Exception as e:
                        info_log("CORE_IO", f"❌ Ошибка обработки сообщения: {e}")
        except Exception as e:
            info_log("CORE_IO", f"❌ Ошибка чтения из потока: {e}")
            await asyncio.sleep(3)