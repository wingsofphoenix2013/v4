import asyncio
import logging
import json
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

CHANNEL = "tickers_v4_events"
state = {
    "tickers": set(),  # uppercased symbols
    "precision": {}     # symbol -> precision_price
}


async def preload_tickers(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, precision_price FROM tickers_v4 WHERE status = 'enabled'")
        for row in rows:
            symbol = row["symbol"].upper()
            state["tickers"].add(symbol)
            state["precision"][symbol] = row["precision_price"]
        logging.info(f"[AUDITOR] Загружено активных тикеров: {len(state['tickers'])}")


async def listen_ticker_events(redis):
    pubsub = redis.pubsub()
    await pubsub.subscribe(CHANNEL)
    logging.info(f"[AUDITOR] Подписан на канал: {CHANNEL}")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        try:
            data = json.loads(msg["data"])
            symbol = data.get("symbol", "").upper()
            action_type = data.get("type")
            action = data.get("action")

            if action_type == "status":
                if action == "enabled":
                    state["tickers"].add(symbol)
                    logging.info(f"[AUDITOR] Активирован тикер: {symbol}")
                elif action == "disabled":
                    state["tickers"].discard(symbol)
                    logging.info(f"[AUDITOR] Отключён тикер: {symbol}")
        except Exception as e:
            logging.warning(f"[AUDITOR] Ошибка обработки события: {e}")


def r(val, precision):
    return Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)


async def restore_missing(interval, offset_minutes, pg, redis):
    await asyncio.sleep(300)
    log = logging.getLogger("AUDITOR")
    interval_sec = {"m1": 60, "m5": 300, "m15": 900}[interval]

    while True:
        log.info(f"[AUDITOR] ⏳ Старт проверки дыр {interval.upper()}")

        now = datetime.utcnow().replace(second=0, microsecond=0)
        start_time = now - timedelta(hours=24)
        audit_end = now - timedelta(minutes=offset_minutes)

        for symbol in state["tickers"]:
            precision = state["precision"].get(symbol)
            if precision is None:
                continue

            async with pg.acquire() as conn:
                rows = await conn.fetch(f"""
                    WITH gaps AS (
                        SELECT
                            LAG(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_time,
                            open_time AS next_time
                        FROM ohlcv4_{interval}
                        WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
                    )
                    SELECT generate_series(
                        prev_time + interval '1 minute',
                        next_time - interval '1 minute',
                        interval '1 minute'
                    ) AS missing_time
                    FROM gaps
                    WHERE next_time - prev_time > interval '1 minute'
                """, symbol, start_time, audit_end)

            for row in rows:
                t = row["missing_time"]
                if (t.minute % (interval_sec // 60)) != 0:
                    continue  # пропускаем если не выровнено

                ts = int(t.timestamp() * 1000)
                redis_key = f"ohlcv:{symbol.lower()}:{interval}:{ts}"

                try:
                    raw = await redis.execute_command("JSON.GET", redis_key, "$")
                    if not raw:
                        continue
                    candle = json.loads(raw)[0]

                    async with pg.acquire() as conn:
                        await conn.execute(f"""
                            INSERT INTO ohlcv4_{interval} (symbol, open_time, open, high, low, close, volume, source, inserted_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, 'auditor', now())
                            ON CONFLICT DO NOTHING
                        """,
                            symbol,
                            t,
                            r(candle["o"], precision),
                            r(candle["h"], precision),
                            r(candle["l"], precision),
                            r(candle["c"], precision),
                            r(candle["v"], 2),
                        )

                        log.info(f"[AUDITOR] ✅ Восстановлено {interval.upper()}: {symbol} @ {t}")
                except Exception as e:
                    log.warning(f"[AUDITOR] ⚠️ Ошибка Redis/PG для {symbol} @ {t}: {e}")

        log.info(f"[AUDITOR] 💤 Завершено {interval.upper()}. Спим 5 минут.")
        await asyncio.sleep(300)


async def run_auditor(pg, redis):
    await preload_tickers(pg)
    asyncio.create_task(restore_missing("m1", 5, pg, redis))
    asyncio.create_task(restore_missing("m5", 6, pg, redis))
    asyncio.create_task(restore_missing("m15", 16, pg, redis))
    await listen_ticker_events(redis)