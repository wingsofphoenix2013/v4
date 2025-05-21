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


async def restore_missing_m1(pg, redis):
    await asyncio.sleep(300)  # ⏱ подождать 5 минут после запуска
    log = logging.getLogger("AUDITOR")

    while True:
        log.info("[AUDITOR] ⏳ Старт проверки дыр M1")

        now = datetime.utcnow().replace(second=0, microsecond=0)
        start_time = now - timedelta(hours=24)
        audit_end = now - timedelta(minutes=5)  # ⛔️ не проверяем последние 5 минут

        for symbol in state["tickers"]:
            precision = state["precision"].get(symbol)
            if precision is None:
                continue

            async with pg.acquire() as conn:
                rows = await conn.fetch("""
                    WITH gaps AS (
                        SELECT
                            LAG(open_time) OVER (PARTITION BY symbol ORDER BY open_time) AS prev_time,
                            open_time AS next_time
                        FROM ohlcv4_m1
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
                ts = int(t.timestamp() * 1000)
                redis_key = f"ohlcv:{symbol.lower()}:m1:{ts}"

                try:
                    raw = await redis.execute_command("JSON.GET", redis_key, "$")
                    if not raw:
                        continue
                    candle = json.loads(raw)[0]

                    async with pg.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO ohlcv4_m1 (symbol, open_time, open, high, low, close, volume, source, inserted_at)
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

                        log.info(f"[AUDITOR] ✅ Восстановлено: {symbol} @ {t}")
                except Exception as e:
                    log.warning(f"[AUDITOR] ⚠️ Ошибка Redis/PG для {symbol} @ {t}: {e}")

        log.info("[AUDITOR] 💤 Завершено. Спим 5 минут.")
        await asyncio.sleep(300)


async def run_auditor(pg, redis):
    await preload_tickers(pg)
    asyncio.create_task(restore_missing_m1(pg, redis))
    await listen_ticker_events(redis)