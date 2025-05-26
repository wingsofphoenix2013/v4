# indicators_v4_main.py (временная отладочная версия с точной загрузкой precision)

import asyncio
import logging
import json
from infra import init_pg_pool, init_redis_client, setup_logging
from indicators.compute_and_store import compute_and_store

active_tickers = {}
indicator_instances = {}

# 🔸 Загрузка тикеров из PostgreSQL при старте
async def load_initial_tickers(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        for row in rows:
            active_tickers[row["symbol"]] = int(row["precision_price"])
            log.info(f"[DEBUG] Loaded ticker: {row['symbol']} → precision={row['precision_price']}")

# 🔸 Загрузка расчётов индикаторов и параметров
async def load_initial_indicators(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        instances = await conn.fetch("""
            SELECT id, indicator, timeframe, stream_publish
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        for inst in instances:
            params = await conn.fetch("""
                SELECT param, value FROM indicator_parameters_v4
                WHERE instance_id = $1
            """, inst["id"])
            param_map = {p["param"]: p["value"] for p in params}
            indicator_instances[inst["id"]] = {
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "stream_publish": inst["stream_publish"],
                "params": param_map
            }
            log.info(f"[DEBUG] Loaded instance id={inst['id']} → {inst['indicator']} {param_map}")

# 🔸 Подписка на обновления тикеров
async def watch_ticker_updates(redis):
    log = logging.getLogger("TICKER_UPDATES")
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data["symbol"]
            field = data["type"]
            action = data["action"]

            if field in ("status", "tradepermission"):
                if action == "enabled":
                    # 🔸 При включении тикера — загрузка precision из PG
                    async with redis.connection_pool.get_connection() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4
                            WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
                        """, symbol)
                        if row:
                            active_tickers[symbol] = int(row["precision_price"])
                            log.info(f"✅ Тикер включён: {symbol} → precision = {row['precision_price']}")
                else:
                    active_tickers.pop(symbol, None)
                    log.info(f"⛔️ Тикер отключён: {symbol}")
        except Exception as e:
            log.warning(f"Ошибка в ticker event: {e}")

# 🔸 Подписка на обновления расчётов индикаторов
async def watch_indicator_updates(pg, redis):
    log = logging.getLogger("INDICATOR_UPDATES")
    pubsub = redis.pubsub()
    await pubsub.subscribe("indicators_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            iid = int(data["id"])
            field = data["type"]
            action = data["action"]

            if field == "enabled":
                if action == "true":
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT id, indicator, timeframe, stream_publish
                            FROM indicator_instances_v4 WHERE id = $1
                        """, iid)
                        if row:
                            params = await conn.fetch("""
                                SELECT param, value FROM indicator_parameters_v4
                                WHERE instance_id = $1
                            """, iid)
                            param_map = {p["param"]: p["value"] for p in params}
                            indicator_instances[iid] = {
                                "indicator": row["indicator"],
                                "timeframe": row["timeframe"],
                                "stream_publish": row["stream_publish"],
                                "params": param_map
                            }
                            log.info(f"✅ Индикатор включён: id={iid} {row['indicator']} {param_map}")
                else:
                    indicator_instances.pop(iid, None)
                    log.info(f"⛔️ Индикатор отключён: id={iid}")

            elif field == "stream_publish" and iid in indicator_instances:
                indicator_instances[iid]["stream_publish"] = (action == "true")
                log.info(f"🔁 stream_publish обновлён: id={iid} → {action}")
        except Exception as e:
            log.warning(f"Ошибка в indicator event: {e}")
            
async def main():
    setup_logging()
    pg = await init_pg_pool()
    redis = await init_redis_client()

    await load_initial_tickers(pg)
    await load_initial_indicators(pg)

    await asyncio.gather(
        watch_ticker_updates(redis),
        watch_indicator_updates(pg, redis)
    )

if __name__ == "__main__":
    asyncio.run(main())