# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging
import asyncio

# 🔸 Загрузка тикеров с точностью округления из PostgreSQL
async def load_active_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price
            FROM tickers_v4
            WHERE status = 'enabled'
        """)
        return {row['symbol']: row['precision_price'] for row in rows}

# 🔸 Обработка событий включения/отключения тикеров через Redis Stream
async def handle_ticker_events(redis, state):
    group = "aggregator_group"
    stream = "tickers_v4_events"
    logger = logging.getLogger("TICKER_STREAM")

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        resp = await redis.xreadgroup(group, "aggregator", streams={stream: ">"}, count=10, block=5000)
        for _, messages in resp:
            for msg_id, data in messages:
                symbol = data.get("symbol")
                action = data.get("action")
                if not symbol or not action:
                    continue

                if action == "enabled" and symbol in state["tickers"]:
                    logger.info(f"Активирован тикер: {symbol}")
                    state["active"].add(symbol.lower())

                elif action == "disabled" and symbol in state["tickers"]:
                    logger.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())

                await redis.xack(stream, group, msg_id)

# 🔸 Основной запуск компонента
async def run_feed_and_aggregator(pg, redis):
    log = logging.getLogger("FEED+AGGREGATOR")

    # Загрузка тикеров и точности округления
    tickers = await load_active_tickers(pg)
    active = set([s.lower() for s in tickers])
    log.info(f"Загружено тикеров: {len(tickers)} → {list(tickers.keys())}")

    for s in tickers:
        log.info(f"Активен по умолчанию: {s}")

    # Общее состояние
    state = {
        "tickers": tickers,   # symbol -> precision_price
        "active": active      # set of lowercase symbols
    }

    # Запуск подписки на Redis Stream
    asyncio.create_task(handle_ticker_events(redis, state))

    # Заглушка — цикл ожидания
    while True:
        await asyncio.sleep(5)