# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging
import asyncio

# 🔸 Загрузка всех тикеров, точности и статуса из PostgreSQL
async def load_all_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price, status FROM tickers_v4
        """)
        tickers = {}
        active = set()
        for row in rows:
            tickers[row['symbol']] = row['precision_price']
            if row['status'] == 'enabled':
                active.add(row['symbol'].lower())
        return tickers, active

# 🔸 Обработка событий включения/отключения тикеров через Redis Stream
async def handle_ticker_events(redis, state, pg):
    group = "aggregator_group"
    stream = "tickers_status_stream"
    logger = logging.getLogger("TICKER_STREAM")

    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        resp = await redis.xreadgroup(group, "aggregator", streams={stream: ">"}, count=50, block=1000)
        for _, messages in resp:
            for msg_id, data in messages:
                symbol = data.get("symbol")
                action = data.get("action")
                if not symbol or not action:
                    continue

                # Пополняем state["tickers"] при необходимости
                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4 WHERE symbol = $1
                        """, symbol)
                        if row:
                            state["tickers"][symbol] = row["precision_price"]
                            logger.info(f"Добавлен тикер из БД: {symbol}")

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

    # Загрузка всех тикеров (enabled + disabled)
    tickers, active = await load_all_tickers(pg)
    log.info(f"Загружено тикеров: {len(tickers)} → {list(tickers.keys())}")

    for s in tickers:
        if s.lower() in active:
            log.info(f"Активен по умолчанию: {s}")
        else:
            log.info(f"Выключен по умолчанию: {s}")

    # Общее состояние
    state = {
        "tickers": tickers,   # symbol -> precision_price
        "active": active      # set of lowercase symbols
    }

    # Запуск подписки на Redis Stream
    asyncio.create_task(handle_ticker_events(redis, state, pg))

    # Заглушка — цикл ожидания
    while True:
        await asyncio.sleep(5)
