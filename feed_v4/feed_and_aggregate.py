# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging

# 🔸 Загрузка тикеров с точностью округления из PostgreSQL
async def load_active_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price
            FROM tickers_v4
            WHERE status = 'enabled'
        """)
        return {row['symbol']: row['precision_price'] for row in rows}

# 🔸 Основной запуск компонента
async def run_feed_and_aggregator(pg, redis):
    log = logging.getLogger("FEED")

    # Загрузка тикеров
    tickers = await load_active_tickers(pg)
    log.info(f"Загружено тикеров: {len(tickers)} → {list(tickers.keys())}")

    # Заглушка — цикл ожидания
    while True:
        await asyncio.sleep(5)