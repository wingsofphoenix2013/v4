# feed_and_aggregate.py — основной агрегатор свечей M1 + агрегация M5/M15 + контроль тикеров
import asyncio
import logging
import time
from asyncpg import Pool
from redis.asyncio import Redis
from datetime import datetime

log = logging.getLogger("FEED+AGGREGATOR")

# 🔸 Загрузка всех тикеров из БД
async def load_all_tickers(pg: Pool):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, precision_price, precision_qty, status FROM tickers_v4")

    now = time.time()
    tickers = {}
    active = set()
    activated_at = {}

    for row in rows:
        symbol = row["symbol"]
        tickers[symbol] = {
            "precision_price": row["precision_price"],
            "precision_qty": row["precision_qty"]
        }

        if row["status"] == "enabled":
            active.add(symbol)
            activated_at[symbol] = now
            log.info(f"[{symbol}] Активен по умолчанию — activated_at={datetime.utcfromtimestamp(now).isoformat()}Z")
        else:
            log.info(f"[{symbol}] Неактивен по умолчанию")

    return tickers, active, activated_at

# 🔸 Отслеживание включения/выключения тикеров через Redis Stream
async def handle_ticker_events(redis: Redis, state: dict, pg: Pool, refresh_queue: asyncio.Queue):
    group = "aggregator_group"
    stream = "tickers_status_stream"

    try:
        await redis.xgroup_create(stream, group, id='0', mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        resp = await redis.xreadgroup(group, "aggregator", streams={stream: '>'}, count=50, block=1000)
        for _, messages in resp:
            for msg_id, data in messages:
                symbol = data.get("symbol")
                action = data.get("action")

                if not symbol or not action:
                    continue

                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT precision_price, precision_qty FROM tickers_v4 WHERE symbol = $1",
                            symbol
                        )
                        if row:
                            state["tickers"][symbol] = {
                                "precision_price": row["precision_price"],
                                "precision_qty": row["precision_qty"]
                            }
                            log.info(f"[{symbol}] Добавлен тикер из БД")

                if action == "enabled" and symbol in state["tickers"]:
                    state["active"].add(symbol)
                    ts = time.time()
                    state["activated_at"][symbol] = ts
                    log.info(f"[{symbol}] Активирован тикер — activated_at={datetime.utcfromtimestamp(ts).isoformat()}Z")
                    await refresh_queue.put("refresh")

                elif action == "disabled" and symbol in state["active"]:
                    state["active"].remove(symbol)
                    log.info(f"[{symbol}] Деактивирован тикер")
                    await refresh_queue.put("refresh")

# 🔸 Заглушка для запуска модуля в системе
async def run_feed_and_aggregator(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.info("🔸 Заглушка: run_feed_and_aggregator запущен")
    while True:
        await asyncio.sleep(60)
