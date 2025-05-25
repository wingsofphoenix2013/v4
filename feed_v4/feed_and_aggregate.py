# feed_and_aggregate.py — приём и логирование M1 и M5 свечей с реактивным управлением
import asyncio
import logging
import time
import json
from asyncpg import Pool
from redis.asyncio import Redis
from datetime import datetime
from websockets import connect
from itertools import islice

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
        pass

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

# 🔸 Разбиение тикеров на группы
def chunked(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk

# 🔸 Подключение к WebSocket Binance по группе тикеров
async def listen_kline_stream(group_key, symbols, queue, interval="1m"):
    stream_names = [f"{s.lower()}@kline_{interval}" for s in symbols]
    stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    try:
        async with connect(stream_url) as ws:
            log.info(f"[KLINE:{group_key}] Подключено к WebSocket: {stream_url}")
            async for msg in ws:
                data = json.loads(msg)
                kline = data.get("data", {}).get("k")
                if not kline or not kline.get("x"):
                    continue
                await queue.put(kline)
    except Exception as e:
        log.error(f"[KLINE:{group_key}] Ошибка WebSocket: {e}", exc_info=True)

# 🔸 Воркер для логирования свечей
async def kline_worker(queue, interval="M1"):
    while True:
        kline = await queue.get()
        try:
            symbol = kline["s"]
            open_time = datetime.utcfromtimestamp(kline["t"] / 1000)
            received_time = datetime.utcnow()
            log.info(f"[{interval}] {symbol} @ {open_time.isoformat()} получена в {received_time.isoformat()}Z")
        except Exception as e:
            log.warning(f"Ошибка при логировании kline: {e}", exc_info=True)

# 🔸 M1: Реактивный запуск и управление WebSocket-группами
async def run_feed_and_aggregator(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.info("🔸 Запуск приёма M1 свечей с реактивным управлением")
    queue = asyncio.Queue()
    state["kline_tasks"] = {}

    for _ in range(5):
        asyncio.create_task(kline_worker(queue, interval="M1"))

    await refresh_queue.put("initial")

    while True:
        await refresh_queue.get()
        log.info("🔁 Пересборка групп WebSocket после изменения активных тикеров")

        active_symbols = sorted(state["active"])
        log.info(f"[M1] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")
        new_groups = {
            ",".join(group): group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["kline_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="1m"))
            state["kline_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["kline_tasks"].pop(group_key)
            task.cancel()
            log.info(f"[KLINE:{group_key}] Поток остановлен — тикеры неактивны")

# 🔸 M5: Реактивный запуск и логирование M5 свечей
async def run_feed_and_aggregator_m5(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.info("🔸 Запуск приёма M5 свечей")
    queue = asyncio.Queue()
    state["m5_tasks"] = {}

    for _ in range(2):
        asyncio.create_task(kline_worker(queue, interval="M5"))

    await refresh_queue.put("initial-m5")

    while True:
        await refresh_queue.get()
        log.info("🔁 [M5] Пересборка групп WebSocket")

        active_symbols = sorted(state["active"])
        log.info(f"[M5] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")
        new_groups = {
            ",".join(group): group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["m5_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="5m"))
            state["m5_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["m5_tasks"].pop(group_key)
            task.cancel()
            log.info(f"[KLINE:M5:{group_key}] Поток остановлен — тикеры неактивны")