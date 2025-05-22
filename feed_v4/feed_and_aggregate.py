# feed_and_aggregate.py — приём и агрегация рыночных данных - 22/05/2025

import logging
import asyncio
import websockets
import json
import aiohttp
from infra import setup_logging
from decimal import Decimal, ROUND_DOWN
import time
from datetime import datetime, timedelta

# Получаем логгер для модуля
log = logging.getLogger("FEED+AGGREGATOR")

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
async def handle_ticker_events(redis, state, pg, refresh_queue):
    group = "aggregator_group"
    stream = "tickers_status_stream"

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

                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4 WHERE symbol = $1
                        """, symbol)
                        if row:
                            state["tickers"][symbol] = row["precision_price"]
                            log.info(f"Добавлен тикер из БД: {symbol}")

                if action == "enabled" and symbol in state["tickers"]:
                    log.info(f"Активирован тикер: {symbol}")
                    state["active"].add(symbol.lower())
                    await refresh_queue.put("refresh")

                    # запуск потока markPrice и отслеживание задачи
                    precision = state["tickers"][symbol]
                    task = asyncio.create_task(watch_mark_price(symbol, redis, precision))
                    state["markprice_tasks"][symbol] = task

                elif action == "disabled" and symbol in state["tickers"]:
                    log.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())
                    await refresh_queue.put("refresh")

                    # отмена потока markPrice, если он есть
                    task = state["markprice_tasks"].pop(symbol, None)
                    if task:
                        task.cancel()

                await redis.xack(stream, group, msg_id)

# 🔸 Сохранение полной свечи M1 в RedisTS (временно отключено, используется заглушка)
async def store_and_publish_m1(redis, symbol, open_time, kline, precision):
    log.info("Я тут: store_and_publish_m1")
# 🔸 Агрегация M5 на основе RedisJSON M1-свечей (временно отключено, используется заглушка)
async def try_aggregate_m5(redis, symbol, open_time):
    log.info("Я тут: try_aggregate_m5")
# 🔸 Агрегация M15 на основе RedisJSON M1-свечей (временно отключено, используется заглушка)
async def try_aggregate_m15(redis, symbol, open_time):
    log.info("Я тут: try_aggregate_m15")
# 🔸 Поиск пропущенных M1 и запись в missing_m1_log_v4 + system_log_v4 (временно отключено, используется заглушка)
async def detect_missing_m1(redis, pg, symbol, now_ts):
    log.info("Я тут: detect_missing_m1")
# 🔸 Восстановление одной M1 свечи через Binance API  (временно отключено, используется заглушка)
async def restore_missing_m1(symbol, open_time, redis, pg, precision):
    log.info("Я тут: restore_missing_m1")
# 🔸 Циклическое восстановление всех пропущенных свечей из missing_m1_log_v4 (временно отключено, используется заглушка)
async def restore_missing_m1_loop(redis, pg, state):
    log.info("Я тут: restore_missing_m1_loop")
# 🔸 Проверка: 4 и более подряд невосстановленных свечей → отключение тикера (временно отключено, используется заглушка)
async def check_consecutive_m1_failures(pg, symbol):
    log.info("Я тут: check_consecutive_m1_failures")
# 🔸 Слушает WebSocket Binance и переподключается при изменении тикеров
async def listen_kline_stream(redis, state, refresh_queue):

    while True:
        if not state["active"]:
            log.info("Нет активных тикеров для подписки")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@kline_1m" for s in symbols]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        try:
            async with websockets.connect(stream_url) as ws:
                log.info(f"Подключено к WebSocket Binance: {len(symbols)} тикеров")

                async def reader():
                    try:
                        async for msg in ws:
                            data = json.loads(msg)
                            if "data" not in data or "k" not in data["data"]:
                                continue
                            kline = data["data"]["k"]
                            if not kline["x"]:
                                continue
                            symbol = kline["s"]
                            open_time = datetime.utcfromtimestamp(kline["t"] / 1000)

                            await store_and_publish_m1(
                                redis,
                                symbol,
                                open_time,
                                kline,
                                state["tickers"][symbol]
                            )

                    except Exception as e:
                        log.error(f"Ошибка чтения WebSocket: {e}", exc_info=True)

                async def watcher():
                    await refresh_queue.get()
                    log.info("Получен сигнал переподключения WebSocket")
                    await ws.close()

                reader_task = asyncio.create_task(reader())
                watcher_task = asyncio.create_task(watcher())
                await asyncio.wait([reader_task, watcher_task], return_when=asyncio.FIRST_COMPLETED)

        except Exception as e:
            log.error(f"Ошибка WebSocket: {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Поток markPrice для одного тикера с fstream.binance.com
async def watch_mark_price(symbol, redis, precision):

    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s"
    last_update = 0

    while True:
        try:
            async with websockets.connect(url) as ws:
                log.info(f"[{symbol}] Подключение к потоку markPrice (futures)")
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        price = data.get("p")
                        if not price:
                            continue

                        now = time.time()
                        if now - last_update < 1:
                            continue

                        last_update = now
                        rounded = str(Decimal(price).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
                        await redis.set(f"price:{symbol}", rounded)
                        log.debug(f"[{symbol}] Обновление markPrice (futures): {rounded}")
                    except Exception as e:
                        log.warning(f"[{symbol}] Ошибка обработки markPrice: {e}")
        except Exception as e:
            log.error(f"[{symbol}] Ошибка WebSocket markPrice (futures): {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Основной запуск компонента с управлением тасками
async def run_feed_and_aggregator(pg, redis):

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
        "tickers": tickers,            # symbol -> precision_price
        "active": active,              # set of lowercase symbols
        "markprice_tasks": {},         # symbol -> asyncio.Task
        "tasks": set()                 # все фоновые таски
    }

    # Очередь сигналов на переподключение WebSocket
    refresh_queue = asyncio.Queue()

    # Хелпер для создания отслеживаемых тасок
    def create_tracked_task(coro, name):
        task = asyncio.create_task(coro)
        state["tasks"].add(task)
        def on_done(t):
            try:
                t.result()
            except Exception as e:
                log.exception(f"[{name}] Task crashed: {e}")
            finally:
                state["tasks"].discard(t)
        task.add_done_callback(on_done)
        return task

    # Запуск подписки на Redis Stream
    create_tracked_task(handle_ticker_events(redis, state, pg, refresh_queue), "ticker_events")

    # Запуск потоков markPrice для каждого активного тикера (фьючерсный рынок)
    for symbol in state["active"]:
        upper_symbol = symbol.upper()
        precision = state["tickers"].get(upper_symbol)
        if precision is not None:
            task = create_tracked_task(watch_mark_price(upper_symbol, redis, precision), f"markprice_{upper_symbol}")
            state["markprice_tasks"][upper_symbol] = task

    # Постоянный перезапуск слушателя WebSocket
    async def loop_listen():
        while True:
            await listen_kline_stream(redis, state, refresh_queue)

    create_tracked_task(loop_listen(), "listen_kline")

    # Цикл ожидания (можно будет использовать как watchdog или точка завершения)
    try:
        while True:
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        log.info("Aggregator shutdown requested, cancelling tasks...")
        for t in list(state["tasks"]):
            t.cancel()
        await asyncio.gather(*state["tasks"], return_exceptions=True)
        log.info("All tasks shut down cleanly.")