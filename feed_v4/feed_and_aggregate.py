# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging
import asyncio
import websockets
import json
from infra import info_log
from decimal import Decimal, ROUND_DOWN
from datetime import datetime

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

                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4 WHERE symbol = $1
                        """, symbol)
                        if row:
                            state["tickers"][symbol] = row["precision_price"]
                            info_log("TICKER_STREAM", f"Добавлен тикер из БД: {symbol}")

                if action == "enabled" and symbol in state["tickers"]:
                    logger.info(f"Активирован тикер: {symbol}")
                    state["active"].add(symbol.lower())
                    await refresh_queue.put("refresh")

                    # запуск потока markPrice и отслеживание задачи
                    precision = state["tickers"][symbol]
                    task = asyncio.create_task(watch_mark_price(symbol, redis, precision))
                    state["markprice_tasks"][symbol] = task

                elif action == "disabled" and symbol in state["tickers"]:
                    logger.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())
                    await refresh_queue.put("refresh")

                    # отмена потока markPrice, если он есть
                    task = state["markprice_tasks"].pop(symbol, None)
                    if task:
                        task.cancel()

                await redis.xack(stream, group, msg_id)
# 🔸 Сохранение полной свечи M1 в RedisJSON
async def store_and_publish_m1(redis, symbol, open_time, kline, precision):
    from decimal import Decimal, ROUND_DOWN
    import logging

    logger = logging.getLogger("KLINE")
    timestamp = int(open_time.timestamp() * 1000)
    json_key = f"ohlcv:{symbol.lower()}:m1:{timestamp}"

    def r(val):
        return float(Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

    candle = {
        "o": r(kline["o"]),
        "h": r(kline["h"]),
        "l": r(kline["l"]),
        "c": r(kline["c"]),
        "v": r(kline["v"]),
        "ts": timestamp
    }

    await redis.execute_command("JSON.SET", json_key, "$", str(candle).replace("'", '"'))
    logger.info(f"[{symbol}] M1 сохранена и опубликована: {open_time} → C={candle['c']}")

    await try_aggregate_m5(redis, symbol, open_time)
# 🔸 Агрегация M5 на основе RedisJSON M1-свечей
async def try_aggregate_m5(redis, symbol, open_time):
    import logging
    from datetime import timedelta

    logger = logging.getLogger("KLINE")

    if open_time.minute % 5 != 4:
        return

    end_ts = int(open_time.timestamp() * 1000)
    ts_list = [end_ts - 60_000 * i for i in reversed(range(5))]
    candles = []

    for ts in ts_list:
        key = f"ohlcv:{symbol.lower()}:m1:{ts}"
        try:
            data = await redis.execute_command("JSON.GET", key, "$")
            if not data:
                logger.warning(f"[{symbol}] M5: пропущена свеча {ts}")
                return
            import json
            parsed = json.loads(data)[0]
            candles.append(parsed)
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка чтения JSON для M5: {e}")
            return

    o = candles[0]["o"]
    h = max(c["h"] for c in candles)
    l = min(c["l"] for c in candles)
    c = candles[-1]["c"]
    v = sum(c["v"] for c in candles)
    m5_ts = ts_list[0]

    key = f"ohlcv:{symbol.lower()}:m5:{m5_ts}"
    candle = { "o": o, "h": h, "l": l, "c": c, "v": v, "ts": m5_ts }
    await redis.execute_command("JSON.SET", key, "$", str(candle).replace("'", '"'))

    logger.info(f"[{symbol}] Построена M5: {open_time.replace(second=0)} → O:{o} H:{h} L:{l} C:{c}")
# 🔸 Слушает WebSocket Binance и переподключается при изменении тикеров
async def listen_kline_stream(redis, state, refresh_queue):
    logger = logging.getLogger("KLINE")

    while True:
        if not state["active"]:
            info_log("KLINE", "Нет активных тикеров для подписки")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@kline_1m" for s in symbols]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        try:
            async with websockets.connect(stream_url) as ws:
                logger.info(f"Подключено к WebSocket Binance: {len(symbols)} тикеров")

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
                        logger.error(f"Ошибка чтения WebSocket: {e}", exc_info=True)

                async def watcher():
                    await refresh_queue.get()
                    logger.info("Получен сигнал переподключения WebSocket")
                    await ws.close()

                reader_task = asyncio.create_task(reader())
                watcher_task = asyncio.create_task(watcher())
                await asyncio.wait([reader_task, watcher_task], return_when=asyncio.FIRST_COMPLETED)

        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Поток markPrice для одного тикера с fstream.binance.com
async def watch_mark_price(symbol, redis, precision):
    import time
    from decimal import Decimal, ROUND_DOWN
    import logging
    logger = logging.getLogger("KLINE")

    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s"
    last_update = 0

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info(f"[{symbol}] Подключение к потоку markPrice (futures)")
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
                        info_log("KLINE", f"[{symbol}] Обновление markPrice (futures): {rounded}")
                    except Exception as e:
                        logger.warning(f"[{symbol}] Ошибка обработки markPrice: {e}")
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка WebSocket markPrice (futures): {e}", exc_info=True)
            await asyncio.sleep(5)
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
        "active": active,     # set of lowercase symbols
        "markprice_tasks": {}          # symbol -> asyncio.Task
    }

    # Очередь сигналов на переподключение WebSocket
    refresh_queue = asyncio.Queue()

    # Запуск подписки на Redis Stream
    asyncio.create_task(handle_ticker_events(redis, state, pg, refresh_queue))
    
    # Запуск потоков markPrice для каждого активного тикера (фьючерсный рынок)
    for symbol in state["active"]:
        upper_symbol = symbol.upper()
        precision = state["tickers"].get(upper_symbol)
        if precision is not None:
            task = asyncio.create_task(watch_mark_price(upper_symbol, redis, precision))
            state["markprice_tasks"][upper_symbol] = task

    # Постоянный перезапуск слушателя WebSocket
    async def loop_listen():
        while True:
            await listen_kline_stream(redis, state, refresh_queue)

    asyncio.create_task(loop_listen())

    # Цикл ожидания (можно будет использовать как watchdog)
    while True:
        await asyncio.sleep(5)