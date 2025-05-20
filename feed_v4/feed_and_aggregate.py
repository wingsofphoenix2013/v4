# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging
import asyncio
import websockets
import json
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
                    await refresh_queue.put("refresh")

                elif action == "disabled" and symbol in state["tickers"]:
                    logger.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())
                    await refresh_queue.put("refresh")

                await redis.xack(stream, group, msg_id)
# 🔸 Слушает WebSocket Binance и переподключается при изменении тикеров
async def listen_kline_stream(redis, state, refresh_queue):
    logger = logging.getLogger("KLINE")

    while True:
        if not state["active"]:
            logger.info("Нет активных тикеров для подписки")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@kline_1m" for s in symbols]
        stream_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

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
# 🔸 Сохранение M1 в Redis TS и публикация события
async def store_and_publish_m1(redis, symbol, open_time, kline, precision):

    ts_key = f"ohlcv:{symbol.lower()}:m1"
    stream_key = "ohlcv_m1_ready"

    timestamp = int(open_time.timestamp() * 1000)

    # 🔸 Округление значений через Decimal
    def round_str(val):
        return str(Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

    fields = {
        "o": round_str(kline["o"]),
        "h": round_str(kline["h"]),
        "l": round_str(kline["l"]),
        "c": round_str(kline["c"]),
        "v": round_str(kline["v"]),
    }

    try:
        await redis.execute_command(
            "TS.ADD", ts_key, timestamp, fields["c"], "RETENTION", 86400000, "LABELS",
            "symbol", symbol.lower(), "tf", "m1"
        )
    except Exception as e:
        logger = logging.getLogger("TS")
        logger.warning(f"TS.ADD ошибка (ключ мог существовать): {e}")
        await redis.execute_command("TS.CREATE", ts_key, "RETENTION", 86400000, "LABELS",
            "symbol", symbol.lower(), "tf", "m1")
        await redis.execute_command("TS.ADD", ts_key, timestamp, fields["c"])

    await redis.xadd(stream_key, {
        "symbol": symbol,
        "open_time": str(open_time)
    })

    logger = logging.getLogger("KLINE")
    logger.info(f"[{symbol}] M1 сохранена и опубликована: {open_time} → C={fields['c']}")
# 🔸 Обработка потока markPrice с обновлением Redis
async def listen_mark_price(redis, state):
    import time
    logger = logging.getLogger("KLINE")

    last_update = {}

    while True:
        if not state["active"]:
            logger.info("Нет активных тикеров для подписки на markPrice")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@markPrice" for s in symbols]
        stream_url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

        try:
            async with websockets.connect(stream_url) as ws:
                logger.info(f"Подключено к WebSocket Binance (markPrice): {len(symbols)} тикеров")

                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        if "data" not in data or "p" not in data["data"]:
                            continue
                        payload = data["data"]
                        symbol = payload["s"]
                        price = payload["p"]
                        now = time.time()

                        # 🔸 Частотный фильтр — не чаще 1/сек
                        if symbol in last_update and now - last_update[symbol] < 1:
                            continue

                        last_update[symbol] = now
                        precision = state["tickers"].get(symbol)
                        if precision is None:
                            continue

                        from decimal import Decimal, ROUND_DOWN
                        rounded = str(Decimal(price).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

                        await redis.set(f"price:{symbol}", rounded)
                        logger.info(f"[{symbol}] Обновление markPrice: {rounded}")

                    except Exception as e:
                        logger.warning(f"Ошибка обработки markPrice: {e}")

        except Exception as e:
            logger.error(f"Ошибка WebSocket markPrice: {e}", exc_info=True)
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
        "active": active      # set of lowercase symbols
    }

    # Очередь сигналов на переподключение WebSocket
    refresh_queue = asyncio.Queue()

    # Запуск подписки на Redis Stream
    asyncio.create_task(handle_ticker_events(redis, state, pg, refresh_queue))
    
    # Вызов внутри run_feed_and_aggregator()
    asyncio.create_task(listen_mark_price(redis, state))

    # Постоянный перезапуск слушателя WebSocket
    async def loop_listen():
        while True:
            await listen_kline_stream(redis, state, refresh_queue)

    asyncio.create_task(loop_listen())

    # Цикл ожидания (можно будет использовать как watchdog)
    while True:
        await asyncio.sleep(5)