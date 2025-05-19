# feed_and_aggregate.py — приём и агрегация рыночных данных

import logging
import asyncio
import websockets
import json
from decimal import Decimal
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
# 🔸 Слушает WebSocket Binance и автоматически переподключается при изменении тикеров
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

                while True:
                    done, pending = await asyncio.wait(
                        [
                            asyncio.create_task(ws.recv()),
                            asyncio.create_task(refresh_queue.get())
                        ],
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    for task in done:
                        result = task.result()

                        if isinstance(result, str):  # WebSocket сообщение
                            data = json.loads(result)
                            if "data" not in data or "k" not in data["data"]:
                                continue

                            kline = data["data"]["k"]
                            if not kline["x"]:
                                continue  # Только is_final == true

                            symbol = kline["s"]
                            open_time = datetime.utcfromtimestamp(kline["t"] / 1000)

                            log_str = (
                                f"[{symbol}] Получена свеча M1: {open_time} — "
                                f"O:{kline['o']} H:{kline['h']} L:{kline['l']} "
                                f"C:{kline['c']} V:{kline['v']}"
                            )
                            logger.info(log_str)

                        elif result == "refresh":
                            logger.info("Получен сигнал переподключения WebSocket")
                            return  # прерываем `with ws`, начнётся новый цикл

        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}", exc_info=True)
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

    # Запуск приёма свечей через WebSocket
    asyncio.create_task(listen_kline_stream(redis, state, refresh_queue))

    # Цикл ожидания (можно будет использовать как watchdog)
    while True:
        await asyncio.sleep(5)