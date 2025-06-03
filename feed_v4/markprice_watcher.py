# markprice_watcher.py — отслеживание актуального mark price
import asyncio
import json
import logging
import time
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import websockets

log = logging.getLogger("MARKPRICE")

# 🔸 Отслеживание потока mark price по одному тикеру
async def watch_mark_price(symbol, redis, state):
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s"
    last_update = 0

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=None,  # отключаем внутренние ping/pong
                close_timeout=5
            ) as ws:
                log.info(f"[{symbol}] Подключено к потоку markPrice")

                # 🔁 Явная отправка pong
                async def keep_alive():
                    try:
                        while True:
                            await ws.pong()
                            log.debug(f"[{symbol}] → pong (keepalive)")
                            await asyncio.sleep(180)
                    except asyncio.CancelledError:
                        log.debug(f"[{symbol}] keep_alive завершён")
                    except Exception as e:
                        log.warning(f"[{symbol}] Ошибка keep_alive: {e}")

                pong_task = asyncio.create_task(keep_alive())

                try:
                    async for msg in ws:
                        if symbol not in state["active"]:
                            log.info(f"[{symbol}] Поток markPrice остановлен — тикер деактивирован")
                            return

                        try:
                            data = json.loads(msg)
                            price = data.get("p")
                            if not price:
                                continue

                            now = time.time()
                            if now - last_update < 1:
                                continue
                            last_update = now

                            precision = state["tickers"][symbol]["precision_price"]
                            rounded = str(Decimal(price).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

                            await redis.set(f"price:{symbol}", rounded)
                            log.debug(f"[{symbol}] Обновление markPrice: {rounded}")

                        except (InvalidOperation, ValueError, TypeError) as e:
                            log.warning(f"[{symbol}] Ошибка обработки markPrice: {type(e)}")
                finally:
                    pong_task.cancel()

        except Exception as e:
            log.error(f"[{symbol}] Ошибка WebSocket markPrice: {e}", exc_info=True)
            log.info(f"[{symbol}] Переподключение через 5 секунд...")
            await asyncio.sleep(5)

# 🔸 Запуск всех mark price потоков по активным тикерам
async def run_markprice_watcher(state, redis):
    log.info("Запуск отслеживания mark price для активных тикеров")

    async def spawn_for(symbol):
        task = asyncio.create_task(watch_mark_price(symbol, redis, state))
        state["markprice_tasks"][symbol] = task

    while True:
        active_symbols = set(state["active"])
        known_symbols = set(state["markprice_tasks"].keys())

        # Запустить новые
        for symbol in active_symbols - known_symbols:
            await spawn_for(symbol)

        # Завершить неактуальные
        for symbol in known_symbols - active_symbols:
            task = state["markprice_tasks"].pop(symbol, None)
            if task:
                task.cancel()
                log.info(f"[{symbol}] Остановлен поток markPrice — тикер деактивирован")

        await asyncio.sleep(3)
