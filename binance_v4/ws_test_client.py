# ws_test_client.py

import asyncio
import aiohttp
import logging
from binance.um_futures import UMFutures
import os

log = logging.getLogger("WS_TEST")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

client = UMFutures(key=api_key, secret=api_secret, base_url="https://testnet.binancefuture.com")

# 🔸 Основной запуск WebSocket для диагностики
async def run_ws_test_listener():
    try:
        # Получение listenKey
        listen_key_resp = client.new_listen_key()
        listen_key = listen_key_resp["listenKey"]
        log.info(f"🧾 Получен listenKey: {listen_key}")

        # Подключение к WebSocket (stream режим)
        url = f"wss://fstream.binance.com/stream?streams={listen_key}"
        log.info(f"🔌 Подключение к WebSocket: {url}")

        session = aiohttp.ClientSession()
        ws = await session.ws_connect(url)
        log.info("✅ WebSocket соединение установлено. Ожидаем сообщения...")

        while True:
            msg = await ws.receive()

            if msg.type == aiohttp.WSMsgType.PING:
                await ws.pong(msg.data)
                log.info("📡 Получен PING от Binance — отправлен PONG")
                continue

            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                log.info(f"📨 WS TEXT сообщение: {data}")
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                log.warning("🔌 WebSocket соединение закрыто")
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                log.error(f"❌ Ошибка WebSocket: {msg}")
                break

    except Exception as e:
        log.exception(f"❌ Ошибка инициализации WS-теста: {e}")