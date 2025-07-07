# ws_test_client_minimal.py

import asyncio
import aiohttp
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("WS_MIN")

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")  # не нужен для listenKey

async def get_listen_key():
    url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": API_KEY}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            return data["listenKey"]

async def run_ws_listener():
    listen_key = await get_listen_key()
    log.info(f"🧾 Получен listenKey: {listen_key}")

    # ✅ Правильный URL по документации: /ws/<listenKey>
    ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
    log.info(f"🔌 Подключение к WebSocket: {ws_url}")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            log.info("✅ WebSocket подключён. Ждём сообщения...")

            while True:
                msg = await ws.receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    log.info(f"📨 TEXT: {msg.data}")
                elif msg.type == aiohttp.WSMsgType.PING:
                    await ws.pong(msg.data)
                    log.info("📡 PING получен — отправлен PONG")
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    log.warning("🔌 Соединение закрыто")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error("❌ Ошибка в WebSocket")
                    break

if __name__ == "__main__":
    asyncio.run(run_ws_listener())