# ws_test_client_minimal.py

import asyncio
import aiohttp
import os
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("WS_TEST")

API_KEY = os.getenv("BINANCE_API_KEY")

async def get_listen_key():
    url = "https://testnet.binancefuture.com/fapi/v1/listenKey"
    headers = {"X-MBX-APIKEY": API_KEY}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as resp:
            data = await resp.json()
            return data["listenKey"]

async def listen_to_user_stream():
    listen_key = await get_listen_key()
    ws_url = f"wss://stream.binancefuture.com/ws/{listen_key}"
    log.info(f"🔌 WebSocket URL: {ws_url}")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            log.info("✅ WebSocket соединение установлено.")
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    log.info(f"📨 Сообщение: {msg.data}")
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error(f"❌ Ошибка WebSocket: {msg}")
                    break

# 💡 Обязательная конструкция для запуска как main-скрипт
if __name__ == "__main__":
    asyncio.run(listen_to_user_stream())