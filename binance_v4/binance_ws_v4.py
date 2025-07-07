# binance_ws_v4.py

import asyncio
import aiohttp
import logging
from infra import get_binance_listen_key, keep_alive_binance_listen_key

log = logging.getLogger("BINANCE_WS")


async def run_binance_ws_listener():
    while True:
        try:
            log.info("🔌 Запуск подключения к Binance User Data Stream")

            # Получаем listenKey
            listen_key = await get_binance_listen_key()

            # Запускаем фоновое продление ключа
            asyncio.create_task(keep_alive_binance_listen_key())

            # Адрес WebSocket Binance Futures (Testnet)
            ws_url = f"wss://stream.binancefuture.com/ws/{listen_key}"
            log.info(f"🌐 Подключение к WebSocket: {ws_url}")

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    log.info("✅ WebSocket соединение установлено")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            log.info(f"📨 Сообщение: {msg.data}")
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            log.warning("⚠️ Ошибка WebSocket-соединения, выход из цикла")
                            break

        except Exception as e:
            log.exception(f"❌ Ошибка в Binance WebSocket слушателе: {e}")

        log.info("⏳ Перезапуск подключения через 5 секунд...")
        await asyncio.sleep(5)