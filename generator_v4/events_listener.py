# events_listener.py

import asyncio
import logging

log = logging.getLogger("GEN")

# 🔸 Заглушка слушателя событий Pub/Sub
async def run_event_listener():
    while True:
        log.info("[PUBSUB_WATCHER] Заглушка запущена, ожидаю...")
        await asyncio.sleep(60)