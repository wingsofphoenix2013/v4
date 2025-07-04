# redis_consumer.py

import asyncio
import logging

log = logging.getLogger("REDIS_CONSUMER")

# 🔸 Заглушка воркера, который будет читать события из Redis Stream
async def run_redis_consumer():
    while True:
        log.info("⏳ Ожидание событий из Redis Stream (заглушка)...")
        await asyncio.sleep(60)