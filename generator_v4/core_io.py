# core_io.py

import asyncio
import logging

log = logging.getLogger("GEN")

# 🔸 Заглушка логирования сигналов
async def run_core_io():
    while True:
        log.info("[CORE_IO] Заглушка запущена, ожидаю...")
        await asyncio.sleep(60)