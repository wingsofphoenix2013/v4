# position_handler.py

import asyncio
import logging
log = logging.getLogger("POSITION_HANDLER")

async def run_position_loop():
    log.info("🎯 [POSITION_HANDLER] Запуск цикла слежения за позициями (заглушка)")
    while True:
        await asyncio.sleep(5)