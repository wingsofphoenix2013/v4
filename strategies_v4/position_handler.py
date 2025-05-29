# position_handler.py

import asyncio
import logging
log = logging.getLogger("POSITION_HANDLER")

async def run_position_loop(position_registry):
    log.info(f"🎯 [POSITION_HANDLER] Активных позиций: {len(position_registry)}")
    while True:
        await asyncio.sleep(5)