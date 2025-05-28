# signal_processor.py

import asyncio
import logging
log = logging.getLogger("SIGNAL_PROCESSOR")

async def run_signal_loop(strategy_registry):
    log.info("🚦 [SIGNAL_PROCESSOR] Запуск цикла обработки сигналов (заглушка)")
    while True:
        await asyncio.sleep(5)