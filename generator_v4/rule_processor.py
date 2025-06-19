# rule_processor.py

import asyncio
import logging

log = logging.getLogger("GEN")

# 🔸 Заглушка воркера обработки правил
async def run_rule_processor():
    while True:
        log.info("[RULE_PROCESSOR] Заглушка запущена, ожидаю...")
        await asyncio.sleep(10)