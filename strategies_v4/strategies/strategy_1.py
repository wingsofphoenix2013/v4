# strategies/strategy_1.py

import logging
log = logging.getLogger("STRATEGY_1")

class Strategy1:
    def run(self, signal, context):
        log.info("Я — тестовая стратегия")
        return {"status": "ok"}