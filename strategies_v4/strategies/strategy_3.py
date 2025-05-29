# strategies/strategy_3.py

import logging
log = logging.getLogger("STRATEGY_3")

class Strategy3:
    def run(self, signal, context):
        log.info("Я — тестовая стратегия 3")
        return {"status": "ok"}