# strategies/strategy_2.py

import logging
log = logging.getLogger("STRATEGY_2")

class Strategy2:
    def run(self, signal, context):
        log.info("Я — тестовая стратегия 2")
        return {"status": "ok"}