# strategies/strategy_103.py

import logging
from infra import load_indicators

log = logging.getLogger("STRATEGY_103")

class Strategy103:
    # 🔸 Валидация сигнала
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"]

        log.debug(f"⚙️ [Strategy103] Валидация сигнала: symbol={symbol}, direction={direction}")

        if direction != "long":
            return ("ignore", "допущен только long")

        return True

    # 🔸 Запуск стратегии (будет реализовано позже)
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy103] Запуск стратегии на сигнале: {signal['symbol']} {signal['direction']}")