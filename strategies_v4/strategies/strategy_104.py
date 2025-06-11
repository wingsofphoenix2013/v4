# strategies/strategy_104.py

import logging
from infra import load_indicators

log = logging.getLogger("STRATEGY_104")

class Strategy104:
    # 🔸 Валидация сигнала
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"]

        log.debug(f"⚙️ [Strategy104] Транзитная стратегия: сигнал принят без проверки: symbol={symbol}, direction={direction}")
        return True

    # 🔸 Запуск стратегии (будет реализовано позже)
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy104] Запуск стратегии на сигнале: {signal['symbol']} {signal['direction']}")