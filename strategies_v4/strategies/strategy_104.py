# strategies/strategy_104.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_104")

class Strategy104:
    # 🔸 Валидация сигнала
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"]

        log.debug(f"⚙️ [Strategy104] Транзитная стратегия: сигнал принят без проверки: symbol={symbol}, direction={direction}")
        return True

    # 🔸 Запуск стратегии (отправка команды на открытие позиции)
    async def run(self, signal, context):
        log.info(f"🚀 [Strategy104] run() вызван для {signal['symbol']}")

        redis = context.get("redis")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal["log_uid"],
            "route": "new_entry"
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.info(f"📤 [Strategy104] Сигнал отправлен в strategy_opener_stream: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка отправки сигнала в strategy_opener_stream: {e}")