# strategies/strategy_103.py

import logging
import json
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

    # 🔸 Запуск стратегии (отправка команды на открытие позиции)
    async def run(self, signal, context):
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
            log.info(f"📤 [Strategy103] Сигнал отправлен в strategy_opener_stream: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка отправки сигнала в strategy_opener_stream: {e}")