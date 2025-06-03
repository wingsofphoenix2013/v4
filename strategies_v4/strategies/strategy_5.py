# strategies/strategy_5.py

import logging
import json
from datetime import datetime

log = logging.getLogger("STRATEGY_5")


class Strategy5:
    # 🔸 Валидация сигнала — всегда True (пропускаем)
    async def validate_signal(self, signal, context) -> bool:
        return True

    # 🔸 Отправка сигнала на открытие позиции
    async def run(self, signal, context):
        redis = context.get("redis")
        if redis:
            payload = {
                "strategy_id": signal["strategy_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "log_id": signal["log_id"],
                "route": "new_entry"
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug(
                    f"📤 [Strategy5] Сигнал передан в strategy_opener_stream: {payload}"
                )
            except Exception as e:
                log.warning(f"⚠️ [Strategy5] Ошибка при отправке сигнала: {e}")