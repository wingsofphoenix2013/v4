# strategy_333_level1.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_333_LEVEL1")

class Strategy333Level1:
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        if direction == "long":
            return True
        return ("ignore", "разрешён только long")
        
    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("❌ Redis клиент не передан в context")

        payload = {
            "strategy_id": str(signal["strategy_id"]),
            "symbol": signal["symbol"],
            "direction": signal["direction"],
            "log_uid": signal.get("log_uid"),
            "route": "new_entry",
            "received_at": signal.get("received_at")
        }

        try:
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")