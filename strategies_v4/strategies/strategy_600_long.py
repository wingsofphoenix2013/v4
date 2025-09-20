# strategy_600_long.py

import logging
import json

log = logging.getLogger("strategy_600_long")

class Strategy600Long:
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()

        if direction == "long":
            return True
        elif direction == "short":
            return ("ignore", "short сигналы отключены")
        else:
            return ("ignore", f"неизвестное направление: {direction}")

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