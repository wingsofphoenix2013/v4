# strategy_352_reverse.py

import logging
import json

log = logging.getLogger("STRATEGY_352_REVERSE")

class Strategy352Reverse:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"]
        log.debug(f"⚙️ [352 REVERSE] Пропускаем сигнал без фильтрации: symbol={symbol}, direction={direction}")
        return True

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