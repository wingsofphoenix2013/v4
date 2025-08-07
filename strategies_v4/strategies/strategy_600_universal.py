import logging
import json

log = logging.getLogger("strategy_600_universal")

class Strategy600Universal:
    async def validate_signal(self, signal, context):
        direction = signal["direction"].lower()
        if direction in ("long", "short"):
            return True
        return ("ignore", f"❌ неизвестное направление: {direction}")

    async def run(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})

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
            # Отправка сигнала на открытие позиции
            await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
            log.debug(f"📤 [600] Сигнал отправлен: {payload}")

            # Отправка запроса на голосование
            voting_payload = {
                "strategy_id": str(signal["strategy_id"]),
                "direction": signal["direction"],
                "tf": strategy_meta.get("timeframe", "m5"),  # по умолчанию m5
                "symbol": signal["symbol"],
                "log_uid": signal.get("log_uid")
            }
            await redis.xadd("strategy_voting_request", voting_payload)
            log.debug(f"🗳️ [600] Запрос на голосование отправлен: {voting_payload}")

        except Exception as e:
            log.warning(f"⚠️ [600] Ошибка при отправке: {e}")