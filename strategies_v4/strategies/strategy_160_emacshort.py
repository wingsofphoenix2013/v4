import logging
import json

log = logging.getLogger("strategy_160_emacshort")

class Strategy160Emacshort:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        symbol = signal["symbol"]
        tf = "m5"

        # Только для шортов
        if signal["direction"].lower() != "short":
            return ("ignore", "long сигналы отключены")

        if redis is None:
            log.warning("Нет Redis в context")
            return ("ignore", "нет Redis")

        try:
            adx_key = f"ind:{symbol}:{tf}:adx_dmi14_adx"
            raw_adx = await redis.get(adx_key)
            if raw_adx is None:
                return ("ignore", f"нет ADX в Redis: {adx_key}")

            try:
                adx_value = float(raw_adx)
            except ValueError:
                return ("ignore", f"некорректное значение ADX: {raw_adx}")

            if adx_value > 20:
                return True
            else:
                return ("ignore", f"ADX={adx_value} <= 20")

        except Exception:
            log.exception("Ошибка в strategy_160_emacshort")
            return ("ignore", "ошибка в стратегии")

    async def run(self, signal, context):
        redis = context.get("redis")
        if redis is None:
            raise RuntimeError("Redis клиент не передан в context")

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
            log.debug(f"Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"Ошибка при отправке сигнала: {e}")