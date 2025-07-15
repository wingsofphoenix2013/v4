# strategy_532_level1.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_532_LEVEL1")

class Strategy532Level1:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, ["mfi14"], tf)
            mfi = indicators.get("mfi14")

            log.debug(f"[532] symbol={symbol}, tf={tf}, direction={direction}, price={price}, mfi={mfi}")

            if mfi is None:
                return ("ignore", "нет значения MFI")

            if direction == "long":
                if mfi < 30:
                    return True
                return ("ignore", f"MFI выше порога для long: mfi={mfi}")

            elif direction == "short":
                if mfi > 70:
                    return True
                return ("ignore", f"MFI ниже порога для short: mfi={mfi}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy_532_level1")
            return ("ignore", "ошибка в стратегии")

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