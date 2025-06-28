# strategy_309_reversepl.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_309_REVERSEPL")

class Strategy309Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "mfi14"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            mfi = indicators.get("mfi14")

            log.debug(f"🔍 [309 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, adx={adx}, mfi={mfi}")

            if adx is None or mfi is None:
                return ("ignore", "нет значений ADX или MFI")

            if adx <= 15:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if mfi > 60:
                    return True
                return ("ignore", f"фильтр MFI long не пройден: mfi={mfi}")

            elif direction == "short":
                if mfi < 40:
                    return True
                return ("ignore", f"фильтр MFI short не пройден: mfi={mfi}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в validate_signal")
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