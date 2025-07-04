# strategy_224_slprotect.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_224_SLPROTECT")

class Strategy224Slprotect:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, ["rsi14", "mfi14", "adx_dmi14_adx"], tf)
            rsi = indicators.get("rsi14")
            mfi = indicators.get("mfi14")
            adx = indicators.get("adx_dmi14_adx")

            log.debug(f"🔍 [224 SLPROTECT] symbol={symbol}, direction={direction}, tf={tf}, rsi={rsi}, mfi={mfi}, adx={adx}")

            if rsi is None or mfi is None or adx is None:
                return ("ignore", "нет значений RSI/MFI/ADX")

            if not (25 <= adx <= 40):
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if rsi < 28 and mfi < 30:
                    return True
                return ("ignore", f"фильтр long не пройден: rsi={rsi}, mfi={mfi}")

            elif direction == "short":
                if rsi > 72 and mfi > 70:
                    return True
                return ("ignore", f"фильтр short не пройден: rsi={rsi}, mfi={mfi}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception as e:
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