# strategy_305_reversepl.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_305_REVERSEPL")

class Strategy305Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx",
                "macd12_macd", "macd12_macd_signal"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            macd = indicators.get("macd12_macd")
            macd_signal = indicators.get("macd12_macd_signal")

            log.debug(f"🔍 [305 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"adx={adx}, macd={macd}, macd_signal={macd_signal}")

            if None in (adx, macd, macd_signal):
                return ("ignore", "нет значений ADX или MACD")

            if adx <= 15:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if macd > macd_signal:
                    return True
                return ("ignore", f"фильтр MACD long не пройден: macd={macd}, signal={macd_signal}")

            elif direction == "short":
                if macd < macd_signal:
                    return True
                return ("ignore", f"фильтр MACD short не пройден: macd={macd}, signal={macd_signal}")

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