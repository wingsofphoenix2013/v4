# strategy_410_reversepl.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_410_REVERSEPL")

class Strategy410Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "ema21"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            ema21 = indicators.get("ema21")

            log.debug(f"🔍 [410 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"adx={adx}, ema21={ema21}, price={price}")

            if adx is None or ema21 is None:
                return ("ignore", "нет значений ADX или EMA21")

            if not (25 <= adx <= 40):
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if price > ema21:
                    return True
                return ("ignore", f"фильтр long не пройден: price={price}, ema21={ema21}")

            elif direction == "short":
                if price < ema21:
                    return True
                return ("ignore", f"фильтр short не пройден: price={price}, ema21={ema21}")

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