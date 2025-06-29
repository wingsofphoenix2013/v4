# strategy_419_reversepl.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_419_REVERSEPL")

class Strategy419Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "ema21", "rsi14"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            ema21 = indicators.get("ema21")
            rsi = indicators.get("rsi14")

            log.debug(f"🔍 [419 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"adx={adx}, ema21={ema21}, rsi={rsi}, price={price}")

            if None in (adx, ema21, rsi):
                return ("ignore", "нет значений ADX, EMA21 или RSI")

            if not (25 <= adx <= 40):
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if price > ema21 and 55 <= rsi <= 75:
                    return True
                return ("ignore", f"фильтр long не пройден: price={price}, ema21={ema21}, rsi={rsi}")

            elif direction == "short":
                if price < ema21 and 25 <= rsi <= 45:
                    return True
                return ("ignore", f"фильтр short не пройден: price={price}, ema21={ema21}, rsi={rsi}")

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