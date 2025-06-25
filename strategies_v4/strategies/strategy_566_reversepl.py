# strategy_566_reversepl.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_566_REVERSEPL")

class Strategy566Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        
        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены для символа")
               
            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "rsi14",
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            rsi = indicators.get("rsi14")
            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")

            log.debug(f"🔍 [566 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"adx={adx}, rsi={rsi}, price={price}, "
                      f"bb_center={bb_center}, bb_upper={bb_upper}, bb_lower={bb_lower}")

            if None in (adx, rsi, bb_center, bb_upper, bb_lower):
                return ("ignore", "недостаточно данных ADX, RSI или BB")

            if adx <= 35:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if rsi <= 50:
                    return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")
                bb_limit = bb_center + (bb_upper - bb_center) / 2
                if price > bb_limit:
                    return True
                return ("ignore", f"фильтр BB long не пройден: price={price}, bb_limit={bb_limit}")

            elif direction == "short":
                if rsi >= 50:
                    return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")
                bb_limit = bb_center - (bb_center - bb_lower) / 2
                if price < bb_limit:
                    return True
                return ("ignore", f"фильтр BB short не пройден: price={price}, bb_limit={bb_limit}")

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