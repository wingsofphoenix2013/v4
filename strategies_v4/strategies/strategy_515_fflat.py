# strategy_515_fflat.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_515_FFLAT")

class Strategy515Fflat:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены для символа")

            indicators = await load_indicators(symbol, [
                "rsi14", "lr50_angle",
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower",
                "bb20_2_5_upper", "bb20_2_5_lower"
            ], tf)

            rsi = indicators.get("rsi14")
            lr_angle = indicators.get("lr50_angle")
            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")
            bb_upper_25 = indicators.get("bb20_2_5_upper")
            bb_lower_25 = indicators.get("bb20_2_5_lower")

            log.debug(f"🔍 [515 FFLAT] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"rsi={rsi}, lr_angle={lr_angle}, price={price}, "
                      f"bb_center={bb_center}, bb_upper={bb_upper}, bb_lower={bb_lower}, "
                      f"bb_upper_25={bb_upper_25}, bb_lower_25={bb_lower_25}")

            if None in (rsi, lr_angle, bb_center, bb_upper, bb_lower, bb_upper_25, bb_lower_25):
                return ("ignore", "недостаточно данных RSI, LR или BB")

            if direction == "long":
                bb_limit_lower = bb_center + (bb_upper - bb_center) / 3
                if 55 <= rsi <= 80 and lr_angle >= 0 and price <= bb_upper_25 and price >= bb_limit_lower:
                    return True
                return ("ignore", f"фильтр long не пройден: rsi={rsi}, lr_angle={lr_angle}, price={price}, "
                                  f"bb_upper_25={bb_upper_25}, bb_limit_lower={bb_limit_lower}")

            elif direction == "short":
                bb_limit_upper = bb_center - (bb_center - bb_lower) / 3
                if 20 <= rsi <= 45 and lr_angle >= 0 and price >= bb_lower_25 and price <= bb_limit_upper:
                    return True
                return ("ignore", f"фильтр short не пройден: rsi={rsi}, lr_angle={lr_angle}, price={price}, "
                                  f"bb_lower_25={bb_lower_25}, bb_limit_upper={bb_limit_upper}")

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