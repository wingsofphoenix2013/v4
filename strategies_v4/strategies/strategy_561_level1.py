# strategy_561_level1.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_561_LEVEL1")

class Strategy561Level1:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")

            log.debug(f"[561] symbol={symbol}, tf={tf}, direction={direction}, price={price}, "
                      f"bb_center={bb_center}, bb_upper={bb_upper}, bb_lower={bb_lower}")

            if None in (bb_center, bb_upper, bb_lower):
                return ("ignore", "недостаточно данных BB")

            if direction == "long":
                bb_limit = bb_lower + (bb_center - bb_lower) * 2 / 3
                if price < bb_limit:
                    return True
                return ("ignore", f"фильтр BB long не пройден: price={price}, limit={bb_limit}")

            elif direction == "short":
                bb_limit = bb_upper - (bb_upper - bb_center) * 2 / 3
                if price > bb_limit:
                    return True
                return ("ignore", f"фильтр BB short не пройден: price={price}, limit={bb_limit}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy_561_level1")
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