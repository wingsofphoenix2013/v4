# strategy_101_flat.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_101_FLAT")

class Strategy101Flat:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        price = float(signal["price"])

        try:
            indicators = await load_indicators(symbol, [
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")

            log.info(f"[101 FLAT] symbol={symbol}, tf={tf}, direction={direction}, "
                      f"price={price}, bb_center={bb_center}, bb_upper={bb_upper}, bb_lower={bb_lower}")

            if None in (bb_center, bb_upper, bb_lower):
                return ("ignore", "нет значений Bollinger Bands")

            if direction == "long":
                bb_limit = bb_center - (bb_center - bb_lower) / 2
                log.info(f"[BB LONG] price={price} < bb_limit={bb_limit}")
                if price < bb_limit:
                    return True
                return ("ignore", f"фильтр long не пройден: price={price}, bb_limit={bb_limit}")

            elif direction == "short":
                bb_limit = bb_center + (bb_upper - bb_center) / 2
                log.info(f"[BB SHORT] price={price} > bb_limit={bb_limit}")
                if price > bb_limit:
                    return True
                return ("ignore", f"фильтр short не пройден: price={price}, bb_limit={bb_limit}")

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
            log.info(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")