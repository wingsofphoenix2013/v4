# strategy_565_reversepl.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_565_REVERSEPL")

class Strategy565Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        price = float(signal["price"])

        try:
            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx",
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")

            log.debug(f"🔍 [565 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, price={price}, "
                      f"adx={adx}, bb_center={bb_center}, bb_upper={bb_upper}, bb_lower={bb_lower}")

            if None in (adx, bb_center, bb_upper, bb_lower):
                return ("ignore", "недостаточно данных ADX или BB")

            if adx <= 35:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                bb_limit = bb_center + (bb_upper - bb_center) / 2
                if price > bb_limit:
                    return True
                return ("ignore", f"фильтр BB long не пройден: price={price}, bb_limit={bb_limit}")

            elif direction == "short":
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