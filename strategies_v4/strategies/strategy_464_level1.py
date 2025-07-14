# strategy_464_level1.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_464_LEVEL1")

class Strategy464Level1:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        redis = context["redis"]

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "rsi14",
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower",
                "bb20_2_5_upper", "bb20_2_5_lower"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            rsi = indicators.get("rsi14")
            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")
            bb_upper_25 = indicators.get("bb20_2_5_upper")
            bb_lower_25 = indicators.get("bb20_2_5_lower")

            log.debug(f"[464] symbol={symbol}, tf={tf}, direction={direction}, price={price}, "
                      f"adx={adx}, rsi={rsi}, bb_center={bb_center}, bb_upper={bb_upper}, "
                      f"bb_lower={bb_lower}, bb_upper_25={bb_upper_25}, bb_lower_25={bb_lower_25}")

            # Проверка данных
            if None in (adx, rsi, bb_center, bb_upper, bb_lower, bb_upper_25, bb_lower_25):
                return ("ignore", "недостаточно данных BB, ADX или RSI")

            if adx <= 30:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if not (55 < rsi < 80):
                    return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")

                bb_limit_lower = bb_center + (bb_upper - bb_center) / 3
                if price > bb_upper_25 or price < bb_limit_lower:
                    return ("ignore", f"фильтр BB long не пройден: price={price}, "
                                      f"upper_25={bb_upper_25}, bb_limit_lower={bb_limit_lower}")

                return True

            elif direction == "short":
                if not (20 < rsi < 45):
                    return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")

                bb_limit_upper = bb_center - (bb_center - bb_lower) / 3
                if price < bb_lower_25 or price > bb_limit_upper:
                    return ("ignore", f"фильтр BB short не пройден: price={price}, "
                                      f"lower_25={bb_lower_25}, bb_limit_upper={bb_limit_upper}")

                return True

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy_464_level1")
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