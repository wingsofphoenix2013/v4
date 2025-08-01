# strategy_328_short.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("strategy_328_short")

class Strategy328Short:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "bb20_2_0_center", "bb20_2_0_upper"
            ], tf)

            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")

            log.debug(f"[328_SHORTONLY] symbol={symbol}, tf={tf}, direction={direction}, price={price}, "
                      f"bb_center={bb_center}, bb_upper={bb_upper}")

            if None in (bb_center, bb_upper):
                return ("ignore", "недостаточно данных BB")

            if direction == "short":
                bb_limit = bb_upper - (bb_upper - bb_center) * (1 / 3)
                if price >= bb_limit:
                    return True
                return ("ignore", f"фильтр BB short не пройден: price={price}, limit={bb_limit}")

            elif direction == "long":
                return ("ignore", "long сигналы отключены")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy_328_short")
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