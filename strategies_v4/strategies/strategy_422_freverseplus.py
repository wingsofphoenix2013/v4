# strategy_422_freverseplus.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_422_FREVERSEPLUS")

class Strategy422Freverseplus:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены для символа")

            indicators = await load_indicators(symbol, [
                "rsi14", "lr50_angle", "mfi14"
            ], tf)

            rsi = indicators.get("rsi14")
            lr_angle = indicators.get("lr50_angle")
            mfi = indicators.get("mfi14")

            log.debug(f"🔍 [422 FREVERSEPLUS] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"rsi={rsi}, lr_angle={lr_angle}, mfi={mfi}, price={price}")

            if None in (rsi, lr_angle, mfi):
                return ("ignore", "недостаточно данных RSI, LR или MFI")

            if direction == "long":
                if 60 <= rsi <= 80 and lr_angle >= 0 and mfi <= 90:
                    return True
                return ("ignore", f"фильтр long не пройден: rsi={rsi}, lr_angle={lr_angle}, mfi={mfi}")

            elif direction == "short":
                if 20 <= rsi <= 40 and lr_angle >= 0 and mfi >= 10:
                    return True
                return ("ignore", f"фильтр short не пройден: rsi={rsi}, lr_angle={lr_angle}, mfi={mfi}")

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