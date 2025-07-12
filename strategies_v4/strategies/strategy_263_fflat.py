# strategy_263_fflat.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_263_FFLAT")

class Strategy263Fflat:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены для символа")

            indicators = await load_indicators(symbol, [
                "rsi14", "lr50_angle"
            ], tf)

            rsi = indicators.get("rsi14")
            lr_angle = indicators.get("lr50_angle")

            log.debug(f"🔍 [213 FFLAT] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"rsi={rsi}, lr_angle={lr_angle}, price={price}")

            if rsi is None or lr_angle is None:
                return ("ignore", "недостаточно данных RSI или LR")

            if direction == "long":
                if 55 <= rsi <= 80 and lr_angle >= 0:
                    return True
                return ("ignore", f"фильтр long не пройден: rsi={rsi}, lr_angle={lr_angle}")

            elif direction == "short":
                if 20 <= rsi <= 45 and lr_angle >= 0:
                    return True
                return ("ignore", f"фильтр short не пройден: rsi={rsi}, lr_angle={lr_angle}")

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