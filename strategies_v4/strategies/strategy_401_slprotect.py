# strategy_401_slprotect.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_401_SLPROTECT")

class Strategy401Slprotect:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            current_price = await get_price(symbol)
            if current_price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, ["ema21"], tf)
            ema21 = indicators.get("ema21")

            if ema21 is None:
                return ("ignore", "нет значения ema21")

            log.debug(f"🔍 [401 SLPROTECT] symbol={symbol}, direction={direction}, tf={tf}, current_price={current_price}, ema21={ema21}")

            if direction == "long":
                if current_price > ema21:
                    return True
                return ("ignore", f"current_price={current_price} <= ema21={ema21}")

            elif direction == "short":
                if current_price < ema21:
                    return True
                return ("ignore", f"current_price={current_price} >= ema21={ema21}")

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