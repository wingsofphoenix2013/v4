# strategy_251_level1.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_251_LEVEL1")

class Strategy251Level1:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        redis = context["redis"]

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, ["adx_dmi14_adx", "rsi14"], tf)
            adx = indicators.get("adx_dmi14_adx")
            rsi = indicators.get("rsi14")

            log.debug(f"[251] symbol={symbol}, tf={tf}, direction={direction}, price={price}, adx={adx}, rsi={rsi}")

            if adx is None or rsi is None:
                return ("ignore", "недостаточно данных ADX или RSI")

            if adx <= 30:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if not (50 < rsi < 80):
                    return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")
                return True

            elif direction == "short":
                if not (20 < rsi < 50):
                    return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")
                return True

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy_251_level1")
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