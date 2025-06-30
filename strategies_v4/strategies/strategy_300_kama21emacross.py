# strategy_300_kama21emacross.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_300_KAMA21EMACROSS")

class Strategy300Kama21emacross:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, ["adx_dmi14_adx", "rsi14"], tf)
            adx = indicators.get("adx_dmi14_adx")
            rsi = indicators.get("rsi14")

            log.debug(f"🔍 [300 KAMA21EMACROSS] symbol={symbol}, direction={direction}, tf={tf}, adx={adx}, rsi={rsi}")

            if adx is None or rsi is None:
                return ("ignore", "нет значения ADX или RSI")

            if adx <= 25:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if rsi > 50:
                    return True
                return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")

            elif direction == "short":
                if rsi < 50:
                    return True
                return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")

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