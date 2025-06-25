# strategy_521_reversepl.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_521_REVERSEPL")

class Strategy521Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, ["adx_dmi14_adx"], tf)
            adx = indicators.get("adx_dmi14_adx")

            log.debug(f"🔍 [521 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, adx={adx}")

            if adx is None:
                return ("ignore", "нет значения ADX")

            if adx > 25:
                return True
            return ("ignore", f"фильтр ADX не пройден: adx={adx}")

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