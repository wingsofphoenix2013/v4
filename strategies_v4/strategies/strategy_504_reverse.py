# strategy_504_reverse.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_504_REVERSE")

class Strategy504Reverse:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            indicators = await load_indicators(symbol, ["adx_dmi14_adx"], tf)
            adx = indicators.get("adx_dmi14_adx")

            if adx is None:
                return ("ignore", "нет значения ADX")

            log.debug(f"🔍 [504 REVERSE] symbol={symbol}, direction={direction}, tf={tf}, adx={adx}")

            if adx > 22:
                return True
            return ("ignore", f"adx={adx} <= 22")

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