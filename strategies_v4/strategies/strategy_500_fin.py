# strategy_500_fin.py

import logging
import json

log = logging.getLogger("STRATEGY_500_FIN")

class Strategy500Fin:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, ["ema200"], tf)
            ema200 = indicators.get("ema200")

            log.debug(f"[EMA500] symbol={symbol}, tf={tf}, direction={direction}, price={price}, ema200={ema200}")

            if ema200 is None:
                return ("ignore", "нет значения EMA200")

            if direction == "long":
                if price > ema200:
                    return True
                return ("ignore", f"фильтр EMA long не пройден: price={price}, ema200={ema200}")

            elif direction == "short":
                if price < ema200:
                    return True
                return ("ignore", f"фильтр EMA short не пройден: price={price}, ema200={ema200}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в strategy (EMA200-only)")
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