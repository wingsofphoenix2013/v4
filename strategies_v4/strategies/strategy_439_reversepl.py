# strategy_439_reversepl.py

import logging
import json
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_439_REVERSEPL")

class Strategy439Reversepl:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "ema21", "rsi14",
                "adx_dmi14_plus_di", "adx_dmi14_minus_di"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            ema21 = indicators.get("ema21")
            rsi = indicators.get("rsi14")
            plus_di = indicators.get("adx_dmi14_plus_di")
            minus_di = indicators.get("adx_dmi14_minus_di")

            log.debug(f"🔍 [439 REVERSEPL] symbol={symbol}, direction={direction}, tf={tf}, "
                      f"adx={adx}, ema21={ema21}, rsi={rsi}, price={price}, DI+={plus_di}, DI-={minus_di}")

            if None in (adx, ema21, rsi, plus_di, minus_di):
                return ("ignore", "недостаточно данных ADX, EMA21, RSI или DMI")

            if not (25 <= adx <= 40):
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if price > ema21 and 55 <= rsi <= 75 and plus_di > minus_di:
                    return True
                return ("ignore", f"фильтр long не пройден: price={price}, ema21={ema21}, rsi={rsi}, DI+={plus_di}, DI-={minus_di}")

            elif direction == "short":
                if price < ema21 and 25 <= rsi <= 45 and minus_di > plus_di:
                    return True
                return ("ignore", f"фильтр short не пройден: price={price}, ema21={ema21}, rsi={rsi}, DI-={minus_di}, DI+={plus_di}")

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