# strategies/strategy_101.py

import logging
import json
from infra import load_indicators

log = logging.getLogger("STRATEGY_101")

class Strategy101:
    # 🔸 Валидация сигнала
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"]

        log.debug(f"⚙️ [Strategy101] Валидация сигнала: symbol={symbol}, direction={direction}")

        redis = context.get("redis")
        try:
            timeframe = "m5"
            indicators = await load_indicators(symbol, ["rsi14", "mfi14"], timeframe)
            price_raw = await redis.get(f"price:{symbol}")
            if price_raw is None:
                return ("ignore", "отклонено: отсутствует цена")

            rsi = indicators.get("rsi14")
            mfi = indicators.get("mfi14")
            if None in [rsi, mfi]:
                return ("ignore", "отклонено: недостаточно данных индикаторов")

            rsi = float(rsi)
            mfi = float(mfi)

            if direction == "long":
                if rsi >= 25:
                    return ("ignore", f"отклонено: RSI14 >= 25 (rsi={rsi})")
                if mfi >= 15:
                    return ("ignore", f"отклонено: MFI14 >= 15 (mfi={mfi})")
            elif direction == "short":
                if rsi <= 75:
                    return ("ignore", f"отклонено: RSI14 <= 75 (rsi={rsi})")
                if mfi <= 85:
                    return ("ignore", f"отклонено: MFI14 <= 85 (mfi={mfi})")

        except Exception as e:
            return ("ignore", f"ошибка при валидации фильтров: {e}")

        return True

    # 🔸 Запуск стратегии (будет реализовано позже)
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy101] Запуск стратегии на сигнале: {signal['symbol']} {signal['direction']}")