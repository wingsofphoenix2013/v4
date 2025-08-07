import logging
import json
from infra import get_price, load_indicators

log = logging.getLogger("strategy_800_universal")

class Strategy800Universal:
    async def validate_signal(self, signal, context):
        redis = context.get("redis")
        strategy_meta = context.get("strategy", {})
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = strategy_meta.get("timeframe", "m5").lower()

        if direction not in ("long", "short"):
            return ("ignore", f"❌ неизвестное направление: {direction}")

        try:
            # Получение цены
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "❌ нет текущей цены")

            # Загрузка BB-индикаторов
            if direction == "long":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_lower"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_lower = indicators.get("bb20_2_0_lower")

                if None in (bb_center, bb_lower):
                    return ("ignore", "❌ недостаточно данных BB (long)")

                bb_limit = bb_lower + (bb_center - bb_lower) * (1 / 3)
                if price <= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 фильтр BB long не пройден: price={price}, limit={bb_limit}")

            elif direction == "short":
                indicators = await load_indicators(symbol, ["bb20_2_0_center", "bb20_2_0_upper"], tf)
                bb_center = indicators.get("bb20_2_0_center")
                bb_upper = indicators.get("bb20_2_0_upper")

                if None in (bb_center, bb_upper):
                    return ("ignore", "❌ недостаточно данных BB (short)")

                bb_limit = bb_upper - (bb_upper - bb_center) * (1 / 3)
                if price >= bb_limit:
                    return True
                else:
                    return ("ignore", f"🟥 фильтр BB short не пройден: price={price}, limit={bb_limit}")

        except Exception:
            log.exception("❌ Ошибка в strategy_800_universal")
            return ("ignore", "❌ ошибка в стратегии")

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
            log.debug(f"📤 [800] Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ [800] Ошибка при отправке сигнала: {e}")