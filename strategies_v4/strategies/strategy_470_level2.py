# strategy_470_level2.py

import logging
import json
from datetime import datetime, timedelta
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_470_LEVEL2")

class Strategy470Level2:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        redis = context["redis"]

        try:
            price = await get_price(symbol)
            if price is None:
                return ("ignore", "нет текущей цены")

            indicators = await load_indicators(symbol, [
                "adx_dmi14_adx", "rsi14",
                "bb20_2_0_center", "bb20_2_0_upper", "bb20_2_0_lower",
                "bb20_2_5_upper", "bb20_2_5_lower"
            ], tf)

            adx = indicators.get("adx_dmi14_adx")
            rsi = indicators.get("rsi14")
            bb_center = indicators.get("bb20_2_0_center")
            bb_upper = indicators.get("bb20_2_0_upper")
            bb_lower = indicators.get("bb20_2_0_lower")
            bb_upper_25 = indicators.get("bb20_2_5_upper")
            bb_lower_25 = indicators.get("bb20_2_5_lower")

            log.debug(f"[470] symbol={symbol}, tf={tf}, direction={direction}, price={price}, "
                      f"adx={adx}, rsi={rsi}, bb_center={bb_center}, bb_upper={bb_upper}, "
                      f"bb_lower={bb_lower}, bb_upper_25={bb_upper_25}, bb_lower_25={bb_lower_25}")

            if None in (adx, rsi, bb_center, bb_upper, bb_lower, bb_upper_25, bb_lower_25):
                return ("ignore", "недостаточно данных BB, ADX или RSI")

            if adx <= 30:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            if direction == "long":
                if not (55 < rsi < 80):
                    return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")
                bb_limit_lower = bb_center + (bb_upper - bb_center) / 3
                if price > bb_upper_25 or price < bb_limit_lower:
                    return ("ignore", f"фильтр BB long не пройден: price={price}, upper_25={bb_upper_25}, bb_limit_lower={bb_limit_lower}")

            elif direction == "short":
                if not (20 < rsi < 45):
                    return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")
                bb_limit_upper = bb_center - (bb_center - bb_lower) / 3
                if price < bb_lower_25 or price > bb_limit_upper:
                    return ("ignore", f"фильтр BB short не пройден: price={price}, lower_25={bb_lower_25}, bb_limit_upper={bb_limit_upper}")

            else:
                return ("ignore", f"неизвестное направление: {direction}")

            # 🔹 RSItrendcheck
            received_at = signal.get("received_at")
            if not received_at:
                return ("ignore", "нет received_at в сигнале")

            dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
            tf_minutes = int(tf[1:]) if tf.startswith("m") else None
            if not tf_minutes:
                return ("ignore", f"неподдерживаемый таймфрейм: {tf}")

            current_open = dt.replace(second=0, microsecond=0)
            current_open -= timedelta(minutes=dt.minute % tf_minutes)
            candle_1 = current_open - timedelta(minutes=tf_minutes)
            candle_2 = candle_1 - timedelta(minutes=tf_minutes)
            t1 = int(candle_1.timestamp() * 1000)
            t2 = int(candle_2.timestamp() * 1000)

            rsi_key = f"ts_ind:{symbol}:{tf}:rsi14"
            rsi_data = await redis.ts().range(rsi_key, t2, t1)

            if not rsi_data or len(rsi_data) < 2:
                return ("ignore", "недостаточно точек RSI для анализа тренда")

            rsi_2 = next((float(v) for ts, v in rsi_data if int(ts) <= t2), None)
            rsi_1 = next((float(v) for ts, v in reversed(rsi_data) if int(ts) <= t1), None)

            log.debug(f"[470:RSITREND] rsi_2={rsi_2}, rsi_1={rsi_1}")

            if rsi_1 is None or rsi_2 is None:
                return ("ignore", f"нет значений RSI на свечах t2={t2}, t1={t1}")

            if direction == "long" and rsi_1 <= rsi_2:
                return ("ignore", f"фильтр RSItrend long не пройден: rsi_2={rsi_2}, rsi_1={rsi_1}")
            if direction == "short" and rsi_1 >= rsi_2:
                return ("ignore", f"фильтр RSItrend short не пройден: rsi_2={rsi_2}, rsi_1={rsi_1}")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_470_level2")
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