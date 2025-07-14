# strategy_255_level1.py

import logging
import json
from datetime import datetime, timedelta
from infra import load_indicators, get_price

log = logging.getLogger("STRATEGY_255_LEVEL1")

class Strategy255Level1:
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

            log.debug(f"[255] symbol={symbol}, tf={tf}, direction={direction}, price={price}, adx={adx}, rsi={rsi}")

            if adx is None or rsi is None:
                return ("ignore", "недостаточно данных ADX или RSI")

            if adx <= 25:
                return ("ignore", f"фильтр ADX не пройден: adx={adx}")

            # 🔸 Проверка RSI
            if direction == "long":
                if not (55 < rsi < 80):
                    return ("ignore", f"фильтр RSI long не пройден: rsi={rsi}")
            elif direction == "short":
                if not (20 < rsi < 45):
                    return ("ignore", f"фильтр RSI short не пройден: rsi={rsi}")
            else:
                return ("ignore", f"неизвестное направление: {direction}")

            # 🔸 CandleCheck: последняя закрытая свеча
            received_at = signal.get("received_at")
            if not received_at:
                return ("ignore", "нет received_at в сигнале")

            dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
            tf_minutes = int(tf[1:]) if tf.startswith("m") else None
            if not tf_minutes:
                return ("ignore", f"неподдерживаемый таймфрейм: {tf}")

            current_open = dt.replace(second=0, microsecond=0)
            current_open -= timedelta(minutes=dt.minute % tf_minutes)
            candle_time = current_open - timedelta(minutes=tf_minutes)
            t_ms = int(candle_time.timestamp() * 1000)

            key_open = f"ts:{symbol}:{tf}:o"
            key_close = f"ts:{symbol}:{tf}:c"

            open_data = await redis.ts().range(key_open, t_ms, t_ms)
            close_data = await redis.ts().range(key_close, t_ms, t_ms)

            if not open_data or not close_data:
                return ("ignore", "нет данных open/close для свечи")

            open_val = float(open_data[0][1])
            close_val = float(close_data[0][1])

            log.debug(f"[255:CANDLE] time={t_ms}, open={open_val}, close={close_val}")

            if direction == "long":
                if close_val <= open_val:
                    return ("ignore", f"фильтр CandleCheck long не пройден: open={open_val}, close={close_val}")
            elif direction == "short":
                if close_val >= open_val:
                    return ("ignore", f"фильтр CandleCheck short не пройден: open={open_val}, close={close_val}")

            return True

        except Exception:
            log.exception("❌ Ошибка в strategy_255_level1")
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