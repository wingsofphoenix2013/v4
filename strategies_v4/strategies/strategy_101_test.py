# strategy_101_test.py

import logging
import json
from datetime import datetime, timedelta
from infra import get_price

log = logging.getLogger("STRATEGY_101_TEST")

class Strategy101Test:
    async def validate_signal(self, signal, context):
        symbol = signal["symbol"]
        direction = signal["direction"].lower()
        tf = context["strategy"]["timeframe"].lower()
        redis = context["redis"]

        received_at = signal.get("received_at")
        if not received_at:
            return ("ignore", "нет received_at в сигнале")

        try:
            # 🔹 Преобразуем received_at в datetime
            dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))

            # 🔹 Интервал в минутах
            tf_minutes = int(tf[1:]) if tf.startswith("m") else None
            if not tf_minutes:
                return ("ignore", f"неподдерживаемый таймфрейм: {tf}")

            # 🔹 Последняя закрытая свеча
            current_open = dt.replace(second=0, microsecond=0)
            current_open -= timedelta(minutes=dt.minute % tf_minutes)
            candle_time = current_open - timedelta(minutes=tf_minutes)
            t_ms = int(candle_time.timestamp() * 1000)

            # 🔹 Ключи
            key_open = f"ts:{symbol}:{tf}:o"
            key_close = f"ts:{symbol}:{tf}:c"

            # 🔹 Получаем open и close с помощью TS.RANGE
            open_data = await redis.ts().range(key_open, t_ms, t_ms)
            close_data = await redis.ts().range(key_close, t_ms, t_ms)

            if not open_data or not close_data:
                return ("ignore", "нет данных open/close для последней свечи")

            open_val = float(open_data[0][1])
            close_val = float(close_data[0][1])

            log.info(f"[CANDLECHECK] symbol={symbol}, tf={tf}, time={t_ms}, open={open_val}, close={close_val}, direction={direction}")

            if direction == "long":
                if close_val > open_val:
                    return True
                return ("ignore", f"предыдущая свеча не бычья: open={open_val}, close={close_val}")

            elif direction == "short":
                if close_val < open_val:
                    return True
                return ("ignore", f"предыдущая свеча не медвежья: open={open_val}, close={close_val}")

            return ("ignore", f"неизвестное направление: {direction}")

        except Exception:
            log.exception("❌ Ошибка в проверке свечи")
            return ("ignore", "ошибка в проверке свечи")

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
            log.info(f"📤 Сигнал отправлен: {payload}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка при отправке сигнала: {e}")