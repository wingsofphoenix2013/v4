# strategy_100_test.py

import logging
import json
from datetime import datetime, timedelta
from infra import get_price

log = logging.getLogger("STRATEGY_100_TEST")

class Strategy100Test:
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

            # 🔹 Определяем время двух последних закрытых свечей
            current_open = dt.replace(second=0, microsecond=0)
            current_open -= timedelta(minutes=dt.minute % tf_minutes)
            candle_1 = current_open - timedelta(minutes=tf_minutes)
            candle_2 = current_open - timedelta(minutes=2 * tf_minutes)

            # 🔹 Время в миллисекундах
            t1 = int(candle_1.timestamp() * 1000)
            t2 = int(candle_2.timestamp() * 1000)

            # 🔹 Запрос RSI значений из Redis TimeSeries
            rsi_key = f"ts_ind:{symbol}:{tf}:rsi14"
            rsi_data = await redis.ts().range(rsi_key, t2, t1)

            if not rsi_data or len(rsi_data) < 2:
                return ("ignore", "недостаточно точек RSI для анализа")

            # 🔹 Извлекаем значения ближе к t2 и t1
            rsi_2 = next((float(v) for ts, v in rsi_data if int(ts) <= t2), None)
            rsi_1 = next((float(v) for ts, v in reversed(rsi_data) if int(ts) <= t1), None)

            log.debug(f"🔍 [RSITREND] symbol={symbol}, tf={tf}, t2={t2}, t1={t1}, rsi_2={rsi_2}, rsi_1={rsi_1}")

            if rsi_1 is None or rsi_2 is None:
                return ("ignore", f"нет RSI значений на свечах t2={t2}, t1={t1}")

            if direction == "long" and rsi_1 > rsi_2:
                return True
            if direction == "short" and rsi_1 < rsi_2:
                return True

            return ("ignore", f"RSI не поддерживает тренд {direction}: rsi_2={rsi_2}, rsi_1={rsi_1}")

        except Exception:
            log.exception("❌ Ошибка в RSItrendcheck")
            return ("ignore", "ошибка RSItrendcheck")

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
            log.debug(f"📤 Сигнал отправ