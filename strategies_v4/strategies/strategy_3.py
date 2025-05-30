# strategies/strategy_3.py

import logging
import json
from datetime import datetime
from position_opener import open_position
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_3")

class Strategy3:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = int(signal.get("strategy_id"))
        log_id = signal.get("log_id")

        log.info(f"⚙️ [Strategy3] Валидация сигнала: symbol={symbol}, direction={direction}")

        redis = context.get("redis")
        try:
            # 🔹 Получаем таймфрейм из конфигурации стратегии
            timeframe = config.strategies[strategy_id]["meta"]["timeframe"]

            # 🔹 Загружаем rsi14 из Redis
            ind = await load_indicators(symbol, ["rsi14"], timeframe)
            rsi = ind.get("rsi14")

            if rsi is None:
                note = "не удалось получить rsi14"
            elif direction == "long" and rsi >= 55:
                return True
            elif direction == "short" and rsi <= 35:
                return True
            else:
                note = f"rsi={rsi:.2f} не соответствует направлению: {direction}"

        except Exception as e:
            note = f"ошибка при получении индикатора: {e}"

        # 🔹 Логируем отклонение
        log.info(f"🚫 [Strategy3] {note}")
        if redis:
            log_record = {
                "log_id": log_id,
                "strategy_id": strategy_id,
                "status": "ignore",
                "position_id": None,
                "note": note,
                "logged_at": datetime.utcnow().isoformat()
            }
            try:
                await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
            except Exception as e:
                log.warning(f"⚠️ [Strategy3] Ошибка записи в Redis log_queue: {e}")

        return "logged"

        return True
    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.info("🚀 [Strategy3] Я — тестовая стратегия 3")

        redis = context.get("redis")
        if redis:
            payload = {
                "strategy_id": signal["strategy_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "log_id": signal["log_id"],
                "route": "new_entry"
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.info(f"📤 [Strategy3] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy3] Ошибка при отправке в stream: {e}")