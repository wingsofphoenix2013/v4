# strategies/strategy_1.py

import logging
import json
from datetime import datetime
from position_opener import open_position  # ✅ корректный импорт с учётом Root Directory = strategies_v4

log = logging.getLogger("STRATEGY_1")

class Strategy1:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = signal.get("strategy_id")
        log_id = signal.get("log_id")

        log.info(f"⚙️ [Strategy1] Валидация сигнала: symbol={symbol}, direction={direction}")

        if direction != "long":
            log.info(f"🚫 [Strategy1] Отклонено: только 'long' разрешён")

            redis = context.get("redis")
            if redis:
                log_record = {
                    "log_id": log_id,
                    "strategy_id": strategy_id,
                    "status": "ignore",
                    "position_id": None,
                    "note": "отклонено: только long разрешён",
                    "logged_at": datetime.utcnow().isoformat()
                }
                try:
                    await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
                except Exception as e:
                    log.warning(f"⚠️ [Strategy1] Ошибка записи в Redis log_queue: {e}")

            return "logged"

        return True

    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.info("🚀 [Strategy1] Я — тестовая стратегия 1")
        result = await open_position(signal, self, context)
        return result