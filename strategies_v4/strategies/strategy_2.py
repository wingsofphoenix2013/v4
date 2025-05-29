# strategies/strategy_2.py

import logging
import json
from datetime import datetime

log = logging.getLogger("STRATEGY_2")

class Strategy2:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = signal.get("strategy_id")
        signal_id = signal.get("signal_id")

        log.info(f"⚙️ [Strategy2] Валидация сигнала: symbol={symbol}, direction={direction}")

        if direction != "short":
            log.info(f"🚫 [Strategy2] Отклонено: только 'short' разрешён")

            redis = context.get("redis")
            if redis:
                log_record = {
                    "log_id": signal_id,
                    "strategy_id": strategy_id,
                    "status": "ignore",
                    "position_id": None,
                    "note": "отклонено: только short разрешён",
                    "logged_at": datetime.utcnow().isoformat()
                }
                try:
                    await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
                except Exception as e:
                    log.warning(f"⚠️ [Strategy2] Ошибка записи в Redis log_queue: {e}")

            return False

        return True

    # 🔸 Основной метод запуска стратегии
    def run(self, signal, context):
        log.info("🚀 [Strategy2] Я — тестовая стратегия 2")
        return {"status": "ok"}