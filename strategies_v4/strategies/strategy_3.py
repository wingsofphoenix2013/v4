# strategies/strategy_3.py

import logging
import json
from datetime import datetime

log = logging.getLogger("STRATEGY_3")

class Strategy3:
    # 🔸 Метод валидации сигнала перед входом
    def validate_signal(self, signal, context) -> bool:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = signal.get("strategy_id")
        signal_id = signal.get("signal_id")

        log.info(f"⚙️ [Strategy3] Валидация сигнала: symbol={symbol}, direction={direction}")

        if direction != "long":
            log.info(f"🚫 [Strategy3] Отклонено: только 'long' разрешён")

            redis = context.get("redis")
            if redis:
                log_record = {
                    "log_id": signal_id,
                    "strategy_id": strategy_id,
                    "status": "ignore",
                    "position_id": None,
                    "note": "отклонено: только long разрешён",
                    "logged_at": datetime.utcnow().isoformat()
                }
                try:
                    redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
                except Exception as e:
                    log.warning(f"⚠️ Ошибка записи в Redis log_queue: {e}")

            return False

        return True

    # 🔸 Основной метод запуска стратегии
    def run(self, signal, context):
        log.info("🚀 [Strategy3] Я — тестовая стратегия 3")
        return {"status": "ok"}