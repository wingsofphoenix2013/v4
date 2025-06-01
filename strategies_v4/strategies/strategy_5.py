# strategies/strategy_5.py

import logging
import json
from datetime import datetime
from position_opener import open_position
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_5")

class Strategy5:
    # 🔸 Метод валидации сигнала перед входом
    async def validate_signal(self, signal, context) -> bool | str:
        symbol = signal.get("symbol")
        direction = signal.get("direction")
        strategy_id = int(signal.get("strategy_id"))
        log_id = signal.get("log_id")

        log.info(f"⚙️ [Strategy5] Валидация сигнала: symbol={symbol}, direction={direction}")

        # 🔹 Проверка направления
        if direction != "long":
            note = "отклонено: только long разрешён"
            log.info(f"🚫 [Strategy5] {note}")

            redis = context.get("redis")
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
                    log.warning(f"⚠️ [Strategy5] Ошибка записи в Redis log_queue: {e}")

            return "logged"

        return True

    # 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.info("🚀 [Strategy5] Я — тестовая стратегия 5")

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
                log.info(f"📤 [Strategy5] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy5] Ошибка при отправке в stream: {e}")