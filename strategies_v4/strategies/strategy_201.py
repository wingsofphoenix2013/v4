# strategies/strategy_201.py
import logging
import json
from datetime import datetime
from infra import load_indicators
from config_loader import config

log = logging.getLogger("STRATEGY_201")

class Strategy201:
    # 🔸 Метод валидации сигнала перед входом (пропускает всё)
    async def validate_signal(self, signal, context) -> bool | str:
        return True
# 🔸 Основной метод запуска стратегии
    async def run(self, signal, context):
        log.debug(f"🚀 [Strategy201] Запуск стратегии на сигнале: symbol={signal['symbol']}, direction={signal['direction']}")

        redis = context.get("redis")
        if redis:
            payload = {
                "strategy_id": signal["strategy_id"],
                "symbol": signal["symbol"],
                "direction": signal["direction"],
                "log_uid": signal["log_uid"],
                "route": "new_entry"
            }
            try:
                await redis.xadd("strategy_opener_stream", {"data": json.dumps(payload)})
                log.debug(f"📤 [Strategy201] Сигнал отправлен в strategy_opener_stream")
            except Exception as e:
                log.warning(f"⚠️ [Strategy201] Ошибка при отправке в stream: {e}")