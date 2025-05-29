# strategies/strategy_1.py

import logging
log = logging.getLogger("STRATEGY_1")

class Strategy1:
    # 🔸 Метод валидации сигнала перед входом
    def validate_signal(self, signal, context) -> bool:
        log.info(f"⚙️ [Strategy1] Валидация сигнала: symbol={signal.get('symbol')}, direction={signal.get('direction')}")
        return True

    # 🔸 Основной метод запуска стратегии
    def run(self, signal, context):
        log.info("🚀 [Strategy1] Я — тестовая стратегия 1")
        return {"status": "ok"}