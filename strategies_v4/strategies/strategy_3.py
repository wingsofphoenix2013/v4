# strategies/strategy_3.py

import logging
log = logging.getLogger("STRATEGY_3")

class Strategy3:
    # 🔸 Метод валидации сигнала перед входом
    def validate_signal(self, signal, context) -> bool:
        log.info(f"⚙️ [Strategy3] Валидация сигнала: symbol={signal.get('symbol')}, direction={signal.get('direction')}")
        return True

    # 🔸 Основной метод запуска стратегии
    def run(self, signal, context):
        log.info("🚀 [Strategy3] Я — тестовая стратегия 3")
        return {"status": "ok"}