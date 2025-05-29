# strategies/strategy_2.py

import logging
log = logging.getLogger("STRATEGY_2")

class Strategy2:
    # 🔸 Метод валидации сигнала перед входом
    def validate_signal(self, signal, context) -> bool:
        log.info(f"⚙️ [Strategy2] Валидация сигнала: symbol={signal.get('symbol')}, direction={signal.get('direction')}")
        return True

    # 🔸 Основной метод запуска стратегии
    def run(self, signal, context):
        log.info("🚀 [Strategy2] Я — тестовая стратегия 2")
        return {"status": "ok"}