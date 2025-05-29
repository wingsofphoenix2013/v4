# position_opener.py

import logging
log = logging.getLogger("POSITION_OPENER")

# 🔸 Основная функция открытия позиции (заглушка)
async def open_position(signal: dict, strategy_obj, context: dict) -> dict:
    symbol = signal.get("symbol")
    direction = signal.get("direction")
    strategy_id = signal.get("strategy_id")

    log.info(f"📥 [OPEN_POSITION] Открытие позиции: strategy={strategy_id}, symbol={symbol}, direction={direction}")
    # Здесь позже будет: расчёт размера, регистрация позиции, публикация события

    return {"status": "opened (mock)"}