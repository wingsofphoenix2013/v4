import logging
from infra import ENABLED_SIGNALS

# 🔸 Обработка одного сигнала из Redis Stream
async def process_signal(data: dict):
    log = logging.getLogger("PROCESSOR")

    symbol = data.get("symbol")
    message = data.get("message")

    if not symbol or not message:
        log.warning(f"Пропущен сигнал без symbol/message: {data}")
        return

    # 🔍 Определение направления сигнала
    direction = None
    signal_id = None

    for sid, phrases in ENABLED_SIGNALS.items():
        if message == phrases["long"]:
            direction = "long"
            signal_id = sid
            break
        elif message == phrases["short"]:
            direction = "short"
            signal_id = sid
            break

    if not direction:
        log.warning(f"Не удалось определить направление сигнала: {message}")
        return

    log.info(f"Распознан сигнал: {symbol} | {direction} | signal_id={signal_id}")