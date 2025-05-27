import logging

# 🔸 Обработка одного сигнала из Redis Stream
async def process_signal(data: dict):
    log = logging.getLogger("PROCESSOR")

    symbol = data.get("symbol")
    message = data.get("message")

    if not symbol or not message:
        log.warning(f"Пропущен сигнал без symbol/message: {data}")
        return

    log.info(f"Принят сигнал: {symbol} | {message}")