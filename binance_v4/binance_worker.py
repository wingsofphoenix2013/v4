# binance_worker.py

import logging

log = logging.getLogger("BINANCE_WORKER")


# 🔹 Обработка одного события из binance_стримов
async def process_binance_event(event: dict):
    event_type = event.get("event_type")
    strategy_id = event.get("strategy_id")
    symbol = event.get("symbol")

    log.info(f"🛠️ BINANCE WORKER: событие {event_type} для стратегии {strategy_id}, символ {symbol}")

    if event_type == "opened":
        await handle_opened(event)
    elif event_type == "tp_hit":
        await handle_tp_hit(event)
    elif event_type == "closed":
        await handle_closed(event)
    else:
        log.warning(f"⚠️ Неизвестный event_type: {event_type}")


# 🔹 Обработка открытия позиции
async def handle_opened(event: dict):
    log.info(f"📥 [opened] Стратегия {event.get('strategy_id')} | {event.get('symbol')} | Объём: {event.get('quantity')}")
    # TODO: реализовать отправку MARKET-ордера, SL и TP


# 🔹 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или закрытие позиции


# 🔹 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: отменить ордера на Binance