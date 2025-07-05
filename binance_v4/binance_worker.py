# binance_worker.py

import logging
import json
from infra import infra
from strategy_registry import get_leverage

log = logging.getLogger("BINANCE_WORKER")


# 🔸 Обработка одного события из binance_стримов
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
# 🔸 Обработка открытия позиции (минимальный MARKET ордер)
async def handle_opened(event: dict):
    client = infra.binance_client
    if client is None:
        log.warning("⚠️ Binance клиент не инициализирован, пропуск handle_opened")
        return

    try:
        symbol = event.get("symbol", "BTCUSDT")  # ← можно хардкодить для теста
        quantity = float(event.get("quantity", 0.001))
        side = "BUY"

        log.info(f"📥 Отправка MARKET ордера: {side} {symbol} x {quantity}")

        result = client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
        )

        log.info(f"✅ Ордер отправлен: orderId={result['orderId']}, статус={result['status']}")

    except Exception as e:
        log.exception("❌ Ошибка при отправке MARKET-ордера")
# 🔸 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или частичное закрытие


# 🔸 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: реализовать отмену SL/TP и закрытие позиции