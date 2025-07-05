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
# 🔸 Обработка открытия позиции (минимальная проверка подключения)
async def handle_opened(event: dict):
    client = infra.binance_client
    if client is None:
        log.warning("⚠️ Binance клиент не инициализирован, пропуск handle_opened")
        return

    try:
        balances = client.futures_account_balance()
        usdt = next((b for b in balances if b["asset"] == "USDT"), None)

        if usdt:
            log.info(f"💰 Binance USDT баланс: {usdt['balance']} | доступно: {usdt['availableBalance']}")
        else:
            log.warning("⚠️ Баланс USDT не найден")

    except Exception as e:
        log.exception("❌ Ошибка при запросе баланса на Binance Testnet")
# 🔸 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или частичное закрытие


# 🔸 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: реализовать отмену SL/TP и закрытие позиции