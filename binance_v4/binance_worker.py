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

# 🔸 Обработка открытия позиции
async def handle_opened(event: dict):
    client = infra.binance_client
    if client is None:
        log.warning("⚠️ Binance клиент не инициализирован, пропуск handle_opened")
        return

    try:
        strategy_id = int(event["strategy_id"])
        symbol = event["symbol"]
        direction = event["direction"]
        side = "BUY" if direction == "long" else "SELL"
        quantity = float(event["quantity"])
        leverage = get_leverage(strategy_id)

        log.info(f"📥 [opened] Стратегия {strategy_id} | {symbol} | side={side} | qty={quantity} | lev={leverage}")

        # 🔸 Пропущена смена маржи: предполагается, что уже ISOLATED
        log.info(f"ℹ️ Пропуск change_margin_type — предполагается ISOLATED уже установлен")

        # 🔸 Установка плеча
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
        log.info(f"📌 Плечо установлено: {leverage}x")

        # 🔸 Открытие позиции MARKET
        entry_order = client.futures_create_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
        )

        log.info(f"✅ Позиция открыта (orderId={entry_order['orderId']})")

        # 🔸 Установка SL
        sl_targets = json.loads(event.get("sl_targets", "[]"))
        active_sl = next((sl for sl in sl_targets if not sl["hit"] and not sl["canceled"] and sl["price"]), None)

        if active_sl:
            stop_price = float(active_sl["price"])
            opposite_side = "SELL" if side == "BUY" else "BUY"

            sl_order = client.futures_create_order(
                symbol=symbol,
                side=opposite_side,
                type="STOP_MARKET",
                stopPrice=stop_price,
                closePosition=True,
                timeInForce="GTC",
                workingType="MARK_PRICE"
            )

            log.info(f"🛡️ SL установлен: {stop_price} (orderId={sl_order['orderId']})")
        else:
            log.info("ℹ️ SL не задан или отменён")

        # 🔸 Установка TP целей
        tp_targets = json.loads(event.get("tp_targets", "[]"))
        for tp in tp_targets:
            if tp.get("price") and not tp.get("canceled") and not tp.get("hit"):
                tp_price = float(tp["price"])
                tp_qty = float(tp["quantity"])

                tp_order = client.futures_create_order(
                    symbol=symbol,
                    side=opposite_side,
                    type="LIMIT",
                    price=tp_price,
                    quantity=tp_qty,
                    reduceOnly=True,
                    timeInForce="GTC"
                )

                log.info(f"🎯 TP-{tp['level']} установлен: {tp_price}, qty={tp_qty}, orderId={tp_order['orderId']}")

    except Exception as e:
        log.exception(f"❌ Ошибка при обработке события 'opened': {e}")
# 🔸 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или частичное закрытие


# 🔸 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: реализовать отмену SL/TP и закрытие позиции