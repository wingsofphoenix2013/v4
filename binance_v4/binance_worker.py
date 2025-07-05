# binance_worker.py

import logging
import json
from decimal import Decimal, ROUND_DOWN

from infra import infra
from strategy_registry import get_leverage, get_precision_for_symbol

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
# 🔸 Обработка открытия позиции с SL и TP
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
        opposite_side = "SELL" if side == "BUY" else "BUY"
        raw_quantity = float(event["quantity"])
        leverage = get_leverage(strategy_id)

        # 🔸 Округление quantity
        precision_qty = get_precision_for_symbol(symbol)
        quantize_mask = Decimal("1").scaleb(-precision_qty)
        rounded_qty = Decimal(str(raw_quantity)).quantize(quantize_mask, rounding=ROUND_DOWN)
        quantity = float(rounded_qty)

        log.info(f"📥 Открытие позиции: {side} {symbol} x {quantity} | плечо: {leverage}")

        # 🔸 Маржа: ISOLATED
        try:
            client.change_margin_type(symbol=symbol, marginType="ISOLATED")
            log.info(f"🧲 Маржа установлена: ISOLATED для {symbol}")
        except Exception as e:
            if "No need to change margin type" in str(e):
                log.debug(f"ℹ️ Маржа уже ISOLATED для {symbol}")
            else:
                log.warning(f"⚠️ Не удалось установить маржу ISOLATED для {symbol}: {e}")

        # 🔸 Плечо
        try:
            result = client.change_leverage(symbol=symbol, leverage=leverage)
            log.info(f"📌 Плечо установлено: {result['leverage']}x для {symbol}")
        except Exception as e:
            log.warning(f"⚠️ Не удалось установить плечо для {symbol}, используется по умолчанию: {e}")

        # 🔸 MARKET-ордер
        result = client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
        )

        log.info(f"✅ MARKET ордер отправлен: orderId={result['orderId']}, статус={result['status']}")

        # 🔸 SL
        sl_targets = json.loads(event.get("sl_targets", "[]"))
        active_sl = next((sl for sl in sl_targets if not sl.get("hit") and not sl.get("canceled") and sl.get("price")), None)

        if active_sl:
            stop_price = float(active_sl["price"])

            sl_order = client.new_order(
                symbol=symbol,
                side=opposite_side,
                type="STOP_MARKET",
                stopPrice=stop_price,
                closePosition=True,
                workingType="MARK_PRICE",
                timeInForce="GTC"
            )

            log.info(f"🛡️ SL установлен: {stop_price} (orderId={sl_order['orderId']})")
        else:
            log.info("ℹ️ SL не задан или отменён")

        # 🔸 TP
        tp_targets = json.loads(event.get("tp_targets", "[]"))
        for tp in tp_targets:
            if tp.get("price") and not tp.get("canceled") and not tp.get("hit"):
                tp_price = float(tp["price"])
                tp_qty = float(Decimal(str(tp["quantity"])).quantize(quantize_mask, rounding=ROUND_DOWN))

                tp_order = client.new_order(
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
        log.exception("❌ Ошибка при обработке события 'opened'")        
# 🔸 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или частичное закрытие


# 🔸 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: реализовать отмену SL/TP и закрытие позиции