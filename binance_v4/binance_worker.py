# binance_worker.py

import logging
import json
from decimal import Decimal, ROUND_DOWN
from datetime import datetime

from infra import infra
from strategy_registry import get_leverage, get_precision_for_symbol, get_price_precision_for_symbol

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
        
# 🔸 Обработка открытия позиции на Binance (без TP/SL)
async def handle_opened(event: dict):
    client = infra.binance_client
    if client is None:
        log.warning("⚠️ Binance клиент не инициализирован, пропуск handle_opened")
        return

    try:
        log.info(f"📩 Получен сигнал на открытие позиции: {event}")

        strategy_id = int(event["strategy_id"])
        position_uid = event["position_uid"]
        symbol = event["symbol"]
        direction = event["direction"]
        side = "BUY" if direction == "long" else "SELL"
        raw_quantity = float(event["quantity"])

        leverage = get_leverage(strategy_id)

        # 🔸 Округление quantity
        precision_qty = get_precision_for_symbol(symbol)
        quantize_mask = Decimal("1").scaleb(-precision_qty)
        rounded_qty = Decimal(str(raw_quantity)).quantize(quantize_mask, rounding=ROUND_DOWN)
        quantity = float(rounded_qty)

        log.info(f"📥 Открытие позиции: {side} {symbol} x {quantity} | плечо: {leverage}")

        # 🔸 Установка маржи: ISOLATED
        try:
            client.change_margin_type(symbol=symbol, marginType="ISOLATED")
            log.info(f"🧲 Маржа установлена: ISOLATED для {symbol}")
        except Exception as e:
            if "No need to change margin type" in str(e):
                log.debug(f"ℹ️ Маржа уже ISOLATED для {symbol}")
            else:
                log.warning(f"⚠️ Не удалось установить маржу ISOLATED: {e}")

        # 🔸 Установка плеча
        try:
            result = client.change_leverage(symbol=symbol, leverage=leverage)
            log.info(f"📌 Плечо установлено: {result['leverage']}x для {symbol}")
        except Exception as e:
            log.warning(f"⚠️ Не удалось установить плечо: {e}")

        # 🔸 Отправка MARKET ордера
        result = client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=quantity
        )

        order_id = result["orderId"]
        log.info(f"✅ MARKET ордер отправлен: orderId={order_id}, статус={result['status']}")

        # 🔸 Кэширование позиции в infra
        infra.inflight_positions[position_uid] = {
            "strategy_id": strategy_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "order_id": order_id,
            "created_at": datetime.utcnow()
        }

        log.info(f"💾 Позиция временно сохранена в infra.inflight_positions: {position_uid}")

        # 🔸 Запись MARKET ордера в binance_orders_v4
        await infra.pg_pool.execute(
            """
            INSERT INTO binance_orders_v4 (
                position_uid, strategy_id, symbol, binance_order_id,
                side, type, status, purpose, quantity, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, 'MARKET', 'NEW', 'entry', $6, NOW()
            )
            """,
            position_uid, strategy_id, symbol, order_id, side, Decimal(quantity)
        )

        log.info(f"📝 Ордер записан в binance_orders_v4: {order_id} → {position_uid}")

    except Exception as e:
        log.exception("❌ Ошибка при обработке открытия позиции на Binance")
# 🔸 Обработка TP
async def handle_tp_hit(event: dict):
    log.info(f"🎯 [tp_hit] Стратегия {event.get('strategy_id')} | TP уровень: {event.get('tp_level')}")
    # TODO: реализовать обновление SL или частичное закрытие


# 🔸 Обработка закрытия позиции
async def handle_closed(event: dict):
    log.info(f"🔒 [closed] Стратегия {event.get('strategy_id')} | Причина: {event.get('close_reason')}")
    # TODO: реализовать отмену SL/TP и закрытие позиции