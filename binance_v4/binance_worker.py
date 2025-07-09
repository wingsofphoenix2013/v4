# binance_worker.py

import logging
from infra import infra
from strategy_registry import (
    get_precision_for_symbol,
    get_leverage,
    is_strategy_binance_enabled,
)
from binance_ws_v4 import filled_order_map
from core_io import insert_binance_order

log = logging.getLogger("BINANCE_WORKER")

# 🔸 Обработка события "opened": открытие MARKET-ордера на Binance
async def handle_open_position(payload: dict):
    strategy_id = int(payload["strategy_id"])
    symbol = payload["symbol"]
    direction = payload["direction"]
    quantity = float(payload["quantity"])
    position_uid = payload["position_uid"]

    # 🔸 Проверка: стратегия должна быть разрешена для Binance
    if not is_strategy_binance_enabled(strategy_id):
        log.debug(f"🚫 Стратегия {strategy_id} не разрешена для Binance — игнорируем сигнал")
        return

    side = "BUY" if direction == "long" else "SELL"
    qty_precision = get_precision_for_symbol(symbol)
    qty = round(quantity, qty_precision)
    qty_str = f"{qty:.{qty_precision}f}"

    leverage = get_leverage(strategy_id)

    log.info(f"⚙️ Установка плеча {leverage} и режима 'ISOLATED' для {symbol}")
    try:
        infra.binance_client.change_margin_type(symbol=symbol, marginType="ISOLATED")
        infra.binance_client.change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        log.debug(f"⚠️ Не удалось установить плечо/маржу для {symbol}: {e}")

    log.info(f"📤 Отправка MARKET-ордера: {symbol} {side} qty={qty_str} (strategy_id={strategy_id})")

    try:
        resp = infra.binance_client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=qty_str
        )
        order_id = resp["orderId"]

        filled_order_map[order_id] = {
            "strategy_id": strategy_id,
            "direction": direction,
            "quantity": quantity,
            "position_uid": position_uid
        }

        log.info(f"✅ Binance order sent: orderId={order_id}")

        # 🔸 Запись ордера в базу
        try:
            await insert_binance_order(
                position_uid=position_uid,
                strategy_id=strategy_id,
                symbol=symbol,
                binance_order_id=order_id,
                side=side,
                type_="MARKET",
                status="NEW",
                purpose="entry",
                level=None,
                price=None,
                quantity=qty,
                reduce_only=False,
                close_position=False,
                time_in_force=None,
                raw_data=resp
            )
        except Exception as db_exc:
            log.exception(f"⚠️ Ошибка при записи entry-ордера {order_id} в базу: {db_exc}")

    except Exception as e:
        log.exception(f"❌ Ошибка при открытии позиции: {e}")