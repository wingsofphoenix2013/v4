# binance_v4/binance_worker.py

import logging
from infra import infra
from strategy_registry import (
    get_precision_for_symbol,
    is_strategy_binance_enabled,
)

log = logging.getLogger("BINANCE_WORKER")


# 🔸 Обработка события "opened": открытие MARKET-ордера на Binance
async def handle_open_position(payload: dict):
    strategy_id = int(payload["strategy_id"])
    symbol = payload["symbol"]
    direction = payload["direction"]
    quantity = float(payload["quantity"])

    # 🔸 Проверка: стратегия должна быть разрешена для Binance
    if not is_strategy_binance_enabled(strategy_id):
        log.warning(f"🚫 Стратегия {strategy_id} не разрешена для Binance — игнорируем сигнал")
        return

    side = "BUY" if direction == "long" else "SELL"
    qty_precision = get_precision_for_symbol(symbol)
    qty = round(quantity, qty_precision)
    qty_str = f"{qty:.{qty_precision}f}"

    log.info(f"📤 Отправка MARKET-ордера: {symbol} {side} qty={qty_str} (strategy_id={strategy_id})")

    try:
        resp = infra.binance_client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=qty_str
        )
        log.info(f"✅ Binance order sent: orderId={resp['orderId']}")

        # 🔸 TODO: ждать подтверждение FILLED через WS
        # 🔸 TODO: на основе avgPrice рассчитать и выставить TP/SL

    except Exception as e:
        log.exception(f"❌ Ошибка при открытии позиции: {e}")