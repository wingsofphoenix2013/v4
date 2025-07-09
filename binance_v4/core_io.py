# core_io.py

import logging
import json
from decimal import Decimal, ROUND_DOWN

from infra import infra

log = logging.getLogger("CORE_IO")

# 🔸 Запись нового ордера в binance_orders_v4
async def insert_binance_order(
    *,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    binance_order_id: int,
    side: str,
    type_: str,
    status: str,
    purpose: str,
    quantity: Decimal,
    price: Decimal | None = None,
    level: int | None = None,
    reduce_only: bool | None = None,
    close_position: bool | None = None,
    time_in_force: str | None = None,
    raw_data: dict | None = None
):
    query = """
        INSERT INTO binance_orders_v4 (
            position_uid, strategy_id, symbol, binance_order_id,
            side, type, status, purpose,
            level, price, quantity,
            reduce_only, close_position, time_in_force,
            created_at, updated_at, raw_data
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, $11,
            $12, $13, $14,
            NOW(), NOW(), $15
        )
        ON CONFLICT (binance_order_id) DO NOTHING
    """
    try:
        raw_data_str = json.dumps(raw_data) if raw_data is not None else None

        if price is not None:
            price = price.quantize(Decimal("1.00000000"), rounding=ROUND_DOWN)
        quantity = quantity.quantize(Decimal("1.000"), rounding=ROUND_DOWN)

        await infra.pg_pool.execute(query,
            position_uid, strategy_id, symbol, binance_order_id,
            side, type_, status, purpose,
            level, price, quantity,
            reduce_only, close_position, time_in_force,
            raw_data_str
        )

        log.info(f"📥 Ордер записан: {binance_order_id} ({purpose})")

    except Exception as e:
        log.exception(f"❌ Ошибка insert_binance_order: {e}")
# 🔸 Запись открытой позиции в binance_positions_v4
async def insert_binance_position(
    *,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    direction: str,
    entry_price: Decimal,
    entry_time,
    leverage: int,
    position_side: str,
    executed_qty: Decimal,
    notional_value: Decimal,
    raw_data: dict | None = None
):
    query = """
        INSERT INTO binance_positions_v4 (
            position_uid, strategy_id, symbol, direction,
            entry_price, entry_time, leverage, position_side,
            executed_qty, notional_value,
            status, binance_update_ts, raw_data
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10,
            'open', NOW(), $11
        )
        ON CONFLICT (position_uid) DO NOTHING
    """
    try:
        raw_data_str = json.dumps(raw_data) if raw_data is not None else None
        entry_time_naive = entry_time.replace(tzinfo=None)

        entry_price = entry_price.quantize(Decimal("1.00000000"), rounding=ROUND_DOWN)
        executed_qty = executed_qty.quantize(Decimal("1.000"), rounding=ROUND_DOWN)
        notional_value = notional_value.quantize(Decimal("1.0000"), rounding=ROUND_DOWN)

        await infra.pg_pool.execute(query,
            position_uid, strategy_id, symbol, direction,
            entry_price, entry_time_naive, leverage, position_side,
            executed_qty, notional_value, raw_data_str
        )

        log.info(f"📌 Позиция записана: {position_uid}")

    except Exception as e:
        log.exception(f"❌ Ошибка insert_binance_position: {e}")
        
# 🔸 Обновление статуса ордера по событию WebSocket
async def update_binance_order_status(order_id: int, new_status: str):
    query = """
        UPDATE binance_orders_v4
        SET status = $1, updated_at = NOW()
        WHERE binance_order_id = $2
    """
    try:
        await infra.pg_pool.execute(query, new_status, order_id)
        log.info(f"🔄 Обновлён статус ордера {order_id} → {new_status}")
    except Exception as e:
        log.exception(f"❌ Ошибка при обновлении статуса ордера {order_id}: {e}")