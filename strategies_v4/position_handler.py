# position_handler.py

import asyncio
import logging
from datetime import datetime
from decimal import Decimal

from infra import get_price
from position_state_loader import position_registry

log = logging.getLogger("POSITION_HANDLER")

# 🔸 Обработка всех позиций (раз в секунду)
async def _process_positions():
    if not position_registry:
        return

    # Сбор уникальных тикеров
    symbols = {p.symbol for p in position_registry.values() if p.status == "open"}
    price_snapshot: dict[str, Decimal] = {}

    # Получение цен
    for symbol in symbols:
        price = await get_price(symbol)
        if price is not None:
            price_snapshot[symbol] = Decimal(str(price))

    # Обработка позиций
    for position in position_registry.values():
        if position.status != "open" or position.quantity_left <= 0:
            continue

        price = price_snapshot.get(position.symbol)
        if price is None:
            continue

        await _process_tp_for_position(position, price)
# 🔸 Обработка TP для одной позиции
async def _process_tp_for_position(position, price: Decimal):
    log.info(f"📊 Проверка TP: {position.symbol} (позиция {position.uid}) при цене {price}")

    active_tp = next((
        tp for tp in sorted(position.tp_targets, key=lambda t: t.level)
        if not tp.hit and not tp.canceled and tp.price is not None
    ), None)

    if not active_tp:
        log.info(f"ℹ️ Нет активных TP для позиции {position.uid}")
        return

    log.info(f"🎯 Активный TP-{active_tp.level}: {active_tp.price} для {position.uid}")

    # Проверка условия достижения TP
    if position.direction == "long" and price >= active_tp.price:
        log.info(f"✅ TP-{active_tp.level} достигнут (long): цена {price} ≥ {active_tp.price}")
        await _handle_tp_hit(position, active_tp, price)

    elif position.direction == "short" and price <= active_tp.price:
        log.info(f"✅ TP-{active_tp.level} достигнут (short): цена {price} ≤ {active_tp.price}")
        await _handle_tp_hit(position, active_tp, price)

    else:
        log.info(f"🔸 TP-{active_tp.level} не достигнут: текущая цена {price}")
# 🔸 Главный воркер: проверка целей TP и SL
async def run_position_handler():
    while True:
        try:
            await _process_positions()
        except Exception:
            log.exception("❌ Ошибка в run_position_handler")
        await asyncio.sleep(1)