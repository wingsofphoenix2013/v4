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
    active_tp = next((
        tp for tp in sorted(position.tp_targets, key=lambda t: t.level)
        if not tp.hit and not tp.canceled and tp.price is not None
    ), None)

    if not active_tp:
        return

    if position.direction == "long" and price >= active_tp.price:
        log.info(f"✅ TP-{active_tp.level} достигнут (long) {position.symbol}: цена {price} ≥ {active_tp.price}")
        await _handle_tp_hit(position, active_tp, price)

    elif position.direction == "short" and price <= active_tp.price:
        log.info(f"✅ TP-{active_tp.level} достигнут (short) {position.symbol}: цена {price} ≤ {active_tp.price}")
        await _handle_tp_hit(position, active_tp, price)
# 🔸 Заглушка обработки TP-срабатывания
async def _handle_tp_hit(position, tp, price: Decimal):
    log.info(f"🟡 [ЗАГЛУШКА] TP-{tp.level} достигнут для позиции {position.uid}, цена: {price}")
    await asyncio.sleep(0)  # заглушка на async совместимость
# 🔸 Главный воркер: проверка целей TP и SL
async def run_position_handler():
    while True:
        try:
            await _process_positions()
        except Exception:
            log.exception("❌ Ошибка в run_position_handler")
        await asyncio.sleep(1)