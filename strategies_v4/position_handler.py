# position_handler.py

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from infra import get_price
from config_loader import get_ticker_config, get_tp_sl_config
from position_state_loader import position_registry, Target

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
# 🔸 Обработка достижения TP
async def _handle_tp_hit(position, tp, price: Decimal):
    async with position.lock:
        now = datetime.utcnow()

        tp.hit = True
        tp.hit_at = now

        log.info(f"📍 TP-{tp.level} отмечен как выполненный для {position.uid} (цель: {tp.price}, исполнение: {price})")

        # 🔸 Обновление позиции
        precision_qty = get_ticker_config(position.symbol)["precision_qty"]
        quantize_mask = Decimal("1").scaleb(-precision_qty)

        # Округляем количество
        closed_qty = tp.quantity.quantize(quantize_mask, rounding=ROUND_DOWN)
        position.quantity_left = (position.quantity_left - closed_qty).quantize(quantize_mask, rounding=ROUND_DOWN)

        position.planned_risk = Decimal("0")
        position.close_reason = f"tp-{tp.level}-hit"

        # 🔸 Расчёт PnL
        entry = position.entry_price
        if position.direction == "long":
            pnl_delta = (price - entry) * closed_qty
        else:
            pnl_delta = (entry - price) * closed_qty

        pnl_delta = pnl_delta.quantize(Decimal("1.00"))
        position.pnl += pnl_delta

        log.info(f"💰 Обновление позиции {position.uid}: закрыто {closed_qty}, PnL = {pnl_delta:+.2f}")

        # 🔸 Обновление SL по политике
        sl_policy = get_tp_sl_config(position.strategy_id, tp.level)

        if sl_policy and sl_policy["sl_mode"] != "none":
            for sl in position.sl_targets:
                if not sl.hit and not sl.canceled:
                    sl.canceled = True
                    log.info(f"🛑 SL отменён для {position.uid}")

            sl_mode = sl_policy["sl_mode"]
            sl_value = Decimal(str(sl_policy["sl_value"]))

            if sl_mode == "entry":
                new_sl_price = position.entry_price

            elif sl_mode == "percent":
                delta = (position.entry_price * sl_value / 100).quantize(Decimal("0.0001"))
                if position.direction == "long":
                    new_sl_price = (position.entry_price - delta)
                else:
                    new_sl_price = (position.entry_price + delta)

            else:
                log.warning(f"⚠️ SL режим {sl_mode} пока не поддерживается")
                return

            new_sl = position.sl_target_type(
                type="sl",
                level=1,
                price=new_sl_price,
                quantity=position.quantity_left,
                hit=False,
                hit_at=None,
                canceled=False
            )
            position.sl_targets.append(new_sl)

            log.info(f"🛡️ Новый SL установлен: {new_sl_price} для {position.uid}")
# 🔸 Главный воркер: проверка целей TP и SL
async def run_position_handler():
    while True:
        try:
            await _process_positions()
        except Exception:
            log.exception("❌ Ошибка в run_position_handler")
        await asyncio.sleep(1)