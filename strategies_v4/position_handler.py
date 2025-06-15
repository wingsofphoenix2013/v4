# position_handler.py

import json
import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from infra import infra, get_price
from config_loader import config
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
    to_remove = []

    for position in list(position_registry.values()):
        if position.status != "open" or position.quantity_left <= 0:
            continue

        price = price_snapshot.get(position.symbol)
        if price is None:
            continue

        await _process_tp_for_position(position, price)

        if position.quantity_left == 0:
            to_remove.append((position.strategy_id, position.symbol))

    for key in to_remove:
        del position_registry[key]
# 🔸 Обработка TP для одной позиции
async def _process_tp_for_position(position, price: Decimal):
    for tp in sorted(position.tp_targets, key=lambda t: t.level):
        if not tp.hit and not tp.canceled:
            if tp.price is None:
                log.info(f"⏸️ TP-{tp.level} активен без цены — ожидание: {position.uid}")
                return

            if position.direction == "long" and price >= tp.price:
                log.info(f"✅ TP-{tp.level} достигнут (long) {position.symbol}: цена {price} ≥ {tp.price}")
                await _handle_tp_hit(position, tp, price)

            elif position.direction == "short" and price <= tp.price:
                log.info(f"✅ TP-{tp.level} достигнут (short) {position.symbol}: цена {price} ≤ {tp.price}")
                await _handle_tp_hit(position, tp, price)

            # 🔸 Проверка на полное закрытие
            if position.quantity_left == 0:
                await _finalize_position_close(position, price, reason="full-tp-hit")

            break  # проверяем только один TP
# 🔸 Формирование текста события TP для логов и сериализации
def format_tp_hit_note(tp_level: int, price: Decimal, pnl: Decimal) -> str:
    price_str = f"{price:.4f}"
    pnl_str = f"{pnl:+.2f}"
    return f"сработал TP-{tp_level} по цене {price_str}, PnL = {pnl_str}"
# 🔸 Обработка достижения TP
async def _handle_tp_hit(position, tp, price: Decimal):
    async with position.lock:
        now = datetime.utcnow()

        tp.hit = True
        tp.hit_at = now

        log.info(f"📍 TP-{tp.level} отмечен как выполненный для {position.uid} (цель: {tp.price}, исполнение: {price})")

        # 🔸 Обновление позиции
        precision_qty = config.tickers[position.symbol]["precision_qty"]
        quantize_mask = Decimal("1").scaleb(-precision_qty)

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
        sl_policy = next(
            (row for row in config.strategies[position.strategy_id]["sl_rules"]
             if row["level"] == tp.level),
            None
        )

        log.info(f"📐 SL-политика для TP-{tp.level}: {sl_policy}")

        if sl_policy and sl_policy["sl_mode"] != "none":
            # Отмена всех активных SL
            for sl in position.sl_targets:
                if not sl.hit and not sl.canceled:
                    sl.canceled = True
                    log.info(f"🛑 SL отменён для {position.uid} (цель: {sl.price})")

            sl_mode = sl_policy["sl_mode"]

            if sl_mode == "entry":
                new_sl_price = position.entry_price
                log.info(f"🧮 SL-режим entry → цена = {new_sl_price}")

            elif sl_mode == "percent":
                sl_value = Decimal(str(sl_policy["sl_value"]))
                delta = (position.entry_price * sl_value / 100).quantize(Decimal("0.0001"))
                if position.direction == "long":
                    new_sl_price = (position.entry_price - delta)
                else:
                    new_sl_price = (position.entry_price + delta)
                log.info(f"🧮 SL-режим percent → delta = {delta}, цена = {new_sl_price}")

            else:
                log.warning(f"⚠️ SL-режим {sl_mode} не поддерживается")
                return

            new_sl = Target(
                type="sl",
                level=1,
                price=new_sl_price,
                quantity=position.quantity_left,
                hit=False,
                hit_at=None,
                canceled=False
            )
            position.sl_targets.append(new_sl)

            log.info(f"🛡️ SL установлен: {new_sl_price} для {position.uid}, объём: {position.quantity_left}")

        # 🔸 Отправка события в Redis
        note = format_tp_hit_note(tp.level, price, pnl_delta)

        event_data = {
            "event_type": "tp_hit",
            "position_uid": str(position.uid),
            "strategy_id": position.strategy_id,
            "symbol": position.symbol,
            "tp_level": tp.level,
            "quantity_left": str(position.quantity_left),
            "pnl": str(position.pnl),
            "close_reason": position.close_reason,
            "note": note,
        }

        if sl_policy and sl_policy["sl_mode"] != "none":
            event_data["sl_replaced"] = True
            event_data["new_sl_price"] = str(new_sl_price)
            event_data["new_sl_quantity"] = str(position.quantity_left)

        await infra.redis_client.xadd("positions_update_stream", {"data": json.dumps(event_data)})

        log.info(f"📤 Событие TP-{tp.level} отправлено в positions_update_stream для {position.uid}")
# 🔸 Финальное закрытие позиции
async def _finalize_position_close(position, price: Decimal, reason: str):
    now = datetime.utcnow()

    position.status = "closed"
    position.exit_price = price
    position.closed_at = now
    position.close_reason = reason

    # Удаляем из памяти
    key = (position.strategy_id, position.symbol)
    if key in position_registry:
        del position_registry[key]

    log.info(f"🔒 Позиция закрыта {position.uid}: причина={reason}, цена={price}, pnl={position.pnl}")

    # Формируем событие для core_io
    event_data = {
        "event_type": "closed",
        "position_uid": str(position.uid),
        "strategy_id": position.strategy_id,
        "symbol": position.symbol,
        "exit_price": str(price),
        "pnl": str(position.pnl),
        "close_reason": reason,
        "note": f"позиция закрыта по {reason} по цене {price}"
    }

    await infra.redis_client.xadd("positions_update_stream", {"data": json.dumps(event_data)})
    log.info(f"📤 Событие closed отправлено в positions_update_stream для {position.uid}")
# 🔸 Главный воркер: проверка целей TP и SL
async def run_position_handler():
    while True:
        try:
            await _process_positions()
        except Exception:
            log.exception("❌ Ошибка в run_position_handler")
        await asyncio.sleep(1)