# position_handler.py

import json
import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from dataclasses import asdict

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

    # Параллельное получение цен
    prices_raw = await asyncio.gather(*(get_price(symbol) for symbol in symbols))
    price_snapshot: dict[str, Decimal] = {
        symbol: Decimal(str(price))
        for symbol, price in zip(symbols, prices_raw)
        if price is not None
    }

    # Обработка позиций
    to_remove = []

    for position in list(position_registry.values()):
        if position.status != "open" or position.quantity_left <= 0:
            continue

        price = price_snapshot.get(position.symbol)
        if price is None:
            continue

        await _process_tp_for_position(position, price)
        await _process_sl_for_position(position, price)

        if position.quantity_left == 0:
            to_remove.append((position.strategy_id, position.symbol))

    for key in to_remove:
        if key in position_registry:
            del position_registry[key]
# 🔸 Обработка TP для одной позиции
async def _process_tp_for_position(position, price: Decimal):
    for tp in sorted(position.tp_targets, key=lambda t: t.level):
        if not tp.hit and not tp.canceled:
            if tp.price is None:
                log.debug(f"⏸️ TP-{tp.level} активен без цены — ожидание: {position.uid}")
                return

            if position.direction == "long" and price >= tp.price:
                log.debug(f"✅ TP-{tp.level} достигнут (long) {position.symbol}: цена {price} ≥ {tp.price}")
                await _handle_tp_hit(position, tp, price)

            elif position.direction == "short" and price <= tp.price:
                log.debug(f"✅ TP-{tp.level} достигнут (short) {position.symbol}: цена {price} ≤ {tp.price}")
                await _handle_tp_hit(position, tp, price)

            break  # проверяем только один TP
# 🔸 Обработка SL для одной позиции
async def _process_sl_for_position(position, price: Decimal):
    active_sl = next(
        (sl for sl in position.sl_targets
         if not sl.hit and not sl.canceled and sl.price is not None),
        None
    )

    if not active_sl:
        return

    if position.direction == "long" and price > active_sl.price:
        return
    if position.direction == "short" and price < active_sl.price:
        return

    async with position.lock:
        now = datetime.utcnow()

        # 🔸 Отметить SL как исполненный
        active_sl.hit = True
        active_sl.hit_at = now

        # 🔸 Определить причину закрытия
        is_original_sl = active_sl.quantity == position.quantity
        reason = "full-sl-hit" if is_original_sl else "sl-tp-hit"

        # 🔸 Отменить все TP, которые ещё активны
        for tp in position.tp_targets:
            if not tp.hit and not tp.canceled:
                tp.canceled = True
                log.debug(f"🛑 TP отменён (SL-hit): {position.uid} (TP-{tp.level})")

        # 🔸 Расчёт PnL по текущему остатку
        qty = position.quantity_left
        entry = position.entry_price

        if position.direction == "long":
            pnl = (price - entry) * qty
        else:
            pnl = (entry - price) * qty

        pnl = pnl.quantize(Decimal("1.00"))
        position.pnl += pnl

        log.debug(f"💀 Позиция закрыта по SL {position.uid}: причина={reason}, цена={price}, pnl={pnl:+.2f}")

        # 🔸 Финализировать закрытие через централизованную функцию
        await _finalize_position_close(position, price, reason)
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

        log.debug(f"📍 TP-{tp.level} отмечен как выполненный для {position.uid} (цель: {tp.price}, исполнение: {price})")

        # 🔸 Обновление позиции
        precision_qty = config.tickers[position.symbol]["precision_qty"]
        quantize_mask = Decimal("1").scaleb(-precision_qty)

        closed_qty = tp.quantity.quantize(quantize_mask, rounding=ROUND_DOWN)
        position.quantity_left = (position.quantity_left - closed_qty).quantize(quantize_mask, rounding=ROUND_DOWN)

        position.planned_risk = Decimal("0")
        position.close_reason = f"tp-{tp.level}-hit"  # используется только как промежуточное состояние

        # 🔸 Расчёт PnL
        entry = position.entry_price
        pnl_delta = (entry - price if position.direction == "short" else price - entry) * closed_qty
        pnl_delta = pnl_delta.quantize(Decimal("1.00"))
        position.pnl += pnl_delta

        log.debug(f"💰 Обновление позиции {position.uid}: закрыто {closed_qty}, PnL = {pnl_delta:+.2f}")

        # 🔸 Обновление SL по политике
        sl_policy = next(
            (row for row in config.strategies[position.strategy_id]["sl_rules"]
             if row["level"] == tp.level),
            None
        )

        log.debug(f"📐 SL-политика для TP-{tp.level}: {sl_policy}")
        new_sl_price = None

        if sl_policy and sl_policy["sl_mode"] != "none":
            for sl in position.sl_targets:
                if not sl.hit and not sl.canceled:
                    sl.canceled = True
                    log.debug(f"🛑 SL отменён для {position.uid} (цель: {sl.price})")

            sl_mode = sl_policy["sl_mode"]

            if sl_mode == "entry":
                new_sl_price = position.entry_price
                log.debug(f"🧮 SL-режим entry → цена = {new_sl_price}")

            elif sl_mode == "percent":
                sl_value = Decimal(str(sl_policy["sl_value"]))
                delta = (position.entry_price * sl_value / 100).quantize(Decimal("0.0001"))
                new_sl_price = (
                    position.entry_price - delta if position.direction == "long"
                    else position.entry_price + delta
                )
                log.debug(f"🧮 SL-режим percent → delta = {delta}, цена = {new_sl_price}")

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
            log.debug(f"🛡️ SL установлен: {new_sl_price} для {position.uid}, объём: {position.quantity_left}")

        # 🔸 Подготовка события TP
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

        if new_sl_price is not None:
            event_data["sl_replaced"] = True
            event_data["new_sl_price"] = str(new_sl_price)
            event_data["new_sl_quantity"] = str(position.quantity_left)

        await infra.redis_client.xadd("positions_update_stream", {"data": json.dumps(event_data)})
        log.debug(f"📤 Событие TP-{tp.level} отправлено в positions_update_stream для {position.uid}")

        # 🔸 Финализация позиции, если она полностью закрыта
        if position.quantity_left == 0:
            await _finalize_position_close(position, price, reason="tp-full-hit")
# 🔸 Финализирует закрытие позиции — вызывается при TP на 100% или аналогичной ситуации
async def _finalize_position_close(position, exit_price: Decimal, reason: str):
    now = datetime.utcnow()

    # Обновление полей позиции
    precision_qty = config.tickers[position.symbol]["precision_qty"]
    quantize_mask = Decimal("1").scaleb(-precision_qty)

    position.quantity_left = Decimal("0").quantize(quantize_mask)
    position.planned_risk = Decimal("0")
    position.status = "closed"
    position.close_reason = reason
    position.exit_price = exit_price
    position.closed_at = now

    # Событие для Redis / CORE_IO
    event_data = {
        "event_type": "closed",
        "position_uid": str(position.uid),
        "strategy_id": position.strategy_id,
        "symbol": position.symbol,
        "exit_price": str(exit_price),
        "pnl": str(position.pnl),
        "close_reason": reason,
        "quantity_left": str(position.quantity_left),
        "planned_risk": str(position.planned_risk),
        "note": f"позиция закрыта по {reason} по цене {exit_price}"
    }

    # Если есть SL-цели (например, при TP-1 → SL), сериализуем их
    if position.sl_targets:
        event_data["sl_targets"] = json.dumps(
            [asdict(sl) for sl in position.sl_targets],
            default=str
        )

    await infra.redis_client.xadd("positions_update_stream", {"data": json.dumps(event_data)})
    log.debug(f"📤 Событие closed отправлено в positions_update_stream для {position.uid}")
# 🔸 Закрытие позиции по SL-защите (protect)
async def full_protect_stop(position):
    async with position.lock:
        # Отмена всех целей TP и SL
        for t in position.tp_targets + position.sl_targets:
            if not t.hit and not t.canceled:
                t.canceled = True

        # Получение текущей цены
        price = await get_price(position.symbol)
        if price is None:
            log.warning(f"❌ PROTECT: цена не получена для {position.symbol}, остановка невозможна")
            return

        price = Decimal(str(price))
        now = datetime.utcnow()

        # Закрытие позиции и установка финальных атрибутов
        position.status = "closed"
        position.closed_at = now
        position.exit_price = price
        position.close_reason = "sl-protect-stop"

        # Расчёт и фиксация PnL
        qty = position.quantity_left
        entry = position.entry_price

        if position.direction == "long":
            pnl = (price - entry) * qty
        else:
            pnl = (entry - price) * qty

        position.pnl += pnl.quantize(Decimal("1.00"))

        # Обнуление остатка и риска
        position.quantity_left = Decimal("0")
        position.planned_risk = Decimal("0")

        # Подготовка события для core_io
        event_data = {
            "event_type": "closed",
            "position_uid": str(position.uid),
            "strategy_id": position.strategy_id,
            "symbol": position.symbol,
            "exit_price": str(price),
            "pnl": str(position.pnl),
            "close_reason": "sl-protect-stop",
            "quantity_left": "0",
            "planned_risk": "0",
            "note": "позиция закрыта через SL-protect",
            "sl_targets": json.dumps(
                [asdict(sl) for sl in position.sl_targets],
                default=str
            ),
            "tp_targets": json.dumps(
                [asdict(tp) for tp in position.tp_targets],
                default=str
            )
        }

        await infra.redis_client.xadd("positions_update_stream", {
            "data": json.dumps(event_data)
        })

        log.debug(f"🔒 PROTECT: позиция {position.uid} закрыта через SL-protect")
# 🔸 Замена SL на цену входа при SL-protect
async def apply_sl_replacement(position):
    async with position.lock:
        # Поиск активного SL
        sl = next((
            s for s in position.sl_targets
            if not s.hit and not s.canceled and s.price is not None
        ), None)

        if not sl:
            log.warning(f"⚠️ PROTECT: не найден активный SL для позиции {position.uid}")
            return

        entry = position.entry_price
        sl_below_entry = (
            sl.price < entry if position.direction == "long"
            else sl.price > entry
        )

        if not sl_below_entry:
            log.info(f"🛡️ PROTECT: SL уже на входе или выше ({sl.price} vs {entry}) — замена не требуется")
            return

        # Отмена текущего SL
        sl.canceled = True

        # Создание нового SL на уровне entry
        new_sl = Target(
            type="sl",
            level=1,
            price=entry,
            quantity=sl.quantity,
            hit=False,
            hit_at=None,
            canceled=False
        )
        position.sl_targets.append(new_sl)
        position.planned_risk = Decimal("0")

        log.info(f"🛡️ PROTECT: SL заменён на уровень входа {entry} для позиции {position.uid}")

        # Подготовка события для core_io
        event_data = {
            "event_type": "sl_replaced",
            "position_uid": str(position.uid),
            "strategy_id": position.strategy_id,
            "symbol": position.symbol,
            "note": "SL переставлен на уровень entry",
            "planned_risk": "0",
            "sl_targets": json.dumps(
                [asdict(sl) for sl in position.sl_targets],
                default=str
            ),
            "logged_at": datetime.utcnow().isoformat()
        }

        await infra.redis_client.xadd("positions_update_stream", {
            "data": json.dumps(event_data)
        })
# 🔸 Главный воркер: проверка целей TP и SL
async def run_position_handler():
    while True:
        try:
            await _process_positions()
        except Exception:
            log.exception("❌ Ошибка в run_position_handler")
        await asyncio.sleep(1)