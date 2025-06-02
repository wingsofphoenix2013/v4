# position_handler.py

import asyncio
import logging
from decimal import Decimal

from infra import infra
from position_state_loader import position_registry

# 🔸 Логгер для обработчика позиций
log = logging.getLogger("POSITION_HANDLER")


# 🔸 Универсальный безопасный доступ к полю цели (dict или Target)
def get_field(obj, field, default=None):
    return obj.get(field, default) if isinstance(obj, dict) else getattr(obj, field, default)


# 🔸 Главный цикл мониторинга всех позиций
async def run_position_monitor_loop():
    log.info("✅ [POSITION_HANDLER] Цикл мониторинга позиций запущен")
    while True:
        try:
            for position in list(position_registry.values()):
                asyncio.create_task(process_position(position))
            await asyncio.sleep(1)
        except Exception:
            log.exception("❌ [POSITION_HANDLER] Ошибка в основном цикле")


# 🔸 Обработка одной позиции под lock
async def process_position(position):
    async with position.lock:
        log.debug(f"🔒 [POSITION_HANDLER] LOCK: позиция {position.uid}")
        await check_tp(position)
        await check_sl(position)
        await check_protect(position)

# 🔸 Проверка TP-уровней позиции (по цене)
async def check_tp(position):
    active_tp = sorted(
        [
            t for t in position.tp_targets
            if get_field(t, "type") == "tp"
            and get_field(t, "source") == "price"
            and not get_field(t, "hit")
            and not get_field(t, "canceled")
        ],
        key=lambda t: get_field(t, "level")
    )

    if not active_tp:
        return

    tp = active_tp[0]

    redis = infra.redis_client
    mark_str = await redis.get(f"price:{position.symbol}")
    if not mark_str:
        log.warning(f"[TP] Позиция {position.uid}: не удалось получить цену markprice")
        return

    mark = Decimal(mark_str)
    tp_price = get_field(tp, "price")
    tp_level = get_field(tp, "level")

    log.debug(
        f"[TP-CHECK] Позиция {position.uid} | symbol={position.symbol} | mark={mark} "
        f"vs target={tp_price} (level {tp_level})"
    )

    if position.direction == "long" and mark < tp_price:
        return
    if position.direction == "short" and mark > tp_price:
        return

    # TP сработал
    qty = get_field(tp, "quantity")
    entry_price = position.entry_price
    pnl_gain = (tp_price - entry_price) * qty if position.direction == "long" else (entry_price - tp_price) * qty

    tp["hit"] = True
    tp["hit_at"] = datetime.utcnow()

    position.quantity_left -= qty
    position.planned_risk = Decimal("0")
    position.close_reason = f"tp-{tp_level}-hit"
    position.pnl += pnl_gain

    log.info(
        f"🎯 TP сработал: позиция {position.uid} | уровень {tp_level} | объём {qty} | pnl += {pnl_gain:.6f}"
    )
    log.info(f"📉 Остаток позиции: quantity_left = {position.quantity_left}")

    if position.quantity_left <= 0:
        position.status = "closed"
        position.exit_price = mark
        position.close_reason = "full-tp-hit"
        for sl in position.sl_targets:
            if not get_field(sl, "hit") and not get_field(sl, "canceled"):
                sl["canceled"] = True
        log.info(f"✅ Позиция {position.uid} полностью закрыта по TP")

# 🔸 Заглушка: проверка SL
async def check_sl(position):
    log.debug(f"[SL] Позиция {position.uid}: проверка SL (заглушка)")


# 🔸 Заглушка: проверка защитной логики
async def check_protect(position):
    log.debug(f"[PROTECT] Позиция {position.uid}: проверка защиты (заглушка)")