# position_handler.py

import asyncio
import logging
from position_state_loader import position_registry

# 🔸 Логгер для обработчика позиций
log = logging.getLogger("POSITION_HANDLER")

# 🔸 Основной цикл мониторинга всех позиций
async def run_position_monitor_loop():
    log.info("✅ [POSITION_HANDLER] Цикл мониторинга позиций запущен")
    while True:
        try:
            for position in list(position_registry.values()):
                asyncio.create_task(process_position(position))
            await asyncio.sleep(1)
        except Exception as e:
            log.exception("❌ [POSITION_HANDLER] Ошибка в основном цикле")

# 🔸 Обработка одной позиции под lock
async def process_position(position):
    async with position.lock:
        log.info(f"🔒 [POSITION_HANDLER] LOCK: позиция {position.uid}")
        await check_tp(position)
        await check_sl(position)
        await check_protect(position)

# 🔸 Проверка TP-уровней позиции (по цене)
async def check_tp(position):
    # Отбираем активные TP-цели с source='price', не hit и не canceled
    active_tp = sorted(
        [
            t for t in position.tp_targets
            if (getattr(t, "type", None) or t.get("type")) == "tp"
            and (getattr(t, "source", None) or t.get("source")) == "price"
            and not (getattr(t, "hit", False) or t.get("hit"))
            and not (getattr(t, "canceled", False) or t.get("canceled"))
        ],
        key=lambda t: getattr(t, "level", t.get("level"))
    )

    if not active_tp:
        return

    tp = active_tp[0]

    # Получаем текущую цену из Redis
    redis = infra.redis_client
    mark_str = await redis.get(f"price:{position.symbol}")
    if not mark_str:
        log.warning(f"[TP] Позиция {position.uid}: не удалось получить цену markprice")
        return

    mark = Decimal(mark_str)
    tp_price = getattr(tp, "price", tp.get("price"))
    tp_level = getattr(tp, "level", tp.get("level"))

    # Сравнение текущей цены и TP-уровня
    log.info(
        f"[TP-CHECK] Позиция {position.uid} | symbol={position.symbol} | mark={mark} "
        f"vs target={tp_price} (level {tp_level})"
    )

# 🔹 Заглушка: проверка SL
async def check_sl(position):
    log.info(f"[SL] Позиция {position.uid}: проверка SL (заглушка)")

# 🔹 Заглушка: проверка защитного SL
async def check_protect(position):
    log.info(f"[PROTECT] Позиция {position.uid}: проверка защиты (заглушка)")