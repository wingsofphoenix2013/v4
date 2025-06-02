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


# 🔹 Заглушка: проверка TP
async def check_tp(position):
    log.info(f"[TP] Позиция {position.uid}: проверка TP (заглушка)")


# 🔹 Заглушка: проверка SL
async def check_sl(position):
    log.info(f"[SL] Позиция {position.uid}: проверка SL (заглушка)")


# 🔹 Заглушка: проверка защитного SL
async def check_protect(position):
    log.info(f"[PROTECT] Позиция {position.uid}: проверка защиты (заглушка)")