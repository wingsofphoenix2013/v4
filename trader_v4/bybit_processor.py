# bybit_processor.py — базовый воркер: читает positions_bybit_orders (только новые) и логирует краткую сводку

# 🔸 Импорты
import os
import json
import asyncio
import logging
from typing import Optional, Dict, Any

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("BYBIT_PROCESSOR")

# 🔸 Константы стримов/CG
ORDERS_STREAM = "positions_bybit_orders"
BYBIT_PROC_CG = "bybit_processor_cg"
BYBIT_PROC_CONSUMER = os.getenv("BYBIT_PROC_CONSUMER", "bybit-proc-1")

# 🔸 Параллелизм
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_PROC_MAX_TASKS", "200"))


# 🔸 Основной запуск воркера
async def run_bybit_processor():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(ORDERS_STREAM, BYBIT_PROC_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", BYBIT_PROC_CG, ORDERS_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", ORDERS_STREAM, BYBIT_PROC_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", BYBIT_PROC_CG, ORDERS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", BYBIT_PROC_CG, ORDERS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    # чтение из стрима в вечном цикле
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=BYBIT_PROC_CG,
                consumername=BYBIT_PROC_CONSUMER,
                streams={ORDERS_STREAM: ">"},
                count=100,
                block=1000,  # мс
            )
            if not entries:
                continue

            tasks = []
            for _, records in entries:
                for entry_id, fields in records:
                    tasks.append(asyncio.create_task(_handle_order_entry(sem, entry_id, fields)))

            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка чтения/обработки из стрима %s", ORDERS_STREAM)
            await asyncio.sleep(1)


# 🔸 Обработка одной записи из positions_bybit_orders (только лог, без бизнес-логики)
async def _handle_order_entry(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client

        try:
            # поле data содержит JSON с payload
            data_raw: Optional[str] = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")

            if not data_raw:
                log.info("⚠️ Пустой payload в %s (id=%s) — ACK", ORDERS_STREAM, entry_id)
                await redis.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
                return

            payload = json.loads(data_raw)

            # достаём ключевые поля (без падения, подстрахуемся .get)
            order_link_id = payload.get("order_link_id")
            position_uid = payload.get("position_uid")
            sid = payload.get("strategy_id")
            stype = payload.get("strategy_type")
            symbol = payload.get("symbol")
            direction = payload.get("direction")
            side = payload.get("side")
            lev = payload.get("leverage")
            qty = payload.get("qty")
            order_mode = payload.get("order_mode")
            src_stream_id = payload.get("source_stream_id")

            # логируем краткую сводку
            log.info(
                "📥 BYBIT PROC: order received "
                "[link=%s, sid=%s, type=%s, %s, side=%s, qty=%s, lev=%s, mode=%s, pos_uid=%s, src=%s]",
                order_link_id, sid, stype, symbol, side, qty, lev, order_mode, position_uid, src_stream_id
            )

            # здесь позже будет бизнес-логика: установка плеча/маржи/режима, dry_run/live, отправка ордера и т. п.

            # ACK после успешной обработки
            await redis.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)

        except Exception:
            # при ошибке парсинга всё равно ACK, чтобы не застревать хвостом; ошибка на уровне логирования
            log.exception("❌ Ошибка обработки записи (id=%s) — ACK", entry_id)
            try:
                await redis.xack(ORDERS_STREAM, BYBIT_PROC_CG, entry_id)
            except Exception:
                pass