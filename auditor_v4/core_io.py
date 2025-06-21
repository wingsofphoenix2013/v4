# core_io.py

import asyncio
import logging
import infra

# 🔸 Логгер для PostgreSQL операций
log = logging.getLogger("CORE_IO")


# 🔸 Загрузка неаудированных закрытых позиций
async def load_unprocessed_positions(limit: int = 100) -> list[dict]:
    log.info("📥 Загрузка неаудированных позиций из базы...")
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT *
            FROM positions_v4_test
            WHERE status = 'closed' AND audited = false
            ORDER BY created_at
            LIMIT $1
        """, limit)
    log.info(f"📊 Загружено {len(rows)} позиций на аудит")
    return [dict(r) for r in rows]


# 🔸 Основной воркер PostgreSQL
async def pg_task(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            log.info("🔁 Начало аудиторского прохода")
            positions = await load_unprocessed_positions()

            if not positions:
                log.info("✅ Нет новых позиций для аудита — пауза")
                await asyncio.sleep(60)
                continue

            # Здесь позже появится логика обработки позиций

            log.info("⏸ Пауза до следующего цикла")
            await asyncio.sleep(60)

        except Exception:
            log.exception("❌ Ошибка в pg_task — продолжаем выполнение")
            await asyncio.sleep(5)