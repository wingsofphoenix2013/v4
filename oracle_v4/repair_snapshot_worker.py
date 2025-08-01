# repair_snapshot_worker.py

import asyncio
import logging
from datetime import datetime

import infra

log = logging.getLogger("SNAPSHOT_REPAIR")
logging.basicConfig(level=logging.INFO)

EPSILON = 0.0005  # 0.05%

# 🔸 Нормализация ordering: сортировка EMA по числу, PRICE — в конец
def normalize_ordering(ordering: str) -> str:
    def sort_key(x):
        if x == "PRICE":
            return 999
        return int(x.replace("EMA", ""))

    groups = ordering.split(" > ")
    normalized = ["=".join(sorted(group.split("="), key=sort_key)) for group in groups]
    return " > ".join(normalized)

# 🔸 Основная функция
async def run_snapshot_repair():
    await infra.setup_pg()
    log.info("🛠️ Начинаем проверку и исправление ordering в oracle_ema_snapshot_v4")

    query = """
        SELECT symbol, interval, open_time, ordering
        FROM oracle_ema_snapshot_v4
        WHERE open_time >= now() - interval '1 day'
    """

    update_query = """
        UPDATE oracle_ema_snapshot_v4
        SET ordering = $1
        WHERE symbol = $2 AND interval = $3 AND open_time = $4
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        updated = 0

        for row in rows:
            current = row["ordering"]
            fixed = normalize_ordering(current)
            if fixed != current:
                await conn.execute(update_query, fixed, row["symbol"], row["interval"], row["open_time"])
                log.info(f"🔁 Обновлено: {row['symbol']} | {row['interval']} | {row['open_time']}")
                log.info(f"    до    : {current}")
                log.info(f"    после : {fixed}")
                updated += 1

        log.info(f"✅ Завершено. Обновлено записей: {updated}")