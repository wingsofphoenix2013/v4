# emasnapshot_worker.py

import asyncio
import logging

from infra import pg_pool

# 🔸 Логгер
log = logging.getLogger("EMASNAPSHOT_WORKER")

# 🔸 Основная точка входа воркера
async def run_emasnapshot_worker():
    log.info("🚀 Воркер EMA Snapshot запущен")

    async with pg_pool.acquire() as conn:
        # Получаем все стратегии, где включён флаг emasnapshot
        strategies = await conn.fetch("""
            SELECT id FROM strategies_v4
            WHERE emasnapshot = true
        """)

        strategy_ids = [row["id"] for row in strategies]
        log.info(f"🔍 Найдено стратегий с emasnapshot = true: {len(strategy_ids)}")

        if not strategy_ids:
            log.info("⛔ Стратегий для анализа не найдено")
            return

        # Получаем позиции по этим стратегиям
        positions = await conn.fetch("""
            SELECT id FROM positions_v4
            WHERE strategy_id = ANY($1)
              AND status = 'closed'
              AND emasnapshot_checked = false
        """, strategy_ids)

        log.info(f"📦 Найдено позиций для обработки: {len(positions)}")