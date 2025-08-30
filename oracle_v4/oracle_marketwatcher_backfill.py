# 🔸 oracle_marketwatcher_backfill.py — Этап 1: часовик с задержкой старта (2 мин), поиск кандидатов без апдейтов

import asyncio
import logging
from datetime import datetime, timezone

import infra

BATCH_LIMIT = 500  # только логируем кандидатов, без обработки
START_DELAY_SEC = 120  # пауза перед первым прогоном

log = logging.getLogger("ORACLE_MW_BF")


# 🔸 Один проход: найти закрытые, но не учтённые позиции и залогировать выборку
async def run_oracle_marketwatcher_backfill_once():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT position_uid, strategy_id, symbol, direction, created_at
            FROM positions_v4
            WHERE status = 'closed'
              AND COALESCE(mrk_watcher_checked, false) = false
            ORDER BY created_at ASC
            LIMIT $1
        """, BATCH_LIMIT)

    cnt = len(rows)
    sample = [r["position_uid"] for r in rows[:10]]
    log.info("[BF-STAGE1] candidates=%d sample=%s", cnt, sample)


# 🔸 Периодический цикл: старт с задержкой 2 минуты, затем каждый час
async def run_oracle_marketwatcher_backfill_periodic():
    log.info("🚀 Этап 1 (BF): старт через %d сек, затем каждый час", START_DELAY_SEC)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            await run_oracle_marketwatcher_backfill_once()
        except asyncio.CancelledError:
            log.info("⏹️ Бэкофилл остановлен")
            raise
        except Exception as e:
            log.exception("❌ Ошибка в бэкофилле: %s", e)

        await asyncio.sleep(3600)  # 1 час