# oracle_rsibins_snapshot_backfill.py — RSI-bins backfill: Этап A (каркас, выборка uid'ов батчами по 200)

import os
import asyncio
import logging

import infra

log = logging.getLogger("ORACLE_RSIBINS_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE        = int(os.getenv("RSI_BF_BATCH_SIZE", "200"))     # размер пачки UID'ов
MAX_CONCURRENCY   = int(os.getenv("RSI_BF_MAX_CONCURRENCY", "8"))  # зарезервировано на след. этап
SLEEP_MS          = int(os.getenv("RSI_BF_SLEEP_MS", "200"))       # пауза между пустыми выборками

# 🔸 SQL выборки кандидатов (закрытые, rsi_checked=false, валидные стратегии)
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.rsi_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

# 🔸 Выбрать пачку UID'ов кандидатов
async def _fetch_candidates(batch_size: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Основной цикл backfill'а (Этап A: только план, без обработки)
async def run_oracle_rsibins_snapshot_backfill():
    log.info("🚀 RSI-BINS BF: старт, batch=%d, sleep=%dms", BATCH_SIZE, SLEEP_MS)
    while True:
        try:
            uids = await _fetch_candidates(BATCH_SIZE)
            if not uids:
                # нет кандидатов — короткий сон и повтор
                await asyncio.sleep(SLEEP_MS / 1000)
                continue

            # На Этапе A — только логируем план; обработку добавим на следующем шаге
            log.info("[RSI-BINS BF] planned uids=%d (пример: %s)", len(uids), uids[:3])

            # Следующий этап добавит фактическую обработку этих UID'ов

        except asyncio.CancelledError:
            log.info("⏹️ RSI-BINS backfill остановлен")
            raise
        except Exception as e:
            log.exception("❌ RSI-BINS BF loop error: %s", e)
            await asyncio.sleep(1)