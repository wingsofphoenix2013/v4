# oracle_bbbins_snapshot_backfill.py — BB-bins backfill: задержка старта, батчи, переиспользование логики

import os
import asyncio
import logging

import infra
from oracle_bbbins_snapshot_aggregator import (
    _load_position_and_strategy,   # FOR UPDATE + bb_checked/стратегия
    _load_bb_bins,                 # чтение BB20_2_0 upper/lower из PIS и биннинг entry_price
    _update_bb_aggregates,         # UPSERT в таблицы + Redis + bb_checked=true
)

log = logging.getLogger("ORACLE_BBBINS_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE        = int(os.getenv("BB_BF_BATCH_SIZE", "200"))
MAX_CONCURRENCY   = int(os.getenv("BB_BF_MAX_CONCURRENCY", "8"))
SLEEP_MS          = int(os.getenv("BB_BF_SLEEP_MS", "200"))
START_DELAY_SEC   = int(os.getenv("BB_BF_START_DELAY_SEC", "120"))

# 🔸 SQL кандидатов (закрытые, bb_checked=false, валидные стратегии)
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.bb_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

# 🔸 Выбрать пачку UID'ов
async def _fetch_candidates(batch_size: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Обработка одного UID (валидация → бины → апдейт)
async def _process_uid(uid: str):
    try:
        pos, strat, verdict = await _load_position_and_strategy(uid)
        v_code, v_reason = verdict
        if v_code != "ok":
            return ("skip", v_reason, uid)

        entry = pos["entry_price"]
        bins = await _load_bb_bins(uid, float(entry) if entry is not None else None)
        if not bins:
            return ("skip", "no_bb_bounds_or_entry", uid)

        await _update_bb_aggregates(pos, bins)
        return ("updated", bins, uid)

    except Exception as e:
        log.exception("❌ BB-BF uid=%s error: %s", uid, e)
        return ("error", "exception", uid)

# 🔸 Основной цикл backfill'а
async def run_oracle_bbbins_snapshot_backfill():
    if START_DELAY_SEC > 0:
        log.info("⏳ BB-BINS BF: задержка старта %d сек", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    log.info("🚀 BB-BINS BF: старт, batch=%d, max_conc=%d, sleep=%dms",
             BATCH_SIZE, MAX_CONCURRENCY, SLEEP_MS)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            uids = await _fetch_candidates(BATCH_SIZE)
            if not uids:
                await asyncio.sleep(SLEEP_MS / 1000)
                continue

            log.debug("[BB-BINS BF] planned uids=%d (пример: %s)", len(uids), uids[:3])

            results = []
            async def worker(u: str):
                async with gate:
                    res = await _process_uid(u)
                    results.append(res)

            await asyncio.gather(*[asyncio.create_task(worker(u)) for u in uids])

            updated = sum(1 for r in results if r[0] == "updated")
            skipped = sum(1 for r in results if r[0] == "skip")
            errors  = sum(1 for r in results if r[0] == "error")
            log.info("[BB-BINS BF] batch_done total=%d updated=%d skipped=%d errors=%d",
                     len(results), updated, skipped, errors)

        except asyncio.CancelledError:
            log.info("⏹️ BB-BINS backfill остановлен")
            raise
        except Exception as e:
            log.exception("❌ BB-BINS BF loop error: %s", e)
            await asyncio.sleep(1)