# oracle_adxbins_snapshot_backfill.py — ADX-bins backfill: задержка старта, батчи, параллельная обработка

import os
import asyncio
import logging

import infra
from oracle_adxbins_snapshot_aggregator import (
    _load_position_and_strategy,   # FOR UPDATE + adx_checked/стратегия
    _load_adx_bins,                # чтение ADX из PIS и биннинг 0..100 шагом 5
    _update_adx_aggregates,        # UPSERT в таблицы + Redis + adx_checked=true
)

log = logging.getLogger("ORACLE_ADXBINS_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE        = int(os.getenv("ADX_BF_BATCH_SIZE", "200"))
MAX_CONCURRENCY   = int(os.getenv("ADX_BF_MAX_CONCURRENCY", "8"))
SLEEP_MS          = int(os.getenv("ADX_BF_SLEEP_MS", "200"))
START_DELAY_SEC   = int(os.getenv("ADX_BF_START_DELAY_SEC", "120"))

# 🔸 SQL кандидатов (закрытые, adx_checked=false, валидные стратегии)
_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.adx_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

# 🔸 Выборка UID'ов
async def _fetch_candidates(batch_size: int):
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Обработка одного UID
async def _process_uid(uid: str):
    try:
        pos, strat, verdict = await _load_position_and_strategy(uid)
        v_code, v_reason = verdict
        if v_code != "ok":
            return ("skip", v_reason, uid)

        bins = await _load_adx_bins(uid)
        if not bins:
            return ("skip", "no_adx_in_pis", uid)

        await _update_adx_aggregates(pos, bins)
        return ("updated", bins, uid)

    except Exception as e:
        log.exception("❌ ADX-BF uid=%s error: %s", uid, e)
        return ("error", "exception", uid)

# 🔸 Основной цикл backfill'а
async def run_oracle_adxbins_snapshot_backfill():
    if START_DELAY_SEC > 0:
        log.info("⏳ ADX-BINS BF: задержка старта %d сек", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    log.info("🚀 ADX-BINS BF: старт, batch=%d, max_conc=%d, sleep=%dms",
             BATCH_SIZE, MAX_CONCURRENCY, SLEEP_MS)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            uids = await _fetch_candidates(BATCH_SIZE)
            if not uids:
                await asyncio.sleep(SLEEP_MS / 1000)
                continue

            log.debug("[ADX-BINS BF] planned uids=%d (пример: %s)", len(uids), uids[:3])

            results = []
            async def worker(u: str):
                async with gate:
                    res = await _process_uid(u)
                    results.append(res)

            await asyncio.gather(*[asyncio.create_task(worker(u)) for u in uids])

            updated = sum(1 for r in results if r[0] == "updated")
            skipped = sum(1 for r in results if r[0] == "skip")
            errors  = sum(1 for r in results if r[0] == "error")
            log.info("[ADX-BINS BF] batch_done total=%d updated=%d skipped=%d errors=%d",
                     len(results), updated, skipped, errors)

        except asyncio.CancelledError:
            log.info("⏹️ ADX-BINS backfill остановлен")
            raise
        except Exception as e:
            log.exception("❌ ADX-BINS BF loop error: %s", e)
            await asyncio.sleep(1)