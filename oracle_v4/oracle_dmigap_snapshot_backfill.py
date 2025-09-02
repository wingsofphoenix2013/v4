# oracle_dmigap_snapshot_backfill.py — DMI-GAP backfill: задержка старта, батчи, параллельная обработка

import os
import asyncio
import logging

import infra
from oracle_dmigap_snapshot_aggregator import (
    _load_position_and_strategy,    # FOR UPDATE + dmi_gap_checked/стратегия
    _load_dmigap_bins_trends,       # чтение PIS(t) + TS(t-1,t-2), расчёт gap-бинов и тренда
    _update_dmigap_aggregates,      # UPSERT в таблицы + Redis + dmi_gap_checked=true
)

log = logging.getLogger("ORACLE_DMIGAP_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE        = int(os.getenv("DMIGAP_BF_BATCH_SIZE", "200"))
MAX_CONCURRENCY   = int(os.getenv("DMIGAP_BF_MAX_CONCURRENCY", "8"))
SLEEP_MS          = int(os.getenv("DMIGAP_BF_SLEEP_MS", "200"))
START_DELAY_SEC   = int(os.getenv("DMIGAP_BF_START_DELAY_SEC", "120"))

# 🔸 SQL кандидатов (закрытые, dmi_gap_checked=false, валидные стратегии)
_CANDIDATES_SQL = """
SELECT p.position_uid, p.symbol, p.created_at
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.dmi_gap_checked, false) = false
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
    # вернём (uid, symbol, created_at) чтобы не вычислять лишний раз
    return [(r["position_uid"], r["symbol"], r["created_at"]) for r in rows]

# 🔸 Обработка одной позиции
async def _process_candidate(uid: str, symbol: str, created_at):
    try:
        pos, strat, verdict = await _load_position_and_strategy(uid)
        v_code, v_reason = verdict
        if v_code != "ok":
            return ("skip", v_reason, uid)

        bins, trends = await _load_dmigap_bins_trends(uid, symbol, created_at)
        if not bins:
            return ("skip", "no_gap_values", uid)

        await _update_dmigap_aggregates(pos, bins, trends)
        return ("updated", (bins, trends), uid)

    except Exception as e:
        log.exception("❌ DMIGAP-BF uid=%s error: %s", uid, e)
        return ("error", "exception", uid)

# 🔸 Основной цикл backfill'а
async def run_oracle_dmigap_snapshot_backfill():
    if START_DELAY_SEC > 0:
        log.debug("⏳ DMI-GAP BF: задержка старта %d сек", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    log.debug("🚀 DMI-GAP BF: старт, batch=%d, max_conc=%d, sleep=%dms",
             BATCH_SIZE, MAX_CONCURRENCY, SLEEP_MS)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            cands = await _fetch_candidates(BATCH_SIZE)
            if not cands:
                await asyncio.sleep(SLEEP_MS / 1000)
                continue

            log.debug("[DMI-GAP BF] planned=%d (пример: %s)", len(cands), cands[:2])

            results = []
            async def worker(rec):
                uid, sym, created_at = rec
                async with gate:
                    res = await _process_candidate(uid, sym, created_at)
                    results.append(res)

            await asyncio.gather(*[asyncio.create_task(worker(rec)) for rec in cands])

            updated = sum(1 for r in results if r[0] == "updated")
            skipped = sum(1 for r in results if r[0] == "skip")
            errors  = sum(1 for r in results if r[0] == "error")

            log.debug("[DMI-GAP BF] batch_done total=%d updated=%d skipped=%d errors=%d",
                     len(results), updated, skipped, errors)

        except asyncio.CancelledError:
            log.debug("⏹️ DMI-GAP backfill остановлен")
            raise
        except Exception as e:
            log.exception("❌ DMI-GAP BF loop error: %s", e)
            await asyncio.sleep(1)