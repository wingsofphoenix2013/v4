# 🔸 oracle_emastatus_snapshot_backfill.py — EMA-status backfill: пакетная обработка закрытых позиций (batch=500, conc=10, старт через 120с, повтор каждые 4ч)

import os
import asyncio
import logging
from typing import List, Tuple

import infra
from oracle_emastatus_snapshot_aggregator import (
    _load_position_and_strategy,
    _load_ema_status_bins,
    _update_aggregates_and_mark,
)

log = logging.getLogger("ORACLE_EMASTATUS_BF")

# 🔸 Конфиг backfill'а
BATCH_SIZE           = int(os.getenv("EMA_BF_BATCH_SIZE", "500"))
MAX_CONCURRENCY      = int(os.getenv("EMA_BF_MAX_CONCURRENCY", "10"))
SHORT_SLEEP_MS       = int(os.getenv("EMA_BF_SLEEP_MS", "250"))           # пауза между батчами в активной фазе
START_DELAY_SEC      = int(os.getenv("EMA_BF_START_DELAY_SEC", "120"))    # задержка перед стартом
RECHECK_INTERVAL_SEC = int(os.getenv("EMA_BF_RECHECK_INTERVAL_SEC", str(4*3600)))  # переобход раз в 4 часа

_CANDIDATES_SQL = """
SELECT p.position_uid
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.emastatus_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
ORDER BY p.closed_at NULLS LAST, p.id
LIMIT $1
"""

_COUNT_SQL = """
SELECT COUNT(*)
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.emastatus_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
"""

# 🔸 Выборка пачки UID'ов кандидатов
async def _fetch_candidates(batch_size: int) -> List[str]:
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        rows = await conn.fetch(_CANDIDATES_SQL, batch_size)
    return [r["position_uid"] for r in rows]

# 🔸 Подсчёт оставшихся (для периодических отчётов)
async def _count_remaining() -> int:
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        val = await conn.fetchval(_COUNT_SQL)
    return int(val or 0)

# 🔸 Обработка одного UID (idempotent)
async def _process_uid(uid: str) -> Tuple[str, str]:
    try:
        pos, strat, verdict = await _load_position_and_strategy(uid)
        v_code, v_reason = verdict
        if v_code != "ok":
            return ("skip", v_reason)

        ema_bins = await _load_ema_status_bins(uid)
        await _update_aggregates_and_mark(pos, ema_bins)
        return ("updated", "ok" if ema_bins else "no_ema_status")

    except Exception as e:
        log.exception("❌ EMA-BF uid=%s error: %s", uid, e)
        return ("error", "exception")

# 🔸 Основной цикл backfill'а: до исчерпания кандидатов, затем пауза RECHECK_INTERVAL_SEC
async def run_oracle_emastatus_snapshot_backfill():
    # стартовая задержка
    if START_DELAY_SEC > 0:
        log.info("⏳ EMA-BF: задержка старта %d сек (batch=%d, conc=%d)", START_DELAY_SEC, BATCH_SIZE, MAX_CONCURRENCY)
        await asyncio.sleep(START_DELAY_SEC)

    gate = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        try:
            log.info("🚀 EMA-BF: старт прохода")
            batch_idx = 0
            total_updated = total_skipped = total_errors = 0

            # активная фаза — пока есть кандидаты
            while True:
                uids = await _fetch_candidates(BATCH_SIZE)
                if not uids:
                    break

                batch_idx += 1
                updated = skipped = errors = 0

                results = []
                async def worker(one_uid: str):
                    async with gate:
                        res = await _process_uid(one_uid)
                        results.append(res)

                await asyncio.gather(*[asyncio.create_task(worker(u)) for u in uids])

                for status, reason in results:
                    if status == "updated":
                        updated += 1
                    elif status == "skip":
                        skipped += 1
                    else:
                        errors += 1

                total_updated += updated
                total_skipped += skipped
                total_errors  += errors

                # отчёт по батчу — INFO
                # по возможности добавляем «remaining≈» (не на каждый батч, чтобы не грузить БД)
                remaining = None
                if batch_idx % 5 == 1:  # примерно каждый 5-й батч
                    try:
                        remaining = await _count_remaining()
                    except Exception:
                        remaining = None

                if remaining is None:
                    log.info("[EMA-BF] batch=%d size=%d updated=%d skipped=%d errors=%d",
                             batch_idx, len(uids), updated, skipped, errors)
                else:
                    log.info("[EMA-BF] batch=%d size=%d updated=%d skipped=%d errors=%d remaining≈%d",
                             batch_idx, len(uids), updated, skipped, errors, remaining)

                # короткая пауза между батчами, чтобы сгладить нагрузку
                await asyncio.sleep(SHORT_SLEEP_MS / 1000)

            # финальный отчёт прохода
            log.info("✅ EMA-BF: проход завершён batches=%d updated=%d skipped=%d errors=%d — следующий запуск через %ds",
                     batch_idx, total_updated, total_skipped, total_errors, RECHECK_INTERVAL_SEC)

            # режим «дежурного» запуска — спим RECHECK_INTERVAL_SEC и начинаем новый проход
            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.info("⏹️ EMA-BF остановлен")
            raise
        except Exception as e:
            log.exception("❌ EMA-BF loop error: %s", e)
            await asyncio.sleep(1)