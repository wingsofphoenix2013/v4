# 🔸 oracle_ema_snapshot_backfill.py — EMA snapshot backfill: параллельно, батч=200, без бюджета, непрерывная догрузка

import os
import asyncio
import logging
from datetime import datetime

import infra
from oracle_ema_snapshot_aggregator import _process_closed

# 🔸 Логгер
log = logging.getLogger("ORACLE_EMA_SNAP_BF")

# 🔸 Конфиг бэкофилла
BATCH_LIMIT       = int(os.getenv("ORACLE_EMASNAP_BF_BATCH", "200"))            # размер батча
CONCURRENCY       = int(os.getenv("ORACLE_EMASNAP_BF_CONCURRENCY", "4"))        # одновременных задач
START_DELAY_SEC   = int(os.getenv("ORACLE_EMASNAP_BF_START_DELAY_SEC", "120"))  # задержка старта
EMPTY_SLEEP_SEC   = int(os.getenv("ORACLE_EMASNAP_BF_EMPTY_SLEEP_SEC", "900"))  # пауза если хвост пуст (5 мин)
BATCH_PAUSE_MS    = int(os.getenv("ORACLE_EMASNAP_BF_BATCH_PAUSE_MS", "50"))    # пауза между запусками задач (смягчение)
AFTER_CYCLE_SLEEP = int(os.getenv("ORACLE_EMASNAP_BF_AFTER_CYCLE_SLEEP", "5"))  # пауза между батч-циклами (сек)

# 🔸 Выборка кандидатов (только стратегии enabled & market_watcher, позиции закрыты и не учтены)
async def _fetch_candidates(limit: int) -> list[str]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.position_uid
            FROM positions_v4 p
            JOIN strategies_v4 s ON s.id = p.strategy_id
            WHERE p.status = 'closed'
              AND COALESCE(p.emastatus_checked, false) = false
              AND s.enabled = true
              AND COALESCE(s.market_watcher, false) = true
            ORDER BY p.created_at ASC
            LIMIT $1
        """, limit)
    return [r["position_uid"] for r in rows]

# 🔸 Обработка одного position_uid (переиспользуем транзакционный обработчик из агрегатора)
async def _process_one(uid: str) -> tuple[bool, str]:
    try:
        ok, reason = await _process_closed(uid)
        return ok, reason or "ok"
    except Exception as e:
        log.exception("❌ EMA-SNAP BF pos=%s error: %s", uid, e)
        return False, "exception"

# 🔸 Один батч: параллельно CONCURRENCY задач, суммарные логи
async def _run_batch(candidates: list[str]) -> None:
    if not candidates:
        log.info("[EMA-SNAP BF] хвост пуст, ждём %d сек", EMPTY_SLEEP_SEC)
        await asyncio.sleep(EMPTY_SLEEP_SEC)
        return

    sem = asyncio.Semaphore(CONCURRENCY)
    processed = 0
    deferred  = 0
    skipped   = 0

    # 🔹 Внутренняя задача с семафором
    async def worker(uid: str):
        nonlocal processed, deferred, skipped
        async with sem:
            ok, reason = await _process_one(uid)
            if ok:
                processed += 1
            else:
                if reason in ("skip", "strategy_inactive"):
                    skipped += 1
                else:
                    deferred += 1
            await asyncio.sleep(BATCH_PAUSE_MS / 1000)

    # 🔹 Запуск задач
    tasks = [asyncio.create_task(worker(uid)) for uid in candidates]
    # 🔹 Прогресс-лог раз в 2 секунды
    while True:
        done = sum(1 for t in tasks if t.done())
        if done == len(tasks):
            break
        log.debug("[EMA-SNAP BF] progress: %d/%d (proc=%d, def=%d, skip=%d)",
                  done, len(tasks), processed, deferred, skipped)
        await asyncio.sleep(2)

    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("[EMA-SNAP BF] batch processed: %d, deferred=%d, skipped=%d (total=%d)",
             processed, deferred, skipped, len(candidates))

# 🔸 Непрерывный цикл: берём батч → обрабатываем параллельно → повторяем
async def run_oracle_ema_snapshot_backfill_periodic():
    log.info("🚀 EMA-SNAP BF: старт через %d сек, параллелизм=%d, батч=%d, без лимита времени",
             START_DELAY_SEC, CONCURRENCY, BATCH_LIMIT)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            start_ts = datetime.utcnow()
            candidates = await _fetch_candidates(BATCH_LIMIT)
            await _run_batch(candidates)
            log.debug("[EMA-SNAP BF] цикл занял ~%ds, следующий через %ds",
                      int((datetime.utcnow() - start_ts).total_seconds()), AFTER_CYCLE_SLEEP)
        except asyncio.CancelledError:
            log.info("⏹️ EMA-SNAP BF остановлен")
            raise
        except Exception as e:
            log.exception("❌ EMA-SNAP BF loop error: %s", e)

        await asyncio.sleep(AFTER_CYCLE_SLEEP)