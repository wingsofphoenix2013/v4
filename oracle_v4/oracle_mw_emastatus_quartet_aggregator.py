# 🔸 oracle_mw_emastatus_quartet_aggregator.py — MW×EMA(m5) квартеты: пока только поиск кандидатов и сводка в логах

# 🔸 Импорты и базовая настройка
import os
import asyncio
import logging

import infra

log = logging.getLogger("ORACLE_MW_EMA_Q")

# 🔸 Конфиг сканера/агрегатора (пока без записи)
START_DELAY_SEC      = int(os.getenv("MW_EMA_Q_START_DELAY_SEC", "5"))
RECHECK_INTERVAL_SEC = int(os.getenv("MW_EMA_Q_RECHECK_INTERVAL_SEC", "300"))
LOG_TOP_STRATS       = int(os.getenv("MW_EMA_Q_LOG_TOP_STRATS", "0"))  # 0 = не логировать по стратегиям

# 🔸 SQL-запросы
_CANDIDATES_ANY_SQL = """
SELECT 1
FROM positions_v4 p
JOIN strategies_v4 s ON s.id = p.strategy_id
WHERE p.status = 'closed'
  AND COALESCE(p.mrk_watcher_checked, false) = true
  AND COALESCE(p.emastatus_checked, false) = true
  AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
  AND s.enabled = true
  AND COALESCE(s.market_watcher, false) = true
LIMIT 1
"""

_TOTAL_SQL = """
WITH candidates AS (
  SELECT p.position_uid
  FROM positions_v4 p
  JOIN strategies_v4 s ON s.id = p.strategy_id
  WHERE p.status = 'closed'
    AND COALESCE(p.mrk_watcher_checked, false) = true
    AND COALESCE(p.emastatus_checked, false) = true
    AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
    AND s.enabled = true
    AND COALESCE(s.market_watcher, false) = true
)
SELECT COUNT(*)::bigint AS total_candidates
FROM candidates
"""

_DIST_SQL = """
WITH candidates AS (
  SELECT p.position_uid
  FROM positions_v4 p
  JOIN strategies_v4 s ON s.id = p.strategy_id
  WHERE p.status = 'closed'
    AND COALESCE(p.mrk_watcher_checked, false) = true
    AND COALESCE(p.emastatus_checked, false) = true
    AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
    AND s.enabled = true
    AND COALESCE(s.market_watcher, false) = true
),
per_pos_lens AS (
  SELECT c.position_uid,
         COUNT(*) AS lens_found
  FROM candidates c
  JOIN LATERAL (
    SELECT DISTINCT ON (pis.param_name)
           pis.param_name
    FROM positions_indicators_stat pis
    WHERE pis.position_uid = c.position_uid
      AND pis.timeframe = 'm5'
      AND pis.using_current_bar = true
      AND pis.param_name IN ('ema9_status','ema21_status','ema50_status','ema100_status','ema200_status')
    ORDER BY pis.param_name, pis.snapshot_at DESC
  ) t ON TRUE
  GROUP BY c.position_uid
),
all_pos AS (
  SELECT c.position_uid, COALESCE(p.lens_found, 0) AS lens_found
  FROM candidates c
  LEFT JOIN per_pos_lens p USING (position_uid)
),
dist AS (
  SELECT lens_found, COUNT(*) AS cnt
  FROM all_pos
  GROUP BY lens_found
)
SELECT lens_found, cnt, (SELECT COUNT(*) FROM all_pos) AS total
FROM dist
ORDER BY lens_found
"""

_PER_STRATEGY_SQL = """
WITH candidates AS (
  SELECT p.position_uid, p.strategy_id
  FROM positions_v4 p
  JOIN strategies_v4 s ON s.id = p.strategy_id
  WHERE p.status = 'closed'
    AND COALESCE(p.mrk_watcher_checked, false) = true
    AND COALESCE(p.emastatus_checked, false) = true
    AND COALESCE(p.mw_emastatus_quartet_checked, false) = false
    AND s.enabled = true
    AND COALESCE(s.market_watcher, false) = true
),
per_pos_lens AS (
  SELECT c.position_uid,
         c.strategy_id,
         COUNT(*) AS lens_found
  FROM candidates c
  JOIN LATERAL (
    SELECT DISTINCT ON (pis.param_name) pis.param_name
    FROM positions_indicators_stat pis
    WHERE pis.position_uid = c.position_uid
      AND pis.timeframe = 'm5'
      AND pis.using_current_bar = true
      AND pis.param_name IN ('ema9_status','ema21_status','ema50_status','ema100_status','ema200_status')
    ORDER BY pis.param_name, pis.snapshot_at DESC
  ) t ON TRUE
  GROUP BY c.position_uid, c.strategy_id
),
all_pos AS (
  SELECT c.position_uid, c.strategy_id, COALESCE(p.lens_found, 0) AS lens_found
  FROM candidates c
  LEFT JOIN per_pos_lens p USING (position_uid, strategy_id)
),
sum_by_strat AS (
  SELECT strategy_id,
         SUM(CASE WHEN lens_found = 5 THEN 1 ELSE 0 END) AS full_5,
         SUM(CASE WHEN lens_found BETWEEN 1 AND 4 THEN 1 ELSE 0 END) AS partial_1_4,
         SUM(CASE WHEN lens_found = 0 THEN 1 ELSE 0 END) AS empty_0,
         COUNT(*) AS total
  FROM all_pos
  GROUP BY strategy_id
)
SELECT strategy_id, full_5, partial_1_4, empty_0, total
FROM sum_by_strat
ORDER BY total DESC
LIMIT $1
"""

# 🔸 Утилиты (поиск и логгирование сводки)
async def _any_candidates() -> bool:
    pg = infra.pg_pool
    async with pg.acquire() as conn:
        row = await conn.fetchrow(_CANDIDATES_ANY_SQL)
    return row is not None

async def _log_summary():
    pg = infra.pg_pool

    # общий счётчик
    async with pg.acquire() as conn:
        total_row = await conn.fetchrow(_TOTAL_SQL)
    total = int(total_row["total_candidates"] or 0)

    if total == 0:
        log.info("[MW×EMA-Q] кандидатов нет (total=0)")
        return

    # распределение по числу найденных EMA-длин (0..5)
    async with pg.acquire() as conn:
        dist_rows = await conn.fetch(_DIST_SQL)

    # подготовка компактного вывода 0..5
    total_from_dist = int(dist_rows[0]["total"]) if dist_rows else 0
    pct_parts = []
    cnt_parts = []
    for k in range(0, 6):
        cnt = next((int(r["cnt"]) for r in dist_rows if int(r["lens_found"]) == k), 0)
        pct = (100.0 * cnt / total_from_dist) if total_from_dist else 0.0
        pct_parts.append(f"{k}=>{pct:.2f}%")
        cnt_parts.append(f"{k}=>{cnt}")

    log.info("[MW×EMA-Q] candidates=%d", total)
    log.info("[MW×EMA-Q] lens_found distribution (pct): %s", " ".join(pct_parts))
    log.info("[MW×EMA-Q] lens_found distribution (cnt): %s", " ".join(cnt_parts))

    # по стратегиям — опционально топ-N
    if LOG_TOP_STRATS > 0:
        async with pg.acquire() as conn:
            srows = await conn.fetch(_PER_STRATEGY_SQL, int(LOG_TOP_STRATS))
        for r in srows:
            # формат строки по стратегии
            sid = int(r["strategy_id"])
            full5 = int(r["full_5"]); part = int(r["partial_1_4"]); empty = int(r["empty_0"]); tot = int(r["total"])
            p_full = (100.0 * full5 / tot) if tot else 0.0
            p_part = (100.0 * part / tot) if tot else 0.0
            p_empty = (100.0 * empty / tot) if tot else 0.0
            log.info(
                "[MW×EMA-Q] strat=%s total=%d full5=%d(%.2f%%) partial=%d(%.2f%%) empty=%d(%.2f%%)",
                sid, tot, full5, p_full, part, p_part, empty, p_empty
            )

# 🔸 Основной цикл (пока только поиск и лог-сводка)
async def run_oracle_mw_emastatus_quartet_aggregator():
    # задержка старта (как у других воркеров)
    if START_DELAY_SEC > 0:
        log.debug("⏳ MW×EMA-Q: задержка старта %d сек", START_DELAY_SEC)
        await asyncio.sleep(START_DELAY_SEC)

    log.debug("🚀 MW×EMA-Q: старт цикла (режим: поиск и сводка, без записи)")

    while True:
        try:
            # условия достаточности
            any_cand = await _any_candidates()
            if not any_cand:
                log.info("[MW×EMA-Q] кандидатов нет → следующая проверка через %ds", RECHECK_INTERVAL_SEC)
                await asyncio.sleep(RECHECK_INTERVAL_SEC)
                continue

            # разовая сводка по актуальному набору кандидатов
            await _log_summary()

            # ожидание до следующего прохода
            log.debug("✅ MW×EMA-Q: сводка готова — следующий запуск через %ds", RECHECK_INTERVAL_SEC)
            await asyncio.sleep(RECHECK_INTERVAL_SEC)

        except asyncio.CancelledError:
            log.debug("⏹️ MW×EMA-Q агрегатор остановлен")
            raise
        except Exception as e:
            log.exception("❌ MW×EMA-Q loop error: %s", e)
            await asyncio.sleep(1)