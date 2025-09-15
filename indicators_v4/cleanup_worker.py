# cleanup_worker.py — регулярная очистка TS/DB/Streams для indicators_v4

import asyncio
import logging
from datetime import datetime, timedelta

log = logging.getLogger("IND_CLEANUP")

# 🔸 Константы политики
TS_RETENTION_MS = 14 * 24 * 60 * 60 * 1000             # 14 суток
DB_KEEP_DAYS    = 30                                   # 30 суток
STREAM_LIMITS = {
    "indicator_stream_core": 10000,
    "indicator_stream":      10000,
    "iv4_inserted":          20000,
    "indicator_request":     10000,
    "indicator_response":    10000,
}

# 🔹 Тайм-бюджет на ретенцию TS в одном цикле, чтобы не блокировать воркер надолго
TS_RETENTION_TIME_BUDGET_SEC = 30

# 🔹 Периодичность чистки позиций не-MW стратегий
NON_MW_CLEANUP_PERIOD_HOURS = 4

# 🔸 Пройтись по ts_ind:* и выставить RETENTION=14 суток (с тайм-бюджетом)
async def enforce_ts_retention(redis):
    """
    Идемпотентная установка RETENTION на ts_ind:* с ограничением по времени на один цикл.
    Используем SCAN с count=500; если за отведённое время не закончили — продолжим на следующей итерации.
    """
    try:
        start = datetime.utcnow()
        cursor = "0"
        pattern = "ts_ind:*"
        changed = 0

        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=500)
            for k in keys:
                try:
                    await redis.execute_command("TS.ALTER", k, "RETENTION", TS_RETENTION_MS)
                    changed += 1
                except Exception as e:
                    log.warning(f"TS.ALTER {k} error: {e}")

            if cursor == "0":
                break

            if (datetime.utcnow() - start).total_seconds() >= TS_RETENTION_TIME_BUDGET_SEC:
                log.debug(f"[TS] RETENTION pass time-budget reached, changed ~{changed}, will continue next loop")
                break

        if cursor == "0":
            log.debug(f"[TS] RETENTION=14d применён (полный проход), изменено ~{changed} ключей ts_ind:*")
    except Exception as e:
        log.error(f"[TS] enforce_ts_retention error: {e}", exc_info=True)

# 🔸 Удалить старые значения индикаторов из БД (open_time < NOW()-30d)
async def cleanup_db(pg):
    try:
        async with pg.acquire() as conn:
            cutoff = DB_KEEP_DAYS
            res1 = await conn.execute(
                f"""
                DELETE FROM indicator_values_v4
                WHERE open_time < NOW() - INTERVAL '{cutoff} days'
                """
            )
        log.debug(f"[DB] indicator_values_v4 удалено: {res1}")
    except Exception as e:
        log.error(f"[DB] cleanup_db error: {e}", exc_info=True)

# 🔸 Трим всех стримов до разумного хвоста
async def trim_streams(redis):
    for key, maxlen in STREAM_LIMITS.items():
        try:
            trimmed = await redis.execute_command("XTRIM", key, "MAXLEN", "~", maxlen)
            log.debug(f"[STREAM] {key} → XTRIM ~{maxlen}, удалено ~{trimmed}")
        except Exception as e:
            log.warning(f"[STREAM] {key} XTRIM error: {e}")

# 🔸 Очистка позиций индикаторов для стратегий без market_watcher
async def cleanup_positions_indicators_non_mw(pg):
    """
    Удаляет все строки из positions_indicators_stat для стратегий, у которых market_watcher = false.
    Использует join по strategy_id, чтобы захватить как старые записи, так и любые случайно записанные в будущем.
    """
    try:
        async with pg.acquire() as conn:
            res = await conn.execute(
                """
                DELETE FROM positions_indicators_stat pis
                USING strategies_v4 s
                WHERE pis.strategy_id = s.id
                  AND s.market_watcher = false
                """
            )
        log.debug(f"[DB] positions_indicators_stat (non-MW) удалено: {res}")
    except Exception as e:
        log.error(f"[DB] cleanup_positions_indicators_non_mw error: {e}", exc_info=True)

# 🔸 Основной воркер: запускает периодические задачи
async def run_indicators_cleanup(pg, redis):
    log.info("IND_CLEANUP started")
    last_db = datetime.min
    last_non_mw = datetime.min

    while True:
        try:
            # каждые ~5 минут — TS retention и стримы
            await enforce_ts_retention(redis)
            await trim_streams(redis)

            now = datetime.utcnow()

            # раз в сутки — очистка indicator_values_v4
            if (now - last_db) >= timedelta(days=1):
                await cleanup_db(pg)
                last_db = now

            # раз в 4 часа — чистка позиций индикаторов для стратегий без market_watcher
            if (now - last_non_mw) >= timedelta(hours=NON_MW_CLEANUP_PERIOD_HOURS):
                await cleanup_positions_indicators_non_mw(pg)
                last_non_mw = now

            await asyncio.sleep(300)  # пауза 5 минут

        except Exception as e:
            log.error(f"IND_CLEANUP loop error: {e}", exc_info=True)
            await asyncio.sleep(10)