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

# 🔸 Пройтись по ts_ind:* и выставить RETENTION=14 суток (идемпотентно)
async def enforce_ts_retention(redis):
    try:
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
        log.debug(f"[TS] RETENTION=14d применён к {changed} ключам ts_ind:*")
    except Exception as e:
        log.error(f"[TS] enforce_ts_retention error: {e}", exc_info=True)

# 🔸 Удалить старые значения индикаторов из БД (open_time < NOW()-30d)
async def cleanup_db(pg):
    try:
        async with pg.acquire() as conn:
            cutoff = DB_KEEP_DAYS
            # indicator_values_v4 — значения для онлайна
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

# 🔸 Основной воркер: запускает периодические задачи
async def run_indicators_cleanup(pg, redis):
    log.debug("IND_CLEANUP запущен")
    # Циклы: TS/Streams — почаще; БД — раз в сутки
    last_db = datetime.min

    while True:
        try:
            # каждые 5 минут — TS retention и стримы
            await enforce_ts_retention(redis)
            await trim_streams(redis)

            # раз в сутки — БД
            now = datetime.utcnow()
            if (now - last_db) >= timedelta(days=1):
                await cleanup_db(pg)
                last_db = now

            await asyncio.sleep(300)  # 5 минут пауза

        except Exception as e:
            log.error(f"IND_CLEANUP loop error: {e}", exc_info=True)
            await asyncio.sleep(10)