# cleanup_worker.py — регулярная очистка TS/DB/Streams для indicators_v4 (очищено от positions_indicators_stat)

import asyncio
import logging
from datetime import datetime, timedelta

# 🔸 Логгер модуля
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

# 🔸 Тайм-бюджет на ретенцию TS в одном цикле, чтобы не блокировать воркер надолго
TS_RETENTION_TIME_BUDGET_SEC = 30


# 🔸 Пройтись по ts_ind:* и выставить RETENTION=14 суток (с тайм-бюджетом)
async def enforce_ts_retention(redis):
    """
    Идемпотентная установка RETENTION на ts_ind:* с ограничением по времени на один цикл.
    Используем SCAN с count=500; если за отведённое время не закончили — продолжим на следующей итерации.
    Возвращает кортеж (changed_count: int, finished_pass: bool).
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
                # полный проход завершён
                log.debug(f"[TS] RETENTION=14d применён (полный проход), изменено ~{changed} ключей ts_ind:*")
                return changed, True

            # проверка тайм-бюджета
            if (datetime.utcnow() - start).total_seconds() >= TS_RETENTION_TIME_BUDGET_SEC:
                log.debug(f"[TS] RETENTION pass time-budget reached, changed ~{changed}, will continue next loop")
                return changed, False

    except Exception as e:
        log.error(f"[TS] enforce_ts_retention error: {e}", exc_info=True)
        return 0, False


# 🔸 Удалить старые значения индикаторов из БД (open_time < NOW()-30d)
async def cleanup_db(pg):
    """
    Возвращает количество удалённых строк (если возможно определить), иначе None.
    """
    try:
        async with pg.acquire() as conn:
            cutoff = DB_KEEP_DAYS
            res = await conn.execute(
                f"""
                DELETE FROM indicator_values_v4
                WHERE open_time < NOW() - INTERVAL '{cutoff} days'
                """
            )
        # res обычно строка формата 'DELETE <n>'
        try:
            deleted = int(res.split()[-1])
        except Exception:
            deleted = None
        log.debug(f"[DB] indicator_values_v4 удалено: {res}")
        return deleted
    except Exception as e:
        log.error(f"[DB] cleanup_db error: {e}", exc_info=True)
        return None


# 🔸 Трим всех стримов до разумного хвоста
async def trim_streams(redis):
    """
    Выполняет XTRIM для известных стримов. Возвращает dict {stream: trimmed_count}.
    """
    results = {}
    for key, maxlen in STREAM_LIMITS.items():
        try:
            trimmed = await redis.execute_command("XTRIM", key, "MAXLEN", "~", maxlen)
            results[key] = int(trimmed) if trimmed is not None else 0
            log.debug(f"[STREAM] {key} → XTRIM ~{maxlen}, удалено ~{trimmed}")
        except Exception as e:
            log.warning(f"[STREAM] {key} XTRIM error: {e}")
            results[key] = 0
    return results


# 🔸 Основной воркер: запускает периодические задачи
async def run_indicators_cleanup(pg, redis):
    log.info("IND_CLEANUP: воркер запущен")
    last_db = datetime.min

    while True:
        try:
            # каждые ~5 минут — TS retention и стримы
            changed, finished = await enforce_ts_retention(redis)
            trim_stats = await trim_streams(redis)

            # итоговая инфо-метрика по проходу ретенции/стримам
            total_trimmed = sum(trim_stats.values())
            log.info(
                f"IND_CLEANUP: TS_RETENTION changed={changed}, full_pass={finished}; "
                f"Streams trimmed total={total_trimmed} ({', '.join(f'{k}:{v}' for k,v in trim_stats.items())})"
            )

            now = datetime.utcnow()

            # раз в сутки — очистка indicator_values_v4
            if (now - last_db) >= timedelta(days=1):
                deleted = await cleanup_db(pg)
                if deleted is not None:
                    log.info(f"IND_CLEANUP: DB purge indicator_values_v4 — deleted={deleted} rows (older than {DB_KEEP_DAYS}d)")
                else:
                    log.info(f"IND_CLEANUP: DB purge indicator_values_v4 — completed (older than {DB_KEEP_DAYS}d)")
                last_db = now

            await asyncio.sleep(300)  # пауза 5 минут

        except Exception as e:
            log.error(f"IND_CLEANUP loop error: {e}", exc_info=True)
            await asyncio.sleep(10)