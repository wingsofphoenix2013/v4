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

# 🔹 Настройки очистки positions_indicators_stat
PIS_BATCH_SIZE        = 1_000   # сколько ID выбираем за один проход
PIS_DELETE_CHUNK_SIZE = 100    # сколько ID удаляем в одном SQL-запросе
PIS_CONCURRENCY       = 10       # параллельных задач удаления
PIS_FIRST_RUN_DELAY   = timedelta(minutes=2)           # первый запуск через 2 минуты
PIS_RUN_PERIOD        = timedelta(days=1)              # затем раз в сутки

# 🔹 Тайм-бюджет на ретенцию TS в одном цикле, чтобы не блокировать воркер надолго
TS_RETENTION_TIME_BUDGET_SEC = 30

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

            # если прошли все ключи — выходим
            if cursor == "0":
                break

            # проверяем бюджет времени
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

# 🔹 Выборка батча ID для удаления из positions_indicators_stat
async def _fetch_pis_batch_ids(pg, limit: int) -> list[int]:
    sql = """
        SELECT pis.id
        FROM positions_indicators_stat pis
        JOIN strategies_v4 s ON s.id = pis.strategy_id
        WHERE COALESCE(s.market_watcher, false) = false
        LIMIT $1
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(sql, limit)
    return [r["id"] for r in rows]

# 🔹 Удаление пачки ID (одним запросом)
async def _delete_pis_ids_chunk(pg, ids: list[int]):
    if not ids:
        return
    sql = "DELETE FROM positions_indicators_stat WHERE id = ANY($1::bigint[])"
    async with pg.acquire() as conn:
        await conn.execute(sql, ids)

# 🔹 Полная очистка PIS батчами с параллелизмом
async def cleanup_positions_indicators_stat(pg,
                                            batch_size: int = PIS_BATCH_SIZE,
                                            chunk_size: int = PIS_DELETE_CHUNK_SIZE,
                                            concurrency: int = PIS_CONCURRENCY):
    try:
        total_deleted = 0
        sem = asyncio.Semaphore(concurrency)

        while True:
            ids = await _fetch_pis_batch_ids(pg, batch_size)
            if not ids:
                break

            tasks = []
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i:i+chunk_size]

                async def _task(c=chunk):
                    async with sem:
                        await _delete_pis_ids_chunk(pg, c)

                tasks.append(asyncio.create_task(_task()))

            await asyncio.gather(*tasks, return_exceptions=False)
            total_deleted += len(ids)
            log.info(f"[DB] PIS cleanup progress: deleted {len(ids)} this batch (total {total_deleted})")

        if total_deleted:
            log.info(f"[DB] PIS cleanup removed rows: {total_deleted}")
        else:
            log.info("[DB] PIS cleanup: nothing to delete")

    except Exception as e:
        log.error(f"[DB] cleanup_positions_indicators_stat error: {e}", exc_info=True)

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
    log.info("IND_CLEANUP started")
    last_db = datetime.min

    # расписание очистки PIS — первый запуск через 2 минуты, далее раз в сутки
    now = datetime.utcnow()
    next_pis_run_at = now + PIS_FIRST_RUN_DELAY
    log.info(f"[DB] PIS cleanup scheduled at (UTC): {next_pis_run_at.isoformat()}")

    while True:
        try:
            # 1) сначала — проверка и запуск PIS по расписанию (чтобы не блокировалось долгими задачами)
            now = datetime.utcnow()
            if now >= next_pis_run_at:
                log.info("[DB] PIS cleanup: start")
                await cleanup_positions_indicators_stat(pg)
                log.info("[DB] PIS cleanup: done")
                next_pis_run_at = now + PIS_RUN_PERIOD
                log.info(f"[DB] PIS next run at (UTC): {next_pis_run_at.isoformat()}")

            # 2) затем — короткие периодические задачи Redis
            await enforce_ts_retention(redis)
            await trim_streams(redis)

            # 3) раз в сутки — чистка indicator_values_v4
            now = datetime.utcnow()
            if (now - last_db) >= timedelta(days=1):
                await cleanup_db(pg)
                last_db = now

            # 4) динамический сон: до ближайшего события, но не больше 300 сек
            now = datetime.utcnow()
            sleep_sec = min(300, max(1, int((next_pis_run_at - now).total_seconds())))
            await asyncio.sleep(sleep_sec)

        except Exception as e:
            log.error(f"IND_CLEANUP loop error: {e}", exc_info=True)
            await asyncio.sleep(10)