# cleanup_worker.py — регулярная очистка TS/DB/Streams для indicators_v4 + pack retention (ind_pack_stream_core / ind_pack_events_v4) + очистка indicator_gap_v4 (healed_ts)

import asyncio
import logging
from datetime import datetime, timedelta

# 🔸 Логгер модуля
log = logging.getLogger("IND_CLEANUP")

# 🔸 Константы политики (indicators_v4)
TS_RETENTION_MS = 60 * 24 * 60 * 60 * 1000             # 60 суток
DB_KEEP_DAYS = 60                                      # 60 суток (indicator_values_v4)
STREAM_LIMITS = {
    "indicator_stream_core": 10000,
    "indicator_stream":      10000,
    "iv4_inserted":          20000,
    "indicator_request":     10000,
    "indicator_response":    10000,
}

# 🔸 Политика очистки indicator_gap_v4
GAP_KEEP_HOURS = 48                                    # 48 часов (только healed_ts, считаем от healed_ts_at)

# 🔸 Политика retention (packs)
PACK_STREAM_KEY = "ind_pack_stream_core"
PACK_STREAM_KEEP_HOURS = 48                            # 48 часов
PACK_EVENTS_KEEP_HOURS = 48                            # 48 часов (ind_pack_events_v4)

# 🔸 Тайм-бюджет на ретенцию TS в одном цикле, чтобы не блокировать воркер надолго
TS_RETENTION_TIME_BUDGET_SEC = 30


# 🔸 Пройтись по ts_ind:* и выставить RETENTION=60 суток (с тайм-бюджетом)
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
                log.debug(f"[TS] RETENTION=60d применён (полный проход), изменено ~{changed} ключей ts_ind:*")
                return changed, True

            # проверка тайм-бюджета
            if (datetime.utcnow() - start).total_seconds() >= TS_RETENTION_TIME_BUDGET_SEC:
                log.debug(f"[TS] RETENTION pass time-budget reached, changed ~{changed}, will continue next loop")
                return changed, False

    except Exception as e:
        log.error(f"[TS] enforce_ts_retention error: {e}", exc_info=True)
        return 0, False


# 🔸 Удалить старые значения индикаторов из БД (open_time < NOW()-60d)
async def cleanup_indicators_db(pg):
    """
    Возвращает количество удалённых строк (если возможно определить), иначе None.
    """
    try:
        async with pg.acquire() as conn:
            res = await conn.execute(
                f"""
                DELETE FROM indicator_values_v4
                WHERE open_time < NOW() - INTERVAL '{int(DB_KEEP_DAYS)} days'
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
        log.error(f"[DB] cleanup_indicators_db error: {e}", exc_info=True)
        return None


# 🔸 Удалить старые события pack из БД (created_at < NOW()-48h)
async def cleanup_pack_events_db(pg):
    """
    Возвращает количество удалённых строк (если возможно определить), иначе None.
    """
    try:
        async with pg.acquire() as conn:
            res = await conn.execute(
                f"""
                DELETE FROM ind_pack_events_v4
                WHERE created_at < NOW() - INTERVAL '{int(PACK_EVENTS_KEEP_HOURS)} hours'
                """
            )

        # res обычно строка формата 'DELETE <n>'
        try:
            deleted = int(res.split()[-1])
        except Exception:
            deleted = None

        log.debug(f"[DB] ind_pack_events_v4 удалено: {res}")
        return deleted
    except Exception as e:
        log.error(f"[DB] cleanup_pack_events_db error: {e}", exc_info=True)
        return None


# 🔸 Удалить вылеченные дырки из indicator_gap_v4 (status=healed_ts и healed_ts_at < NOW()-48h)
async def cleanup_indicator_gap_healed_ts_db(pg):
    """
    Удаляем только полностью вылеченные (healed_ts) и старше GAP_KEEP_HOURS часов от момента healed_ts_at.
    Возвращает количество удалённых строк (если возможно определить), иначе None.
    """
    try:
        async with pg.acquire() as conn:
            res = await conn.execute(
                f"""
                DELETE FROM indicator_gap_v4
                WHERE status = 'healed_ts'
                  AND healed_ts_at IS NOT NULL
                  AND healed_ts_at < NOW() - INTERVAL '{int(GAP_KEEP_HOURS)} hours'
                """
            )

        # res обычно строка формата 'DELETE <n>'
        try:
            deleted = int(res.split()[-1])
        except Exception:
            deleted = None

        log.debug(f"[DB] indicator_gap_v4 (healed_ts) удалено: {res}")
        return deleted
    except Exception as e:
        log.error(f"[DB] cleanup_indicator_gap_healed_ts_db error: {e}", exc_info=True)
        return None


# 🔸 Трим всех стримов indicators_v4 до разумного хвоста (MAXLEN)
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


# 🔸 Трим pack stream по времени (48 часов) через MINID
async def trim_pack_stream_by_time(redis) -> int:
    """
    Для Redis Streams ID имеет форму <ms>-<seq>. Поэтому можно резать по времени через:
    XTRIM <stream> MINID ~ <cutoff_ms>-0
    Возвращает trimmed_count (если Redis вернул число), иначе 0.
    """
    try:
        cutoff_ms = int((datetime.utcnow() - timedelta(hours=PACK_STREAM_KEEP_HOURS)).timestamp() * 1000)
        min_id = f"{cutoff_ms}-0"
        trimmed = await redis.execute_command("XTRIM", PACK_STREAM_KEY, "MINID", "~", min_id)
        n = int(trimmed) if trimmed is not None else 0
        log.debug(f"[PACK_STREAM] {PACK_STREAM_KEY} → XTRIM MINID ~{min_id}, удалено ~{trimmed}")
        return n
    except Exception as e:
        log.warning(f"[PACK_STREAM] {PACK_STREAM_KEY} XTRIM MINID error: {e}")
        return 0


# 🔸 Основной воркер: запускает периодические задачи
async def run_indicators_cleanup(pg, redis):
    log.debug("IND_CLEANUP: воркер запущен")

    last_db_ind = datetime.min
    last_db_pack = datetime.min
    last_db_gap = datetime.min
    last_info = datetime.min

    while True:
        try:
            # каждые ~5 минут — TS retention + trim streams
            changed, finished = await enforce_ts_retention(redis)
            trim_stats = await trim_streams(redis)
            pack_trimmed = await trim_pack_stream_by_time(redis)

            total_trimmed = sum(trim_stats.values())

            # итоговая debug-метрика по проходу
            log.debug(
                f"IND_CLEANUP: TS_RETENTION changed={changed}, full_pass={finished}; "
                f"Streams trimmed total={total_trimmed} ({', '.join(f'{k}:{v}' for k,v in trim_stats.items())}); "
                f"Pack stream trimmed={pack_trimmed} (keep={PACK_STREAM_KEEP_HOURS}h)"
            )

            now = datetime.utcnow()

            # суммирующий info-лог раз в час (не шумим)
            if (now - last_info) >= timedelta(hours=1):
                log.info(
                    "IND_CLEANUP: hourly summary — ts_retention_changed=%s, ts_full_pass=%s, streams_trimmed=%s, pack_stream_trimmed=%s",
                    changed,
                    finished,
                    total_trimmed,
                    pack_trimmed,
                )
                last_info = now

            # раз в час — очистка ind_pack_events_v4 (48 часов)
            if (now - last_db_pack) >= timedelta(hours=1):
                deleted = await cleanup_pack_events_db(pg)
                if deleted is not None:
                    log.info(
                        f"IND_CLEANUP: DB purge ind_pack_events_v4 — deleted={deleted} rows (older than {PACK_EVENTS_KEEP_HOURS}h)"
                    )
                else:
                    log.info(
                        f"IND_CLEANUP: DB purge ind_pack_events_v4 — completed (older than {PACK_EVENTS_KEEP_HOURS}h)"
                    )
                last_db_pack = now

            # раз в час — очистка indicator_gap_v4 (healed_ts, healed_ts_at older than 48h)
            if (now - last_db_gap) >= timedelta(hours=1):
                deleted = await cleanup_indicator_gap_healed_ts_db(pg)
                if deleted is not None:
                    log.info(
                        f"IND_CLEANUP: DB purge indicator_gap_v4 — deleted={deleted} rows (status=healed_ts, older than {GAP_KEEP_HOURS}h from healed_ts_at)"
                    )
                else:
                    log.info(
                        f"IND_CLEANUP: DB purge indicator_gap_v4 — completed (status=healed_ts, older than {GAP_KEEP_HOURS}h from healed_ts_at)"
                    )
                last_db_gap = now

            # раз в сутки — очистка indicator_values_v4 (60 суток)
            if (now - last_db_ind) >= timedelta(days=1):
                deleted = await cleanup_indicators_db(pg)
                if deleted is not None:
                    log.info(
                        f"IND_CLEANUP: DB purge indicator_values_v4 — deleted={deleted} rows (older than {DB_KEEP_DAYS}d)"
                    )
                else:
                    log.info(
                        f"IND_CLEANUP: DB purge indicator_values_v4 — completed (older than {DB_KEEP_DAYS}d)"
                    )
                last_db_ind = now

            await asyncio.sleep(300)  # пауза 5 минут

        except Exception as e:
            log.error(f"IND_CLEANUP loop error: {e}", exc_info=True)
            await asyncio.sleep(10)