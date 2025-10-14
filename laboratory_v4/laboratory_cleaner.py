# 🔸 laboratory_cleaner.py — воркер очистки: по событию PACK_LISTS READY чистит старые записи и подрезает decision_response (с троттлингом)

# 🔸 Импорты
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import List

import laboratory_infra as infra

# 🔸 Логгер
log = logging.getLogger("LAB_CLEANER")

# 🔸 Константы
TRIGGER_STREAM = "oracle:pack_lists:reports_ready"
CLEANER_CONSUMER_GROUP = "LAB_CLEANER_GROUP"
CLEANER_CONSUMER_NAME  = "LAB_CLEANER_WORKER"

# ретенции данных
RETENTION_DAYS = 8                 # держим аналитику/хиды/позиции N суток
STREAM_RETENTION_HOURS = 24        # ответы в стриме держим N часов

# троттлинг и взаимное исключение
COOLDOWN_KEY     = "lab:cleaner:cooldown"   # флаг «недавно чистили»
COOLDOWN_EX_SEC  = 3600                     # 1 час
RUNNING_LOCK_KEY = "lab:cleaner:running"    # замок «очистка выполняется»
RUNNING_EX_SEC   = 3600                     # макс. длительность одной чистки

# что чистим в БД (см. _cleanup_once):
# 1) laboratory_bl_analysis, laboratory_wl_analysis — по computed_at
# 2) laboratory_request_tf по head.finished_at; затем laboratory_request_head — по finished_at
# 3) laboratory_positions_stat — по updated_at
# стрим laboratory:decision_response — XTRIM MINID < now-24h


# 🔸 Публичная точка входа воркера
async def run_laboratory_cleaner():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск LAB_CLEANER: PG/Redis не инициализированы")
        return

    # создать consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=TRIGGER_STREAM,
            groupname=CLEANER_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 LAB_CLEANER: создана consumer group для %s", TRIGGER_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB_CLEANER: ошибка создания consumer group")
            return

    log.info(
        "🚀 LAB_CLEANER: слушаю %s (retention=%dd, stream=%dh, cooldown=%dm)",
        TRIGGER_STREAM, RETENTION_DAYS, STREAM_RETENTION_HOURS, COOLDOWN_EX_SEC // 60
    )

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CLEANER_CONSUMER_GROUP,
                consumername=CLEANER_CONSUMER_NAME,
                streams={TRIGGER_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            acks: List[str] = []
            triggered = False

            for _, msgs in resp:
                for msg_id, _fields in msgs:
                    try:
                        # если уже запускали чистку на этом батче, просто ACK
                        if triggered:
                            acks.append(msg_id)
                            continue

                        # троттлинг: пытаемся поставить cooldown (SET NX EX)
                        if not await _try_set_cooldown():
                            acks.append(msg_id)
                            continue

                        # взаимное исключение: пытаемся взять «running»-замок
                        got_lock = await _try_acquire_running()
                        if not got_lock:
                            acks.append(msg_id)
                            continue

                        # выполняем чистку
                        try:
                            await _cleanup_once()
                            triggered = True
                        finally:
                            await _release_running()

                        acks.append(msg_id)
                    except Exception:
                        log.exception("❌ LAB_CLEANER: сбой обработки сообщения")
                        acks.append(msg_id)

            # ACK всех сообщений
            if acks:
                try:
                    await infra.redis_client.xack(TRIGGER_STREAM, CLEANER_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ LAB_CLEANER: ошибка ACK (ids=%s)", acks)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB_CLEANER: остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB_CLEANER: ошибка цикла — пауза 5 секунд")
            await asyncio.sleep(5)


# устанавливаем cooldown на 1 час (если ещё не стоял)
async def _try_set_cooldown() -> bool:
    try:
        ok = await infra.redis_client.set(COOLDOWN_KEY, "1", ex=COOLDOWN_EX_SEC, nx=True)
        if ok:
            log.debug("⏲️ LAB_CLEANER: cooldown установлен на %d сек", COOLDOWN_EX_SEC)
            return True
        log.debug("⏲️ LAB_CLEANER: пропуск — cooldown активен")
        return False
    except Exception:
        log.exception("⚠️ LAB_CLEANER: ошибка установки cooldown")
        return False


# берём «running»-замок, чтобы не запускаться параллельно
async def _try_acquire_running() -> bool:
    try:
        ok = await infra.redis_client.set(RUNNING_LOCK_KEY, "1", ex=RUNNING_EX_SEC, nx=True)
        if ok:
            log.debug("🔒 LAB_CLEANER: running-lock установлен (EX=%d)", RUNNING_EX_SEC)
            return True
        log.debug("🔒 LAB_CLEANER: пропуск — уже выполняется другая чистка")
        return False
    except Exception:
        log.exception("⚠️ LAB_CLEANER: ошибка установки running-lock")
        return False


# снимаем «running»-замок
async def _release_running():
    try:
        await infra.redis_client.delete(RUNNING_LOCK_KEY)
        log.debug("🔓 LAB_CLEANER: running-lock снят")
    except Exception:
        log.exception("⚠️ LAB_CLEANER: ошибка снятия running-lock")


# 🔸 Один цикл очистки (БД + стрим)
async def _cleanup_once():
    cutoff_dt = datetime.utcnow().replace(tzinfo=None) - timedelta(days=RETENTION_DAYS)

    # чистка БД в транзакции
    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            bl_del   = await _exec_delete(conn, "DELETE FROM laboratory_bl_analysis WHERE computed_at < $1", cutoff_dt)
            wl_del   = await _exec_delete(conn, "DELETE FROM laboratory_wl_analysis WHERE computed_at < $1", cutoff_dt)
            tf_del   = await _exec_delete(
                conn,
                """
                DELETE FROM laboratory_request_tf tf
                USING laboratory_request_head h
                WHERE tf.req_id = h.req_id
                  AND h.finished_at < $1
                """,
                cutoff_dt
            )
            head_del = await _exec_delete(conn, "DELETE FROM laboratory_request_head WHERE finished_at < $1", cutoff_dt)
            pos_del  = await _exec_delete(conn, "DELETE FROM laboratory_positions_stat WHERE updated_at < $1", cutoff_dt)

    log.info(
        "🧹 LAB_CLEANER: DB cleanup done (older than %s) — bl=%d wl=%d tf=%d head=%d pos=%d",
        cutoff_dt.isoformat(), bl_del, wl_del, tf_del, head_del, pos_del
    )

    # чистим стрим ответов по MINID < now-24h
    await _trim_decision_response_stream()


# выполняем DELETE и возвращаем число затронутых строк
async def _exec_delete(conn, sql: str, *args) -> int:
    res = await conn.execute(sql, *args)
    try:
        return int(res.split()[-1])
    except Exception:
        return 0


# подрезка стрима laboratory:decision_response по MINID < now-24h
async def _trim_decision_response_stream():
    stream = "laboratory:decision_response"
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - STREAM_RETENTION_HOURS * 3600 * 1000
    minid = f"{cutoff_ms}-0"

    trimmed = None
    try:
        trimmed = await infra.redis_client.xtrim(stream, minid=minid, approximate=True)
    except Exception:
        try:
            trimmed = await infra.redis_client.execute_command("XTRIM", stream, "MINID", "~", minid)
        except Exception:
            log.exception("⚠️ LAB_CLEANER: XTRIM MINID не удалось (stream=%s)", stream)
            return

    try:
        cnt = int(trimmed) if trimmed is not None else -1
    except Exception:
        cnt = -1
    log.info("🧽 LAB_CLEANER: stream trimmed (stream=%s, MINID<%s, removed=%s)", stream, minid, cnt if cnt >= 0 else "unknown")