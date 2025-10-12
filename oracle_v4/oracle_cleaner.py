# 🔸 oracle_cleaner.py — воркер очистки: ретеншн по БД (7 суток) и Redis Streams (24 часа)

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_CLEANER")

# 🔸 Константы воркера / параметры очистки
# глубина хранения БД (скользящее окно)
DB_RETENTION_DAYS = 7
# глубина хранения сообщений в Redis Streams (часы, скользящее окно)
STREAM_RETENTION_HOURS = 24
# параметры чтения стримов-триггеров
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Стримы для триггера (по сообщениям этих стримов запускаем уборку)
CLEAN_TRIGGER_STREAMS: Tuple[str, str] = (
    "oracle:mw_whitelist:reports_ready",
    "oracle:pack_lists:reports_ready",
)

# 🔸 Полный список стримов oracle_v4 для чистки по времени
ALL_ORACLE_STREAMS: Tuple[str, ...] = (
    "oracle:mw:reports_ready",
    "oracle:mw_sense:reports_ready",
    "oracle:mw_whitelist:reports_ready",
    "oracle:pack:reports_ready",
    "oracle:pack_sense:reports_ready",
    "oracle:pack_lists:build_ready",
    "oracle:pack_lists:reports_ready",
)

# 🔸 Группа/имя потребителя для «уборщика»
CLEANER_CONSUMER_GROUP = "oracle_cleaner_group"
CLEANER_CONSUMER_NAME = "oracle_cleaner_worker"


# 🔸 Публичная точка входа воркера (запускать из oracle_v4_main.py через run_safe_loop)
async def run_oracle_cleaner():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск CLEANER: PG/Redis не инициализированы")
        return

    # создаём consumer group для обоих триггер-стримов (идемпотентно)
    await _ensure_consumer_groups()

    log.debug("🚀 Старт воркера CLEANER (db_retention=%sd, stream_retention=%sh)", DB_RETENTION_DAYS, STREAM_RETENTION_HOURS)

    # основной цикл чтения сообщений из двух стримов
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=CLEANER_CONSUMER_GROUP,
                consumername=CLEANER_CONSUMER_NAME,
                streams={CLEAN_TRIGGER_STREAMS[0]: ">", CLEAN_TRIGGER_STREAMS[1]: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            # собираем id сообщений по каждому стриму
            to_ack: Dict[str, List[str]] = {}
            for stream_name, msgs in resp:
                ids = [mid for (mid, _fields) in msgs]
                if ids:
                    to_ack.setdefault(stream_name, []).extend(ids)

            # выполняем единичный проход уборки (БД + все стримы)
            await _cleanup_once()

            # ACK всех сообщений только после успешной уборки
            for stream_name, ids in to_ack.items():
                try:
                    await infra.redis_client.xack(stream_name, CLEANER_CONSUMER_GROUP, *ids)
                except Exception:
                    log.exception("⚠️ Ошибка ACK в стриме %s (ids=%s)", stream_name, ids)

        except asyncio.CancelledError:
            log.debug("⏹️ CLEANER остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла CLEANER — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Вспомогательные функции

async def _ensure_consumer_groups():
    # создаём группу для каждого триггер-стрима (идемпотентно)
    for s in CLEAN_TRIGGER_STREAMS:
        try:
            await infra.redis_client.xgroup_create(name=s, groupname=CLEANER_CONSUMER_GROUP, id="$", mkstream=True)
            log.debug("📡 Создана consumer group для стрима: %s", s)
        except Exception as e:
            # если группа уже существует — это норм
            if "BUSYGROUP" in str(e):
                continue
            log.exception("❌ Ошибка создания consumer group для стрима %s", s)
            raise


async def _cleanup_once():
    # вычислим срезы для логов (SQL использует now() на стороне БД)
    cutoff_db = (datetime.utcnow().replace(tzinfo=None) - timedelta(days=DB_RETENTION_DAYS)).isoformat()
    # уборка БД (в одной транзакции)
    await _cleanup_db()
    # механическая чистка всех стримов oracle_v4
    await _trim_streams()

    # финальный лог-итог прохода
    log.info("🧹 Уборка завершена: cutoff_db=%s, stream_retention=%sh", cutoff_db, STREAM_RETENTION_HOURS)

async def _cleanup_db():
    retention_td = timedelta(days=DB_RETENTION_DAYS)

    async with infra.pg_pool.acquire() as conn:
        async with conn.transaction():
            # удаляем маркеры processed (MW/PACK)
            conf_mw_deleted = await conn.fetchval(
                """
                WITH del AS (
                  DELETE FROM oracle_conf_processed
                   WHERE window_end < (now() - $1)
                   RETURNING 1
                )
                SELECT COUNT(*)::int FROM del
                """,
                retention_td,
            )
            conf_pack_deleted = await conn.fetchval(
                """
                WITH del AS (
                  DELETE FROM oracle_pack_conf_processed
                   WHERE window_end < (now() - $1)
                   RETURNING 1
                )
                SELECT COUNT(*)::int FROM del
                """,
                retention_td,
            )

            # удаляем шапки отчётов (каскадно почистит агрегаты/sense/WL/BL)
            reports_deleted = await conn.fetchval(
                """
                WITH del AS (
                  DELETE FROM oracle_report_stat
                   WHERE window_end < (now() - $1)
                   RETURNING 1
                )
                SELECT COUNT(*)::int FROM del
                """,
                retention_td,
            )

    log.info(
        "🗄️ DB cleanup: reports_deleted=%d, conf_mw_deleted=%d, conf_pack_deleted=%d (retention=%sd)",
        int(reports_deleted or 0),
        int(conf_mw_deleted or 0),
        int(conf_pack_deleted or 0),
        DB_RETENTION_DAYS,
    )

async def _trim_streams():
    # узнаём серверное время Redis (секунды, микросекунды) и считаем minid для XTRIM MINID
    try:
        tsec, tmicro = await infra.redis_client.time()
    except Exception:
        # если команда TIME недоступна — используем локальное время как fallback
        now_ms = int(datetime.utcnow().timestamp() * 1000)
    else:
        now_ms = int(tsec) * 1000 + int(tmicro) // 1000

    cutoff_ms = now_ms - (STREAM_RETENTION_HOURS * 3600 * 1000)
    minid = f"{cutoff_ms}-0"

    total_deleted = 0
    # проходим по всем известным стримам комплекса и подрезаем старые сообщения
    for stream in ALL_ORACLE_STREAMS:
        try:
            # XTRIM MINID ~ <minid>
            deleted = await infra.redis_client.xtrim(name=stream, minid=minid, approximate=True)
            # redis-py возвращает число удалённых, приведём к int
            d = int(deleted or 0)
            total_deleted += d
            if d > 0:
                log.info("🧽 Redis trim: stream=%s minid=%s deleted=%d", stream, minid, d)
        except Exception:
            log.exception("⚠️ Ошибка XTRIM MINID для стрима %s (minid=%s)", stream, minid)

    # сводка по стримам
    log.info("📬 Redis streams cleanup: total_deleted=%d, retention=%sh (minid=%s)", total_deleted, STREAM_RETENTION_HOURS, minid)