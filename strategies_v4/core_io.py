# core_io.py

import asyncio
import logging
from datetime import datetime
from infra import infra

# 🔸 Логгер для I/O-операций
log = logging.getLogger("CORE_IO")

# 🔸 Воркер: запись логов сигналов с использованием Consumer Group
async def run_signal_log_writer():
    stream_name = "signal_log_queue"
    group_name = "core_io_group"
    consumer_name = "core_io_1"
    buffer_limit = 100
    flush_interval_sec = 1.0
    redis = infra.redis_client
    pg = infra.pg_pool

    # 🔹 Попытка создать группу (один раз при запуске)
    try:
        await redis.xgroup_create(stream_name, group_name, id="$", mkstream=True)
        log.info(f"🔧 Группа {group_name} создана для {stream_name}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Группа {group_name} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info(f"📡 Подписка через Consumer Group: {stream_name} → {group_name}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=buffer_limit,
                block=int(flush_interval_sec * 1000)
            )

            if not entries:
                continue

            for stream_key, records in entries:
                buffer = []
                ack_ids = []

                for record_id, data in records:
                    try:
                        buffer.append(_parse_signal_log_data(data))
                        ack_ids.append(record_id)
                    except Exception:
                        log.exception(f"❌ Ошибка парсинга записи (id={record_id})")

                if buffer:
                    await write_log_entry_batch(buffer)
                    for rid in ack_ids:
                        await redis.xack(stream_name, group_name, rid)

        except Exception:
            log.exception("❌ Ошибка в loop Consumer Group")
            await asyncio.sleep(5)  
# 🔸 Преобразование данных из Redis Stream
def _parse_signal_log_data(data: dict) -> tuple:
    return (
        data["log_uid"],
        int(data["strategy_id"]),
        data["status"],
        data.get("note"),
        data.get("position_uid"),
        datetime.fromisoformat(data["logged_at"])
    )

# 🔸 Батч-запись логов в PostgreSQL
async def write_log_entry_batch(batch: list[tuple]):
    if not batch:
        return

    try:
        await infra.pg_pool.executemany(
            '''
            INSERT INTO signal_log_entries_v4 (
                log_uid, strategy_id, status, note, position_uid, logged_at
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ''',
            batch
        )
        log.info(f"✅ Записано логов сигналов: {len(batch)}")
    except Exception:
        log.exception("❌ Ошибка записи логов сигналов в БД")