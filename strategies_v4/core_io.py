# core_io.py

import asyncio
import logging
from datetime import datetime
from infra import infra

# 🔸 Логгер для I/O-операций
log = logging.getLogger("CORE_IO")

# 🔸 Воркер: запись логов сигналов из Redis в PostgreSQL
async def run_signal_log_writer():
    stream_name = "signal_log_queue"
    redis = infra.redis_client
    pg = infra.pg_pool
    last_id = "0"
    buffer = []
    buffer_limit = 100
    flush_interval_sec = 1.0

    log.info(f"📡 Подписка на Redis Stream: {stream_name}")

    while True:
        try:
            entries = await redis.xread(
                {stream_name: last_id},
                count=buffer_limit,
                block=int(flush_interval_sec * 1000)
            )

            if not entries:
                continue

            for stream_key, records in entries:
                for record_id, data in records:
                    try:
                        last_id = record_id
                        buffer.append(_parse_signal_log_data(data))
                    except Exception:
                        log.exception("❌ Ошибка парсинга записи лога сигнала")

            if buffer:
                await write_log_entry_batch(buffer)
                buffer.clear()

        except Exception:
            log.exception("❌ Ошибка при обработке Redis Stream")
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