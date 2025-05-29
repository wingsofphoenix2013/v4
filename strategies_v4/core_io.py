# core_io.py

import asyncio
import logging
import json
from datetime import datetime
from infra import infra

log = logging.getLogger("CORE_IO")

# 🔸 Название стрима, из которого получаем логи сигналов
SIGNAL_LOG_STREAM = "signal_log_queue"

# 🔸 Главный цикл записи логов сигналов в PG
async def run_signal_log_writer():
    log.info("📝 [CORE_IO] Запуск логгера сигналов")

    redis = infra.redis_client
    pool = infra.pg_pool
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={SIGNAL_LOG_STREAM: last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for _, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id

                    try:
                        payload = json.loads(msg_data.get("data"))
                        await write_log_entry(pool, payload)
                    except Exception as e:
                        log.warning(f"⚠️ Ошибка обработки лог-записи: {e}")

        except Exception as e:
            log.exception("❌ Ошибка чтения из Redis — повтор через 5 сек")
            await asyncio.sleep(5)

# 🔸 Функция записи одной строки в таблицу signal_log_entries_v4
async def write_log_entry(pool, record: dict):
    query = """
        INSERT INTO signal_log_entries_v4
        (log_id, strategy_id, status, position_id, note, logged_at)
        VALUES ($1, $2, $3, $4, $5, $6)
    """
    values = (
        int(record.get("log_id")),
        int(record.get("strategy_id")),
        record.get("status"),
        int(record["position_id"]) if record.get("position_id") is not None else None,
        record.get("note"),
        datetime.fromisoformat(record.get("logged_at"))
    )

    async with pool.acquire() as conn:
        await conn.execute(query, *values)
        log.info(f"💾 Записан лог сигнала: strategy={values[1]}, status={values[2]}")