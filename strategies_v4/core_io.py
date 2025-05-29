# core_io.py

import asyncio
import logging
import json
from datetime import datetime

from infra import infra

log = logging.getLogger("CORE_IO")

SIGNAL_LOG_STREAM = "signal_log_queue"

# 🔸 Функция записи одной строки в таблицу signal_log_entries_v4
async def write_log_entry(pool, record: dict):
    query = """
        INSERT INTO signal_log_entries_v4
        (log_id, strategy_id, status, position_id, note, logged_at)
        VALUES ($1, $2, $3, $4, $5, $6)
    """

    async with pool.acquire() as conn:
        try:
            # 🔸 Восстановление log_id, если отсутствует
            if record.get("log_id") is None and record.get("raw_message"):
                try:
                    msg = json.loads(record["raw_message"])
                    symbol = msg.get("symbol")
                    bar_time = msg.get("bar_time")
                    received_at = msg.get("received_at")

                    if not all([symbol, bar_time, received_at]):
                        raise ValueError("Недостаточно данных для восстановления log_id")

                    # 🔸 Преобразование ISO строк в datetime объекты
                    bar_time = datetime.fromisoformat(bar_time.replace("Z", "+00:00"))
                    received_at = datetime.fromisoformat(received_at.replace("Z", "+00:00"))

                    log_id = await conn.fetchval(
                        """
                        SELECT id FROM signals_v4_log
                        WHERE symbol = $1 AND bar_time = $2 AND received_at = $3
                        ORDER BY id DESC LIMIT 1
                        """,
                        symbol, bar_time, received_at
                    )

                    if log_id is None:
                        raise LookupError("log_id не найден в signals_v4_log")

                except Exception as e:
                    log.warning(f"⚠️ Не удалось восстановить log_id из raw_message: {e}")
                    return  # пропустить запись, не создавать ошибочную строку

            else:
                log_id = int(record.get("log_id"))

            values = (
                log_id,
                int(record.get("strategy_id")),
                record.get("status"),
                int(record["position_id"]) if record.get("position_id") is not None else None,
                record.get("note"),
                datetime.fromisoformat(record.get("logged_at"))
            )

            await conn.execute(query, *values)
            log.info(f"💾 Записан лог сигнала: strategy={values[1]}, status={values[2]}")

        except Exception as e:
            log.warning(f"⚠️ Ошибка обработки лог-записи: {e}")

# 🔸 Обработка сигналов из очереди Redis
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

            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id
                    try:
                        record = json.loads(msg_data["data"])
                        await write_log_entry(pool, record)
                    except Exception as e:
                        log.warning(f"⚠️ Ошибка обработки записи: {e}")
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(5)