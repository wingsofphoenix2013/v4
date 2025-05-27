import asyncio
import logging
import json
import infra
from dateutil import parser

# 🔸 Вставка записи в таблицу signals_v4_log
async def insert_signal_log(data: dict):
    log = logging.getLogger("CORE_IO")

    required_fields = [
        "signal_id", "symbol", "direction", "source", "message", "raw_message",
        "bar_time", "sent_at", "received_at", "status", "uid"
    ]
    for field in required_fields:
        if field not in data:
            log.warning(f"Пропущен лог: отсутствует поле {field} в {data}")
            return

    async with infra.PG_POOL.acquire() as conn:
        await conn.execute("""
            INSERT INTO signals_v4_log (
                signal_id,
                symbol,
                direction,
                source,
                message,
                raw_message,
                bar_time,
                sent_at,
                received_at,
                status,
                uid
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
            )
            ON CONFLICT (uid) DO NOTHING
        """,
        int(data["signal_id"]),
        data["symbol"],
        data["direction"],
        data["source"],
        data["message"],
        data["raw_message"],
        parser.isoparse(data["bar_time"]).replace(tzinfo=None),
        parser.isoparse(data["sent_at"]).replace(tzinfo=None),
        parser.isoparse(data["received_at"]).replace(tzinfo=None),
        data["status"],
        data["uid"])

    log.debug(f"Лог записан в БД: {data['uid']}")
# 🔸 Запуск логгера сигналов: чтение из Redis Stream и запись в БД
async def run_core_io():
    log = logging.getLogger("CORE_IO")
    stream = "signals_log_stream"
    group = "core_io"
    consumer = "writer-1"

    try:
        await infra.REDIS.xgroup_create(stream, group, id="0", mkstream=True)
        log.info(f"Группа {group} создана для {stream}")
    except Exception:
        pass  # группа уже существует

    while True:
        try:
            messages = await infra.REDIS.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=3000
            )
            if messages:
                for _, entries in messages:
                    for entry_id, entry_data in entries:
                        await insert_signal_log(dict(entry_data))
                        await infra.REDIS.xack(stream, group, entry_id)
        except Exception as e:
            log.exception(f"Ошибка в run_core_io: {e}")
            await asyncio.sleep(1)