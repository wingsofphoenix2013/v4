import asyncio
import logging
import json
import infra
from dateutil import parser
from collections import deque
from datetime import datetime
import json

# 🔸 Вставка записи в таблицу signals_v4_log и публикация в стратегии
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

    # 🔹 Добавление в буфер
    signal_log_buffer.append(data)

    # 🔹 Рассылка по стратегиям
    if data["status"] == "dispatched":
        try:
            raw = json.loads(data["raw_message"])
            strategy_ids = raw.get("strategies", [])
        except Exception as e:
            log.warning(f"Ошибка разбора raw_message: {e}")
            return

        for strategy_id in strategy_ids:
            try:
                await infra.REDIS.xadd(
                    "strategy_input_stream",
                    {
                        "strategy_id": str(strategy_id),
                        "signal_id": str(data["signal_id"]),
                        "symbol": data["symbol"],
                        "direction": data["direction"],
                        "time": data["bar_time"],
                        "received_at": data["received_at"],
                        "log_uid": data["uid"]  # вместо log_id
                    }
                )
                await infra.record_counter("strategies_dispatched_total")
            except Exception as e:
                log.warning(f"xadd в strategy_input_stream не удался: {e}")
# 🔸 Буфер логов сигналов
signal_log_buffer = deque()
BUFFER_SIZE = 25         # Максимум логов за одну вставку
FLUSH_INTERVAL = 1.0     # Интервал проверки (секунды)

# 🔸 Пакетная вставка логов в signals_v4_log
async def flush_signal_logs():
    if not signal_log_buffer:
        return

    log = logging.getLogger("CORE_IO")
    batch = []
    while signal_log_buffer and len(batch) < BUFFER_SIZE:
        batch.append(signal_log_buffer.popleft())

    try:
        # 🔹 Задержка: считаем по последнему сигналу
        try:
            last_sent_at = parser.isoparse(batch[-1]["sent_at"]).replace(tzinfo=None)
            latency_ms = (datetime.utcnow() - last_sent_at).total_seconds() * 1000
            await infra.record_gauge("processing_latency_ms", latency_ms)
        except Exception:
            pass

        async with infra.PG_POOL.acquire() as conn:
            await conn.executemany("""
                INSERT INTO signals_v4_log (
                    signal_id, symbol, direction, source, message, raw_message,
                    bar_time, sent_at, received_at, status, uid
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                )
                ON CONFLICT (uid) DO NOTHING
            """, [
                (
                    int(d["signal_id"]),
                    d["symbol"],
                    d["direction"],
                    d["source"],
                    d["message"],
                    d["raw_message"],
                    parser.isoparse(d["bar_time"]).replace(tzinfo=None),
                    parser.isoparse(d["sent_at"]).replace(tzinfo=None),
                    parser.isoparse(d["received_at"]).replace(tzinfo=None),
                    d["status"],
                    d["uid"]
                ) for d in batch
            ])
        log.debug(f"Записан batch логов: {len(batch)}")
    except Exception as e:
        log.warning(f"Ошибка при batch insert: {e}")
        
# 🔸 Фоновая задача: флаш логов сигналов
async def run_flusher():
    while True:
        try:
            await flush_signal_logs()
        except Exception as e:
            logging.getLogger("CORE_IO").warning(f"Ошибка во flusher: {e}")
        await asyncio.sleep(FLUSH_INTERVAL)
        
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

    # 🔸 Фоновый запуск flusher
    asyncio.create_task(run_flusher())

    while True:
        try:
            messages = await infra.REDIS.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=500
            )
            if messages:
                for _, entries in messages:
                    for entry_id, entry_data in entries:
                        await insert_signal_log(dict(entry_data))
                        await infra.REDIS.xack(stream, group, entry_id)
        except Exception as e:
            log.exception(f"Ошибка в run_core_io: {e}")
            await asyncio.sleep(1)