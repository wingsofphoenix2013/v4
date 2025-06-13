# core_io.py

import asyncio
import logging
from datetime import datetime
from infra import infra
import json
from decimal import Decimal

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
        log.debug(f"🔧 Группа {group_name} создана для {stream_name}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {group_name} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug(f"📡 Подписка через Consumer Group: {stream_name} → {group_name}")

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
        log.debug(f"✅ Записано логов сигналов: {len(batch)}")
    except Exception:
        log.exception("❌ Ошибка записи логов сигналов в БД")
# 🔹 Воркер: запись открытых позиций из positions_open_stream
async def run_position_open_writer():
    stream_name = "positions_open_stream"
    group_name = "core_io_position_group"
    consumer_name = "core_io_position_1"
    redis = infra.redis_client
    pg = infra.pg_pool

    try:
        await redis.xgroup_create(stream_name, group_name, id="$", mkstream=True)
        log.debug(f"🔧 Группа {group_name} создана для {stream_name}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {group_name} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group для позиций")
            return

    log.debug(f"📡 Подписка через Consumer Group: {stream_name} → {group_name}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=10,
                block=1000
            )

            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    try:
                        await _handle_open_position(data)
                        await redis.xack(stream_name, group_name, record_id)
                    except Exception:
                        log.exception(f"❌ Ошибка обработки позиции (id={record_id})")

        except Exception:
            log.exception("❌ Ошибка в loop обработки позиций")
            await asyncio.sleep(5)

# 🔹 Обработка одной позиции из Redis Stream
async def _handle_open_position(data: dict):
    # 🔸 Декодирование TP/SL целей
    tp_targets = json.loads(data["tp_targets"])
    sl_targets = json.loads(data["sl_targets"])

    # 🔸 Основные поля позиции
    position_uid = data["position_uid"]
    strategy_id = int(data["strategy_id"])
    symbol = data["symbol"]
    direction = data["direction"]
    entry_price = Decimal(data["entry_price"])
    quantity = Decimal(data["quantity"])
    quantity_left = Decimal(data["quantity_left"])
    created_at = datetime.fromisoformat(data["created_at"])
    planned_risk = Decimal(data["planned_risk"])
    route = data["route"]
    log_uid = data["log_uid"]
    event_type = data["event_type"]
    logged_at = datetime.utcnow()

    # 🔸 Получение точного received_at из signals_v4_log
    try:
        row = await infra.pg_pool.fetchrow(
            "SELECT received_at::timestamp AS received_at FROM signals_v4_log WHERE uid = $1",
            log_uid
        )
        if row and row["received_at"]:
            dt = row["received_at"]
            received_at_dt = dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
        else:
            received_at_dt = logged_at
    except Exception:
        log.warning(f"⚠️ Не удалось получить received_at из signals_v4_log для uid={log_uid}")
        received_at_dt = logged_at

    # 🔹 INSERT: positions_v4
    await infra.pg_pool.execute(
        '''
        INSERT INTO positions_v4 (
            position_uid, strategy_id, symbol, direction, entry_price,
            quantity, quantity_left, status, created_at, planned_risk,
            route, log_uid
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,'open',$8,$9,$10,$11)
        ''',
        position_uid, strategy_id, symbol, direction, entry_price,
        quantity, quantity_left, created_at, planned_risk,
        route, log_uid
    )

    # 🔹 INSERT: position_targets_v4
    targets = []
    for t in tp_targets + sl_targets:
        targets.append((
            t["type"],
            t["level"],
            Decimal(t["price"]) if t["price"] is not None else None,
            Decimal(t["quantity"]),
            t["hit"],
            t.get("hit_at"),
            t["canceled"],
            position_uid
        ))

    await infra.pg_pool.executemany(
        '''
        INSERT INTO position_targets_v4 (
            type, level, price, quantity, hit, hit_at, canceled, position_uid
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ''',
        targets
    )

    # 🔹 INSERT: positions_log_v4
    await infra.pg_pool.execute(
        '''
        INSERT INTO positions_log_v4 (
            position_uid, strategy_id, symbol, event_type,
            note, received_at, logged_at, latency_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ''',
        position_uid, strategy_id, symbol, event_type,
        "открытие позиции",
        received_at_dt,
        logged_at,
        int((logged_at - received_at_dt).total_seconds() * 1000)
    )

    log.debug(f"✅ Позиция {position_uid} записана в БД")