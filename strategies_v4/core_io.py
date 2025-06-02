# core_io.py

import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal

from infra import infra

log = logging.getLogger("CORE_IO")

SIGNAL_LOG_STREAM = "signal_log_queue"
POSITIONS_STREAM = "positions_stream"

# 🔸 Запись лога сигнала
async def write_log_entry(pool, record: dict):
    query = """
        INSERT INTO signal_log_entries_v4
        (log_id, strategy_id, status, position_uid, note, logged_at)
        VALUES ($1, $2, $3, $4, $5, $6)
    """
    async with pool.acquire() as conn:
        try:
            values = (
                int(record["log_id"]),
                int(record["strategy_id"]),
                record["status"],
                record.get("position_uid"),  # ← безопасно
                record.get("note"),
                datetime.fromisoformat(record["logged_at"])
            )
            await conn.execute(query, *values)
            log.debug(f"💾 Записан лог сигнала: strategy={values[1]}, status={values[2]}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка обработки лог-записи: {e}")
            
# 🔸 Запись позиции и целей
async def write_position_and_targets(pool, record: dict):
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()

        try:
            await conn.execute(
                """
                INSERT INTO positions_v4 (
                    position_uid, strategy_id, symbol, direction, entry_price,
                    quantity, quantity_left, status, created_at,
                    exit_price, closed_at, close_reason, pnl,
                    planned_risk, notional_value, route, log_id
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
                """,
                record["position_uid"],
                int(record["strategy_id"]),
                record["symbol"],
                record["direction"],
                Decimal(record["entry_price"]),
                Decimal(record["quantity"]),
                Decimal(record["quantity_left"]),
                record["status"],
                datetime.fromisoformat(record["created_at"]),
                None,  # exit_price
                None,  # closed_at
                record.get("close_reason"),
                Decimal(record.get("pnl", "0")),
                Decimal(record["planned_risk"]),
                Decimal(record["notional_value"]),
                record["route"],
                int(record["log_id"])
            )

            for target in record.get("tp_targets", []) + record.get("sl_targets", []):
                await conn.execute(
                    """
                    INSERT INTO position_targets_v4 (
                        position_uid, type, level, price, quantity,
                        hit, hit_at, canceled, source
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    """,
                    record["position_uid"],
                    target["type"],
                    int(target["level"]),
                    Decimal(target["price"]) if target["price"] is not None else None,
                    Decimal(target["quantity"]),
                    target["hit"],
                    datetime.fromisoformat(target["hit_at"]) if target["hit_at"] else None,
                    target["canceled"],
                    target["source"]
                )

            await tx.commit()
            log.debug(f"💾 Позиция записана в БД: uid={record['position_uid']}")
        except Exception as e:
            await tx.rollback()
            log.warning(f"❌ Ошибка записи позиции: {e}")
# 🔸 Чтение логов сигналов
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

# 🔸 Чтение позиций из Redis и запись в БД
async def run_position_writer():
    log.info("📝 [CORE_IO] Запуск воркера записи позиций")

    redis = infra.redis_client
    pool = infra.pg_pool
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={POSITIONS_STREAM: last_id},
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
                        await write_position_and_targets(pool, record)
                    except Exception as e:
                        log.warning(f"⚠️ Ошибка обработки позиции: {e}")
        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream (positions)")
            await asyncio.sleep(5)