# core_io.py

import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal

from infra import infra
from infra import get_field, set_field

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
# 🔸 Обновление позиции и целей в БД по потоку обновлений
async def update_position_and_targets(pool, record: dict):
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            # Обновление основной информации о позиции
            await conn.execute(
                """
                UPDATE positions_v4
                SET
                    quantity_left = $1,
                    status = $2,
                    exit_price = $3,
                    close_reason = $4,
                    pnl = $5,
                    closed_at = $6,
                    planned_risk = $7
                WHERE position_uid = $8
                """,
                Decimal(record["quantity_left"]),
                record["status"],
                Decimal(record["exit_price"]) if record.get("exit_price") else None,
                record.get("close_reason"),
                Decimal(record["pnl"]),
                datetime.fromisoformat(record["closed_at"]) if record.get("closed_at") else None,
                Decimal(record["planned_risk"]),
                record["position_uid"]
            )

            # Обновление или вставка TP и SL целей
            for target in record.get("tp_targets", []) + record.get("sl_targets", []):
                result = await conn.execute(
                    """
                    UPDATE position_targets_v4
                    SET
                        hit = $1,
                        hit_at = $2,
                        canceled = $3
                    WHERE position_uid = $4 AND level = $5 AND type = $6
                    """,
                    target["hit"],
                    datetime.fromisoformat(target["hit_at"]) if target.get("hit_at") else None,
                    target["canceled"],
                    record["position_uid"],
                    int(target["level"]),
                    target["type"]
                )

                if result == "UPDATE 0":
                    await conn.execute(
                        """
                        INSERT INTO position_targets_v4 (
                            position_uid, type, level, price, quantity,
                            hit, hit_at, canceled, source
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                        """,
                        record["position_uid"],
                        target["type"],
                        int(target["level"]),
                        Decimal(target["price"]) if target.get("price") is not None else None,
                        Decimal(target["quantity"]),
                        target["hit"],
                        datetime.fromisoformat(target["hit_at"]) if target.get("hit_at") else None,
                        target["canceled"],
                        target["source"]
                    )

            await tx.commit()
            log.info(f"💾 Обновление позиции завершено: uid={record['position_uid']}")
        except Exception as e:
            await tx.rollback()
            log.warning(f"❌ Ошибка обновления позиции: {e}")
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
# 🔸 Повторная маршрутизация противоположного сигнала после реверса
async def reverse_entry(position_uid: str):
    from infra import infra
    import logging
    import json
    from datetime import datetime

    log = logging.getLogger("REVERSE_ENTRY")

    try:
        # 1. Получить параметры закрытой позиции
        pos = await infra.pg_pool.fetchrow("""
            SELECT strategy_id, symbol, direction, closed_at
            FROM positions_v4
            WHERE position_uid = $1
        """, position_uid)

        if not pos:
            log.warning(f"[REVERSE_ENTRY] Позиция не найдена: uid={position_uid}")
            return

        strategy_id = pos["strategy_id"]
        symbol = pos["symbol"]
        direction = pos["direction"]
        closed_at = pos["closed_at"]

        # 2. Получить сигнальные фразы стратегии
        strategy_row = await infra.pg_pool.fetchrow("""
            SELECT long_phrase, short_phrase
            FROM signals_v4
            WHERE id = $1
        """, strategy_id)

        if not strategy_row:
            log.warning(f"[REVERSE_ENTRY] Не найдены фразы для стратегии id={strategy_id}")
            return

        long_phrase = strategy_row["long_phrase"]
        short_phrase = strategy_row["short_phrase"]

        # 3. Определить противоположную фразу
        opposite_phrase = short_phrase if direction == "long" else long_phrase

        # 4. Найти последний противоположный сигнал
        row = await infra.pg_pool.fetchrow("""
            SELECT raw_message
            FROM signals_v4_log
            WHERE raw_message->>'symbol' = $1
              AND raw_message->>'message' LIKE $2
              AND created_at <= $3
            ORDER BY id DESC
            LIMIT 1
        """, symbol, f"%{opposite_phrase}", closed_at)

        if not row or not row["raw_message"]:
            log.warning(f"[REVERSE_ENTRY] Не найден противоположный сигнал для {symbol}")
            return

        raw_data = json.loads(row["raw_message"])

        # 5. Сформировать и отправить новый сигнал
        payload = {
            "message": raw_data.get("message"),
            "symbol": raw_data.get("symbol"),
            "bar_time": raw_data.get("bar_time", ""),
            "sent_at": raw_data.get("sent_at", ""),
            "received_at": datetime.utcnow().isoformat()
        }

        await infra.redis_client.xadd("signals_stream", payload)

        log.info(f"[REVERSE_ENTRY] Повторно отправлен сигнал: symbol={payload['symbol']}, message={payload['message']}")

    except Exception as e:
        log.exception(f"[REVERSE_ENTRY] Ошибка обработки позиции uid={position_uid}: {e}")
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
# 🔸 Воркер обновлений позиции из Redis-потока
async def run_position_update_writer():
    log.info("🛠 [CORE_IO] Запуск обработчика обновлений позиций")

    redis = infra.redis_client
    pool = infra.pg_pool
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={"positions_update_stream": last_id},
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
                        await update_position_and_targets(pool, record)
                    except Exception as e:
                        log.warning(f"⚠️ [CORE_IO] Ошибка обработки обновления позиции: {e}")
        except Exception:
            log.exception("❌ [CORE_IO] Ошибка чтения из Redis Stream (positions_update_stream)")
            await asyncio.sleep(5)