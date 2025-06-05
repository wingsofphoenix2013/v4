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

# 📌 Универсальный доступ к полям TP/SL цели
def get_field(obj, field, default=None):
    return obj.get(field, default) if isinstance(obj, dict) else getattr(obj, field, default)

def set_field(obj, field, value):
    if isinstance(obj, dict):
        obj[field] = value
    else:
        setattr(obj, field, value)
        
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
                    get_field(target, "type"),
                    int(get_field(target, "level")),
                    Decimal(get_field(target, "price")) if get_field(target, "price") is not None else None,
                    Decimal(get_field(target, "quantity")),
                    get_field(target, "hit"),
                    datetime.fromisoformat(get_field(target, "hit_at")) if get_field(target, "hit_at") else None,
                    get_field(target, "canceled"),
                    get_field(target, "source")
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
                    get_field(target, "hit"),
                    datetime.fromisoformat(get_field(target, "hit_at")) if get_field(target, "hit_at") else None,
                    get_field(target, "canceled"),
                    record["position_uid"],
                    int(get_field(target, "level")),
                    get_field(target, "type")
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
                        get_field(target, "type"),
                        int(get_field(target, "level")),
                        Decimal(get_field(target, "price")) if get_field(target, "price") is not None else None,
                        Decimal(get_field(target, "quantity")),
                        get_field(target, "hit"),
                        datetime.fromisoformat(get_field(target, "hit_at")) if get_field(target, "hit_at") else None,
                        get_field(target, "canceled"),
                        get_field(target, "source")
                    )

            await tx.commit()
            log.info(f"💾 Обновление позиции завершено: uid={record['position_uid']}")
        except Exception as e:
            await tx.rollback()
            log.warning(f"❌ Ошибка обновления позиции: {e}")
# 🔸 Повторная активация сигнала реверса через signals_stream
async def reverse_entry(payload: dict):
    position_uid = payload["position_uid"]
    redis = infra.redis_client
    pool = infra.pg_pool

    log.info(f"[REVERSE_ENTRY] Запуск реверса для позиции {position_uid}")

    async with pool.acquire() as conn:
        # Получаем log_id, closed_at и symbol из позиции
        row = await conn.fetchrow("""
            SELECT log_id, closed_at, symbol
            FROM positions_v4
            WHERE position_uid = $1
        """, position_uid)

        if not row:
            log.warning(f"[REVERSE_ENTRY] Позиция {position_uid} не найдена в БД — выход")
            return

        log_id = row["log_id"]
        closed_at = row["closed_at"]
        symbol = row["symbol"]

        if closed_at is None:
            closed_at = datetime.utcnow()
            log.warning(f"[REVERSE_ENTRY] closed_at был None — заменён на now(): {closed_at.isoformat()}")

        # Получаем направление сигнала, открывшего позицию
        sig = await conn.fetchrow("""
            SELECT direction
            FROM signals_v4_log
            WHERE id = $1
        """, log_id)

        if not sig or not sig["direction"]:
            log.warning(f"[REVERSE_ENTRY] Не удалось получить direction по log_id={log_id}")
            return

        direction = sig["direction"]
        reverse_direction = "short" if direction == "long" else "long"

        # Ищем последний сигнал обратного направления до закрытия позиции
        counter_sig = await conn.fetchrow("""
            SELECT message, bar_time, sent_at
            FROM signals_v4_log
            WHERE symbol = $1
              AND direction = $2
              AND received_at <= $3
            ORDER BY received_at DESC
            LIMIT 1
        """, symbol, reverse_direction, closed_at)

        if not counter_sig:
            log.warning(f"[REVERSE_ENTRY] Контр-сигнал не найден для {symbol} ({reverse_direction}) до {closed_at}")
            return

        message = counter_sig["message"]
        bar_time = counter_sig["bar_time"]
        sent_at = counter_sig["sent_at"]
        received_at = datetime.utcnow().isoformat()

        new_signal = {
            "symbol": symbol,
            "message": message,
            "bar_time": bar_time.isoformat(),
            "sent_at": sent_at.isoformat(),
            "received_at": received_at
        }

        log.info(f"[REVERSE_ENTRY] 📤 Публикация сигнала в signals_stream: {json.dumps(new_signal)}")

        try:
            # Отправка в Redis по полям, а не через data/json
            await redis.xadd("signals_stream", new_signal)
            log.info(f"📨 [REVERSE_ENTRY] Контр-сигнал отправлен для {symbol}")
        except Exception as e:
            log.warning(f"[REVERSE_ENTRY] Ошибка отправки в Redis: {e}")
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
# 🔄 Воркер: обрабатывает задачи реверса из Redis Stream
async def run_reverse_trigger_loop():
    log.info("🌀 [CORE_IO] Запуск воркера реверса")

    redis = infra.redis_client
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={"reverse_trigger_stream": last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for _, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id
                    try:
                        payload = json.loads(msg_data["data"])
                        position_uid = payload["position_uid"]
                        log.info(f"[REVERSE_TRIGGER] Получен UID позиции: {position_uid}")
                        await reverse_entry({"position_uid": position_uid})
                    except Exception as e:
                        log.warning(f"[REVERSE_TRIGGER] Ошибка обработки задачи: {e}")

        except Exception:
            log.exception("❌ [REVERSE_TRIGGER] Ошибка чтения из Redis Stream")
            await asyncio.sleep(5)