# trader_position_closer.py — воркер закрытия: читает positions_bybit_status(closed.*) → планирует close-заказ → апдейтит БД → кладёт задание исполнителю

# 🔸 Импорты
import os
import json
import asyncio
import logging
import hashlib
from typing import Dict, Tuple

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("TRADER_POS_CLOSER")

# 🔸 Константы стримов/CG
POS_STATUS_STREAM = "positions_bybit_status"       # источник событий (closed.*)
ORDERS_STREAM = "positions_bybit_orders"           # задания для bybit_processor
POS_CLOSE_CG = "trader_pos_close_cg"
POS_CLOSE_CONSUMER = os.getenv("POS_CLOSE_CONSUMER", "pos-close-1")

# 🔸 Параметры воркера
MAX_PARALLEL_TASKS = int(os.getenv("POS_CLOSE_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("POS_CLOSE_LOCK_TTL", "30"))

# 🔸 ENV: режим отправки
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной запуск воркера
async def run_trader_position_closer():
    redis = infra.redis_client

    # создание CG для чтения статусов (id="$" — только новые записи)
    try:
        await redis.xgroup_create(POS_STATUS_STREAM, POS_CLOSE_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", POS_CLOSE_CG, POS_STATUS_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", POS_STATUS_STREAM, POS_CLOSE_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", POS_CLOSE_CG, POS_STATUS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", POS_CLOSE_CG, POS_STATUS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    while True:
        try:
            # читаем только новые записи
            batch = await redis.xreadgroup(
                groupname=POS_CLOSE_CG,
                consumername=POS_CLOSE_CONSUMER,
                streams={POS_STATUS_STREAM: ">"},
                count=100,
                block=1000,  # мс
            )
            if not batch:
                continue

            # собираем задачи в параллель с ограничением
            tasks = []
            for _, records in batch:
                for entry_id, fields in records:
                    tasks.append(
                        asyncio.create_task(
                            _handle_status_entry(sem, entry_id, fields)
                        )
                    )

            # ждём завершения обработки пакета
            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка чтения/обработки из стрима %s", POS_STATUS_STREAM)
            await asyncio.sleep(1)


# 🔸 Обработка одной записи из positions_bybit_status (только closed.*)
async def _handle_status_entry(sem: asyncio.Semaphore, entry_id: str, fields: dict):
    async with sem:
        redis = infra.redis_client

        # фильтруем только closed.*
        event = fields.get("event")
        if not (event and str(event).startswith("closed")):
            try:
                await redis.xack(POS_STATUS_STREAM, POS_CLOSE_CG, entry_id)
                log.info("↷ Пропущено событие %s (id=%s): ACK", event, entry_id)
            except Exception:
                log.exception("❌ Ошибка ACK пропущенного события (id=%s)", entry_id)
            return

        # парсим обязательные поля
        try:
            sid = int(fields["strategy_id"])
            position_uid = fields["position_uid"]
            symbol = fields["symbol"]
            direction = fields.get("direction", "")
            strategy_type = fields.get("strategy_type", "plain")
            stream_id = fields.get("stream_id") or entry_id  # корреляция
            close_reason = event
        except Exception:
            log.exception("❌ Ошибка парсинга обязательных полей closed.* (id=%s)", entry_id)
            # безопасно ACK, чтобы не зациклиться, и оставить след в логах
            try:
                await redis.xack(POS_STATUS_STREAM, POS_CLOSE_CG, entry_id)
            except Exception:
                pass
            return

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок в Redis
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{POS_CLOSE_CONSUMER}-{entry_id}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                # не удалось взять замок — подождём и повторим локально, без ACK
                for _ in range(10):
                    await asyncio.sleep(0.2)
                    if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                        break
                else:
                    log.info("⏳ Не взят замок %s — отложено (id=%s)", gate_key, entry_id)
                    return

            try:
                # поиск строки по position_uid
                row = await infra.pg_pool.fetchrow(
                    """
                    SELECT id, strategy_id, symbol, ext_status, order_mode
                    FROM trader_positions_log
                    WHERE position_uid = $1
                    """,
                    position_uid,
                )

                # если позиции нет — фиксируем и ACK
                if not row:
                    await _update_signal_status(stream_id, "position_not_found", "no trader_positions_log row for position_uid; skipped exchange close")
                    await redis.xack(POS_STATUS_STREAM, POS_CLOSE_CG, entry_id)
                    log.info("❎ position_not_found ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                ext_status = (row["ext_status"] or "").strip()
                order_mode = (row["order_mode"] or TRADER_ORDER_MODE).strip()

                # если уже закрыта — фиксируем и ACK
                if ext_status == "closed":
                    await _update_signal_status(stream_id, "already_closed", "arrived after exchange close")
                    await redis.xack(POS_STATUS_STREAM, POS_CLOSE_CG, entry_id)
                    log.info("🟢 already_closed ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                # проверка «занятости» по БД (рабочие статусы)
                if await _is_busy_in_db(sid, symbol):
                    log.info("🚧 Ключ (sid=%s,symbol=%s) занят по БД — откладываю (id=%s)", sid, symbol, entry_id)
                    return  # замок останется с TTL, запись не ACK — вернёмся позже

                # формируем order_link_id (<=36 симв), на основе stream_id
                order_link_id = _make_order_link_id(stream_id)

                # апдейт existing-строки: планируем close
                await _update_tpl_planned_close(
                    position_uid=position_uid,
                    order_link_id=order_link_id,
                    note=f"close planned: reason={close_reason}",
                )

                # публикация задания исполнителю (минимальный payload)
                orders_stream_id = await _publish_close_task(
                    order_link_id=order_link_id,
                    position_uid=position_uid,
                    strategy_id=sid,
                    symbol=symbol,
                    order_mode=order_mode,
                    source_stream_id=stream_id,
                    close_reason=close_reason,
                )

                # апдейты queued и статуса сигнала
                await _mark_queued_and_update_signal_close(
                    position_uid=position_uid,
                    orders_stream_id=orders_stream_id,
                    note=f"close planned & queued: reason={close_reason}",
                    processing_status="planned",
                    source_stream_id=stream_id,
                )

                # финальный ACK
                await redis.xack(POS_STATUS_STREAM, POS_CLOSE_CG, entry_id)
                log.info("✅ CLOSE planned & queued (sid=%s %s %s) [id=%s]", sid, symbol, direction, entry_id)

            except Exception:
                log.exception("❌ Ошибка обработки закрытия для sid=%s symbol=%s (id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                # освобождение распределённого замка
                await _release_dist_lock(gate_key, owner)


# 🔸 Проверка «занятости» по базе для (sid, symbol)
async def _is_busy_in_db(strategy_id: int, symbol: str) -> bool:
    # занятость: статус в одном из рабочих этапов (очередь/отправка/обработка)
    row = await infra.pg_pool.fetchrow(
        """
        SELECT 1
        FROM trader_positions_log
        WHERE strategy_id = $1
          AND symbol = $2
          AND status IN ('queued','processing','sent')
        LIMIT 1
        """,
        strategy_id,
        symbol,
    )
    return bool(row)


# 🔸 Формирование короткого order_link_id (<=36) на основе stream_id
def _make_order_link_id(stream_id: str) -> str:
    base = f"tv4-{stream_id}"
    if len(base) <= 36:
        return base
    # fallback: детерминированный хэш (sha1) до 32 символов + "tv4-"
    short = hashlib.sha1(stream_id.encode("utf-8")).hexdigest()[:32]
    return f"tv4-{short}"  # длина 36


# 🔸 Апдейт planned (close) в trader_positions_log по position_uid
async def _update_tpl_planned_close(
    *,
    position_uid: str,
    order_link_id: str,
    note: str,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET status       = 'planned',
                order_link_id= $2,
                updated_at   = now(),
                note         = $3
            WHERE position_uid = $1
            """,
            position_uid,
            order_link_id,
            note,
        )
        log.info("📝 trader_positions_log planned(close): uid=%s, order_link_id=%s", position_uid, order_link_id)


# 🔸 Публикация close-задачи исполнителю (минимальный payload)
async def _publish_close_task(
    *,
    order_link_id: str,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    order_mode: str,
    source_stream_id: str,
    close_reason: str,
) -> str:
    redis = infra.redis_client

    payload = {
        "op": "close",
        "order_link_id": order_link_id,
        "position_uid": position_uid,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "order_mode": order_mode,             # dry_run | live
        "source_stream_id": source_stream_id, # корреляция
        "close_reason": close_reason,
    }

    orders_stream_id = await redis.xadd(ORDERS_STREAM, {"data": json.dumps(payload)})
    log.info("📤 Поставлено в %s (close): %s", ORDERS_STREAM, payload)
    return orders_stream_id


# 🔸 Обновление статуса queued и апдейт trader_signals (по stream_id закрытия)
async def _mark_queued_and_update_signal_close(
    *,
    position_uid: str,
    orders_stream_id: str,
    note: str,
    processing_status: str,
    source_stream_id: str,
):
    async with infra.pg_pool.acquire() as conn:
        # апдейт нашей строки
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET status          = 'queued',
                orders_stream_id= $2,
                queued_at       = now(),
                updated_at      = now(),
                note            = $3
            WHERE position_uid = $1
            """,
            position_uid,
            orders_stream_id,
            note,
        )
        # апдейт trader_signals по stream_id закрытия
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = $1,
                processed_at      = now(),
                processing_note   = $3
            WHERE stream_id = $2
            """,
            processing_status,
            source_stream_id,
            note,
        )
        log.info("✅ queued(close) & journal updated: position_uid=%s, stream_id=%s", position_uid, source_stream_id)


# 🔸 Апдейт статуса в trader_signals по stream_id
async def _update_signal_status(stream_id: str, processing_status: str, note: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = $1,
                processed_at      = now(),
                processing_note   = $3
            WHERE stream_id = $2
            """,
            processing_status,
            stream_id,
            note,
        )


# 🔸 Распределённый замок (SET NX EX)
async def _acquire_dist_lock(key: str, value: str, ttl: int) -> bool:
    try:
        ok = await infra.redis_client.set(key, value, ex=ttl, nx=True)
        return bool(ok)
    except Exception:
        log.exception("❌ Ошибка acquire lock %s", key)
        return False


# 🔸 Освобождение замка по владельцу (Lua check-and-del)
async def _release_dist_lock(key: str, value: str):
    # условия достаточности
    if not key:
        return
    try:
        lua = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """
        await infra.redis_client.eval(lua, 1, key, value)
    except Exception:
        # мягко логируем, замок всё равно истечёт по TTL
        log.debug("lock release fallback (key=%s)", key)