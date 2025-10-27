# trader_position_opener.py — воркер открытия: читает positions_bybit_status(opened) → планирует ордер → пишет в БД → кладёт задание исполнителю

# 🔸 Импорты
import os
import json
import asyncio
import logging
import hashlib
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, Tuple, Optional

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("TRADER_POS_OPENER")

# 🔸 Константы стримов/CG
POS_STATUS_STREAM = "positions_bybit_status"       # источник событий (opened v2)
ORDERS_STREAM = "positions_bybit_orders"           # задания для bybit_processor
POS_OPEN_CG = "trader_pos_open_cg"
POS_OPEN_CONSUMER = os.getenv("POS_OPEN_CONSUMER", "pos-open-1")

# 🔸 Параметры воркера
MAX_PARALLEL_TASKS = int(os.getenv("POS_OPEN_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("POS_OPEN_LOCK_TTL", "30"))

# 🔸 ENV: расчёт размера и режим отправки
BYBIT_SIZE_PCT = Decimal(str(os.getenv("BYBIT_SIZE_PCT", "10"))).quantize(Decimal("0.0001"))
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}

# 🔸 Основной запуск воркера
async def run_trader_position_opener():
    redis = infra.redis_client

    # создание CG для чтения позиций (id="$" — только новые записи)
    try:
        await redis.xgroup_create(POS_STATUS_STREAM, POS_OPEN_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", POS_OPEN_CG, POS_STATUS_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", POS_STATUS_STREAM, POS_OPEN_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", POS_OPEN_CG, POS_STATUS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", POS_OPEN_CG, POS_STATUS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    while True:
        try:
            # читаем только новые записи
            batch = await redis.xreadgroup(
                groupname=POS_OPEN_CG,
                consumername=POS_OPEN_CONSUMER,
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

# 🔸 Обработка одной записи из positions_bybit_status
async def _handle_status_entry(sem: asyncio.Semaphore, entry_id: str, fields: dict):
    async with sem:
        redis = infra.redis_client

        # фильтруем только opened; остальное — просто ACK
        event = fields.get("event")
        if event != "opened":
            try:
                await redis.xack(POS_STATUS_STREAM, POS_OPEN_CG, entry_id)
                log.info("↷ Пропущено событие %s (id=%s): ACK", event, entry_id)
            except Exception:
                log.exception("❌ Ошибка ACK пропущенного события (id=%s)", entry_id)
            return

        # парсим обязательные поля opened v2
        try:
            sid = int(fields["strategy_id"])
            position_uid = fields["position_uid"]
            symbol = fields["symbol"]
            direction = fields["direction"]  # long|short
            strategy_type = fields.get("strategy_type", "plain")
            stream_id = fields.get("stream_id") or entry_id  # на всякий случай
            leverage = Decimal(str(fields.get("leverage", "0")))
            qty_left = _as_decimal(fields.get("quantity_left")) or _as_decimal(fields.get("quantity"))
            margin_used_virt = _as_decimal(fields.get("margin_used"))
            if margin_used_virt is None:
                # бракованное событие — фиксируем и ACK
                await _finalize_invalid_event(entry_id, sid, position_uid, symbol, direction, strategy_type, stream_id, fields)
                return
            if qty_left is None or qty_left <= 0:
                # тоже считаем бракованным
                await _finalize_invalid_event(entry_id, sid, position_uid, symbol, direction, strategy_type, stream_id, fields, note="qty_left<=0")
                return
        except Exception:
            log.exception("❌ Ошибка парсинга обязательных полей opened (id=%s)", entry_id)
            # безопасно ACK, чтобы не зациклиться, и оставить след в логах
            try:
                await infra.redis_client.xack(POS_STATUS_STREAM, POS_OPEN_CG, entry_id)
            except Exception:
                pass
            return

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок в Redis
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{POS_OPEN_CONSUMER}-{entry_id}"
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
                # проверка «занятости» по базе (ext_status / статусы обработки)
                if await _is_busy_in_db(sid, symbol):
                    log.info("🚧 Ключ (sid=%s,symbol=%s) занят по БД — откладываю (id=%s)", sid, symbol, entry_id)
                    return  # замок останется с TTL, запись не ACK — вернёмся позже

                # расчёт размера: pct от виртуального количества → квантование вниз → проверка min_qty
                q_plan, q_raw, size_pct = await _plan_quantity(symbol, qty_left)

                # если не прошло min_qty — фиксируем skip и ACK
                if q_plan is None or q_plan <= 0:
                    await _finalize_planned_skip(
                        entry_id, sid, position_uid, symbol, direction, strategy_type, stream_id,
                        qty_left, q_raw, size_pct, margin_used_virt, fields
                    )
                    return

                # плановая маржа пропорционально объёму
                margin_plan = (margin_used_virt * (q_plan / qty_left)).quantize(Decimal("0.00000001"))

                # формируем order_link_id (<=36 симв), на основе stream_id
                order_link_id = _make_order_link_id(stream_id)

                # запись/апдейт в trader_positions_log со статусом planned
                await _upsert_trader_positions_log_planned(
                    source_stream_id=stream_id,
                    position_uid=position_uid,
                    strategy_id=sid,
                    strategy_type=strategy_type,
                    symbol=symbol,
                    direction=direction,
                    order_mode=TRADER_ORDER_MODE,
                    quantity_virt=qty_left,
                    quantity_plan=q_plan,
                    margin_used_virt=margin_used_virt,
                    margin_plan=margin_plan,
                    order_link_id=order_link_id,
                    extras=fields,
                )

                # публикация задания исполнителю
                orders_stream_id = await _publish_order_task(
                    order_link_id=order_link_id,
                    position_uid=position_uid,
                    strategy_id=sid,
                    strategy_type=strategy_type,
                    symbol=symbol,
                    direction=direction,
                    leverage=str(leverage),  # строкой для унификации
                    qty=str(q_plan),
                    size_mode="pct_of_virtual",
                    size_pct=str(size_pct),
                    margin_plan=str(margin_plan),
                    order_mode=TRADER_ORDER_MODE,
                    source_stream_id=stream_id,
                    ts=fields.get("ts"),
                    ts_ms=fields.get("ts_ms"),
                )

                # апдейт строки (queued) + апдейт trader_signals (planned)
                await _mark_queued_and_update_signal(
                    source_stream_id=stream_id,
                    orders_stream_id=orders_stream_id,
                    note=f"order planned & queued: qty={q_plan}",
                    processing_status="planned",
                )

                # финальный ACK
                await infra.redis_client.xack(POS_STATUS_STREAM, POS_OPEN_CG, entry_id)
                log.info("✅ OPEN planned & queued (sid=%s %s %s qty=%s) [id=%s]", sid, symbol, direction, q_plan, entry_id)

            except Exception:
                log.exception("❌ Ошибка обработки opened для sid=%s symbol=%s (id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                # освобождение распределённого замка
                await _release_dist_lock(gate_key, owner)


# 🔸 Подготовка размера: масштабирование и квантование вниз
async def _plan_quantity(symbol: str, qty_virt: Decimal):
    # условия достаточности
    if qty_virt is None or qty_virt <= 0:
        return None, None, BYBIT_SIZE_PCT

    size_pct = BYBIT_SIZE_PCT
    q_raw = (qty_virt * (size_pct / Decimal("100")))

    # получаем правила тикера
    row = await infra.pg_pool.fetchrow(
        """
        SELECT precision_qty, COALESCE(min_qty, 0) AS min_qty
        FROM tickers_bb
        WHERE symbol = $1
        """,
        symbol,
    )

    precision_qty = int(row["precision_qty"]) if row and row["precision_qty"] is not None else 0
    min_qty = _as_decimal(row["min_qty"]) if row else Decimal("0")

    # шаг количества: 10^-precision_qty
    step_exp = Decimal("1").scaleb(-precision_qty)  # = Decimal('1e-precision_qty')
    # округление вниз к шагу
    try:
        q_plan = (q_raw / step_exp).to_integral_value(rounding=ROUND_DOWN) * step_exp
        # нормализуем хвост нулей
        q_plan = q_plan.normalize()
    except (InvalidOperation, Exception):
        q_plan = None

    # проверка min_qty
    if q_plan is None or q_plan <= 0 or q_plan < (min_qty or Decimal("0")):
        return None, q_raw, size_pct

    return q_plan, q_raw, size_pct


# 🔸 Проверка «занятости» по базе для (sid, symbol)
async def _is_busy_in_db(strategy_id: int, symbol: str) -> bool:
    # условия занятости: ext_status='open' ИЛИ статус в одном из рабочих этапов
    row = await infra.pg_pool.fetchrow(
        """
        SELECT 1
        FROM trader_positions_log
        WHERE strategy_id = $1
          AND symbol = $2
          AND (
                ext_status = 'open'
             OR status IN ('queued','processing','sent')
          )
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


# 🔸 Вставка/апдейт planned в trader_positions_log
async def _upsert_trader_positions_log_planned(
    *,
    source_stream_id: str,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    order_mode: str,
    quantity_virt: Decimal,
    quantity_plan: Decimal,
    margin_used_virt: Decimal,
    margin_plan: Decimal,
    order_link_id: str,
    extras: dict,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_positions_log (
                source_stream_id, position_uid, strategy_id, strategy_type, symbol, event,
                int_status, ext_status, direction, order_mode,
                quantity_virt, quantity_plan,
                margin_used_virt, margin_plan,
                order_link_id, status, note, created_at, updated_at, extras
            )
            VALUES (
                $1, $2, $3, $4, $5, 'opened',
                'open', 'closed', $6, $7,
                $8, $9,
                $10, $11,
                $12, 'planned', NULL, now(), now(), $13
            )
            ON CONFLICT (source_stream_id) DO UPDATE
            SET quantity_plan   = EXCLUDED.quantity_plan,
                margin_plan     = EXCLUDED.margin_plan,
                status          = 'planned',
                order_link_id   = EXCLUDED.order_link_id,
                updated_at      = now(),
                extras          = EXCLUDED.extras
            """,
            source_stream_id,
            position_uid,
            strategy_id,
            strategy_type,
            symbol,
            direction,
            order_mode,
            str(quantity_virt),
            str(quantity_plan),
            str(margin_used_virt),
            str(margin_plan),
            order_link_id,
            json.dumps(extras or {}),
        )
        log.info("📝 trader_positions_log planned: uid=%s sid=%s %s qty=%s",
                 position_uid, strategy_id, symbol, quantity_plan)


# 🔸 Публикация задачи исполнителю в positions_bybit_orders
async def _publish_order_task(
    *,
    order_link_id: str,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    leverage: str,
    qty: str,
    size_mode: str,
    size_pct: str,
    margin_plan: str,
    order_mode: str,
    source_stream_id: str,
    ts: Optional[str],
    ts_ms: Optional[str],
) -> str:
    redis = infra.redis_client

    payload = {
        "order_link_id": order_link_id,
        "position_uid": position_uid,
        "strategy_id": strategy_id,
        "strategy_type": strategy_type,
        "symbol": symbol,
        "direction": direction,                 # long|short
        "side": "Buy" if direction == "long" else "Sell",
        "leverage": leverage,
        "qty": qty,
        "size_mode": size_mode,                 # 'pct_of_virtual'
        "size_pct": size_pct,
        "margin_plan": margin_plan,
        "order_mode": order_mode,               # dry_run | live
        "source_stream_id": source_stream_id,   # корреляция с статусом
        "ts": ts,
        "ts_ms": ts_ms,
    }

    orders_stream_id = await redis.xadd(ORDERS_STREAM, {"data": json.dumps(payload)})
    log.info("📤 Поставлено в %s: %s", ORDERS_STREAM, payload)
    return orders_stream_id


# 🔸 Обновление статуса queued и апдейт trader_signals
async def _mark_queued_and_update_signal(
    *,
    source_stream_id: str,
    orders_stream_id: str,
    note: str,
    processing_status: str,
):
    async with infra.pg_pool.acquire() as conn:
        # апдейт нашей строки
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET status = 'queued',
                orders_stream_id = $2,
                queued_at = now(),
                updated_at = now(),
                note = $3
            WHERE source_stream_id = $1
            """,
            source_stream_id,
            orders_stream_id,
            note,
        )
        # апдейт trader_signals по stream_id
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = $1,
                processed_at = now(),
                processing_note = $3
            WHERE stream_id = $2
            """,
            processing_status,
            source_stream_id,
            note,
        )
        log.info("✅ queued & journal updated: stream_id=%s", source_stream_id)


# 🔸 Завершение бракованного события (нет margin_used или qty<=0) → статус invalid_event + ACK
async def _finalize_invalid_event(entry_id, sid, position_uid, symbol, direction, strategy_type, stream_id, fields, note="invalid_event"):
    try:
        await _upsert_invalid_event(
            source_stream_id=stream_id,
            position_uid=position_uid,
            strategy_id=sid,
            strategy_type=strategy_type,
            symbol=symbol,
            direction=direction,
            note=note,
            extras=fields,
        )
        await _update_signal_status(stream_id, "invalid_event", note)
        await infra.redis_client.xack(POS_STATUS_STREAM, POS_OPEN_CG, entry_id)
        log.info("❎ invalid_event ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
    except Exception:
        log.exception("❌ Ошибка фиксации invalid_event (id=%s)", entry_id)


# 🔸 Вставка/апдейт invalid_event в лог
async def _upsert_invalid_event(
    *,
    source_stream_id: str,
    position_uid: str,
    strategy_id: int,
    strategy_type: str,
    symbol: str,
    direction: str,
    note: str,
    extras: dict,
):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO trader_positions_log (
                source_stream_id, position_uid, strategy_id, strategy_type, symbol, event,
                int_status, ext_status, direction, order_mode,
                quantity_virt, quantity_plan,
                margin_used_virt, margin_plan,
                order_link_id, status, note, created_at, updated_at, extras
            )
            VALUES (
                $1, $2, $3, $4, $5, 'opened',
                'open', 'closed', $6, $7,
                0, 0,
                0, 0,
                'tv4-invalid', 'invalid_event', $8, now(), now(), $9
            )
            ON CONFLICT (source_stream_id) DO UPDATE
            SET status     = 'invalid_event',
                note       = EXCLUDED.note,
                updated_at = now(),
                extras     = EXCLUDED.extras
            """,
            source_stream_id,
            position_uid,
            strategy_id,
            strategy_type,
            symbol,
            direction,
            TRADER_ORDER_MODE,
            note,
            json.dumps(extras or {}),
        )
        log.info("📝 trader_positions_log invalid_event: uid=%s sid=%s %s", position_uid, strategy_id, symbol)


# 🔸 Обработка planned_skip (ниже min_qty) → лог и ACK
async def _finalize_planned_skip(
    entry_id: str,
    sid: int,
    position_uid: str,
    symbol: str,
    direction: str,
    strategy_type: str,
    stream_id: str,
    qty_left: Decimal,
    q_raw: Decimal,
    size_pct: Decimal,
    margin_used_virt: Decimal,
    fields: dict,
):
    note = f"planned_skip: q_raw={q_raw} pct={size_pct}% < min_qty"
    try:
        # фиксируем skip в логе
        async with infra.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trader_positions_log (
                    source_stream_id, position_uid, strategy_id, strategy_type, symbol, event,
                    int_status, ext_status, direction, order_mode,
                    quantity_virt, quantity_plan,
                    margin_used_virt, margin_plan,
                    order_link_id, status, note, created_at, updated_at, extras
                )
                VALUES (
                    $1, $2, $3, $4, $5, 'opened',
                    'open', 'closed', $6, $7,
                    $8, 0,
                    $9, 0,
                    'tv4-skip', 'planned_skip', $10, now(), now(), $11
                )
                ON CONFLICT (source_stream_id) DO UPDATE
                SET status     = 'planned_skip',
                    note       = EXCLUDED.note,
                    updated_at = now(),
                    extras     = EXCLUDED.extras
                """,
                stream_id,
                position_uid,
                sid,
                strategy_type,
                symbol,
                direction,
                TRADER_ORDER_MODE,
                str(qty_left),
                str(margin_used_virt),
                note,
                json.dumps(fields or {}),
            )

        # апдейт trader_signals
        await _update_signal_status(stream_id, "planned_skip", note)

        # ACK
        await infra.redis_client.xack(POS_STATUS_STREAM, POS_OPEN_CG, entry_id)
        log.info("🟡 planned_skip ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)

    except Exception:
        log.exception("❌ Ошибка фиксации planned_skip (id=%s)", entry_id)


# 🔸 Апдейт статуса в trader_signals по stream_id
async def _update_signal_status(stream_id: str, processing_status: str, note: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = $1,
                processed_at = now(),
                processing_note = $3
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


# 🔸 Утилиты
def _as_decimal(v) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None