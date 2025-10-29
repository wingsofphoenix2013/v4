# trader_position_protector.py — воркер SL-protect: читает positions_bybit_status(sl_replaced) → решает protect/close → апдейтит БД → кладёт задание исполнителю

# 🔸 Импорты
import os
import json
import asyncio
import logging
import hashlib
from decimal import Decimal
from typing import Dict, Tuple, Optional

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("TRADER_POS_PROTECTOR")

# 🔸 Константы стримов/CG
POS_STATUS_STREAM = "positions_bybit_status"       # источник событий (sl_replaced)
ORDERS_STREAM = "positions_bybit_orders"           # задания для bybit_processor
POS_PROTECT_CG = "trader_pos_protect_cg"
POS_PROTECT_CONSUMER = os.getenv("POS_PROTECT_CONSUMER", "pos-protect-1")

# 🔸 Параметры воркера
MAX_PARALLEL_TASKS = int(os.getenv("POS_PROTECT_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("POS_PROTECT_LOCK_TTL", "30"))

# 🔸 ENV: режим отправки и REST-эндпоинт Bybit (для last price)
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com")

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной запуск воркера
async def run_trader_position_protector():
    redis = infra.redis_client

    # создание CG для чтения статусов (id="$" — только новые записи)
    try:
        await redis.xgroup_create(POS_STATUS_STREAM, POS_PROTECT_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", POS_PROTECT_CG, POS_STATUS_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", POS_STATUS_STREAM, POS_PROTECT_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", POS_PROTECT_CG, POS_STATUS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", POS_PROTECT_CG, POS_STATUS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    while True:
        try:
            # читаем только новые записи
            batch = await redis.xreadgroup(
                groupname=POS_PROTECT_CG,
                consumername=POS_PROTECT_CONSUMER,
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


# 🔸 Обработка одной записи из positions_bybit_status (только sl_replaced)
async def _handle_status_entry(sem: asyncio.Semaphore, entry_id: str, fields: dict):
    async with sem:
        redis = infra.redis_client

        # фильтруем только sl_replaced; остальное — просто ACK
        event = fields.get("event")
        if event != "sl_replaced":
            try:
                await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                log.info("↷ Пропущено событие %s (id=%s): ACK", event, entry_id)
            except Exception:
                log.exception("❌ Ошибка ACK пропущенного события (id=%s)", entry_id)
            return

        # парсим обязательные поля
        try:
            sid = int(fields["strategy_id"])
            position_uid = fields["position_uid"]
            symbol = fields["symbol"]
            direction = fields.get("direction") or ""  # long|short
            strategy_type = fields.get("strategy_type", "plain")
            stream_id = fields.get("stream_id") or entry_id  # на всякий случай
        except Exception:
            log.exception("❌ Ошибка парсинга обязательных полей sl_replaced (id=%s)", entry_id)
            # безопасно ACK, чтобы не зациклиться, и оставить след в логах
            try:
                await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
            except Exception:
                pass
            return

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок в Redis
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{POS_PROTECT_CONSUMER}-{entry_id}"
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
                tpl = await infra.pg_pool.fetchrow(
                    """
                    SELECT strategy_id, symbol, direction, ext_status, order_mode
                    FROM trader_positions_log
                    WHERE position_uid = $1
                    """,
                    position_uid,
                )

                # если позиции нет — фиксируем и ACK
                if not tpl:
                    await _update_signal_status(stream_id, "position_not_found", "no trader_positions_log row for position_uid; skipped sl_protect")
                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("❎ position_not_found ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                ext_status = (tpl["ext_status"] or "").strip()
                order_mode = (tpl["order_mode"] or TRADER_ORDER_MODE).strip()
                if not direction:
                    direction = (tpl["direction"] or "").strip()

                # если уже закрыта — фиксируем и ACK
                if ext_status == "closed":
                    await _update_signal_status(stream_id, "already_closed", "sl_protect arrived after exchange close")
                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("🟢 already_closed ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                # находим биржевой средний вход avg_price
                avg_price = await _load_entry_avg_price(position_uid)
                if avg_price is None:
                    await _update_signal_status(stream_id, "avg_price_missing", "no avg_price in trader_position_orders for entry/sl_protect_entry")
                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("🟡 avg_price_missing ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                # получаем last price с биржи (Bybit REST v5)
                last_price = await _fetch_last_price_from_bybit(symbol)
                if last_price is None:
                    await _update_signal_status(stream_id, "price_unavailable", "failed to fetch last price from Bybit")
                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("🟡 price_unavailable ACK (sid=%s %s %s id=%s)", sid, symbol, direction, entry_id)
                    return

                # сравнение с учётом направления (равно — трактуем как «лучше»)
                better = _is_better_than_entry(direction, last_price, avg_price)

                # формируем order_link_id (<=36 симв), на основе stream_id
                order_link_id = _make_order_link_id(stream_id)

                if better:
                    # планируем SL-protect (перенос SL на avg_price)
                    await _update_tpl_planned(
                        position_uid=position_uid,
                        order_link_id=order_link_id,
                        note=f"sl_protect planned: avg={avg_price} last={last_price} dir={direction}",
                    )

                    orders_stream_id = await _publish_sl_protect_task(
                        order_link_id=order_link_id,
                        position_uid=position_uid,
                        strategy_id=sid,
                        symbol=symbol,
                        sl_price=str(avg_price),
                        order_mode=order_mode,
                        source_stream_id=stream_id,
                    )

                    await _mark_queued_and_update_signal(
                        position_uid=position_uid,
                        orders_stream_id=orders_stream_id,
                        note=f"sl_protect planned & queued: avg={avg_price} last={last_price}",
                        processing_status="planned",
                        source_stream_id=stream_id,
                    )

                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("✅ PROTECT planned & queued (sid=%s %s %s avg=%s last=%s) [id=%s]",
                             sid, symbol, direction, avg_price, last_price, entry_id)
                else:
                    # планируем CLOSE (роль closer'а для SL-protect хуже входа)
                    await _update_tpl_planned(
                        position_uid=position_uid,
                        order_link_id=order_link_id,
                        note=f"close planned (sl_protect worse): avg={avg_price} last={last_price} dir={direction}",
                    )

                    orders_stream_id = await _publish_close_task(
                        order_link_id=order_link_id,
                        position_uid=position_uid,
                        strategy_id=sid,
                        symbol=symbol,
                        order_mode=order_mode,
                        source_stream_id=stream_id,
                        close_reason="sl_protect_worse_than_entry",
                    )

                    await _mark_queued_and_update_signal(
                        position_uid=position_uid,
                        orders_stream_id=orders_stream_id,
                        note=f"close planned & queued (sl_protect): avg={avg_price} last={last_price}",
                        processing_status="planned",
                        source_stream_id=stream_id,
                    )

                    await redis.xack(POS_STATUS_STREAM, POS_PROTECT_CG, entry_id)
                    log.info("✅ CLOSE planned & queued (sl_protect) (sid=%s %s %s avg=%s last=%s) [id=%s]",
                             sid, symbol, direction, avg_price, last_price, entry_id)

            except Exception:
                log.exception("❌ Ошибка обработки sl_protect для sid=%s symbol=%s (id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                # освобождение распределённого замка
                await _release_dist_lock(gate_key, owner)

# 🔸 Загрузка биржевого среднего входа avg_price по position_uid
async def _load_entry_avg_price(position_uid: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT avg_price
        FROM trader_position_orders
        WHERE position_uid = $1
          AND kind IN ('entry','sl_protect_entry')
          AND avg_price IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        position_uid,
    )
    if not row:
        return None
    try:
        val = row["avg_price"]
        return Decimal(str(val)) if val is not None else None
    except Exception:
        return None


# 🔸 Получение last price с Bybit REST (public v5 /market/tickers)
async def _fetch_last_price_from_bybit(symbol: str) -> Optional[Decimal]:
    # используем стандартную библиотеку (urllib) через executor, чтобы не блокировать event loop
    import urllib.request
    import urllib.parse

    def _blocking_fetch() -> Optional[str]:
        try:
            qs = urllib.parse.urlencode({"category": "linear", "symbol": symbol})
            url = f"{BYBIT_REST_BASE}/v5/market/tickers?{qs}"
            req = urllib.request.Request(url, headers={"User-Agent": "tv4-protector/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = resp.read().decode("utf-8", "ignore")
            j = json.loads(data)
            if (j or {}).get("retCode") == 0:
                lst = (((j.get("result") or {}).get("list")) or [])
                if lst:
                    return lst[0].get("lastPrice")
            return None
        except Exception:
            return None

    loop = asyncio.get_running_loop()
    price_str = await loop.run_in_executor(None, _blocking_fetch)
    if not price_str:
        return None
    try:
        return Decimal(str(price_str))
    except Exception:
        return None


# 🔸 Сравнение «лучше/хуже» входа с учётом направления (равно — «лучше»)
def _is_better_than_entry(direction: str, last_price: Decimal, avg_price: Decimal) -> bool:
    d = (direction or "").lower()
    if d == "long":
        return last_price >= avg_price
    if d == "short":
        return last_price <= avg_price
    # неизвестное направление — трактуем как «не лучше», чтобы не делать sl_protect по ошибке
    return False


# 🔸 Формирование короткого order_link_id (<=36) на основе stream_id
def _make_order_link_id(stream_id: str) -> str:
    base = f"tv4-{stream_id}"
    if len(base) <= 36:
        return base
    # fallback: детерминированный хэш (sha1) до 32 символов + "tv4-"
    short = hashlib.sha1(stream_id.encode("utf-8")).hexdigest()[:32]
    return f"tv4-{short}"  # длина 36


# 🔸 Апдейт planned в trader_positions_log по position_uid
async def _update_tpl_planned(
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
        log.info("📝 trader_positions_log planned: uid=%s, order_link_id=%s", position_uid, order_link_id)


# 🔸 Публикация sl_protect-задачи исполнителю (минимальный payload)
async def _publish_sl_protect_task(
    *,
    order_link_id: str,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    sl_price: str,
    order_mode: str,
    source_stream_id: str,
) -> str:
    redis = infra.redis_client

    payload = {
        "op": "sl_protect",
        "order_link_id": order_link_id,
        "position_uid": position_uid,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "sl_price": sl_price,
        "order_mode": order_mode,             # dry_run | live
        "source_stream_id": source_stream_id, # корреляция
    }

    orders_stream_id = await redis.xadd(ORDERS_STREAM, {"data": json.dumps(payload)})
    log.info("📤 Поставлено в %s (sl_protect): %s", ORDERS_STREAM, payload)
    return orders_stream_id


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


# 🔸 Обновление статуса queued и апдейт trader_signals (по stream_id события)
async def _mark_queued_and_update_signal(
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
        # апдейт trader_signals по stream_id
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
        log.info("✅ queued & journal updated (protect/close): position_uid=%s, stream_id=%s", position_uid, source_stream_id)


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