# bybit_activator.py — активатор офчейн-уровней: слушает bybit_order_stream и включает SL-на-entry/SL-переносы после TP

# 🔸 Импорты
import os
import json
import asyncio
import logging
from decimal import Decimal
from typing import Dict, Tuple, Optional, Any

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("BYBIT_ACTIVATOR")

# 🔸 Стримы/CG
ORDER_STREAM = "bybit_order_stream"        # события из bybit_sync (topic=order)
ACTIVATOR_CG = "bybit_activator_cg"
ACTIVATOR_CONSUMER = os.getenv("BYBIT_ACTIVATOR_CONSUMER", "bybit-activator-1")

AUDIT_STREAM = "positions_bybit_audit"     # сюда пишем аудит включения уровней

# 🔸 Параллелизм и замки
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_ACTIVATOR_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_ACTIVATOR_LOCK_TTL", "30"))

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной запуск активатора
async def run_bybit_activator():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(ORDER_STREAM, ACTIVATOR_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", ACTIVATOR_CG, ORDER_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", ORDER_STREAM, ACTIVATOR_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", ACTIVATOR_CG, ORDER_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", ACTIVATOR_CG, ORDER_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    # чтение из стрима
    while True:
        try:
            batch = await redis.xreadgroup(
                groupname=ACTIVATOR_CG,
                consumername=ACTIVATOR_CONSUMER,
                streams={ORDER_STREAM: ">"},
                count=200,
                block=1000,  # мс
            )
            if not batch:
                continue

            tasks = []
            for _, records in batch:
                for entry_id, fields in records:
                    tasks.append(asyncio.create_task(_handle_order_event(sem, entry_id, fields)))

            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка чтения/обработки из стрима %s", ORDER_STREAM)
            await asyncio.sleep(1)


# 🔸 Обработка одной записи bybit_order_stream
async def _handle_order_event(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client

        try:
            data_raw = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")
            payload = json.loads(data_raw or "{}")
        except Exception:
            log.exception("❌ Некорректный payload order (id=%s) — ACK", entry_id)
            try:
                await redis.xack(ORDER_STREAM, ACTIVATOR_CG, entry_id)
            except Exception:
                pass
            return

        # интересуют только полностью исполненные лимитные TP (orderStatus='Filled') с orderLinkId вида ...-tN
        order_status = (payload.get("orderStatus") or "").lower()
        order_link_id = payload.get("orderLinkId")
        if not order_link_id:
            # без ссылки — не сможем коррелировать
            await _ack_ok(entry_id)
            return

        # пропускаем всё, кроме 'filled'
        if order_status != "filled":
            await _ack_ok(entry_id)
            return

        # получаем карточку TP по order_link_id
        tp_row = await _fetch_tpo_by_link(order_link_id, kind="tp")
        if not tp_row:
            # возможно, это не наш TP (или ещё не успели записать) — ACK и лог
            log.info("ℹ️ ORDER filled без известной TP-карточки: %s", order_link_id)
            await _ack_ok(entry_id)
            return

        position_uid = tp_row["position_uid"]
        strategy_id = int(tp_row["strategy_id"])
        symbol = tp_row["symbol"]
        direction = tp_row["direction"]
        level = int(tp_row["level"])
        order_mode = tp_row["order_mode"]

        # сериализация по ключу (strategy_id, symbol)
        key = (strategy_id, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок
            gate_key = f"tv4:gate:{strategy_id}:{symbol}"
            owner = f"{ACTIVATOR_CONSUMER}-{entry_id}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                # короткий локальный ретрай без ACK
                for _ in range(10):
                    await asyncio.sleep(0.2)
                    if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                        break
                else:
                    log.info("⏳ Не взят замок %s — отложено (id=%s)", gate_key, entry_id)
                    return

            try:
                # найдём карточку SL (on_tp, для этого уровня)
                sl_row = await _fetch_sl_on_tp(position_uid, level)
                if not sl_row:
                    # ничего активировать — ACK
                    log.info("ℹ️ Нет SL on_tp для уровня L#%s (uid=%s)", level, position_uid)
                    await _ack_ok(entry_id)
                    return

                # если qty == 0 — бессмысленно активировать
                qty = _as_decimal(sl_row["qty"]) or Decimal("0")
                price = _as_decimal(sl_row["price"]) if sl_row["price"] is not None else None
                if qty <= 0:
                    log.info("ℹ️ SL on_tp qty=0 → skip (uid=%s L#%s)", position_uid, level)
                    await _ack_ok(entry_id)
                    return

                # активируем SL on_tp
                await _activate_sl_on_tp(sl_row_id=sl_row["id"], order_mode=order_mode)

                # «заменяем» стартовый SL (level=0): деактивируем его
                await _deactivate_initial_sl(position_uid, level, reason=f"replaced by SL on TP L#{level}")

                # аудит
                await _publish_audit(
                    event="sl_on_tp_activated",
                    data={
                        "position_uid": position_uid,
                        "strategy_id": strategy_id,
                        "symbol": symbol,
                        "direction": direction,
                        "level": level,
                        "qty": str(qty),
                        "price": str(price) if price is not None else None,
                        "order_mode": order_mode,
                        "tp_order_link_id": order_link_id,
                    },
                )

                # ACK
                await _ack_ok(entry_id)
                log.info("✅ SL on_tp активирован: uid=%s %s L#%s qty=%s", position_uid, symbol, level, qty)

            except Exception:
                log.exception("❌ Ошибка активации SL on_tp (uid=%s L#%s)", position_uid, level)
            finally:
                await _release_dist_lock(gate_key, owner)


# условия достаточности: ACK helper
async def _ack_ok(entry_id: str):
    try:
        await infra.redis_client.xack(ORDER_STREAM, ACTIVATOR_CG, entry_id)
    except Exception:
        pass


# 🔸 Доставание карточки TPO по order_link_id
async def _fetch_tpo_by_link(order_link_id: str, kind: Optional[str] = None) -> Optional[dict]:
    async with infra.pg_pool.acquire() as conn:
        if kind:
            row = await conn.fetchrow(
                """
                SELECT id, position_uid, strategy_id, symbol, direction, kind, level, order_mode
                FROM trader_position_orders
                WHERE order_link_id = $1 AND kind = $2
                """,
                order_link_id, kind,
            )
        else:
            row = await conn.fetchrow(
                """
                SELECT id, position_uid, strategy_id, symbol, direction, kind, level, order_mode
                FROM trader_position_orders
                WHERE order_link_id = $1
                """,
                order_link_id,
            )
        return dict(row) if row else None


# 🔸 Найти SL on_tp для позиции/уровня
async def _fetch_sl_on_tp(position_uid: str, level: int) -> Optional[dict]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, qty, price, status
            FROM trader_position_orders
            WHERE position_uid = $1
              AND kind = 'sl'
              AND activation = 'on_tp'
              AND activation_tp_level = $2
              AND status = 'planned_offchain'
              AND is_active = true
            """,
            position_uid, level,
        )
        return dict(row) if row else None


# 🔸 Активация SL on_tp: в dry_run — статус 'sent'; в live — (будет) фактическая отправка
async def _activate_sl_on_tp(sl_row_id: int, order_mode: str):
    async with infra.pg_pool.acquire() as conn:
        if order_mode == "dry_run":
            await conn.execute(
                """
                UPDATE trader_position_orders
                SET status = 'sent',
                    updated_at = now(),
                    note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'activated by TP'
                WHERE id = $1
                """,
                sl_row_id,
            )
        else:
            # TODO: live — создать ордер на бирже (reduceOnly GTC), сохранить exchange_order_id и статус 'sent'
            await conn.execute(
                """
                UPDATE trader_position_orders
                SET status = 'planned',
                    updated_at = now(),
                    note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || 'activation planned (live)'
                WHERE id = $1
                """,
                sl_row_id,
            )


# 🔸 Деактивация стартового SL (level=0) при замене на SL on_tp
async def _deactivate_initial_sl(position_uid: str, level_triggered: int, reason: str):
    async with infra.pg_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET is_active = false,
                status = CASE WHEN status IN ('planned','sent') THEN 'canceled' ELSE status END,
                updated_at = now(),
                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END || $2
            WHERE position_uid = $1
              AND kind = 'sl'
              AND level = 0
              AND is_active = true
            """,
            position_uid,
            f"replaced by SL on TP L#{level_triggered}: {reason}",
        )


# 🔸 Аудит-событие
async def _publish_audit(event: str, data: dict):
    payload = {"event": event, **(data or {})}
    sid = await infra.redis_client.xadd(AUDIT_STREAM, {"data": json.dumps(payload)})
    log.info("📜 audit %s → %s: %s", event, AUDIT_STREAM, payload)
    return sid


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