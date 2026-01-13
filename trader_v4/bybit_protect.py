# bybit_protect.py — исполнитель SL-protect: читает positions_bybit_orders(op="sl_protect") → переносит позиционный SL на entry → синхронизирует БД

# 🔸 Импорты
import os
import json
import time
import hmac
import hashlib
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, Optional, Any

from trader_infra import infra
from bybit_proxy import httpx_async_client

# 🔸 Логгер
log = logging.getLogger("BYBIT_PROTECT")

# 🔸 Константы стримов/CG
ORDERS_STREAM = "positions_bybit_orders"
BYBIT_PROTECT_CG = "bybit_protect_cg"
BYBIT_PROTECT_CONSUMER = os.getenv("BYBIT_PROTECT_CONSUMER", "bybit-protect-1")
AUDIT_STREAM = "positions_bybit_audit"

# 🔸 Параллелизм и замки
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_PROTECT_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_PROTECT_LOCK_TTL", "90"))

# 🔸 BYBIT ENV
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # USDT-perp

# 🔸 Режим исполнения
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной запуск воркера
async def run_bybit_protect():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(ORDERS_STREAM, BYBIT_PROTECT_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", BYBIT_PROTECT_CG, ORDERS_STREAM)
    except Exception:
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", ORDERS_STREAM, BYBIT_PROTECT_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", BYBIT_PROTECT_CG, ORDERS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", BYBIT_PROTECT_CG, ORDERS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    # чтение в вечном цикле
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=BYBIT_PROTECT_CG,
                consumername=BYBIT_PROTECT_CONSUMER,
                streams={ORDERS_STREAM: ">"},
                count=100,
                block=1000,  # мс
            )
            if not entries:
                continue

            tasks = []
            for _, records in entries:
                for entry_id, fields in records:
                    tasks.append(asyncio.create_task(_handle_order_entry(sem, entry_id, fields)))

            await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка чтения/обработки из стрима %s", ORDERS_STREAM)
            await asyncio.sleep(1)


# 🔸 Обработка одной записи (op="sl_protect")
async def _handle_order_entry(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client

        # парсим payload
        try:
            data_raw = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")
            payload = json.loads(data_raw or "{}")
        except Exception:
            log.exception("❌ Некорректный payload (id=%s) — ACK", entry_id)
            await _ack_ok(entry_id)
            return

        # интересуют только op="sl_protect"
        if (payload.get("op") or "").lower() != "sl_protect":
            await _ack_ok(entry_id)
            return

        # ключевые поля
        position_uid     = payload.get("position_uid")
        sid              = int(payload.get("strategy_id"))
        symbol           = payload.get("symbol")
        sl_price_raw     = payload.get("sl_price")
        order_mode       = payload.get("order_mode", TRADER_ORDER_MODE)
        source_stream_id = payload.get("source_stream_id")

        # валидация
        if not (position_uid and symbol and sid and source_stream_id and sl_price_raw is not None):
            await _publish_audit("sl_protect_payload_incomplete", {"payload": payload})
            await _ack_ok(entry_id)
            return

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{BYBIT_PROTECT_CONSUMER}-{entry_id}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                for _ in range(10):
                    await asyncio.sleep(0.2)
                    if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                        break
                else:
                    # requeue вместо возврата без ACK, чтобы не оставлять запись в PEL
                    try:
                        new_id = await redis.xadd(ORDERS_STREAM, {"data": data_raw})
                        await redis.xack(ORDERS_STREAM, BYBIT_PROTECT_CG, entry_id)
                        log.info("🔁 requeue due to busy gate %s (old_id=%s new_id=%s)", gate_key, entry_id, new_id)
                    except Exception:
                        log.exception("❌ requeue failed (id=%s)", entry_id)
                    return

            try:
                # загрузим карточку позиции (наша истина по uid)
                pos_row = await infra.pg_pool.fetchrow(
                    """
                    SELECT position_uid, strategy_id, symbol, direction, ext_status, order_mode
                    FROM trader_positions_log
                    WHERE position_uid = $1
                    """,
                    position_uid,
                )
                if not pos_row:
                    await _publish_audit("sl_protect_position_not_found", {"position_uid": position_uid, "symbol": symbol, "sid": sid})
                    await _ack_ok(entry_id)
                    return

                ext_status_db = (pos_row["ext_status"] or "").strip()
                direction = (pos_row["direction"] or "").strip().lower()
                sl_price = _as_decimal(sl_price_raw)

                # если уже закрыто → idempotent no-op
                if ext_status_db == "closed":
                    await _publish_audit("sl_protect_already_closed", {"position_uid": position_uid, "symbol": symbol, "sid": sid})
                    await _ack_ok(entry_id)
                    return

                # dry-run — только БД
                if order_mode == "dry_run":
                    await _apply_protect_db_only(position_uid=position_uid, source_stream_id=source_stream_id)
                    await _publish_audit("sl_protect_set", {"position_uid": position_uid, "symbol": symbol, "mode": "dry_run"})
                    await _ack_ok(entry_id)
                    log.info("✅ DRY-RUN sl_protect applied (DB only): sid=%s %s", sid, symbol)
                    return

                # live — перенос стоп-лосса через /v5/position/trading-stop
                # квантуем цену к шагу
                rules = await _fetch_ticker_rules(symbol)
                step_price = rules["step_price"]
                p = _quant_down(sl_price, step_price) if sl_price is not None else None
                if p is None or p <= 0:
                    await _publish_audit("sl_protect_invalid_price", {"position_uid": position_uid, "symbol": symbol, "sl_price": str(sl_price_raw)})
                    await _ack_ok(entry_id)
                    return

                # отправка запроса
                resp = await _set_position_stop_loss(symbol, p)
                ret_code = (resp or {}).get("retCode", 0)
                ret_msg  = (resp or {}).get("retMsg")

                if ret_code == 0:
                    await _apply_protect_db_only(position_uid=position_uid, source_stream_id=source_stream_id)
                    await _publish_audit("sl_protect_set", {"position_uid": position_uid, "symbol": symbol, "price": _to_fixed_str(p), "retCode": ret_code})
                    log.info("📤 sl_protect set: %s @ %s", symbol, _to_fixed_str(p))
                else:
                    await _publish_audit("sl_protect_failed", {"position_uid": position_uid, "symbol": symbol, "price": _to_fixed_str(p), "retCode": ret_code, "retMsg": ret_msg})
                    log.info("⚠️ sl_protect failed: %s ret=%s %s", symbol, ret_code, ret_msg)

                await _ack_ok(entry_id)

            except Exception:
                log.exception("❌ Ошибка обработки sl_protect для sid=%s symbol=%s (id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                # освобождение распределённого замка
                await _release_dist_lock(gate_key, owner)


# 🔸 Применить изменения в БД для sl_protect (без биржи)
async def _apply_protect_db_only(*, position_uid: str, source_stream_id: str):
    async with infra.pg_pool.acquire() as conn:
        # деактивировать стартовый SL (level=0)
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET is_active = false,
                status = CASE WHEN status IN ('planned','sent') THEN 'canceled' ELSE status END,
                updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'sl_protect: replaced by entry-stop'
            WHERE position_uid = $1
              AND kind = 'sl'
              AND level = 0
              AND is_active = true
            """,
            position_uid,
        )
        # отметить sl_protect_entry как отправленный (если есть)
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET status = 'sent',
                updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'sl_protect applied'
            WHERE position_uid = $1
              AND kind = 'sl_protect_entry'
              AND status IN ('planned_offchain','planned')
            """,
            position_uid,
        )
        # пометка в журналах
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'sl_protect applied (position stop @ entry)'
            WHERE source_stream_id = $1
            """,
            source_stream_id,
        )


# 🔸 Установка позиционного стоп-лосса — /v5/position/trading-stop
async def _set_position_stop_loss(symbol: str, trigger_price: Decimal) -> dict:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("missing Bybit API credentials")

    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "positionIdx": 0,
        "stopLoss": _to_fixed_str(trigger_price),
        "slTriggerBy": "LastPrice",
    }
    url = f"{BASE_URL}/v5/position/trading-stop"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    signed = _rest_sign(ts, body_json)
    headers = _private_headers(ts, signed)

    async with httpx_async_client(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()


# 🔸 Параметры тикера из БД
async def _fetch_ticker_rules(symbol: str) -> dict:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT COALESCE(precision_price,0) AS pprice,
               COALESCE(precision_qty,0)   AS pqty,
               COALESCE(min_qty,0)         AS min_qty,
               COALESCE(ticksize,0)        AS ticksize
        FROM tickers_bb
        WHERE symbol = $1
        """,
        symbol,
    )
    pprice = int(row["pprice"]) if row else 0
    pqty = int(row["pqty"]) if row else 0
    min_qty = _as_decimal(row["min_qty"]) if row else Decimal("0")
    ticksize = _as_decimal(row["ticksize"]) if row else Decimal("0")
    step_qty = Decimal("1").scaleb(-pqty) if pqty > 0 else Decimal("1")
    step_price = ticksize if (ticksize and ticksize > 0) else (Decimal("1").scaleb(-pprice) if pprice > 0 else Decimal("0.00000001"))
    return {"step_qty": step_qty, "min_qty": min_qty, "step_price": step_price}


# 🔸 Подпись приватных запросов Bybit v5
def _rest_sign(timestamp_ms: int, query_or_body: str) -> str:
    payload = f"{timestamp_ms}{API_KEY}{RECV_WINDOW}{query_or_body}"
    return hmac.new(API_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

def _private_headers(ts_ms: int, signed: str) -> dict:
    return {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts_ms),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": signed,
        "Content-Type": "application/json",
    }


# 🔸 ACK helper (для ORDERS_STREAM)
async def _ack_ok(entry_id: str):
    try:
        await infra.redis_client.xack(ORDERS_STREAM, BYBIT_PROTECT_CG, entry_id)
    except Exception:
        pass


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
        log.debug("lock release fallback (key=%s)", key)

# 🔸 Аудит-событие
async def _publish_audit(event: str, data: dict):
    payload = {"event": event, **(data or {})}
    sid = await infra.redis_client.xadd(AUDIT_STREAM, {"data": json.dumps(payload)})
    log.info("📜 audit %s → %s: %s", event, AUDIT_STREAM, payload)
    return sid

# 🔸 Утилиты
def _to_fixed_str(d: Decimal) -> str:
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"

def _quant_down(value: Decimal, step: Decimal) -> Optional[Decimal]:
    try:
        if value is None or step is None or step <= 0:
            return None
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    except Exception:
        return None

def _as_decimal(v) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None