# bybit_closer.py — исполнитель закрытия: читает positions_bybit_orders(op="close") → market RO close → cancel-all → clear SL → синхронизирует БД

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

import httpx

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("BYBIT_CLOSER")

# 🔸 Константы стримов/CG
ORDERS_STREAM = "positions_bybit_orders"
BYBIT_CLOSER_CG = "bybit_closer_cg"
BYBIT_CLOSER_CONSUMER = os.getenv("BYBIT_CLOSER_CONSUMER", "bybit-closer-1")
AUDIT_STREAM = "positions_bybit_audit"

# 🔸 Параллелизм и замки
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_CLOSER_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_CLOSER_LOCK_TTL", "90"))

# 🔸 Параметры закрытия/ретраев
CLOSE_MAX_ATTEMPTS = 3
CLOSE_BACKOFF_SEQ = (0.5, 1.0, 1.5)  # секунды

# 🔸 BYBIT ENV
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"

# 🔸 Режим исполнения
TRADER_ORDER_MODE = os.getenv("TRADER_ORDER_MODE", "dry_run")  # dry_run | live

# 🔸 Локальные мьютексы по ключу (strategy_id, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}

# 🔸 Трейлинг: ключи состояния (для разоружения при закрытии)
TRAIL_ACTIVE_SET = "tv4:trail:active"
TRAIL_KEY_FMT = "tv4:trail:{uid}"


# 🔸 Основной запуск воркера
async def run_bybit_closer():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(ORDERS_STREAM, BYBIT_CLOSER_CG, id="$", mkstream=True)
        log.info("📡 Создана CG %s для стрима %s", BYBIT_CLOSER_CG, ORDERS_STREAM)
    except Exception:
        pass

    # сброс offset CG на '$' — читаем строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", ORDERS_STREAM, BYBIT_CLOSER_CG, "$")
        log.info("⏩ CG %s для %s сброшена на $ (только новые)", BYBIT_CLOSER_CG, ORDERS_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", BYBIT_CLOSER_CG, ORDERS_STREAM)

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    # бесконечный цикл чтения
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=BYBIT_CLOSER_CG,
                consumername=BYBIT_CLOSER_CONSUMER,
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


# 🔸 Обработка одной записи из positions_bybit_orders (op="close")
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

        # интересуют только op="close"
        if (payload.get("op") or "").lower() != "close":
            await _ack_ok(entry_id)
            return

        # ключевые поля
        position_uid     = payload.get("position_uid")
        sid              = int(payload.get("strategy_id"))
        symbol           = payload.get("symbol")
        order_mode       = payload.get("order_mode", TRADER_ORDER_MODE)
        source_stream_id = payload.get("source_stream_id")
        close_reason     = payload.get("close_reason") or "close_signal"

        # условия достаточности
        if not (position_uid and symbol and sid and source_stream_id):
            log.info("❎ close payload incomplete — ACK (id=%s)", entry_id)
            await _ack_ok(entry_id)
            return

        # сериализация по ключу (strategy_id, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок в Redis
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"{BYBIT_CLOSER_CONSUMER}-{entry_id}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                for _ in range(10):
                    await asyncio.sleep(0.2)
                    if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                        break
                else:
                    # requeue вместо возврата без ACK, чтобы не оставлять запись в PEL
                    try:
                        new_id = await redis.xadd(ORDERS_STREAM, {"data": data_raw})
                        await redis.xack(ORDERS_STREAM, BYBIT_CLOSER_CG, entry_id)
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

                # если позиции нет — идемпотентный ACK
                if not pos_row:
                    await _publish_audit("close_position_not_found", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "sid": sid,
                    })
                    await _ack_ok(entry_id)
                    return

                ext_status_db = (pos_row["ext_status"] or "").strip()
                direction = (pos_row["direction"] or "").strip().lower()

                # если уже закрыто → idempotent no-op
                if ext_status_db == "closed":
                    await _publish_audit("close_already_closed", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "sid": sid,
                    })
                    await _ack_ok(entry_id)
                    return

                await _publish_audit("close_signal_received", {
                    "position_uid": position_uid,
                    "symbol": symbol,
                    "sid": sid,
                    "reason": close_reason,
                })

                # dry-run — без реальных вызовов
                if order_mode == "dry_run":
                    await _reconcile_db_after_close(position_uid=position_uid, symbol=symbol, source_stream_id=source_stream_id)
                    # разоружить трейл (если был)
                    await _disarm_trailing(position_uid)
                    await _publish_audit("position_closed_by_closer", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "sid": sid,
                        "mode": "dry_run",
                    })
                    await _ack_ok(entry_id)
                    log.info("✅ DRY-RUN closed (reconciled): sid=%s %s", sid, symbol)
                    return

                # фактическое закрытие на бирже
                # шаг 1 — закрыть весь остаток reduce-only Market (до 3 попыток)
                close_side = "sell" if direction == "long" else "buy"
                for attempt in range(CLOSE_MAX_ATTEMPTS):
                    size = await _get_position_size_linear(symbol)
                    if not size or size <= 0:
                        break
                    tail_link = _suffix_link(f"tv4-{source_stream_id}", f"close{attempt+1}")
                    try:
                        resp = await _close_reduce_only_market(symbol, close_side.title(), size, tail_link)
                        await _publish_audit("close_market_sent", {
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "qty": _to_fixed_str(size),
                            "order_link_id": tail_link,
                            "attempt": attempt + 1,
                        })
                        log.info("🛑 close sent (%s): %s qty=%s", close_side, tail_link, size)
                    except Exception as e:
                        await _publish_audit("close_market_failed", {
                            "position_uid": position_uid,
                            "symbol": symbol,
                            "qty": _to_fixed_str(size),
                            "attempt": attempt + 1,
                            "reason": str(e),
                        })
                        log.exception("❌ close market failed attempt=%s", attempt + 1)

                    # подождать чуть и проверить
                    await asyncio.sleep(CLOSE_BACKOFF_SEQ[min(attempt, len(CLOSE_BACKOFF_SEQ) - 1)])

                # шаг 2 — отменить все висящие ордера по символу
                try:
                    await _cancel_all_orders_for_symbol(symbol)
                    await _publish_audit("cancel_all_orders_sent", {"symbol": symbol})
                    log.info("🧹 cancel-all sent: %s", symbol)
                except Exception as e:
                    await _publish_audit("cancel_all_orders_failed", {"symbol": symbol, "reason": str(e)})
                    log.exception("⚠️ cancel-all failed: %s", symbol)

                # шаг 3 — снять позиционный stopLoss (если был)
                try:
                    await _clear_position_stop_loss(symbol)
                    await _publish_audit("position_stop_cleared", {"symbol": symbol})
                    log.info("🧽 position stop cleared: %s", symbol)
                except Exception as e:
                    await _publish_audit("position_stop_clear_failed", {"symbol": symbol, "reason": str(e)})
                    log.exception("⚠️ clear stop failed: %s", symbol)

                # итоговая проверка → и синхронизация БД
                size_final = await _get_position_size_linear(symbol)
                if size_final and size_final > 0:
                    log.info("⚠️ position not zero after close attempts: %s size=%s", symbol, size_final)

                await _reconcile_db_after_close(position_uid=position_uid, symbol=symbol, source_stream_id=source_stream_id)

                # разоружить трейл (если был)
                await _disarm_trailing(position_uid)

                await _publish_audit("position_closed_by_closer", {
                    "position_uid": position_uid,
                    "symbol": symbol,
                    "sid": sid,
                    "mode": "live",
                })
                await _ack_ok(entry_id)
                log.info("✅ LIVE closed (reconciled): sid=%s %s", sid, symbol)

            except Exception:
                log.exception("❌ Ошибка обработки close для sid=%s symbol=%s (id=%s)", sid, symbol, entry_id)
                # не ACK — вернёмся ретраем
            finally:
                # освобождение распределённого замка
                await _release_dist_lock(gate_key, owner)


# 🔸 ACK helper (для ORDERS_STREAM)
async def _ack_ok(entry_id: str):
    try:
        await infra.redis_client.xack(ORDERS_STREAM, BYBIT_CLOSER_CG, entry_id)
    except Exception:
        pass


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


# 🔸 Получить текущий размер позиции (REST /v5/position/list?category=linear&symbol=..)
async def _get_position_size_linear(symbol: str) -> Optional[Decimal]:
    if not API_KEY or not API_SECRET:
        return None
    query = f"category={CATEGORY}&symbol={symbol}"
    url = f"{BASE_URL}/v5/position/list?{query}"
    ts = int(time.time() * 1000)
    signed = _rest_sign(ts, query)
    headers = _private_headers(ts, signed)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            lst = (data.get("result") or {}).get("list") or []
            head = lst[0] if lst else {}
            sz = head.get("size")
            return _as_decimal(sz) or Decimal("0")
    except Exception:
        log.exception("❌ get position size failed for %s", symbol)
        return None


# 🔸 Reduce-only Market для закрытия остатка — /v5/order/create
async def _close_reduce_only_market(symbol: str, side: str, qty: Decimal, order_link_id: str) -> dict:
    rules = await _fetch_ticker_rules(symbol)
    q = _quant_down(qty, rules["step_qty"]) or Decimal("0")
    if q <= 0 or q < (rules["min_qty"] or Decimal("0")):
        raise ValueError(f"qty below min_qty after quantization: q={q}, min={rules['min_qty']}")

    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,                 # 'Buy' | 'Sell'
        "orderType": "Market",
        "qty": _to_fixed_str(q),
        "timeInForce": "IOC",
        "reduceOnly": True,
        "orderLinkId": order_link_id,
    }
    url = f"{BASE_URL}/v5/order/create"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    signed = _rest_sign(ts, body_json)
    headers = _private_headers(ts, signed)

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()


# 🔸 Cancel-all всех ордеров по символу — /v5/order/cancel-all
async def _cancel_all_orders_for_symbol(symbol: str) -> dict:
    if not API_KEY or not API_SECRET:
        return {}
    body = {
        "category": CATEGORY,
        "symbol": symbol,
    }
    url = f"{BASE_URL}/v5/order/cancel-all"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    signed = _rest_sign(ts, body_json)
    headers = _private_headers(ts, signed)

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()


# 🔸 Очистить позиционный stopLoss (установить 0) — /v5/position/trading-stop
async def _clear_position_stop_loss(symbol: str) -> dict:
    if not API_KEY or not API_SECRET:
        return {}
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "positionIdx": 0,
        "stopLoss": "0",
    }
    url = f"{BASE_URL}/v5/position/trading-stop"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    signed = _rest_sign(ts, body_json)
    headers = _private_headers(ts, signed)

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()


# 🔸 Конвергенция БД после ручного close (market RO)
async def _reconcile_db_after_close(*, position_uid: str, symbol: str, source_stream_id: str):
    async with infra.pg_pool.acquire() as conn:
        # всем карточкам is_active=false; TP/SL(не entry) → canceled; entry оставляем как есть
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET
                is_active = false,
                status = CASE
                           WHEN kind = 'entry' THEN status
                           WHEN status IN ('planned','sent','planned_offchain','virtual') THEN 'canceled'
                           ELSE status
                         END,
                updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'closer reconcile: position closed by market RO'
            WHERE position_uid = $1
            """,
            position_uid,
        )
        # журналы → закрыто
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET ext_status = 'closed',
                status = CASE WHEN status IN ('processing','sent','planned') THEN 'filled' ELSE status END,
                updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'closed by closer (market RO)'
            WHERE source_stream_id = $1
            """,
            source_stream_id,
        )
        # сигналы → filled
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = 'filled',
                processed_at = now(),
                processing_note = COALESCE(processing_note,'') ||
                                  CASE WHEN COALESCE(processing_note,'')='' THEN '' ELSE '; ' END ||
                                  'closed by closer (market RO)'
            WHERE stream_id = $1
            """,
            source_stream_id,
        )


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


# 🔸 Разоружение трейлинга для позиции
async def _disarm_trailing(position_uid: str):
    try:
        await infra.redis_client.srem(TRAIL_ACTIVE_SET, position_uid)
        await infra.redis_client.delete(TRAIL_KEY_FMT.format(uid=position_uid))
        log.info("🧹 trailing disarmed: uid=%s", position_uid)
    except Exception:
        # мягкий фолбэк — не мешаем основному потоку
        log.debug("trailing disarm failed silently uid=%s", position_uid)


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
def _suffix_link(base: str, suffix: str) -> str:
    core = f"{base}-{suffix}"
    if len(core) <= 36:
        return core
    h = hashlib.sha1(core.encode("utf-8")).hexdigest()[:36]
    return h

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