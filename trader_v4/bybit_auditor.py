# bybit_auditor.py — аудитор конвергенции: ловит срабатывание SL, добивает хвост и приводит БД к закрытому виду

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
log = logging.getLogger("BYBIT_AUDITOR")

# 🔸 Стримы/CG (читаем факты из приватного канала)
ORDER_STREAM = "bybit_order_stream"        # topic=order (есть stopOrderType/OrderStatus)
POSITION_STREAM = "bybit_position_stream"  # topic=position (есть size)
AUDITOR_CG_ORDER = "bybit_auditor_order_cg"
AUDITOR_CG_POS = "bybit_auditor_pos_cg"
AUDITOR_CONSUMER = os.getenv("BYBIT_AUDITOR_CONSUMER", "bybit-auditor-1")

# 🔸 Параллелизм и замки
MAX_PARALLEL_TASKS = int(os.getenv("BYBIT_AUDITOR_MAX_TASKS", "200"))
LOCK_TTL_SEC = int(os.getenv("BYBIT_AUDITOR_LOCK_TTL", "90"))

# 🔸 BYBIT ENV (для REST-проверок и управляющих вызовов)
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
CATEGORY = "linear"  # USDT-perp

# 🔸 Аудит
AUDIT_STREAM = "positions_bybit_audit"

# 🔸 Трейлинг: ключи состояния (разоружаем при закрытии)
TRAIL_ACTIVE_SET = "tv4:trail:active"
TRAIL_KEY_FMT = "tv4:trail:{uid}"


# 🔸 Запуск воркера: два консюмера (order/position) параллельно
async def run_bybit_auditor():
    redis = infra.redis_client

    # инициализация CG для order
    try:
        await redis.xgroup_create(ORDER_STREAM, AUDITOR_CG_ORDER, id="$", mkstream=True)
        log.info("📡 CG %s создан для %s", AUDITOR_CG_ORDER, ORDER_STREAM)
    except Exception:
        pass
    # инициализация CG для position
    try:
        await redis.xgroup_create(POSITION_STREAM, AUDITOR_CG_POS, id="$", mkstream=True)
        log.info("📡 CG %s создан для %s", AUDITOR_CG_POS, POSITION_STREAM)
    except Exception:
        pass

    # сброс на '$' — только новые записи
    try:
        await redis.execute_command("XGROUP", "SETID", ORDER_STREAM, AUDITOR_CG_ORDER, "$")
    except Exception:
        log.exception("❌ SETID order cg failed")
    try:
        await redis.execute_command("XGROUP", "SETID", POSITION_STREAM, AUDITOR_CG_POS, "$")
    except Exception:
        log.exception("❌ SETID position cg failed")

    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    async def _loop_order():
        while True:
            try:
                batch = await redis.xreadgroup(
                    groupname=AUDITOR_CG_ORDER,
                    consumername=AUDITOR_CONSUMER,
                    streams={ORDER_STREAM: ">"},
                    count=100,
                    block=1000,
                )
                if not batch:
                    continue
                tasks = []
                for _, records in batch:
                    for entry_id, fields in records:
                        tasks.append(asyncio.create_task(_handle_order_event(sem, entry_id, fields)))
                await asyncio.gather(*tasks)
            except Exception:
                log.exception("❌ Ошибка в цикле чтения ORDER_STREAM")
                await asyncio.sleep(1)

    async def _loop_position():
        while True:
            try:
                batch = await redis.xreadgroup(
                    groupname=AUDITOR_CG_POS,
                    consumername=AUDITOR_CONSUMER,
                    streams={POSITION_STREAM: ">"},
                    count=100,
                    block=1000,
                )
                if not batch:
                    continue
                tasks = []
                for _, records in batch:
                    for entry_id, fields in records:
                        tasks.append(asyncio.create_task(_handle_position_event(sem, entry_id, fields)))
                await asyncio.gather(*tasks)
            except Exception:
                log.exception("❌ Ошибка в цикле чтения POSITION_STREAM")
                await asyncio.sleep(1)

    log.info("🚀 BYBIT_AUDITOR запущен (order & position)")
    await asyncio.gather(_loop_order(), _loop_position())


# 🔸 Обработка события topic=order (ищем Filled Stop/StopLoss)
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
            await _ack_ok(ORDER_STREAM, AUDITOR_CG_ORDER, entry_id)
            return

        # фильтр: стоповый Filled reduceOnly market (позиционный SL)
        order_status = (payload.get("orderStatus") or "").lower()
        stop_type = (payload.get("stopOrderType") or "").lower()
        order_type = (payload.get("orderType") or "").lower()
        reduce_only = payload.get("reduceOnly")
        symbol = payload.get("symbol")

        if not symbol:
            await _ack_ok(ORDER_STREAM, AUDITOR_CG_ORDER, entry_id)
            return

        is_sl_filled = (
            order_status == "filled" and
            order_type == "market" and
            (stop_type in ("stop", "stoploss")) and
            (reduce_only is True or str(reduce_only).lower() == "true")
        )

        if not is_sl_filled:
            # не наш случай — просто ACK
            await _ack_ok(ORDER_STREAM, AUDITOR_CG_ORDER, entry_id)
            return

        # конвергенция после SL: находим открытую позицию по символу (у нас одна позиция на тикер)
        pos = await _resolve_open_position_by_symbol(symbol)
        if not pos:
            # уже закрыто/ничего нет — идемпотентность
            await _ack_ok(ORDER_STREAM, AUDITOR_CG_ORDER, entry_id)
            return

        position_uid = pos["position_uid"]
        strategy_id = int(pos["strategy_id"])
        direction = pos["direction"]
        source_stream_id = pos["source_stream_id"]

        # lock на (sid, symbol)
        gate_key = f"tv4:gate:{strategy_id}:{symbol}"
        owner = f"{AUDITOR_CONSUMER}-{entry_id}"
        if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
            # короткий локальный ретрай
            for _ in range(10):
                await asyncio.sleep(0.2)
                if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                    break
            else:
                log.info("⏳ Лок не взят (%s) — отложено id=%s", gate_key, entry_id)
                return

        try:
            # проверка остатка позиции
            size = await _get_position_size_linear(symbol)
            if size is None:
                size = Decimal("0")  # мягкий фолбэк

            # если хвост остался — дожимаем reduce-only market
            if size > 0:
                close_side = "Buy" if direction == "short" else "Sell"
                tail_link = _suffix_link(f"tv4-{source_stream_id}", "sl-tail")
                try:
                    resp = await _close_reduce_only_market(symbol, close_side, size, tail_link)
                    await _publish_audit("position_tail_closed_by_sl", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "qty": _to_fixed_str(size),
                        "order_link_id": tail_link,
                    })
                    log.info("🧵 tail closed by auditor: %s qty=%s", tail_link, size)
                except Exception:
                    await _publish_audit("position_tail_close_failed", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "qty": _to_fixed_str(size),
                    })
                    log.exception("❌ tail close failed (order)")

            # конвергенция БД → закрыто
            await _reconcile_db_after_sl(position_uid=position_uid, symbol=symbol, source_stream_id=source_stream_id)

            # разоружить трейл (если был)
            await _disarm_trailing(position_uid)

            # аудит и ACK
            await _publish_audit("position_closed_by_sl", {
                "position_uid": position_uid,
                "strategy_id": strategy_id,
                "symbol": symbol,
            })
            await _ack_ok(ORDER_STREAM, AUDITOR_CG_ORDER, entry_id)
            log.info("✅ auditor: position closed by SL (%s %s)", strategy_id, symbol)

        except Exception:
            log.exception("❌ Ошибка обработки SL(order) для %s", symbol)
        finally:
            await _release_dist_lock(gate_key, owner)


# 🔸 Обработка события topic=position (size→0)
async def _handle_position_event(sem: asyncio.Semaphore, entry_id: str, fields: Dict[str, Any]):
    async with sem:
        redis = infra.redis_client
        try:
            data_raw = fields.get("data")
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")
            payload = json.loads(data_raw or "{}")
        except Exception:
            log.exception("❌ Некорректный payload position (id=%s) — ACK", entry_id)
            await _ack_ok(POSITION_STREAM, AUDITOR_CG_POS, entry_id)
            return

        symbol = payload.get("symbol")
        size_raw = payload.get("size")
        if not symbol:
            await _ack_ok(POSITION_STREAM, AUDITOR_CG_POS, entry_id)
            return

        size = _as_decimal(size_raw) or Decimal("0")
        # интересует только обнуление
        if size > 0:
            await _ack_ok(POSITION_STREAM, AUDITOR_CG_POS, entry_id)
            return

        # найти открытую позицию по символу
        pos = await _resolve_open_position_by_symbol(symbol)
        if not pos:
            await _ack_ok(POSITION_STREAM, AUDITOR_CG_POS, entry_id)
            return

        position_uid = pos["position_uid"]
        strategy_id = int(pos["strategy_id"])
        direction = pos["direction"]
        source_stream_id = pos["source_stream_id"]

        # lock на (sid, symbol)
        gate_key = f"tv4:gate:{strategy_id}:{symbol}"
        owner = f"{AUDITOR_CONSUMER}-{entry_id}"
        if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
            for _ in range(10):
                await asyncio.sleep(0.2)
                if await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                    break
            else:
                log.info("⏳ Лок не взят (%s) — отложено id=%s", gate_key, entry_id)
                return

        try:
            # sanity-check размер через REST (не обязательно, но полезно)
            size_now = await _get_position_size_linear(symbol)
            if size_now and size_now > 0:
                # хвост всё же есть — добьём
                close_side = "Buy" if direction == "short" else "Sell"
                tail_link = _suffix_link(f"tv4-{source_stream_id}", "sl-tail")
                try:
                    resp = await _close_reduce_only_market(symbol, close_side, size_now, tail_link)
                    await _publish_audit("position_tail_closed_by_sl", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "qty": _to_fixed_str(size_now),
                        "order_link_id": tail_link,
                    })
                    log.info("🧵 tail closed by auditor (pos): %s qty=%s", tail_link, size_now)
                except Exception:
                    await _publish_audit("position_tail_close_failed", {
                        "position_uid": position_uid,
                        "symbol": symbol,
                        "qty": _to_fixed_str(size_now),
                    })
                    log.exception("❌ tail close failed (position)")

            # конвергенция БД → закрыто
            await _reconcile_db_after_sl(position_uid=position_uid, symbol=symbol, source_stream_id=source_stream_id)

            # разоружить трейл (если был)
            await _disarm_trailing(position_uid)

            await _publish_audit("position_closed_by_sl", {
                "position_uid": position_uid,
                "strategy_id": strategy_id,
                "symbol": symbol,
            })
            await _ack_ok(POSITION_STREAM, AUDITOR_CG_POS, entry_id)
            log.info("✅ auditor: position closed by SL (position) (%s %s)", strategy_id, symbol)

        except Exception:
            log.exception("❌ Ошибка обработки SL(position) для %s", symbol)
        finally:
            await _release_dist_lock(gate_key, owner)


# 🔸 Конвергенция БД после SL (снять is_active, проставить статусы, закрыть журналы)
async def _reconcile_db_after_sl(*, position_uid: str, symbol: str, source_stream_id: str):
    async with infra.pg_pool.acquire() as conn:
        # SL level=0 → filled; все TP/SL on_tp/виртуальные → canceled; всем is_active=false
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET
                status = CASE
                            WHEN kind='sl' AND level=0 THEN 'filled'
                            WHEN status IN ('planned','sent','planned_offchain','virtual') THEN
                                 CASE WHEN kind='entry' THEN status ELSE 'canceled' END
                            ELSE status
                         END,
                is_active = false,
                updated_at = now(),
                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                      'auditor reconcile: position closed by SL'
            WHERE position_uid = $1
            """,
            position_uid,
        )
        # Логи позиции → закрыто
        await conn.execute(
            """
            UPDATE trader_positions_log
            SET ext_status = 'closed',
                status = CASE WHEN status IN ('processing','sent','planned') THEN 'filled' ELSE status END,
                updated_at = now(),
                note = COALESCE(note,'') || CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       'closed by position stop (auditor)'
            WHERE source_stream_id = $1
            """,
            source_stream_id,
        )
        # Сигналы → filled
        await conn.execute(
            """
            UPDATE trader_signals
            SET processing_status = 'filled',
                processed_at = now(),
                processing_note = COALESCE(processing_note,'') ||
                                  CASE WHEN COALESCE(processing_note,'')='' THEN '' ELSE '; ' END ||
                                  'closed by position stop (auditor)'
            WHERE stream_id = $1
            """,
            source_stream_id,
        )


# 🔸 Вспомогательные: поиск открытой позиции по символу (одна позиция на тикер)
async def _resolve_open_position_by_symbol(symbol: str) -> Optional[dict]:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT position_uid, strategy_id, strategy_type, direction, source_stream_id
            FROM trader_positions_log
            WHERE symbol = $1
              AND ext_status = 'open'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            symbol,
        )
    return dict(row) if row else None


# 🔸 Аудит-событие
async def _publish_audit(event: str, data: dict):
    payload = {"event": event, **(data or {})}
    sid = await infra.redis_client.xadd(AUDIT_STREAM, {"data": json.dumps(payload)})
    log.info("📜 audit %s → %s: %s", event, AUDIT_STREAM, payload)
    return sid


# 🔸 Получить текущий размер позиции по символу (REST /v5/position/list?category=linear&symbol=..)
async def _get_position_size_linear(symbol: str) -> Optional[Decimal]:
    if not API_KEY or not API_SECRET:
        return None
    query = f"category={CATEGORY}&symbol={symbol}"
    url = f"{BASE_URL}/v5/position/list?{query}"
    ts = int(time.time() * 1000)
    sign = _rest_sign(ts, query)
    headers = _private_headers(ts, sign)
    try:
        async with httpx_async_client(timeout=10) as client:
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


# 🔸 Reduce-only Market для закрытия хвоста (tail) — /v5/order/create
async def _close_reduce_only_market(symbol: str, side: str, qty: Decimal, order_link_id: str) -> dict:
    rules = await _fetch_ticker_rules(symbol)
    q = _quant_down(qty, rules["step_qty"]) or Decimal("0")
    if q <= 0 or q < (rules["min_qty"] or Decimal("0")):
        raise ValueError(f"qty below min_qty after quantization: q={q}, min={rules['min_qty']}")

    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,                 # закрывающая сторона
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


# 🔸 ACK helper
async def _ack_ok(stream: str, cg: str, entry_id: str):
    try:
        await infra.redis_client.xack(stream, cg, entry_id)
    except Exception:
        pass


# 🔸 Разоружение трейлинга для позиции
async def _disarm_trailing(position_uid: str):
    try:
        await infra.redis_client.srem(TRAIL_ACTIVE_SET, position_uid)
        await infra.redis_client.delete(TRAIL_KEY_FMT.format(uid=position_uid))
        log.info("🧹 trailing disarmed: uid=%s", position_uid)
    except Exception:
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