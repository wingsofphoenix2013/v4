# bybit_trailer.py — офчейн трейлинг-стоп: после TP-1 периодически подтягивает позиционный SL к 1.5% от LastPrice

# 🔸 Импорты
import os
import json
import time
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Dict, Optional, Tuple

import httpx

from trader_infra import infra

# 🔸 Логгер
log = logging.getLogger("BYBIT_TRAILER")

# 🔸 Константы трейлинга / окружения
TRAIL_ACTIVE_SET = "tv4:trail:active"                 # множество position_uid с активным трейлом
TRAIL_KEY_FMT    = "tv4:trail:{uid}"                  # хеш состояния трейла позиции

TRAIL_PCT           = Decimal(os.getenv("TRAIL_PCT", "1.5"))           # %-дистанция от Last до SL
TRAIL_INTERVAL_SEC  = int(os.getenv("TRAIL_INTERVAL_SEC", "30"))       # период тика
TRAIL_COOLDOWN_SEC  = int(os.getenv("TRAIL_COOLDOWN_SEC", "30"))       # минимум между обновлениями стопа
MAX_PARALLEL_TASKS  = int(os.getenv("BYBIT_TRAILER_MAX_TASKS", "200")) # параллелизм
LOCK_TTL_SEC        = int(os.getenv("BYBIT_TRAILER_LOCK_TTL", "30"))   # TTL распределённого замка

# 🔸 Bybit env (для public market tickers и live trading-stop)
BYBIT_BASE_URL = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
CATEGORY       = "linear"  # USDT-perp

# 🔸 Локальные мьютексы по (sid, symbol)
_local_locks: Dict[Tuple[int, str], asyncio.Lock] = {}


# 🔸 Основной цикл трейлера (периодический тик)
async def run_bybit_trailer():
    log.info("🚀 BYBIT_TRAILER запущен: interval=%ss, trail_pct=%s%%", TRAIL_INTERVAL_SEC, TRAIL_PCT)
    sem = asyncio.Semaphore(MAX_PARALLEL_TASKS)

    while True:
        try:
            # получаем активные позиции для трейлинга
            uids = await infra.redis_client.smembers(TRAIL_ACTIVE_SET)
            if not uids:
                await asyncio.sleep(TRAIL_INTERVAL_SEC)
                continue

            tasks = []
            for uid in uids:
                tasks.append(asyncio.create_task(_process_trailing_for_position(sem, uid)))

            await asyncio.gather(*tasks)
        except Exception:
            log.exception("❌ Ошибка тика трейлера")
        finally:
            # спим до следующего тика
            await asyncio.sleep(TRAIL_INTERVAL_SEC)


# 🔸 Обработка трейлинга для одной позиции
async def _process_trailing_for_position(sem: asyncio.Semaphore, position_uid: str):
    async with sem:
        redis = infra.redis_client

        # читаем состояние из Redis-хеша
        state = await redis.hgetall(TRAIL_KEY_FMT.format(uid=position_uid))
        if not state:
            # нет состояния — убрать из активного множества
            await redis.srem(TRAIL_ACTIVE_SET, position_uid)
            log.debug("trailing state missing, disarm uid=%s", position_uid)
            return

        # парсим состояние
        try:
            sid         = int(state.get("sid"))
            symbol      = state.get("symbol")
            direction   = (state.get("direction") or "").lower()  # long|short
            entry_raw   = state.get("entry")
            sl_last_raw = state.get("sl_last") or ""
            order_mode  = (state.get("order_mode") or "dry_run").strip()
            last_ts_raw = state.get("last_update_ts") or "0"
        except Exception:
            # плохое состояние — разоружаем
            await _disarm_trailing(position_uid, reason="bad_state")
            return

        # валидации состояния
        if not symbol or direction not in ("long", "short") or not entry_raw:
            await _disarm_trailing(position_uid, reason="incomplete_state")
            return

        # сериализация по ключу (sid, symbol)
        key = (sid, symbol)
        lock = _local_locks.setdefault(key, asyncio.Lock())

        async with lock:
            # распределённый замок
            gate_key = f"tv4:gate:{sid}:{symbol}"
            owner = f"bybit-trailer-{position_uid}"
            if not await _acquire_dist_lock(gate_key, owner, LOCK_TTL_SEC):
                # мягкая пауза — вернёмся на следующем тике
                log.debug("lock busy for %s/%s — skip this tick", sid, symbol)
                return

            try:
                # проверяем, что позиция ещё открыта (по БД)
                is_open, order_mode_db = await _is_position_open_and_mode(position_uid)
                if not is_open:
                    await _disarm_trailing(position_uid, reason="position_closed")
                    return
                # если в БД есть режим — предпочтем его
                if order_mode_db:
                    order_mode = order_mode_db

                # cooldown между обновлениями
                try:
                    last_update_ms = int(last_ts_raw)
                except Exception:
                    last_update_ms = 0
                if int(time.time() * 1000) - last_update_ms < TRAIL_COOLDOWN_SEC * 1000:
                    log.debug("trailing cooldown uid=%s", position_uid)
                    return

                # биржевой pre-check размера: если позиция уже нулевая — разоружаем
                size_now = await _get_position_size_linear(symbol)
                if size_now is not None and size_now <= 0:
                    await _disarm_trailing(position_uid, reason="zero_size_exchange")
                    log.info("🧹 trailing disarmed (zero size on exchange): uid=%s %s", position_uid, symbol)
                    return

                # берём LastPrice и правила тикера
                last_price = await _get_last_price_linear(symbol)
                if last_price is None or last_price <= 0:
                    log.debug("no last price for %s — skip", symbol)
                    return

                step_price = await _get_step_price(symbol)
                if step_price is None or step_price <= 0:
                    log.debug("no step_price for %s — skip", symbol)
                    return

                entry = _as_decimal(entry_raw)
                sl_last = _as_decimal(sl_last_raw) if sl_last_raw else None
                if entry is None or entry <= 0:
                    await _disarm_trailing(position_uid, reason="bad_entry")
                    return

                # вычисляем разрыв относительно entry
                trail_frac = (TRAIL_PCT / Decimal("100"))
                if direction == "long":
                    gap = (last_price / entry) - Decimal("1")
                else:
                    gap = (entry / last_price) - Decimal("1")

                # если gap не превысил порог — ничего не делаем
                if gap <= trail_frac:
                    log.debug("uid=%s gap<=trail (gap=%.5f)", position_uid, float(gap))
                    return

                # целевой стоп на расстоянии TRAIL_PCT от Last
                if direction == "long":
                    target = last_price * (Decimal("1") - trail_frac)
                    sl_new = _quant_down(target, step_price)
                    # монотонность и минимальный уровень — не ниже entry
                    if sl_last is not None and sl_new <= sl_last:
                        log.debug("uid=%s monotonic: sl_new<=sl_last (%s<=%s)", position_uid, sl_new, sl_last)
                        return
                    if sl_new < entry:
                        sl_new = entry
                    # малый шаг — не трогаем
                    if sl_last is not None and (sl_new - sl_last) < step_price:
                        await _publish_audit("trailing_skip_small_delta", {
                            "position_uid": position_uid, "symbol": symbol,
                            "old": _to_fixed_str(sl_last), "new": _to_fixed_str(sl_new),
                            "reason": "delta<tick"
                        })
                        return
                else:
                    target = last_price * (Decimal("1") + trail_frac)
                    sl_new = _quant_up(target, step_price)
                    # монотонность и верхний уровень — не выше entry
                    if sl_last is not None and sl_new >= sl_last:
                        log.debug("uid=%s monotonic: sl_new>=sl_last (%s>=%s)", position_uid, sl_new, sl_last)
                        return
                    if sl_new > entry:
                        sl_new = entry
                    # малый шаг — не трогаем
                    if sl_last is not None and (sl_last - sl_new) < step_price:
                        await _publish_audit("trailing_skip_small_delta", {
                            "position_uid": position_uid, "symbol": symbol,
                            "old": _to_fixed_str(sl_last), "new": _to_fixed_str(sl_new),
                            "reason": "delta<tick"
                        })
                        return

                # применяем стоп: live → trading-stop; dry_run → только БД
                ok = True
                if order_mode == "live":
                    try:
                        resp = await _set_position_stop_loss_live(symbol, sl_new)
                        ret_code = (resp or {}).get("retCode", 0)
                        ret_msg  = (resp or {}).get("retMsg")
                        ok = (ret_code == 0)
                        if not ok:
                            # если нулевая позиция — сразу разоружаем, чтобы не повторять
                            if ret_code == 10001 or (ret_msg and "zero position" in str(ret_msg).lower()):
                                await _disarm_trailing(position_uid, reason="zero_position_retcode")
                            await _publish_audit("trailing_failed", {
                                "position_uid": position_uid, "symbol": symbol,
                                "new": _to_fixed_str(sl_new), "retCode": ret_code, "retMsg": ret_msg
                            })
                            log.info("❗ trailing set failed: %s ret=%s %s", symbol, ret_code, ret_msg)
                            return
                    except Exception:
                        await _publish_audit("trailing_failed", {
                            "position_uid": position_uid, "symbol": symbol,
                            "new": _to_fixed_str(sl_new), "reason": "exception"
                        })
                        log.exception("❌ trailing set exception")
                        return

                # успех — обновляем карточку SL level=0 и состояние в Redis
                updated = await _update_sl0_price(position_uid, sl_new)
                await infra.redis_client.hset(
                    TRAIL_KEY_FMT.format(uid=position_uid),
                    mapping={
                        "sl_last": _to_fixed_str(sl_new),
                        "last_update_ts": str(int(time.time() * 1000)),
                    },
                )
                await _publish_audit("trailing_update", {
                    "position_uid": position_uid, "symbol": symbol,
                    "old": _to_fixed_str(sl_last) if sl_last is not None else None,
                    "new": _to_fixed_str(sl_new),
                    "last": _to_fixed_str(last_price),
                })
                log.info("📈 trailing updated: uid=%s %s %s SL=%s (last=%s)", position_uid, symbol, direction, sl_new, last_price)

            except Exception:
                log.exception("❌ Ошибка обработки трейлинга uid=%s", position_uid)
            finally:
                await _release_dist_lock(gate_key, owner)


# 🔸 Проверка: позиция ещё открыта + режим order_mode (если есть)
async def _is_position_open_and_mode(position_uid: str) -> Tuple[bool, Optional[str]]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT ext_status, order_mode
        FROM trader_positions_log
        WHERE position_uid = $1
        """,
        position_uid,
    )
    if not row:
        return False, None
    ext = (row["ext_status"] or "").strip()
    om  = (row["order_mode"] or "").strip() if row["order_mode"] else None
    return (ext == "open"), (om if om in ("dry_run", "live") else None)


# 🔸 Получение цены шага (ticksize/precision) для квантования цены
async def _get_step_price(symbol: str) -> Optional[Decimal]:
    row = await infra.pg_pool.fetchrow(
        """
        SELECT COALESCE(ticksize,0) AS ticksize,
               COALESCE(precision_price,0) AS pprice
        FROM tickers_bb
        WHERE symbol = $1
        """,
        symbol,
    )
    if not row:
        return None
    ticksize = _as_decimal(row["ticksize"]) or Decimal("0")
    pprice   = int(row["pprice"])
    step_price = ticksize if ticksize > 0 else (Decimal("1").scaleb(-pprice) if pprice > 0 else Decimal("0.00000001"))
    return step_price


# 🔸 Получение LastPrice (public v5)
async def _get_last_price_linear(symbol: str) -> Optional[Decimal]:
    url = f"{BYBIT_BASE_URL}/v5/market/tickers?category={CATEGORY}&symbol={symbol}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            lst = (data.get("result") or {}).get("list") or []
            head = lst[0] if lst else {}
            lp = head.get("lastPrice")
            return _as_decimal(lp)
    except Exception:
        log.debug("get last price failed for %s", symbol)
        return None


# 🔸 Текущий размер позиции (REST /v5/position/list)
async def _get_position_size_linear(symbol: str) -> Optional[Decimal]:
    API_KEY     = os.getenv("BYBIT_API_KEY", "")
    API_SECRET  = os.getenv("BYBIT_API_SECRET", "")
    RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")
    if not API_KEY or not API_SECRET:
        return None
    query = f"category={CATEGORY}&symbol={symbol}"
    url = f"{BYBIT_BASE_URL}/v5/position/list?{query}"
    ts = int(time.time() * 1000)
    # импортировать локально, чтобы не тащить в глобальные импорты
    import hmac as _h, hashlib as _hl
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{query}"
    sign = _h.new(API_SECRET.encode("utf-8"), payload.encode("utf-8"), _hl.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
    }
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
        log.debug("get position size failed (trailer) for %s", symbol)
        return None


# 🔸 Установка позиционного стопа (live) — /v5/position/trading-stop
async def _set_position_stop_loss_live(symbol: str, trigger_price: Decimal) -> dict:
    # условия достаточности
    step_price = await _get_step_price(symbol)
    p = _quant_down(trigger_price, step_price) if trigger_price is not None else None
    if p is None or p <= 0:
        raise ValueError("invalid SL trigger price")

    # подготовка и подпись запроса
    import hmac, hashlib

    API_KEY     = os.getenv("BYBIT_API_KEY", "")
    API_SECRET  = os.getenv("BYBIT_API_SECRET", "")
    RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000")

    if not API_KEY or not API_SECRET:
        raise RuntimeError("missing Bybit API credentials")

    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "positionIdx": 0,            # oneway
        "stopLoss": _to_fixed_str(p),
        "slTriggerBy": "LastPrice",
    }

    url = f"{BYBIT_BASE_URL}/v5/position/trading-stop"
    ts = int(time.time() * 1000)
    body_json = json.dumps(body, separators=(",", ":"))
    payload = f"{ts}{API_KEY}{RECV_WINDOW}{body_json}"
    sign = hmac.new(API_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": str(ts),
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "X-BAPI-SIGN": sign,
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, content=body_json)
        r.raise_for_status()
        return r.json()


# 🔸 Обновить price у SL level=0 в БД (если есть)
async def _update_sl0_price(position_uid: str, new_price: Decimal) -> bool:
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM trader_position_orders
            WHERE position_uid = $1
              AND kind = 'sl'
              AND level = 0
              AND is_active = true
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            position_uid,
        )
        if not row:
            return False

        rid = int(row["id"])
        await conn.execute(
            """
            UPDATE trader_position_orders
            SET price = $2,
                updated_at = now(),
                note = COALESCE(note,'') ||
                       CASE WHEN COALESCE(note,'')='' THEN '' ELSE '; ' END ||
                       ('trailing update to ' || $2::text)
            WHERE id = $1
            """,
            rid, _to_fixed_str(new_price),
        )
        return True


# 🔸 Разоружить трейлинг: убрать ключи/сеттер
async def _disarm_trailing(position_uid: str, reason: str):
    try:
        await infra.redis_client.srem(TRAIL_ACTIVE_SET, position_uid)
        await infra.redis_client.delete(TRAIL_KEY_FMT.format(uid=position_uid))
        await _publish_audit("trailing_disarmed", {"position_uid": position_uid, "reason": reason})
        log.info("🧹 trailing disarmed: uid=%s reason=%s", position_uid, reason)
    except Exception:
        log.debug("trailing disarm failed silently uid=%s", position_uid)


# 🔸 Аудит-событие
async def _publish_audit(event: str, data: dict):
    payload = {"event": event, **(data or {})}
    sid = await infra.redis_client.xadd("positions_bybit_audit", {"data": json.dumps(payload)})
    log.debug("audit %s → positions_bybit_audit: %s", event, payload)
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

def _quant_up(value: Decimal, step: Decimal) -> Optional[Decimal]:
    try:
        if value is None or step is None or step <= 0:
            return None
        return (value / step).to_integral_value(rounding=ROUND_UP) * step
    except Exception:
        return None