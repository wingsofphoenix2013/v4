# bb_lastprice_watcher.py — общий WS (Bybit v5 tickers) → три ключа в Redis
# Публикация last/mark/index цен в отдельные ключи с округлением и тайм-гейтом записи
# Ключи: bb:last_price:{symbol}, bb:mark_price:{symbol}, bb:index_price:{symbol}

# 🔸 Импорты и зависимости
import os
import asyncio
import json
import logging
import random
from decimal import Decimal, ROUND_DOWN, InvalidOperation

import websockets
from websockets.exceptions import ConnectionClosedError

# 🔸 Логгер
log = logging.getLogger("BB_LASTPRICE")

# 🔸 Конфиг
BYBIT_WS_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")
KEEPALIVE_SEC = int(os.getenv("BB_WS_KEEPALIVE_SEC", "20"))
REFRESH_ACTIVE_SEC = int(os.getenv("BB_ACTIVE_REFRESH_SEC", "60"))

# ограничение внутренней очереди клиента websockets (для контроля роста памяти)
WS_MAX_QUEUE = int(os.getenv("BB_WS_MAX_QUEUE", "1000"))

# минимальный интервал публикации цен в Redis, сек (тайм-гейт per-symbol)
# поддерживаем старую переменную для обратной совместимости
MIN_UPDATE_SEC = float(os.getenv("BB_TICKERS_MIN_UPDATE_SEC", os.getenv("BB_LASTPRICE_MIN_UPDATE_SEC", "1.0")))

# TTL кэширования precision (секунды)
PRECISION_CACHE_TTL_SEC = int(os.getenv("BB_PRECISION_CACHE_TTL_SEC", "3600"))

# 🔸 TTL-кэш (для precision)
class _TTLCache:
    def __init__(self, ttl_sec: int):
        self.ttl = ttl_sec
        self._data: dict[str, tuple[object, float]] = {}

    # получить значение из кэша
    def get(self, key: str):
        item = self._data.get(key)
        if not item:
            return None
        val, ts = item
        now = asyncio.get_event_loop().time()
        if now - ts > self.ttl:
            self._data.pop(key, None)
            return None
        return val

    # записать значение в кэш
    def set(self, key: str, value):
        self._data[key] = (value, asyncio.get_event_loop().time())

# 🔸 Кеш точности цены (TTL)
class PricePrecisionCache:
    def __init__(self):
        self.pp = _TTLCache(PRECISION_CACHE_TTL_SEC)

    async def get(self, pg_pool, symbol: str) -> int:
        cached = self.pp.get(symbol)
        if cached is not None:
            return cached
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT precision_price FROM tickers_bb WHERE symbol=%s", (symbol,))
                row = await cur.fetchone()
        val = int(row[0]) if row and row[0] is not None else 0
        self.pp.set(symbol, val)
        return val

prec_price_cache = PricePrecisionCache()

# 🔸 Утилиты
def _round_down_price(v, digits: int) -> str:
    try:
        d = Decimal(str(v))
        if digits <= 0:
            return str(int(d.to_integral_value(rounding=ROUND_DOWN)))
        return str(d.quantize(Decimal(f"1e-{digits}"), rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError, TypeError):
        return str(v)

async def _load_active_symbols(pg_pool):
    try:
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT symbol FROM tickers_bb WHERE status='enabled' AND is_active=true ORDER BY symbol"
                )
                rows = await cur.fetchall()
        syms = [r[0] for r in rows] if rows else []
        log.info("LASTPRICE: активных символов=%d", len(syms))
        return syms
    except Exception as e:
        log.error(f"LASTPRICE: ошибка загрузки активных тикеров: {e}", exc_info=True)
        return []

async def _send_sub(ws, syms):
    if not syms:
        return
    # tickers.{symbol}
    payload = {"op": "subscribe", "args": [f"tickers.{s}" for s in syms]}
    await ws.send(json.dumps(payload))
    log.info("LASTPRICE SUB (tickers) → %d symbols", len(syms))

async def _send_unsub(ws, syms):
    if not syms:
        return
    payload = {"op": "unsubscribe", "args": [f"tickers.{s}" for s in syms]}
    await ws.send(json.dumps(payload))
    log.info("LASTPRICE UNSUB (tickers) → %d symbols", len(syms))

# 🔸 Менеджер одного общего WS: подписка на все активные tickers.{symbol}
async def run_lastprice_watcher_bb(pg_pool, redis):
    log.info(
        "LASTPRICE watcher (Bybit tickers) запущен — общий WS для всех символов "
        "(WS_MAX_QUEUE=%s, MIN_UPDATE=%.3fs)",
        WS_MAX_QUEUE, MIN_UPDATE_SEC
    )

    current = set()
    backoff = 1.0

    # per-symbol: время последней публикации и «последние виденные» цены для схлопывания
    last_pub_ts: dict[str, float] = {}
    # pending_prices[sym] = {"last": "...", "mark": "...", "index": "..."}
    pending_prices: dict[str, dict[str, str]] = {}

    async def keepalive(ws):
        try:
            while True:
                try:
                    await ws.send(json.dumps({"op": "ping"}))
                except Exception:
                    return
                await asyncio.sleep(KEEPALIVE_SEC)
        except asyncio.CancelledError:
            return

    while True:
        try:
            # актуальный список активных символов
            active = set(await _load_active_symbols(pg_pool))
            if not active:
                current.clear()
                await asyncio.sleep(REFRESH_ACTIVE_SEC)
                continue

            # лёгкий stagger при реконнекте
            await asyncio.sleep(random.uniform(0.05, 0.25))

            async with websockets.connect(
                BYBIT_WS_URL,
                ping_interval=None,   # свой keepalive
                close_timeout=5,
                max_queue=WS_MAX_QUEUE,
                open_timeout=10,
            ) as ws:
                # первичная подписка на все активные
                await _send_sub(ws, sorted(active))
                current = set(active)
                backoff = 1.0

                ka = asyncio.create_task(keepalive(ws))
                try:
                    loop = asyncio.get_event_loop()
                    last_refresh = loop.time()

                    async for raw in ws:
                        # парсинг сообщения
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # периодическая сверка активных символов
                        now = loop.time()
                        if now - last_refresh >= REFRESH_ACTIVE_SEC:
                            last_refresh = now
                            active2 = set(await _load_active_symbols(pg_pool))
                            to_unsub = sorted(current - active2)
                            to_sub   = sorted(active2 - current)
                            if to_unsub:
                                await _send_unsub(ws, to_unsub)
                                for s in to_unsub:
                                    current.discard(s)
                                    last_pub_ts.pop(s, None)
                                    pending_prices.pop(s, None)
                            if to_sub:
                                await _send_sub(ws, to_sub)
                                current.update(to_sub)

                        topic = msg.get("topic") or ""
                        if not topic.startswith("tickers."):
                            continue

                        data = msg.get("data")
                        if not data:
                            continue

                        # По спецификации tickers для деривативов: data — объект; но на всякий случай поддержим список
                        items = data if isinstance(data, list) else [data]

                        for item in items:
                            sym = item.get("symbol") or item.get("s")
                            if not sym:
                                continue

                            # исходные значения из сообщения
                            lp = item.get("lastPrice")
                            mp = item.get("markPrice")
                            ip = item.get("indexPrice")

                            if lp is None and mp is None and ip is None:
                                continue

                            # округление вниз до precision_price
                            pp = await prec_price_cache.get(pg_pool, sym)
                            buf = pending_prices.setdefault(sym, {})
                            if lp is not None:
                                buf["last"] = _round_down_price(lp, pp)
                            if mp is not None:
                                buf["mark"] = _round_down_price(mp, pp)
                            if ip is not None:
                                buf["index"] = _round_down_price(ip, pp)

                            # тайм-гейт: публикуем не чаще, чем раз в MIN_UPDATE_SEC
                            t_last = last_pub_ts.get(sym, 0.0)
                            now = loop.time()
                            if now - t_last >= MIN_UPDATE_SEC:
                                sets = []
                                v = pending_prices.get(sym, {})
                                if "last" in v:
                                    sets.append(redis.set(f"bb:last_price:{sym}", v["last"]))
                                if "mark" in v:
                                    sets.append(redis.set(f"bb:mark_price:{sym}", v["mark"]))
                                if "index" in v:
                                    sets.append(redis.set(f"bb:index_price:{sym}", v["index"]))
                                if sets:
                                    await asyncio.gather(*sets)
                                    last_pub_ts[sym] = now
                                # (по желанию можно чистить pending_prices[sym]; оставим последние значения)

                finally:
                    # корректно завершаем keepalive
                    ka.cancel()
                    try:
                        await ka
                    except Exception:
                        pass

        except (ConnectionClosedError, asyncio.IncompleteReadError, OSError) as e:
            # ожидаемые сетевые обрывы — плавный реконнект с джиттером и полным ресабскрайбом
            wait = max(3.0, min(30.0, backoff * (1.5 + random.random() * 0.5)))
            log.info("LASTPRICE reconnect (tickers) in %.1fs (%s)", wait, type(e).__name__)
            await asyncio.sleep(wait)
            backoff = wait

        except Exception as e:
            log.error("LASTPRICE manager error (tickers): %s", e, exc_info=True)
            await asyncio.sleep(3)