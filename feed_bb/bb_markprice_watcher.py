# bb_markprice_watcher.py — общий WS Bybit v5 (linear) для markPrice и lastPrice → Redis (bb:price:{symbol}, bb_last:price:{symbol})

# 🔸 Импорты и зависимости
import os
import asyncio
import json
import logging
import random
from decimal import Decimal, ROUND_DOWN, InvalidOperation

import websockets
from websockets.exceptions import ConnectionClosedError

log = logging.getLogger("BB_MARKPRICE")

# 🔸 Конфиг
BYBIT_WS_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")
KEEPALIVE_SEC = int(os.getenv("BB_WS_KEEPALIVE_SEC", "20"))
REFRESH_ACTIVE_SEC = int(os.getenv("BB_ACTIVE_REFRESH_SEC", "60"))
WS_MAX_QUEUE = int(os.getenv("BB_WS_MAX_QUEUE", "1000"))                 # ограничение очереди клиента WS
PRECISION_CACHE_TTL_SEC = int(os.getenv("BB_PRECISION_CACHE_TTL_SEC", "3600"))  # TTL кэша precision (сек)

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

# 🔸 fire-and-forget helper (поглощает исключение, чтобы не было warn про Future)
def _ff(coro):
    t = asyncio.create_task(coro)
    t.add_done_callback(lambda fut: fut.exception())
    return t

# 🔸 Утилиты
async def _load_active_symbols(pg_pool):
    try:
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT symbol FROM tickers_bb WHERE status='enabled' AND is_active=true ORDER BY symbol"
                )
                rows = await cur.fetchall()
        return [r[0] for r in rows] if rows else []
    except Exception as e:
        log.error(f"MARKPRICE: ошибка загрузки активных тикеров: {e}", exc_info=True)
        return []

def _round_down_price(v, digits: int) -> str:
    try:
        d = Decimal(str(v))
        if digits <= 0:
            return str(int(d.to_integral_value(rounding=ROUND_DOWN)))
        return str(d.quantize(Decimal(f"1e-{digits}"), rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError, TypeError):
        return str(v)

# 🔸 Формирование/отправка подписок
async def _send_sub(ws, syms):
    if not syms:
        return
    payload = {"op": "subscribe", "args": [f"tickers.{s}" for s in syms]}
    await ws.send(json.dumps(payload))
    log.info(f"MARKPRICE SUB → {len(syms)} symbols")

async def _send_unsub(ws, syms):
    if not syms:
        return
    payload = {"op": "unsubscribe", "args": [f"tickers.{s}" for s in syms]}
    await ws.send(json.dumps(payload))
    log.info(f"MARKPRICE UNSUB → {len(syms)} symbols")

# 🔸 Менеджер одного общего WS: подписка на все активные tickers.{symbol}
async def run_markprice_watcher_bb(pg_pool, redis):
    log.info(f"MARKPRICE watcher (Bybit) запущен — общий WS для всех символов (WS_MAX_QUEUE={WS_MAX_QUEUE})")

    current = set()
    backoff = 1.0

    # keepalive-пинг Bybit WS
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
                ping_interval=None,
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
                    last_refresh = asyncio.get_event_loop().time()

                    async for raw in ws:
                        # разбираем JSON; игнорируем мусор
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # периодическая сверка набора символов
                        now = asyncio.get_event_loop().time()
                        if now - last_refresh >= REFRESH_ACTIVE_SEC:
                            last_refresh = now
                            active2 = set(await _load_active_symbols(pg_pool))
                            to_unsub = sorted(current - active2)
                            to_sub = sorted(active2 - current)
                            if to_unsub:
                                await _send_unsub(ws, to_unsub)
                                for s in to_unsub:
                                    current.discard(s)
                            if to_sub:
                                await _send_sub(ws, to_sub)
                                current.update(to_sub)

                        topic = msg.get("topic") or ""
                        if not topic.startswith("tickers."):
                            continue

                        data = msg.get("data")

                        # Bybit иногда шлёт массив объектов в data
                        if isinstance(data, list) and data:
                            for item in data:
                                sym2 = item.get("symbol")
                                if not sym2:
                                    continue

                                # markPrice
                                mp2 = item.get("markPrice")
                                if mp2 is not None:
                                    pp2 = await prec_price_cache.get(pg_pool, sym2)
                                    await redis.set(f"bb:price:{sym2}", _round_down_price(mp2, pp2))

                                # lastPrice
                                lp2 = item.get("lastPrice")
                                if lp2 is not None:
                                    pp2 = await prec_price_cache.get(pg_pool, sym2)
                                    await redis.set(f"bb_last:price:{sym2}", _round_down_price(lp2, pp2))

                            continue

                        # обычный объект в data
                        if not isinstance(data, dict):
                            continue

                        sym = data.get("symbol")
                        if not sym:
                            continue

                        # markPrice (пишем независимо от наличия lastPrice)
                        mp = data.get("markPrice")
                        if mp is not None:
                            pp = await prec_price_cache.get(pg_pool, sym)
                            await redis.set(f"bb:price:{sym}", _round_down_price(mp, pp))

                        # lastPrice (пишем независимо от наличия markPrice)
                        lp = data.get("lastPrice")
                        if lp is not None:
                            pp = await prec_price_cache.get(pg_pool, sym)
                            await redis.set(f"bb_last:price:{sym}", _round_down_price(lp, pp))

                finally:
                    # корректно завершаем keepalive
                    ka.cancel()
                    try:
                        await ka
                    except Exception:
                        pass

        except (ConnectionClosedError, asyncio.IncompleteReadError, OSError) as e:
            # ожидаемые сетевые обрывы — реконнект с джиттером и полным ресабскрайбом
            wait = max(3.0, min(30.0, backoff * (1.5 + random.random() * 0.5)))
            log.info(f"MARKPRICE reconnect in {wait:.1f}s ({type(e).__name__})")
            await asyncio.sleep(wait)
            backoff = wait

        except Exception as e:
            log.error(f"MARKPRICE manager error: {e}", exc_info=True)
            await asyncio.sleep(3)