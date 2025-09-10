# bb_markprice_watcher.py — общий WS для всех tickers.{symbol} (Bybit v5 linear) → bb:price:{symbol}

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

# 🔸 Кеш точности цены
class PricePrecisionCache:
    def __init__(self):
        self.pp = {}

    async def get(self, pg_pool, symbol: str) -> int:
        if symbol in self.pp:
            return self.pp[symbol]
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT precision_price FROM tickers_bb WHERE symbol=%s", (symbol,))
                row = await cur.fetchone()
        val = int(row[0]) if row and row[0] is not None else 0
        self.pp[symbol] = val
        return val

prec_price_cache = PricePrecisionCache()

# 🔸 fire-and-forget helper (поглощает исключение, чтоб не было "Future exception was never retrieved")
def _ff(coro):
    t = asyncio.create_task(coro)
    t.add_done_callback(lambda fut: fut.exception())  # читаем исключение, чтобы подавить warn
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
    log.info("MARKPRICE watcher (Bybit) запущен — общий WS для всех символов")

    current = set()
    backoff = 1.0

    async def keepalive(ws):
        try:
            while True:
                try:
                    await ws.send(json.dumps({"op": "ping"}))  # Bybit ping раз в KEEPALIVE_SEC
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
                max_queue=None,
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
                        # служебные контрол-сообщения пропускаем
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # Периодическая сверка набора символов
                        now = asyncio.get_event_loop().time()
                        if now - last_refresh >= REFRESH_ACTIVE_SEC:
                            last_refresh = now
                            active2 = set(await _load_active_symbols(pg_pool))
                            to_unsub = sorted(current - active2)
                            to_sub   = sorted(active2 - current)
                            if to_unsub:
                                await _send_unsub(ws, to_unsub)
                                for s in to_unsub: current.discard(s)
                            if to_sub:
                                await _send_sub(ws, to_sub)
                                current.update(to_sub)

                        topic = msg.get("topic") or ""
                        if not topic.startswith("tickers."):
                            continue

                        data = msg.get("data") or {}
                        sym = data.get("symbol")
                        if not sym:
                            # Bybit иногда шлёт массив data; нормализуем
                            arr = msg.get("data")
                            if isinstance(arr, list) and arr:
                                for item in arr:
                                    sym2 = item.get("symbol")
                                    mp2 = item.get("markPrice")
                                    if sym2 and mp2 is not None:
                                        pp = await prec_price_cache.get(pg_pool, sym2)
                                        await redis.set(f"bb:price:{sym2}", _round_down_price(mp2, pp))
                            continue

                        price = data.get("markPrice")
                        if price is None:
                            continue

                        pp = await prec_price_cache.get(pg_pool, sym)
                        await redis.set(f"bb:price:{sym}", _round_down_price(price, pp))

                finally:
                    ka.cancel()

        except (ConnectionClosedError, asyncio.IncompleteReadError, OSError) as e:
            # ожидаемые сетевые обрывы — плавный реконнект с джиттером и полным ресабскрайбом
            wait = max(3.0, min(30.0, backoff * (1.5 + random.random() * 0.5)))
            log.info(f"MARKPRICE reconnect in {wait:.1f}s ({type(e).__name__})")
            await asyncio.sleep(wait)
            backoff = wait

        except Exception as e:
            log.error(f"MARKPRICE manager error: {e}", exc_info=True)
            await asyncio.sleep(3)