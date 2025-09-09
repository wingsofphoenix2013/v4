# bb_markprice_watcher.py — отслеживание markPrice по активным тикерам (Bybit v5 linear) и запись в Redis

# 🔸 Импорты и зависимости
import os
import asyncio
import json
import logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import websockets

log = logging.getLogger("BB_MARKPRICE")

# 🔸 Конфиг
BYBIT_WS_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")
KEEPALIVE_SEC = int(os.getenv("BB_WS_KEEPALIVE_SEC", "180"))
REFRESH_ACTIVE_SEC = int(os.getenv("BB_ACTIVE_REFRESH_SEC", "5"))

# 🔸 Кеш точности цены из tickers_bb
class PricePrecisionCache:
    def __init__(self):
        self.pp = {}

    async def get(self, pg_pool, symbol: str) -> int:
        if symbol in self.pp:
            return self.pp[symbol]
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT precision_price FROM tickers_bb WHERE symbol = %s", (symbol,))
                row = await cur.fetchone()
        val = int(row[0]) if row and row[0] is not None else 0
        self.pp[symbol] = val
        return val

prec_price_cache = PricePrecisionCache()

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

def _round_down_price(v: str | float, digits: int) -> str:
    try:
        d = Decimal(str(v))
        if digits <= 0:
            return str(int(d.to_integral_value(rounding=ROUND_DOWN)))
        return str(d.quantize(Decimal(f"1e-{digits}"), rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError, TypeError):
        return str(v)

# 🔸 Поток для одного символа
async def _watch_symbol_markprice(symbol: str, pg_pool, redis):
    url = BYBIT_WS_URL
    last_emit = 0

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
            async with websockets.connect(url, ping_interval=None, close_timeout=5) as ws:
                # подписка на тикер (markPrice внутри)
                sub = {"op": "subscribe", "args": [f"tickers.{symbol}"]}
                await ws.send(json.dumps(sub))
                log.info(f"[{symbol}] MARKPRICE: connected & subscribed")

                ka_task = asyncio.create_task(keepalive(ws))
                try:
                    while True:
                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # служебные ответы
                        if "op" in msg:
                            continue

                        if msg.get("topic") != f"tickers.{symbol}":
                            continue

                        data = msg.get("data") or {}
                        price = data.get("markPrice")
                        if price is None:
                            continue

                        now = asyncio.get_event_loop().time()
                        if now - last_emit < 1.0:
                            continue
                        last_emit = now

                        pp = await prec_price_cache.get(pg_pool, symbol)
                        rounded = _round_down_price(price, pp)
                        await redis.set(f"bb:price:{symbol}", rounded)
                        log.debug(f"[{symbol}] MARKPRICE: {rounded}")

                finally:
                    ka_task.cancel()

        except Exception as e:
            log.error(f"[{symbol}] MARKPRICE WS error: {e}", exc_info=True)
            await asyncio.sleep(3)

# 🔸 Менеджер: спавнит/останавливает таски по активным символам
async def run_markprice_watcher_bb(pg_pool, redis):
    log.info("MARKPRICE watcher (Bybit) запущен")

    tasks: dict[str, asyncio.Task] = {}

    while True:
        try:
            active = set(await _load_active_symbols(pg_pool))
            known = set(tasks.keys())

            # старт новых
            for sym in active - known:
                tasks[sym] = asyncio.create_task(_watch_symbol_markprice(sym, pg_pool, redis))
                log.info(f"[{sym}] MARKPRICE: started")

            # остановка лишних
            for sym in known - active:
                t = tasks.pop(sym, None)
                if t:
                    t.cancel()
                    log.info(f"[{sym}] MARKPRICE: stopped (deactivated)")

            await asyncio.sleep(REFRESH_ACTIVE_SEC)

        except Exception as e:
            log.error(f"MARKPRICE manager error: {e}", exc_info=True)
            await asyncio.sleep(2)