# bb_feed_and_aggregate.py — per-symbol Bybit WS (linear) → Redis TS/Stream для m5/m15/h1

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
import json
import time
import random
from decimal import Decimal, ROUND_DOWN, InvalidOperation

import websockets
from websockets.exceptions import ConnectionClosedError

log = logging.getLogger("BB_FEED_AGGR")

# 🔸 Конфиг/ENV
BYBIT_WS_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")
KEEPALIVE_SEC = int(os.getenv("BB_WS_KEEPALIVE_SEC", "60"))
ACTIVE_REFRESH_SEC = int(os.getenv("BB_ACTIVE_REFRESH_SEC", "60"))
NONCLOSED_THROTTLE_SEC = int(os.getenv("BB_NONCLOSED_THROTTLE_SEC", "10"))
TS_RETENTION_MS = int(os.getenv("BB_TS_RETENTION_MS", str(60 * 24 * 60 * 60 * 1000)))  # ~60 дней

# 🔸 Маппинги интервалов
SUB_IV = {"m5": "5", "m15": "15", "h1": "60"}
ALL_TF = ("m5", "m15", "h1")

# 🔸 Кеш precision из tickers_bb
class PrecisionCache:
    def __init__(self):
        self.price = {}
        self.qty = {}

    async def get(self, pg_pool, symbol: str):
        if symbol in self.price and symbol in self.qty:
            return self.price[symbol], self.qty[symbol]
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT precision_price, precision_qty FROM tickers_bb WHERE symbol = %s",
                    (symbol,)
                )
                row = await cur.fetchone()
        pp = int(row[0]) if row and row[0] is not None else 0
        pq = int(row[1]) if row and row[1] is not None else 0
        self.price[symbol] = pp
        self.qty[symbol] = pq
        return pp, pq

prec_cache = PrecisionCache()

# 🔸 Утилиты округления/чтения активных
def _round_down(value: float, digits: int) -> float:
    if digits <= 0:
        if digits == 0:
            try:
                return float(Decimal(value).to_integral_value(rounding=ROUND_DOWN))
            except Exception:
                return float(int(value))
        return value
    try:
        return float(Decimal(value).quantize(Decimal(f"1e-{digits}"), rounding=ROUND_DOWN))
    except (InvalidOperation, ValueError):
        return value

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
        log.error(f"Ошибка загрузки активных тикеров: {e}", exc_info=True)
        return []

# 🔸 Безопасная запись одной точки в Redis TS
async def _ts_safe_add(redis, key: str, ts_ms: int, value, labels: dict):
    try:
        try:
            await redis.execute_command("TS.INFO", key)
        except Exception:
            await redis.execute_command(
                "TS.CREATE", key,
                "RETENTION", TS_RETENTION_MS,
                "DUPLICATE_POLICY", "last",
                "LABELS", *sum(([k, str(v)] for k, v in labels.items()), [])
            )
        await redis.execute_command("TS.ADD", key, ts_ms, value)
    except Exception as e:
        log.warning(f"TS.ADD ошибка {key}: {e}")

# 🔸 Парсинг Bybit kline
def _parse_bybit_kline(msg: dict):
    topic = msg.get("topic") or ""
    if not topic.startswith("kline."):
        return []
    parts = topic.split(".")
    if len(parts) != 3:
        return []
    iv_bybit, symbol = parts[1], parts[2]
    iv_map = {"5": "m5", "15": "m15", "60": "h1"}
    interval_m = iv_map.get(iv_bybit)
    if not interval_m:
        return []
    data = msg.get("data") or []
    out = []
    for item in data:
        start = item.get("start")
        if start is None:
            continue
        try:
            o = float(item.get("open")); h = float(item.get("high"))
            l = float(item.get("low"));  c = float(item.get("close"))
            v = float(item.get("volume"))
        except Exception:
            continue
        is_closed = bool(item.get("confirm"))
        out.append((symbol, interval_m, int(start), o, h, l, c, v, is_closed))
    return out

# 🔸 per-symbol WS listener (кладёт kline-элементы в очередь)
async def _listen_symbol_tf(symbol: str, bybit_iv: str, queue: asyncio.Queue):
    url = BYBIT_WS_URL
    topic = f"kline.{bybit_iv}.{symbol}"

    async def keepalive(ws):
        try:
            while True:
                try:
                    await ws.ping()                               # стандартный WS ping
                    await ws.send(json.dumps({"op": "ping"}))     # Bybit ping
                except Exception:
                    return
                await asyncio.sleep(KEEPALIVE_SEC)                # рекомендов. 20 c
        except asyncio.CancelledError:
            return

    backoff = 1.0  # экспоненциальный бэкофф с джиттером

    while True:
        try:
            # мягкий старт, чтобы не открывать десятки коннектов одномоментно
            await asyncio.sleep(random.uniform(0.05, 0.25))

            async with websockets.connect(
                url,
                ping_interval=None,       # свой keepalive
                close_timeout=5,
                max_queue=None,           # не ограничиваем очередь кадров
                open_timeout=10,
            ) as ws:
                # подписка
                await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))

                # мягкий ack: подождать до 5с, но НЕ рвать соединение при таймауте
                try:
                    ack_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    try:
                        ack = json.loads(ack_raw)
                        if isinstance(ack, dict) and ack.get("op") == "subscribe":
                            pass  # ок
                    except Exception:
                        pass
                except asyncio.TimeoutError:
                    pass  # продолжаем без ack

                # успешное подключение → сбрасываем бэкофф и запускаем keepalive
                backoff = 1.0
                ka = asyncio.create_task(keepalive(ws))
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("topic") != topic:
                            continue

                        items = _parse_bybit_kline(msg)  # [(sym, iv_m, ts_ms, o,h,l,c,v,is_closed), ...]
                        for it in items:
                            await queue.put(it)
                finally:
                    ka.cancel()

        except (ConnectionClosedError, asyncio.IncompleteReadError, OSError) as e:
            # ожидаемые сетевые обрывы — плавный реконнект с джиттером
            wait = min(30.0, backoff * (1.5 + random.random() * 0.5))
            log.info(f"[WS {bybit_iv}] {symbol} reconnect in {wait:.1f}s ({type(e).__name__})")
            await asyncio.sleep(wait)
            backoff = wait

        except Exception as e:
            # неожиданные ошибки — короткий бэкофф
            log.error(f"[WS {bybit_iv}] {symbol} error: {e}", exc_info=True)
            await asyncio.sleep(3)
                        
# 🔸 worker: берёт из очереди, пишет TS/Stream (троттлит незакрытые)
async def _kline_worker_tf(queue: asyncio.Queue, pg_pool, redis, tf_name: str, throttle_map: dict):
    while True:
        sym, iv_m, ts_ms, o, h, l, c, v, is_closed = await queue.get()
        try:
            pp, pq = await prec_cache.get(pg_pool, sym)
            o_r = _round_down(o, pp); h_r = _round_down(h, pp)
            l_r = _round_down(l, pp); c_r = _round_down(c, pp)
            v_r = _round_down(v, pq)
            labels = {"symbol": sym, "interval": iv_m}

            # троттлинг незакрытых баров
            if not is_closed:
                key = (sym, iv_m)
                now_s = int(time.monotonic())
                last_s = throttle_map.get(key, 0)
                if now_s - last_s < NONCLOSED_THROTTLE_SEC:
                    continue
                throttle_map[key] = now_s

            # запись в TS (как в v4: один ряд, last-перезапись)
            await asyncio.gather(
                _ts_safe_add(redis, f"bb:ts:{sym}:{iv_m}:o", ts_ms, o_r, {**labels, "field": "o"}),
                _ts_safe_add(redis, f"bb:ts:{sym}:{iv_m}:h", ts_ms, h_r, {**labels, "field": "h"}),
                _ts_safe_add(redis, f"bb:ts:{sym}:{iv_m}:l", ts_ms, l_r, {**labels, "field": "l"}),
                _ts_safe_add(redis, f"bb:ts:{sym}:{iv_m}:c", ts_ms, c_r, {**labels, "field": "c"}),
                _ts_safe_add(redis, f"bb:ts:{sym}:{iv_m}:v", ts_ms, v_r, {**labels, "field": "v"}),
            )

            # закрытый бар → в Stream + Pub/Sub
            if is_closed:
                await redis.xadd("bb:ohlcv_stream", {
                    "symbol": sym, "interval": iv_m, "timestamp": str(ts_ms),
                    "o": str(o_r), "h": str(h_r), "l": str(l_r), "c": str(c_r), "v": str(v_r),
                })
                await redis.publish("bb:ohlcv_channel", json.dumps({
                    "symbol": sym, "interval": iv_m, "timestamp": str(ts_ms)
                }))

        except Exception as e:
            log.warning(f"[{tf_name}] worker err {sym}: {e}", exc_info=True)
        finally:
            queue.task_done()

# 🔸 Менеджер одного таймфрейма: per-symbol WS + пул воркеров
async def _run_tf_manager(pg_pool, redis, interval_m: str, workers_num: int = 6):
    bybit_iv = SUB_IV[interval_m]
    tf_name = interval_m
    log.info(f"[{tf_name}] per-symbol WS mode")

    queue: asyncio.Queue = asyncio.Queue(maxsize=20000)
    throttle_map: dict = {}
    workers = [
        asyncio.create_task(_kline_worker_tf(queue, pg_pool, redis, tf_name, throttle_map))
        for _ in range(workers_num)
    ]

    tasks: dict[str, asyncio.Task] = {}

    while True:
        try:
            active = set(await _load_active_symbols(pg_pool))
            known = set(tasks.keys())

            # старт новых слушателей
            for sym in active - known:
                tasks[sym] = asyncio.create_task(_listen_symbol_tf(sym, bybit_iv, queue))
                log.info(f"[{tf_name}] start WS {sym}")
                # лёгкий stagger, чтобы не лупить десятки подключений в одну миллисекунду
                await asyncio.sleep(0.05)

            # стоп лишних
            for sym in known - active:
                t = tasks.pop(sym, None)
                if t:
                    t.cancel()
                    log.info(f"[{tf_name}] stop WS {sym}")

            await asyncio.sleep(ACTIVE_REFRESH_SEC)
        except Exception as e:
            log.error(f"[{tf_name}] manager err: {e}", exc_info=True)
            await asyncio.sleep(2)

# 🔸 Публичные воркеры (для main)
async def run_feed_and_aggregator_m5_bb(pg_pool, redis):
    await _run_tf_manager(pg_pool, redis, "m5", workers_num=int(os.getenv("BB_M5_WORKERS", "8")))

async def run_feed_and_aggregator_m15_bb(pg_pool, redis):
    await _run_tf_manager(pg_pool, redis, "m15", workers_num=int(os.getenv("BB_M15_WORKERS", "6")))

async def run_feed_and_aggregator_h1_bb(pg_pool, redis):
    await _run_tf_manager(pg_pool, redis, "h1", workers_num=int(os.getenv("BB_H1_WORKERS", "4")))