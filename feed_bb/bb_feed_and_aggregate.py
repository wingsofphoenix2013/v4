# bb_feed_and_aggregate.py — приём kline от Bybit (linear) и запись в Redis TS/Stream для m5/m15/h1

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import websockets

log = logging.getLogger("BB_FEED_AGGR")

# 🔸 Конфиг/ENV
BYBIT_WS_URL = os.getenv("BYBIT_WS_PUBLIC_LINEAR", "wss://stream.bybit.com/v5/public/linear")
KEEPALIVE_SEC = int(os.getenv("BB_WS_KEEPALIVE_SEC", "180"))
REFRESH_ACTIVE_SEC = int(os.getenv("BB_ACTIVE_REFRESH_SEC", "15"))
NONCLOSED_THROTTLE_SEC = int(os.getenv("BB_NONCLOSED_THROTTLE_SEC", "10"))
TS_RETENTION_MS = int(os.getenv("BB_TS_RETENTION_MS", str(60 * 24 * 60 * 60 * 1000)))  # ~60 дней

# 🔸 Маппинг интервалов Bybit ↔ внутренние
INTERVAL_MAP_SUB = {"m5": "5", "m15": "15", "h1": "60"}
INTERVALS = ("m5", "m15", "h1")

# 🔸 Кеш точностей (precision_price/precision_qty) из tickers_bb
class PrecisionCache:
    def __init__(self):
        self.price = {}
        self.qty = {}

    async def get(self, pg_pool, symbol: str):
        if symbol in self.price and symbol in self.qty:
            return self.price[symbol], self.qty[symbol]
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT precision_price, precision_qty FROM tickers_bb WHERE symbol = %s", (symbol,))
                row = await cur.fetchone()
        pp = int(row[0]) if row and row[0] is not None else 0
        pq = int(row[1]) if row and row[1] is not None else 0
        self.price[symbol] = pp
        self.qty[symbol] = pq
        return pp, pq

prec_cache = PrecisionCache()

# 🔸 Служебные утилиты
def _round_down(value: float, digits: int) -> float:
    if digits <= 0:
        # округляем вниз до целого, если digits==0
        if digits == 0:
            return float(int(Decimal(value).to_integral_value(rounding=ROUND_DOWN)))
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
                    "SELECT symbol FROM tickers_bb WHERE status = 'enabled' AND is_active = true ORDER BY symbol"
                )
                rows = await cur.fetchall()
        return [r[0] for r in rows] if rows else []
    except Exception as e:
        log.error(f"Ошибка загрузки активных тикеров: {e}", exc_info=True)
        return []

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

def _parse_bybit_kline(msg: dict):
    # ожидаем topic="kline.{5|15|60}.{SYMBOL}", data=[ {...}, ... ]
    topic = msg.get("topic")
    if not topic or not topic.startswith("kline."):
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
        start = item.get("start")  # ms
        if start is None:
            continue
        try:
            o = float(item.get("open"))
            h = float(item.get("high"))
            l = float(item.get("low"))
            c = float(item.get("close"))
            v = float(item.get("volume"))
        except Exception:
            continue
        is_closed = bool(item.get("confirm"))
        out.append((symbol, interval_m, int(start), o, h, l, c, v, is_closed))
    return out

async def _send_sub(ws, topics):
    if not topics:
        return
    payload = {"op": "subscribe", "args": topics}
    await ws.send(json.dumps(payload))
    log.debug(f"SUB → {len(topics)} топиков")

# 🔸 Главный луп для одного таймфрейма (1 WS-подключение на ТФ, подписка на все активные символы)
async def _run_tf_loop(pg_pool, redis, interval_m: str):
    bybit_iv = INTERVAL_MAP_SUB[interval_m]
    log.debug(f"[{interval_m}] старт воркера (Bybit interval={bybit_iv})")

    current_symbols = set()
    ws = None
    last_nonclosed_emit = {}  # (symbol, interval) -> last_ts_seconds

    async def build_topics(symbols):
        return [f"kline.{bybit_iv}.{s}" for s in symbols]

    async def keepalive():
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
            active = set(await _load_active_symbols(pg_pool))
            if not active:
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    ws = None
                current_symbols.clear()
                log.debug(f"[{interval_m}] активных символов нет")
                await asyncio.sleep(REFRESH_ACTIVE_SEC)
                continue

            need_resub = active != current_symbols or ws is None
            if need_resub:
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    ws = None

                log.debug(f"[{interval_m}] соединяюсь к {BYBIT_WS_URL}")
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=None,
                    close_timeout=5
                ) as _ws:
                    ws = _ws
                    topics = await build_topics(sorted(active))
                    await _send_sub(ws, topics)
                    current_symbols = set(active)

                    ka_task = asyncio.create_task(keepalive())
                    try:
                        last_refresh = asyncio.get_event_loop().time()

                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue

                            # служебные ответы/ack
                            if "op" in msg:
                                log.debug(f"[{interval_m}] ctrl: {msg}")
                                continue

                            # данные по свечам
                            items = _parse_bybit_kline(msg)
                            if not items:
                                continue

                            for (sym, iv_m, ts_ms, o, h, l, c, v, is_closed) in items:
                                try:
                                    pp, pq = await prec_cache.get(pg_pool, sym)
                                    # округление вниз по правилам биржи (если precision=0 → целые/как есть)
                                    o_r = _round_down(o, pp) if pp is not None else o
                                    h_r = _round_down(h, pp) if pp is not None else h
                                    l_r = _round_down(l, pp) if pp is not None else l
                                    c_r = _round_down(c, pp) if pp is not None else c
                                    v_r = _round_down(v, pq) if pq is not None else v

                                    labels = {"symbol": sym, "interval": iv_m}

                                    # троттлинг незакрытых баров (не чаще N сек на символ/интервал)
                                    if not is_closed:
                                        key = (sym, iv_m)
                                        now_s = int(asyncio.get_event_loop().time())
                                        if now_s - last_nonclosed_emit.get(key, 0) < NONCLOSED_THROTTLE_SEC:
                                            continue
                                        last_nonclosed_emit[key] = now_s

                                    # запись в TS
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
                                            "symbol": sym,
                                            "interval": iv_m,
                                            "timestamp": str(ts_ms),
                                            "o": str(o_r),
                                            "h": str(h_r),
                                            "l": str(l_r),
                                            "c": str(c_r),
                                            "v": str(v_r),
                                        })
                                        await redis.publish("bb:ohlcv_channel", json.dumps({
                                            "symbol": sym, "interval": iv_m, "timestamp": str(ts_ms)
                                        }))

                                except Exception as e:
                                    log.warning(f"[{interval_m}] ошибка обработки свечи {sym}: {e}", exc_info=True)

                            # периодическая проверка списка активных
                            now = asyncio.get_event_loop().time()
                            if now - last_refresh >= REFRESH_ACTIVE_SEC:
                                last_refresh = now
                                active2 = set(await _load_active_symbols(pg_pool))
                                if active2 != current_symbols:
                                    log.debug(f"[{interval_m}] изменения активных символов → пересабскрайб")
                                    break  # выходим из цикла чтения → переподписка

                    finally:
                        ka_task.cancel()

        except Exception as e:
            log.error(f"[{interval_m}] ошибка WS: {e}", exc_info=True)
            log.debug(f"[{interval_m}] переподключение через 5 секунд...")
            await asyncio.sleep(5)

# 🔸 Публичные воркеры для main (по одному на каждый ТФ)
async def run_feed_and_aggregator_m5_bb(pg_pool, redis):
    await _run_tf_loop(pg_pool, redis, "m5")

async def run_feed_and_aggregator_m15_bb(pg_pool, redis):
    await _run_tf_loop(pg_pool, redis, "m15")

async def run_feed_and_aggregator_h1_bb(pg_pool, redis):
    await _run_tf_loop(pg_pool, redis, "h1")