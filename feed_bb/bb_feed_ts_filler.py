# bb_feed_ts_filler.py — дозаполнение Redis TS из ohlcv_bb_* для точек healed_db → healed_ts

# 🔸 Импорты и зависимости
import asyncio
import logging
from decimal import Decimal, ROUND_DOWN
from datetime import datetime

log = logging.getLogger("BB_TS_FILLER")

TABLE_MAP = {"m5": "ohlcv_bb_m5", "m15": "ohlcv_bb_m15", "h1": "ohlcv_bb_h1"}
TS_RETENTION_MS = 60 * 24 * 60 * 60 * 1000  # ~60 дней

# 🔸 безопасная запись одной точки в TS (создать ключ при отсутствии)
async def ts_safe_add(redis, key, ts_ms, value, labels):
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

# 🔸 кеш точности объёма (precision_qty)
class PrecisionCache:
    def __init__(self):
        self.pq = {}

    async def get_precision_qty(self, pg_pool, symbol):
        if symbol in self.pq:
            return self.pq[symbol]
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT precision_qty FROM tickers_bb WHERE symbol=%s", (symbol,))
                row = await cur.fetchone()
        val = int(row[0]) if row and row[0] is not None else 0
        self.pq[symbol] = val
        return val

prec_cache = PrecisionCache()

# 🔸 выбрать пачку healed_db (которые ещё не healed_ts)
async def fetch_healed_db_batch(conn, limit=500):
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT symbol, interval, open_time
            FROM ohlcv_bb_gap
            WHERE status = 'healed_db'
            ORDER BY detected_at
            LIMIT %s
            """,
            (limit,)
        )
        return await cur.fetchall()

# 🔸 получить OHLCV для набора open_time
async def fetch_ohlcv_rows(conn, table, symbol, open_times):
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            SELECT symbol, open_time, open, high, low, close, volume
            FROM {table}
            WHERE symbol = %s AND open_time = ANY(%s)
            """,
            (symbol, open_times)
        )
        return await cur.fetchall()

# 🔸 отметить healed_ts
async def mark_gaps_healed_ts(conn, symbol, interval, open_times):
    if not open_times:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ohlcv_bb_gap
            SET status='healed_ts', healed_ts_at=NOW()
            WHERE symbol=%s AND interval=%s AND open_time = ANY(%s)
            """,
            (symbol, interval, open_times)
        )

# 🔸 обработка одной пары (symbol, interval)
async def process_symbol_interval(pg_pool, redis, symbol, interval, times):
    table = TABLE_MAP.get(interval)
    if not table or not times:
        return
    async with pg_pool.connection() as conn:
        rows = await fetch_ohlcv_rows(conn, table, symbol, times)
    if not rows:
        log.warning(f"[{symbol}] [{interval}] нет строк OHLCV в БД для {len(times)} open_time")
        return

    pq = await prec_cache.get_precision_qty(pg_pool, symbol)

    for r in rows:
        sym, open_time, o, h, l, c, v = r
        ts_ms = int(open_time.timestamp() * 1000)
        try:
            o = float(o); h = float(h); l = float(l); c = float(c)
            v = float(Decimal(v).quantize(Decimal(f"1e-{pq}"), rounding=ROUND_DOWN)) if pq else float(v)
        except Exception:
            continue

        labels = {"symbol": sym, "interval": interval}
        await asyncio.gather(
            ts_safe_add(redis, f"bb:ts:{sym}:{interval}:o", ts_ms, o, {**labels, "field": "o"}),
            ts_safe_add(redis, f"bb:ts:{sym}:{interval}:h", ts_ms, h, {**labels, "field": "h"}),
            ts_safe_add(redis, f"bb:ts:{sym}:{interval}:l", ts_ms, l, {**labels, "field": "l"}),
            ts_safe_add(redis, f"bb:ts:{sym}:{interval}:c", ts_ms, c, {**labels, "field": "c"}),
            ts_safe_add(redis, f"bb:ts:{sym}:{interval}:v", ts_ms, v, {**labels, "field": "v"}),
        )

    async with pg_pool.connection() as conn:
        await mark_gaps_healed_ts(conn, symbol, interval, times)
    log.debug(f"[{symbol}] [{interval}] TS заполнен для {len(times)} точек")

# 🔸 основной воркер
async def run_feed_ts_filler_bb(pg_pool, redis):
    log.debug("BB_TS_FILLER запущен (реально)")
    while True:
        try:
            async with pg_pool.connection() as conn:
                batch = await fetch_healed_db_batch(conn, limit=500)
            if not batch:
                await asyncio.sleep(2)
                continue

            # группируем по (symbol, interval)
            by_pair = {}
            for sym, iv, ot in batch:
                by_pair.setdefault((sym, iv), []).append(ot)

            # последовательно пары, внутренняя запись параллельная
            for (sym, iv), times in by_pair.items():
                await process_symbol_interval(pg_pool, redis, sym, iv, times)

            await asyncio.sleep(1)

        except Exception as e:
            log.error(f"BB_TS_FILLER ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)