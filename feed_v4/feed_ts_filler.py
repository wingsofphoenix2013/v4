# feed_ts_filler.py — дозаполнение Redis TS по записям healed_db из ohlcv4_gap

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

log = logging.getLogger("FEED_TS_FILLER")

TABLE_MAP = {
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}

# 🔸 кеш точностей для символов (precision_qty)
class PrecisionCache:
    def __init__(self):
        self.prec_qty = {}

    async def get_precision_qty(self, pg, symbol):
        if symbol in self.prec_qty:
            return self.prec_qty[symbol]
        async with pg.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT precision_qty FROM tickers_v4 WHERE symbol = $1",
                symbol
            )
            pq = row["precision_qty"] if row else 0
            self.prec_qty[symbol] = pq
            return pq

prec_cache = PrecisionCache()

# 🔸 безопасная запись одной точки в TS (создание ключа при отсутствии)
async def ts_safe_add(redis, key, ts_ms, value, labels):
    try:
        try:
            await redis.execute_command("TS.INFO", key)
        except Exception:
            await redis.execute_command(
                "TS.CREATE", key,
                "RETENTION", 5184000000,  # ~60 дней
                "DUPLICATE_POLICY", "last",
                "LABELS", *sum(([k, v] for k, v in labels.items()), [])
            )
        await redis.execute_command("TS.ADD", key, ts_ms, value)
    except Exception as e:
        log.warning(f"TS.ADD ошибка {key}: {e}")

# 🔸 выбрать пачку вылеченных в БД дырок (которые ещё не записаны в TS)
async def fetch_healed_db_gaps(pg, limit=500):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, interval, open_time
            FROM ohlcv4_gap
            WHERE status = 'healed_db'
            ORDER BY detected_at
            LIMIT $1
            """,
            limit
        )
    return rows

# 🔸 получить OHLCV для конкретного набора open_time
async def fetch_ohlcv_rows(pg, table, symbol, open_times):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT symbol, open_time, open, high, low, close, volume
            FROM {table}
            WHERE symbol = $1 AND open_time = ANY($2::timestamp[])
            """,
            symbol, open_times
        )
    return rows

# 🔸 отметить, что для указанных точек TS заполнен
async def mark_gaps_healed_ts(pg, symbol, interval, open_times):
    if not open_times:
        return 0
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE ohlcv4_gap
            SET status = 'healed_ts', healed_ts_at = NOW()
            WHERE symbol = $1 AND interval = $2 AND open_time = ANY($3::timestamp[])
            """,
            symbol, interval, open_times
        )
    return len(open_times)

# 🔸 обработка одной пачки записей для одного (symbol, interval)
async def process_symbol_interval(pg, redis, symbol, interval, times):
    table = TABLE_MAP.get(interval)
    if not table or not times:
        return

    # Получаем свечи из БД
    rows = await fetch_ohlcv_rows(pg, table, symbol, times)
    if not rows:
        # Такое возможно, если healed_db проставили, но строки исчезли — пропустим
        log.warning(f"[{symbol}] [{interval}] Нет строк OHLCV в БД для {len(times)} open_time")
        return

    # precision для округления объёма
    precision_qty = await prec_cache.get_precision_qty(pg, symbol)

    # Пишем в TS: пять рядов (o,h,l,c,v)
    for r in rows:
        ts_ms = int(r["open_time"].timestamp() * 1000)
        o = float(r["open"])
        h = float(r["high"])
        l = float(r["low"])
        c = float(r["close"])
        v = float(Decimal(r["volume"]).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN))

        labels = {"symbol": symbol, "interval": interval}

        await asyncio.gather(
            ts_safe_add(redis, f"ts:{symbol}:{interval}:o", ts_ms, o, {**labels, "field": "o"}),
            ts_safe_add(redis, f"ts:{symbol}:{interval}:h", ts_ms, h, {**labels, "field": "h"}),
            ts_safe_add(redis, f"ts:{symbol}:{interval}:l", ts_ms, l, {**labels, "field": "l"}),
            ts_safe_add(redis, f"ts:{symbol}:{interval}:c", ts_ms, c, {**labels, "field": "c"}),
            ts_safe_add(redis, f"ts:{symbol}:{interval}:v", ts_ms, v, {**labels, "field": "v"}),
        )

    # Помечаем healed_ts
    await mark_gaps_healed_ts(pg, symbol, interval, times)
    log.debug(f"[{symbol}] [{interval}] TS заполнен для {len(times)} точек")

# 🔸 основной воркер: берём healed_db из ohlcv4_gap и заполняем TS
async def run_feed_ts_filler(pg, redis):
    log.debug("TS_FILLER запущен (дозаполнение Redis TS)")
    while True:
        try:
            rows = await fetch_healed_db_gaps(pg, limit=500)
            if not rows:
                await asyncio.sleep(2)
                continue

            # группируем по (symbol, interval) для батчевой записи
            by_pair = {}
            for r in rows:
                key = (r["symbol"], r["interval"])
                by_pair.setdefault(key, []).append(r["open_time"])

            # обрабатываем последовательно пары (можно распараллелить при желании)
            for (symbol, interval), times in by_pair.items():
                await process_symbol_interval(pg, redis, symbol, interval, times)

            await asyncio.sleep(1)

        except Exception as e:
            log.error(f"Ошибка FEED_TS_FILLER: {e}", exc_info=True)
            await asyncio.sleep(2)