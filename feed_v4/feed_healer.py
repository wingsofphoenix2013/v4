# feed_healer.py — лечение пропусков: выбор из БД, группировка диапазонов, догрузка с Binance и вставка в PG

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import aiohttp

log = logging.getLogger("FEED_HEALER")

# 🔸 соответствие интервалов таблицам и шагам
TABLE_MAP = {
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
BINANCE_INTERVAL = {"m5": "5m", "m15": "15m", "h1": "1h"}
STEP_MS = {"m5": 5 * 60 * 1000, "m15": 15 * 60 * 1000, "h1": 60 * 60 * 1000}

BINANCE_BASE = "https://fapi.binance.com"  # USDT-M Futures

def align_start(ts, step_min):
    ts = ts.replace(second=0, microsecond=0)
    rem = ts.minute % step_min
    if rem:
        ts = ts - timedelta(minutes=rem)
    return ts

# 🔸 сгруппировать список open_time (отсортированных) в непрерывные диапазоны с шагом step_min
def group_missing_into_ranges(times, step_min):
    if not times:
        return []
    step = timedelta(minutes=step_min)
    ranges = []
    start = prev = times[0]
    for t in times[1:]:
        if t - prev == step:
            prev = t
            continue
        ranges.append((start, prev))
        start = prev = t
    ranges.append((start, prev))
    return ranges

# 🔸 взять «дырки» из ohlcv4_gap по статусу found, сгруппировать по (symbol, interval)
async def fetch_found_gaps_grouped(pg, limit_per_pair=500):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, interval, open_time
            FROM ohlcv4_gap
            WHERE status = 'found'
            ORDER BY symbol, interval, open_time
            """
        )

    by_pair = {}
    for r in rows:
        key = (r["symbol"], r["interval"])
        by_pair.setdefault(key, []).append(r["open_time"])

    for key, lst in by_pair.items():
        by_pair[key] = lst[:limit_per_pair]

    result = []
    for (symbol, interval), times in by_pair.items():
        step_min = STEP_MIN.get(interval)
        if step_min is None:
            continue
        ranges = group_missing_into_ranges(times, step_min)
        result.append((symbol, interval, ranges))
    return result

# 🔸 REST-запрос к Binance за свечами (включительно по времени)
async def fetch_klines(session, symbol, interval, start_ms, end_ms):
    url = f"{BINANCE_BASE}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": BINANCE_INTERVAL[interval],
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1500,
    }
    async with session.get(url, params=params, timeout=15) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data  # список списков

# 🔸 вставка батча свечей в PG
async def insert_klines_pg(pg, table, symbol, rows):
    if not rows:
        return 0
    records = []
    for r in rows:
        # r = [openTime, open, high, low, close, volume, ...]
        open_time = datetime.utcfromtimestamp(int(r[0]) // 1000)
        o = Decimal(r[1])
        h = Decimal(r[2])
        l = Decimal(r[3])
        c = Decimal(r[4])
        v = Decimal(r[5])
        records.append((symbol, open_time, o, h, l, c, v))

    inserted = 0
    async with pg.acquire() as conn:
        async with conn.transaction():
            # чанкуем по 500, чтобы не раздувать запрос
            CHUNK = 500
            for i in range(0, len(records), CHUNK):
                chunk = records[i : i + CHUNK]
                # executemany подходит (у вас уже используется аналогично)
                await conn.executemany(
                    f"""
                    INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'healer')
                    ON CONFLICT (symbol, open_time) DO NOTHING
                    """,
                    chunk,
                )
                inserted += len(chunk)
    return inserted

# 🔸 проверить какие open_time теперь есть в БД (после вставки)
async def select_present_opentimes(pg, table, symbol, times):
    if not times:
        return []
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT open_time
            FROM {table}
            WHERE symbol = $1 AND open_time = ANY($2::timestamp[])
            """,
            symbol,
            times,
        )
    return [r["open_time"] for r in rows]

# 🔸 обновить статусы «вылеченных» дырок
async def mark_gaps_healed_db(pg, symbol, interval, healed_times):
    if not healed_times:
        return 0
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE ohlcv4_gap
            SET status = 'healed_db', healed_db_at = NOW(), error = NULL
            WHERE symbol = $1 AND interval = $2 AND open_time = ANY($3::timestamp[])
            """,
            symbol,
            interval,
            healed_times,
        )
    return len(healed_times)

# 🔸 отметить попытку/ошибку лечения для набора open_time
async def mark_gaps_error(pg, symbol, interval, times, error_msg):
    if not times:
        return
    async with pg.acquire() as conn:
        await conn.execute(
            """
            UPDATE ohlcv4_gap
            SET attempts = attempts + 1, error = $4
            WHERE symbol = $1 AND interval = $2 AND open_time = ANY($3::timestamp[])
            """,
            symbol,
            interval,
            times,
            error_msg[:4000],
        )

# 🔸 Redis-лок на (symbol, interval), чтобы не было параллельного лечения одной пары
async def acquire_lock(redis, symbol, interval, ttl=60):
    key = f"healer:lock:{symbol}:{interval}"
    token = f"{datetime.utcnow().timestamp()}"
    ok = await redis.set(key, token, ex=ttl, nx=True)
    return (key, token) if ok else (None, None)

async def release_lock(redis, key, token):
    if not key:
        return
    try:
        val = await redis.get(key)
        if val == token:
            await redis.delete(key)
    except Exception:
        pass

# 🔸 лечение одного диапазона: тянем у Binance и вставляем в БД, помечаем healed_db
async def heal_range(pg, session, symbol, interval, a, b):
    table = TABLE_MAP[interval]
    step_ms = STEP_MS[interval]
    start_ms = int(a.timestamp() * 1000)
    end_ms = int(b.timestamp() * 1000) + step_ms - 1  # включительно

    # список конкретных open_time, которые должны закрыть дыру
    expected_times = []
    t = a
    step = timedelta(minutes=STEP_MIN[interval])
    while t <= b:
        expected_times.append(t)
        t += step

    expected_set_ms = {int(ts.timestamp() * 1000) for ts in expected_times}

    try:
        kl = await fetch_klines(session, symbol, interval, start_ms, end_ms)
    except Exception as e:
        log.warning(f"[{symbol}] [{interval}] ошибка запроса Binance: {e}")
        await mark_gaps_error(pg, symbol, interval, expected_times, f"fetch error: {e}")
        return

    # фильтруем только нужные openTime
    kl_needed = [r for r in kl if int(r[0]) in expected_set_ms]
    if not kl_needed:
        log.warning(f"[{symbol}] [{interval}] Binance вернул пусто по диапазону {a}..{b}")
        await mark_gaps_error(pg, symbol, interval, expected_times, "empty response for range")
        return

    inserted_try = await insert_klines_pg(pg, table, symbol, kl_needed)

    # проверяем фактическое наличие всех expected_times
    present = await select_present_opentimes(pg, table, symbol, expected_times)
    missing_still = sorted(set(expected_times) - set(present))
    healed = sorted(set(expected_times) & set(present))

    if healed:
        n = await mark_gaps_healed_db(pg, symbol, interval, healed)
        log.debug(f"[{symbol}] [{interval}] диапазон {a}..{b} — вылечено {n}/{len(expected_times)} (вставляли {inserted_try})")

    if missing_still:
        await mark_gaps_error(pg, symbol, interval, missing_still, "partial heal: missing after insert")
        log.warning(f"[{symbol}] [{interval}] диапазон {a}..{b} — не вылечены {len(missing_still)} шт")

# 🔸 основной воркер: находит пропуски, группирует и лечит БД
async def run_feed_healer(pg, redis):
    log.debug("HEALER запущен (лечение пропусков БД)")
    http_timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=http_timeout) as session:
        while True:
            try:
                groups = await fetch_found_gaps_grouped(pg)
                if not groups:
                    await asyncio.sleep(2)
                    continue

                for symbol, interval, ranges in groups:
                    lock_key, token = await acquire_lock(redis, symbol, interval, ttl=60)
                    if not lock_key:
                        continue
                    try:
                        total_missing = sum(
                            int((b - a).total_seconds() // (STEP_MIN[interval] * 60) + 1) for a, b in ranges
                        )
                        log.debug(f"[{symbol}] [{interval}] к лечению: пропусков {total_missing}, диапазонов {len(ranges)}")

                        for a, b in ranges:
                            await heal_range(pg, session, symbol, interval, a, b)

                    finally:
                        await release_lock(redis, lock_key, token)

                await asyncio.sleep(1)

            except Exception as e:
                log.error(f"Ошибка FEED_HEALER: {e}", exc_info=True)
                await asyncio.sleep(2)