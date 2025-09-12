# bb_feed_healer.py — лечение пропусков (Bybit): читаем ohlcv_bb_gap, тянем REST klines и вставляем в PG

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import aiohttp

log = logging.getLogger("BB_FEED_HEALER")

# 🔸 Конфиг
BYBIT_REST_BASE = os.getenv("BYBIT_REST_BASE", "https://api.bybit.com")
CATEGORY = "linear"
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
TABLE_MAP = {"m5": "ohlcv_bb_m5", "m15": "ohlcv_bb_m15", "h1": "ohlcv_bb_h1"}
STEP_MS = {"m5": 5 * 60 * 1000, "m15": 15 * 60 * 1000, "h1": 60 * 60 * 1000}
REST_TIMEOUT = int(os.getenv("BB_HTTP_TIMEOUT_SEC", "15"))
BATCH_LIMIT = int(os.getenv("BB_HEALER_LIMIT", "20"))  # сколько (symbol, interval) обрабатываем за проход

# 🔸 Служебные утилиты (без эмоджи)
def group_missing_into_ranges(times, step_min: int):
    """Группируем последовательные open_time в непрерывные диапазоны [a..b] с шагом step_min."""
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

async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, start_ms: int, end_ms: int):
    """REST Bybit v5 /market/kline — возвращает список списков (open_ts, o,h,l,c,v,turnover, ...)"""
    url = f"{BYBIT_REST_BASE}/v5/market/kline"
    params = {
        "category": CATEGORY,
        "symbol": symbol,
        "interval": {"m5": "5", "m15": "15", "h1": "60"}[interval],
        "start": start_ms,
        "end": end_ms,
        "limit": 1000,
    }
    async with session.get(url, params=params, timeout=REST_TIMEOUT) as resp:
        resp.raise_for_status()
        js = await resp.json()
        if js.get("retCode") != 0:
            raise RuntimeError(f"Bybit error {js.get('retCode')}: {js.get('retMsg')}")
        return (js.get("result") or {}).get("list") or []

# 🔸 Обновления статусов в gap (через UPDATE ... ANY())
async def mark_gaps_healed_db(conn, symbol: str, interval: str, times):
    if not times:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ohlcv_bb_gap
            SET status = 'healed_db', healed_db_at = NOW(), error = NULL
            WHERE symbol = %s AND interval = %s AND open_time = ANY(%s)
            """,
            (symbol, interval, times),
        )

async def mark_gaps_error(conn, symbol: str, interval: str, times, msg: str):
    if not times:
        return
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE ohlcv_bb_gap
            SET attempts = attempts + 1, error = %s
            WHERE symbol = %s AND interval = %s AND open_time = ANY(%s)
            """,
            (msg[:4000], symbol, interval, times),
        )

# 🔸 Вставка свечей в PG (через cursor.executemany)
async def insert_klines_pg(conn, table: str, rows):
    """rows: [(symbol, open_ts_ms, o, h, l, c, v), ...]"""
    if not rows:
        return 0
    async with conn.cursor() as cur:
        await cur.executemany(
            f"""
            INSERT INTO {table}
              (symbol, open_time, open, high, low, close, volume, source)
            VALUES
              (%s, to_timestamp(%s/1000), %s, %s, %s, %s, %s, 'healer')
            ON CONFLICT (symbol, open_time) DO NOTHING
            """,
            rows,
        )
    return len(rows)

# 🔸 Лечение одного диапазона [a..b] (включительно)
async def heal_range(pg_pool, session: aiohttp.ClientSession, symbol: str, interval: str, a: datetime, b: datetime):
    table = TABLE_MAP[interval]
    step_min = STEP_MIN[interval]
    step_ms = STEP_MS[interval]

    # список expected open_time
    expected_times = []
    t = a
    while t <= b:
        expected_times.append(t)
        t += timedelta(minutes=step_min)

    expected_set_ms = {int(ts.timestamp() * 1000) for ts in expected_times}
    start_ms = int(a.timestamp() * 1000)
    end_ms = int(b.timestamp() * 1000) + step_ms - 1  # включительно

    try:
        kl = await fetch_klines(session, symbol, interval, start_ms, end_ms)
    except Exception as e:
        log.warning(f"[{symbol}] [{interval}] REST error: {e}")
        async with pg_pool.connection() as conn:
            await mark_gaps_error(conn, symbol, interval, expected_times, f"fetch error: {e}")
        return

    # фильтруем только нужные open_ts
    to_insert = []
    for r in kl:
        try:
            open_ts = int(r[0])
            if open_ts not in expected_set_ms:
                continue
            o = Decimal(r[1]); h = Decimal(r[2]); l = Decimal(r[3]); c = Decimal(r[4]); v = Decimal(r[5])
            to_insert.append((symbol, open_ts, o, h, l, c, v))
        except Exception:
            continue

    if not to_insert:
        async with pg_pool.connection() as conn:
            await mark_gaps_error(conn, symbol, interval, expected_times, "empty response")
        return

    async with pg_pool.connection() as conn:
        # вставляем
        await insert_klines_pg(conn, table, to_insert)

        # проверяем фактическое наличие всех expected_times
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT open_time FROM {table} WHERE symbol=%s AND open_time = ANY(%s)",
                (symbol, expected_times),
            )
            present_rows = await cur.fetchall()

        present = {r[0] for r in present_rows}
        healed = [t for t in expected_times if t in present]
        missing = [t for t in expected_times if t not in present]

        if healed:
            await mark_gaps_healed_db(conn, symbol, interval, healed)
            log.info(f"[{symbol}] [{interval}] healed {len(healed)}/{len(expected_times)}")

        if missing:
            await mark_gaps_error(conn, symbol, interval, missing, "partial heal")
            log.warning(f"[{symbol}] [{interval}] not healed {len(missing)}")

# 🔸 Основной воркер хилера
async def run_feed_healer_bb(pg_pool, redis):
    log.info("BB_FEED_HEALER запущен (реально)")

    http_timeout = aiohttp.ClientTimeout(total=REST_TIMEOUT + 5)
    async with aiohttp.ClientSession(timeout=http_timeout) as session:
        while True:
            try:
                # берём пачку «дыр» сгруппированных по (symbol, interval)
                async with pg_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            """
                            SELECT symbol, interval, array_agg(open_time ORDER BY open_time)
                            FROM ohlcv_bb_gap
                            WHERE status='found'
                            GROUP BY symbol, interval
                            ORDER BY symbol, interval
                            LIMIT %s
                            """,
                            (BATCH_LIMIT,),
                        )
                        rows = await cur.fetchall()

                if not rows:
                    await asyncio.sleep(5)
                    continue

                for symbol, interval, times in rows:
                    try:
                        ranges = group_missing_into_ranges(times, STEP_MIN[interval])
                        for a, b in ranges:
                            await heal_range(pg_pool, session, symbol, interval, a, b)
                    except Exception as e:
                        log.warning(f"[{symbol}] [{interval}] heal-range error: {e}", exc_info=True)

                await asyncio.sleep(1)

            except Exception as e:
                log.error(f"BB_FEED_HEALER ошибка: {e}", exc_info=True)
                await asyncio.sleep(3)