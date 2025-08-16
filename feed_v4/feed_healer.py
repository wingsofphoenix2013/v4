# feed_healer.py — подготовка к лечению пропусков: выбор из БД и группировка в диапазоны

import asyncio
import logging
from datetime import datetime, timedelta

log = logging.getLogger("FEED_HEALER")

# 🔸 соответствие интервалов таблицам и шагам
TABLE_MAP = {
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

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

    # группируем
    by_pair = {}
    for r in rows:
        key = (r["symbol"], r["interval"])
        by_pair.setdefault(key, []).append(r["open_time"])

    # ограничим глубину на пару (чтобы не тащить тысячи за раз)
    for key, lst in by_pair.items():
        by_pair[key] = lst[:limit_per_pair]

    # в диапазоны
    result = []
    for (symbol, interval), times in by_pair.items():
        step_min = STEP_MIN.get(interval)
        if step_min is None:
            continue
        ranges = group_missing_into_ranges(times, step_min)
        result.append((symbol, interval, ranges))
    return result

# 🔸 основной воркер (пока только логика группировки без сетевых запросов/вставок)
async def run_feed_healer(pg, redis):
    log.info("HEALER запущен (этап подготовки: группировка пропусков)")
    while True:
        try:
            groups = await fetch_found_gaps_grouped(pg)
            if not groups:
                await asyncio.sleep(2)
                continue

            for symbol, interval, ranges in groups:
                total_missing = sum(int((b - a).total_seconds() // (STEP_MIN[interval]*60) + 1) for a, b in ranges)
                log.info(f"[{symbol}] [{interval}] найдено пропусков: {total_missing}, диапазонов: {len(ranges)}")
                for a, b in ranges:
                    log.debug(f"[{symbol}] [{interval}] диапазон: {a} .. {b}")

            # пауза перед следующей итерацией
            await asyncio.sleep(2)

        except Exception as e:
            log.error(f"Ошибка FEED_HEALER: {e}", exc_info=True)
            await asyncio.sleep(2)