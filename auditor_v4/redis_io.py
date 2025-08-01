import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import infra

log = logging.getLogger("REDIS_IO")

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
    "h1": 3600,
}

FIELDS = ["o", "h", "l", "c", "v"]


def clean_decimal(value) -> float:
    return float(Decimal(value).normalize())


# 🔸 Фиксация пропущенных точек в Redis TS из базы данных
async def fix_missing_ts_points():
    log.info("🔧 [TS_FIX] Запуск восстановления Redis TimeSeries")

    semaphore = asyncio.Semaphore(10)
    tasks = []

    for symbol, ticker_data in infra.enabled_tickers.items():
        created_at = ticker_data.get("created_at")
        if not created_at:
            log.warning(f"⏳ [TS_FIX] Пропущен тикер {symbol} — отсутствует created_at")
            continue

        for tf, tf_sec in TF_SECONDS.items():
            tasks.append(process_symbol_tf(symbol, tf, tf_sec, created_at, semaphore))

    await asyncio.gather(*tasks)
    log.info("✅ [TS_FIX] Восстановление Redis TS завершено")
    
async def process_symbol_tf(symbol, tf, tf_sec, created_at, semaphore):
    async with semaphore:
        try:
            tf_ms = tf_sec * 1000
            now = datetime.utcnow()

            to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
            from_time = max(created_at, now - timedelta(hours=24))
            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec

            expected = {
                from_ts * 1000 + tf_ms * i
                for i in range((to_ts - from_ts) // tf_sec + 1)
            }

            # Получаем данные из БД
            table = f"ohlcv4_{tf}"
            async with infra.pg_pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT open_time, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
                    """,
                    symbol,
                    datetime.fromtimestamp(from_ts),
                    datetime.fromtimestamp(to_ts)
                )

            by_time = {
                int(row["open_time"].timestamp() * 1000): row
                for row in rows
            }

            redis = infra.redis_client
            added_counts = {f: 0 for f in FIELDS}

            # Сначала получаем уже существующие метки времени из Redis по каждому полю
            existing = {}
            for field in FIELDS:
                key = f"ts:{symbol}:{tf}:{field}"
                try:
                    results = await redis.execute_command("TS.RANGE", key, from_ts * 1000, to_ts * 1000)
                    existing[field] = {int(ts) for ts, _ in results}
                except Exception as e:
                    log.warning(f"[TS_FIX] Ошибка чтения {key}: {e}")
                    existing[field] = set()

            for ts in expected:
                if ts not in by_time:
                    continue

                row = by_time[ts]

                values = {
                    "o": clean_decimal(row["open"]),
                    "h": clean_decimal(row["high"]),
                    "l": clean_decimal(row["low"]),
                    "c": clean_decimal(row["close"]),
                    "v": clean_decimal(row["volume"]),
                }

                for field in FIELDS:
                    if ts in existing[field]:
                        continue
                    key = f"ts:{symbol}:{tf}:{field}"
                    try:
                        await redis.execute_command("TS.ADD", key, ts, values[field])
                        added_counts[field] += 1
                    except Exception as e:
                        log.warning(f"❌ [TS_FIX] Ошибка TS.ADD {key} @ {ts}: {e}")

            summary = " ".join(f"{f}=+{added_counts[f]}" for f in FIELDS)
            log.debug(f"🔧 [TS_FIX] {symbol} [{tf}] → {summary}")

        except Exception:
            log.exception(f"❌ [TS_FIX] Ошибка при обработке {symbol} [{tf}]")
# 🔸 Сравнение Redis TS и базы за последние 24 часа: подсчёт несовпадений по каждому полю
async def compare_redis_vs_db_once():
    log.info("🔍 [TS_COMPARE] Запуск сравнения Redis TS с PostgreSQL за 24 часа")

    semaphore = asyncio.Semaphore(10)
    tasks = []

    for symbol, ticker_data in infra.enabled_tickers.items():
        created_at = ticker_data.get("created_at")
        if not created_at:
            log.warning(f"⏳ [TS_COMPARE] Пропущен тикер {symbol} — отсутствует created_at")
            continue

        for tf, tf_sec in TF_SECONDS.items():
            tasks.append(compare_symbol_tf(symbol, tf, tf_sec, created_at, semaphore))

    await asyncio.gather(*tasks)

    log.info("✅ [TS_COMPARE] Сравнение Redis TS завершено")
    
async def compare_symbol_tf(symbol, tf, tf_sec, created_at, semaphore):
    async with semaphore:
        try:
            tf_ms = tf_sec * 1000
            now = datetime.utcnow()

            to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
            from_time = max(created_at, now - timedelta(hours=24))
            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec

            expected = {
                from_ts * 1000 + tf_ms * i
                for i in range((to_ts - from_ts) // tf_sec + 1)
            }

            # Данные из базы
            table = f"ohlcv4_{tf}"
            async with infra.pg_pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT open_time, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
                    """,
                    symbol,
                    datetime.fromtimestamp(from_ts),
                    datetime.fromtimestamp(to_ts)
                )

            by_time_db = {
                int(row["open_time"].timestamp() * 1000): {
                    "o": Decimal(row["open"]).normalize(),
                    "h": Decimal(row["high"]).normalize(),
                    "l": Decimal(row["low"]).normalize(),
                    "c": Decimal(row["close"]).normalize(),
                    "v": Decimal(row["volume"]).normalize(),
                }
                for row in rows
            }

            redis = infra.redis_client
            mismatches = {f: 0 for f in FIELDS}

            for field in FIELDS:
                key = f"ts:{symbol}:{tf}:{field}"
                try:
                    results = await redis.execute_command("TS.RANGE", key, from_ts * 1000, to_ts * 1000)
                    redis_data = {
                        int(ts): Decimal(str(value)).normalize()
                        for ts, value in results
                    }
                except Exception as e:
                    log.warning(f"[TS_COMPARE] Ошибка чтения {key}: {e}")
                    continue

                for ts in expected:
                    if ts not in by_time_db or ts not in redis_data:
                        continue

                    db_value = by_time_db[ts][field]
                    redis_value = redis_data[ts]

                    if db_value != redis_value:
                        mismatches[field] += 1

            total = sum(mismatches.values())
            if total > 0:
                summary = " ".join(f"{f}={mismatches[f]}" for f in FIELDS)
                log.warning(f"⚠️ [TS_COMPARE] {symbol} [{tf}] → несовпадений: {summary}")
            else:
                log.info(f"✅ [TS_COMPARE] {symbol} [{tf}] — все значения совпадают")

        except Exception:
            log.exception(f"❌ [TS_COMPARE] Ошибка при сравнении {symbol} [{tf}]")