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
            from_time = max(created_at, now - timedelta(days=30))
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
                    key = f"ts:{symbol}:{tf}:{field}"
                    try:
                        await redis.execute_command("TS.ADD", key, ts, values[field])
                        added_counts[field] += 1
                    except Exception as e:
                        log.warning(f"❌ [TS_FIX] Ошибка TS.ADD {key} @ {ts}: {e}")

            summary = " ".join(f"{f}=+{added_counts[f]}" for f in FIELDS)
            log.info(f"🔧 [TS_FIX] {symbol} [{tf}] → {summary}")

        except Exception:
            log.exception(f"❌ [TS_FIX] Ошибка при обработке {symbol} [{tf}]")
