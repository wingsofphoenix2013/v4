import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import infra

log = logging.getLogger("OHLCV_AUDITOR")

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
    "h1": 3600,
}

BINANCE_INTERVAL_MAP = {
    "m1": "1m",
    "m5": "5m",
    "m15": "15m",
    "h1": "1h",
}

# 🔸 Возвращает from_time и to_time для данного таймфрейма и тикера
def get_audit_window(tf: str, created_at: datetime) -> tuple[datetime, datetime]:
    now = datetime.utcnow()
    tf_sec = TF_SECONDS[tf]

    to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
    to_time = datetime.fromtimestamp(to_ts)
    from_time = max(created_at, to_time - timedelta(days=29))

    return from_time, to_time


# 🔸 Аудит одного тикера и одного таймфрейма
async def audit_symbol_interval(symbol: str, tf: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            created_at = infra.enabled_tickers[symbol].get("created_at")
            if not created_at:
                log.warning(f"⏳ Пропущен тикер {symbol} — отсутствует created_at")
                return

            tf_sec = TF_SECONDS[tf]
            now = datetime.utcnow()
            to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
            to_time = datetime.fromtimestamp(to_ts)

            table = f"ohlcv4_{tf}"
            query_range = f"""
                SELECT MIN(open_time) AS min_open_time
                FROM {table}
                WHERE symbol = $1 AND open_time <= $2
            """

            async with infra.pg_pool.acquire() as conn:
                row = await conn.fetchrow(query_range, symbol, to_time)
                actual_min_time = row["min_open_time"] or to_time

            from_time = max(created_at, actual_min_time, to_time - timedelta(days=29))

            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec
            from_time_aligned = datetime.fromtimestamp(from_ts)

            expected = set(
                from_time_aligned + timedelta(seconds=tf_sec * i)
                for i in range(int((to_time - from_time_aligned).total_seconds() // tf_sec) + 1)
            )

            query_data = f"""
                SELECT open_time FROM {table}
                WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
            """

            async with infra.pg_pool.acquire() as conn:
                rows = await conn.fetch(query_data, symbol, from_time_aligned, to_time)
                actual = set(row["open_time"] for row in rows)

            missing = sorted(expected - actual)

            if missing:
                log.warning(f"📉 {symbol} [{tf}] — пропущено {len(missing)} свечей")

                async with infra.pg_pool.acquire() as conn:
                    await conn.executemany(
                        """
                        INSERT INTO ohlcv_gaps_v4 (symbol, interval, open_time)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        [(symbol, tf, ts) for ts in missing]
                    )
            else:
                log.info(f"✅ {symbol} [{tf}] — без пропусков")

        except Exception:
            log.exception(f"❌ Ошибка при аудите {symbol} [{tf}]")


# 🔸 Запуск аудита по всем тикерам и интервалам
async def run_audit_all_symbols():
    log.info("🔍 [AUDIT] Старт аудита всех тикеров и таймфреймов")

    semaphore = asyncio.Semaphore(50)
    tasks = []

    for symbol in infra.enabled_tickers:
        for tf in TF_SECONDS:
            tasks.append(audit_symbol_interval(symbol, tf, semaphore))

    await asyncio.gather(*tasks)

    log.info("✅ [AUDIT] Завершён аудит всех тикеров и таймфреймов")


# 🔸 Запрашивает и чинит пропущенные свечи из Binance
async def fix_missing_candles():
    log.info("🔧 [FIXER] Запуск обработки пропущенных свечей")
    url = "https://fapi.binance.com/fapi/v1/klines"

    async with aiohttp.ClientSession() as session:
        async with infra.pg_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT symbol, interval, open_time
                FROM ohlcv_gaps_v4
                WHERE fixed = false
                ORDER BY open_time
                LIMIT 50
            """)

        for row in rows:
            symbol = row["symbol"]
            interval = row["interval"]
            open_time = row["open_time"]
            start_ts = int(open_time.timestamp() * 1000)

            params = {
                "symbol": symbol,
                "interval": BINANCE_INTERVAL_MAP[interval],
                "startTime": start_ts,
                "limit": 1
            }

            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        log.warning(f"❌ Binance API error {resp.status} for {symbol} {interval} {open_time}")
                        continue

                    data = await resp.json()
                    if not data:
                        log.warning(f"⚠️ Нет данных от Binance для {symbol} {interval} {open_time}")
                        continue

                    kline = data[0]
                    table = f"ohlcv4_{interval}"
                    insert_query = f"""
                        INSERT INTO {table} (
                            symbol, open_time, open, high, low, close, volume, source
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'binance')
                        ON CONFLICT (symbol, open_time) DO NOTHING
                    """

                    values = (
                        symbol,
                        datetime.fromtimestamp(kline[0] / 1000),
                        kline[1], kline[2], kline[3], kline[4], kline[5]
                    )

                    async with infra.pg_pool.acquire() as conn:
                        await conn.execute(insert_query, *values)
                        await conn.execute("""
                            UPDATE ohlcv_gaps_v4
                            SET fixed = true, fixed_at = now()
                            WHERE symbol = $1 AND interval = $2 AND open_time = $3
                        """, symbol, interval, open_time)

                    log.info(f"✅ Вставлена свеча {symbol} {interval} {open_time}")

            except Exception:
                log.exception(f"❌ Ошибка обработки {symbol} {interval} {open_time}")