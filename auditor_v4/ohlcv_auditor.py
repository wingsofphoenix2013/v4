# ohlcv_auditor.py

import asyncio
import logging
from datetime import datetime, timedelta

import infra

log = logging.getLogger("OHLCV_AUDITOR")

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
    "h1": 3600,
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

            from_time, to_time = get_audit_window(tf, created_at)
            tf_sec = TF_SECONDS[tf]
            expected = set(
                from_time + timedelta(seconds=tf_sec * i)
                for i in range(int((to_time - from_time).total_seconds() // tf_sec) + 1)
            )

            table = f"ohlcv4_{tf}"
            query = f"""
                SELECT open_time FROM {table}
                WHERE symbol = $1 AND open_time BETWEEN $2 AND $3
            """

            async with infra.pg_pool.acquire() as conn:
                rows = await conn.fetch(query, symbol, from_time, to_time)
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