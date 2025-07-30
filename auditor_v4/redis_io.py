import asyncio
import logging
from datetime import datetime, timedelta

import infra

log = logging.getLogger("REDIS_IO")

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
    "h1": 3600,
}


# 🔸 Проверка одного тикера и интервала в Redis TimeSeries
async def audit_symbol_interval_ts(symbol: str, tf: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            created_at = infra.enabled_tickers[symbol].get("created_at")
            if not created_at:
                log.warning(f"⏳ [TS] Пропущен тикер {symbol} — отсутствует created_at")
                return

            tf_sec = TF_SECONDS[tf]
            tf_ms = tf_sec * 1000
            now = datetime.utcnow()
            to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
            to_time = datetime.fromtimestamp(to_ts)

            from_time = max(created_at, to_time - timedelta(days=29))  # пока 29 суток
            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec
            from_time_aligned = datetime.fromtimestamp(from_ts)

            expected = {
                from_ts * 1000 + tf_ms * i
                for i in range((to_ts - from_ts) // tf_sec + 1)
            }

            key = f"ts:{symbol}:{tf}:c"
            results = await infra.redis_client.ts().range(
                key,
                from_ts * 1000,
                to_ts * 1000
            )

            actual = {int(ts) for ts, _ in results}
            missing = sorted(expected - actual)

            if missing:
                log.warning(f"📉 [TS] {symbol} [{tf}] — пропущено {len(missing)} точек в Redis TS")
            else:
                log.info(f"✅ [TS] {symbol} [{tf}] — без пропусков в Redis TS")

        except Exception:
            log.exception(f"❌ [TS] Ошибка при аудите Redis TS {symbol} [{tf}]")


# 🔸 Запуск аудита Redis TS по всем тикерам и таймфреймам
async def run_audit_all_symbols_ts():
    log.info("🔍 [AUDIT_TS] Старт аудита Redis TimeSeries")

    semaphore = asyncio.Semaphore(50)
    tasks = []

    for symbol in infra.enabled_tickers:
        for tf in TF_SECONDS:
            tasks.append(audit_symbol_interval_ts(symbol, tf, semaphore))

    await asyncio.gather(*tasks)

    log.info("✅ [AUDIT_TS] Аудит Redis TimeSeries завершён")