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

FIELDS = ["o", "h", "l", "c", "v"]


# 🔸 Аудит Redis TS по одному тикеру и таймфрейму
async def audit_ts_for_symbol_tf(symbol: str, tf: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            created_at = infra.enabled_tickers[symbol].get("created_at")
            if not created_at:
                log.warning(f"⏳ [TS] Пропущен тикер {symbol} — отсутствует created_at")
                return

            tf_sec = TF_SECONDS[tf]
            tf_ms = tf_sec * 1000
            now = datetime.utcnow()

            # Вычисляем границы аудита
            to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
            from_time = max(created_at, now - timedelta(days=30))
            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec

            # Ожидаемые временные метки
            expected = {
                from_ts * 1000 + tf_ms * i
                for i in range((to_ts - from_ts) // tf_sec + 1)
            }
            expected_count = len(expected)

            # Проверяем по всем полям
            missing_counts = {}

            for field in FIELDS:
                key = f"ts:{symbol}:{tf}:{field}"
                try:
                    results = await infra.redis_client.ts().range(
                        key,
                        from_ts * 1000,
                        to_ts * 1000
                    )
                    actual = {int(ts) for ts, _ in results}
                    missing = expected - actual
                    missing_counts[field] = len(missing)

                except Exception as e:
                    log.exception(f"❌ [TS] Ошибка чтения {key}: {e}")
                    missing_counts[field] = f"ERR"

            # Лог финального результата по тикеру+таймфрейму
            summary = " ".join(f"{f}={missing_counts[f]}" for f in FIELDS)
            log.info(f"📉 [TS] {symbol} [{tf}] — пропущено: {summary} (из {expected_count} точек)")

        except Exception:
            log.exception(f"❌ [TS] Ошибка при проверке {symbol} [{tf}]")


# 🔸 Запуск Redis TS аудита по всем тикерам и таймфреймам
async def run_audit_all_symbols_ts():
    log.info("🔍 [AUDIT_TS] Запуск аудита Redis TimeSeries")

    semaphore = asyncio.Semaphore(10)
    tasks = []

    for symbol in infra.enabled_tickers:
        for tf in TF_SECONDS:
            tasks.append(audit_ts_for_symbol_tf(symbol, tf, semaphore))

    await asyncio.gather(*tasks)
    log.info("✅ [AUDIT_TS] Аудит Redis TS завершён")