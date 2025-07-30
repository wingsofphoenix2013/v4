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


# 🔸 Аудит одного поля Redis TS
async def audit_symbol_field_ts(symbol: str, tf: str, field: str, semaphore: asyncio.Semaphore):
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
            from_time = max(created_at, datetime.fromtimestamp(to_ts - (29 * tf_sec)))
            from_ts = int(from_time.timestamp()) // tf_sec * tf_sec

            expected = {
                from_ts * 1000 + tf_ms * i
                for i in range((to_ts - from_ts) // tf_sec + 1)
            }

            key = f"ts:{symbol}:{tf}:{field}"
            results = await infra.redis_client.ts().range(
                key,
                from_ts * 1000,
                to_ts * 1000
            )

            actual = {int(ts) for ts, _ in results}
            missing = sorted(expected - actual)

            log.info(
                f"[TS] {symbol} [{tf}] → {field}: "
                f"всего ожидается {len(expected)}, найдено {len(actual)}, пропущено {len(missing)}"
            )

            if missing:
                for ts in missing[:5]:  # логируем только первые 5 пропусков
                    dt = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
                    log.warning(f"📉 [TS] {symbol} [{tf}] → {field} отсутствует @ {dt}")

        except Exception:
            log.exception(f"❌ [TS] Ошибка при проверке {symbol} [{tf}] {field}")


# 🔸 Полный аудит Redis TS по всем полям
async def run_audit_all_symbols_ts():
    log.info("🔍 [AUDIT_TS] Запуск аудита Redis TimeSeries")

    semaphore = asyncio.Semaphore(50)
    tasks = []

    for symbol in infra.enabled_tickers:
        for tf in TF_SECONDS:
            for field in FIELDS:
                tasks.append(audit_symbol_field_ts(symbol, tf, field, semaphore))

    await asyncio.gather(*tasks)
    log.info("✅ [AUDIT_TS] Аудит Redis TS завершён")