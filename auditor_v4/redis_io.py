import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

import infra

log = logging.getLogger("REDIS_IO")

TF_SECONDS = {
    "m1": 60,
    "m5": 300,
    "m15": 900,
    "h1": 3600,
}

FIELDS = ["o", "h", "l", "c", "v"]
RETENTION_MS = 5184000000  # 60 дней


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
            from_time = max(created_at, to_ts - (29 * tf_sec))
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

            if missing:
                log.warning(f"📉 [TS] {symbol} [{tf}] → {field} — пропущено {len(missing)} точек")
            else:
                log.info(f"✅ [TS] {symbol} [{tf}] → {field} — без пропусков")

        except Exception:
            log.exception(f"❌ [TS] Ошибка в {symbol} [{tf}] {field}")


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


# 🔸 Восстановление недостающих точек Redis TS из БД
async def fix_missing_ts_points():
    log.info("🔧 [FIXER_TS] Запуск восстановления Redis TS")

    semaphore = asyncio.Semaphore(50)

    async def process_symbol_tf(symbol: str, tf: str):
        async with semaphore:
            try:
                created_at = infra.enabled_tickers[symbol].get("created_at")
                precision_qty = infra.enabled_tickers[symbol].get("precision_qty", 3)
                if not created_at:
                    log.warning(f"⏳ [FIXER_TS] Пропущен тикер {symbol} — отсутствует created_at")
                    return

                tf_sec = TF_SECONDS[tf]
                tf_ms = tf_sec * 1000
                now = datetime.utcnow()
                to_ts = int(now.timestamp()) // tf_sec * tf_sec - tf_sec
                from_time = max(created_at, to_ts - (29 * tf_sec))
                from_ts = int(from_time.timestamp()) // tf_sec * tf_sec

                expected = {
                    from_ts * 1000 + tf_ms * i
                    for i in range((to_ts - from_ts) // tf_sec + 1)
                }

                # Собираем недостающие timestamps по каждому полю
                missing_by_field = {}
                for field in FIELDS:
                    key = f"ts:{symbol}:{tf}:{field}"
                    results = await infra.redis_client.ts().range(
                        key,
                        from_ts * 1000,
                        to_ts * 1000
                    )
                    actual = {int(ts) for ts, _ in results}
                    missing_by_field[field] = sorted(expected - actual)

                table = f"ohlcv4_{tf}"
                async with infra.pg_pool.acquire() as conn:
                    for ts in sorted(expected):
                        dt = datetime.utcfromtimestamp(ts / 1000)

                        row = await conn.fetchrow(f"""
                            SELECT open, high, low, close, volume
                            FROM {table}
                            WHERE symbol = $1 AND open_time = $2
                        """, symbol, dt)

                        if not row:
                            continue

                        values = {
                            "o": float(row["open"]),
                            "h": float(row["high"]),
                            "l": float(row["low"]),
                            "c": float(row["close"]),
                            "v": float(Decimal(row["volume"]).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN))
                        }

                        for field in FIELDS:
                            if ts not in missing_by_field[field]:
                                continue

                            redis_key = f"ts:{symbol}:{tf}:{field}"
                            try:
                                await infra.redis_client.execute_command("TS.ADD", redis_key, ts, values[field])
                            except Exception as e:
                                log.warning(f"⚠️ TS.ADD {redis_key} @ {dt} → {e}")

                log.info(f"🛠️ [FIXER_TS] Восстановлено: {symbol} [{tf}]")

            except Exception:
                log.exception(f"❌ [FIXER_TS] Ошибка при восстановлении {symbol} [{tf}]")

    tasks = []
    for symbol in infra.enabled_tickers:
        for tf in TF_SECONDS:
            tasks.append(process_symbol_tf(symbol, tf))

    await asyncio.gather(*tasks)
    log.info("✅ [FIXER_TS] Восстановление Redis TS завершено")