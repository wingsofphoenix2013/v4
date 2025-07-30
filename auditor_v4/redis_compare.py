import logging
from datetime import datetime

from infra import pg_pool, redis_client

log = logging.getLogger("REDIS_COMPARE")

SYMBOL = "ADAUSDT"
INTERVAL = "m5"
TABLE = "ohlcv4_m5"
TF_SECONDS = 300
FIELDS = ["o", "h", "l", "c", "v"]
EPSILON = 1e-8


async def compare_redis_vs_db_once():
    log.info(f"🔍 Сравнение Redis vs БД: {SYMBOL} [{INTERVAL}]")

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT open_time, open, high, low, close, volume
            FROM {TABLE}
            WHERE symbol = $1
            ORDER BY open_time
        """, SYMBOL)

    mismatches = 0
    checked = 0

    for row in rows:
        ts = int(row["open_time"].timestamp() * 1000)
        values_db = {
            "o": float(row["open"]),
            "h": float(row["high"]),
            "l": float(row["low"]),
            "c": float(row["close"]),
            "v": float(row["volume"])
        }

        values_redis = {}
        for field in FIELDS:
            key = f"ts:{SYMBOL}:{INTERVAL}:{field}"
            try:
                res = await redis_client.execute_command("TS.GET", key, "FILTER_BY_TS", ts)
                if res:
                    values_redis[field] = float(res[1])
                else:
                    values_redis[field] = None
            except Exception as e:
                log.warning(f"⚠️ TS.GET ошибка: {key} @ {ts} → {e}")
                values_redis[field] = None

        for field in FIELDS:
            v_db = values_db[field]
            v_r = values_redis[field]
            checked += 1
            if v_r is None or abs(v_db - v_r) > EPSILON:
                mismatches += 1
                dt = row["open_time"].strftime("%Y-%m-%d %H:%M")
                log.warning(f"❌ {SYMBOL} {INTERVAL} @ {dt} → {field}: DB={v_db} / Redis={v_r}")

    log.info(f"✅ Проверено {checked} значений, расхождений: {mismatches}")