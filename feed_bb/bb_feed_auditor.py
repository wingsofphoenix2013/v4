# bb_feed_auditor.py — аудит БД на целостность баров (bb_*), фиксация пропусков за 12 часов с отсечкой по created_at

# 🔸 Импорты и зависимости
import asyncio
import logging
from datetime import datetime, timedelta, timezone

log = logging.getLogger("BB_FEED_AUDITOR")

# 🔸 Соответствие интервалов таблицам и шагам
TABLE_MAP = {
    "m5": "ohlcv_bb_m5",
    "m15": "ohlcv_bb_m15",
    "h1": "ohlcv_bb_h1",
}

STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Выравнивание времени к началу шага (UTC)
def align_start(ts, step_min):
    ts = ts.replace(second=0, microsecond=0, tzinfo=timezone.utc)
    rem = ts.minute % step_min
    if rem:
        ts = ts - timedelta(minutes=rem)
    return ts

# 🔸 Аудит за 12 часов по одному символу/интервалу с отсечкой по created_at
async def audit_db_12h_bb(pg_pool, symbol, interval, end_ts):
    table = TABLE_MAP.get(interval)
    if not table:
        return 0

    step_min = STEP_MIN[interval]
    start_ts = align_start(end_ts - timedelta(hours=12), step_min)
    end_ts = align_start(end_ts, step_min)
    step_delta = timedelta(minutes=step_min)

    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            # created_at для отсечки «не бежим назад»
            await cur.execute("SELECT created_at FROM tickers_bb WHERE symbol = %s", (symbol,))
            row = await cur.fetchone()
            created_at = row[0] if row else None
            if created_at:
                created_at = align_start(created_at, step_min)
                if created_at > start_ts:
                    start_ts = created_at

            # если отсечка обгоняет конец — аудита нет
            if start_ts > end_ts:
                return 0

            # генерируем сетку и ищем пропуски
            await cur.execute(
                f"""
                WITH gs AS (
                  SELECT generate_series(%s::timestamp, %s::timestamp, %s) AS open_time
                )
                SELECT gs.open_time
                FROM gs
                LEFT JOIN {table} t
                  ON t.symbol = %s AND t.open_time = gs.open_time
                WHERE t.open_time IS NULL
                """,
                (start_ts, end_ts, step_delta, symbol)
            )
            missing_rows = await cur.fetchall()
            missing = [r[0] for r in missing_rows] if missing_rows else []

            if missing:
                # вставляем found (игнорим конфликты)
                vals = [(symbol, interval, ts) for ts in missing]
                await cur.executemany(
                    """
                    INSERT INTO ohlcv_bb_gap (symbol, interval, open_time, status)
                    VALUES (%s, %s, %s, 'found')
                    ON CONFLICT (symbol, interval, open_time) DO NOTHING
                    """,
                    vals
                )

    return len(missing)

# 🔸 Основной воркер аудитора
async def run_feed_auditor_bb(pg_pool, redis):
    group = "bb_auditor_group"
    stream = "bb:pg_candle_inserted"

    try:
        await redis.xgroup_create(stream, group, id='0', mkstream=True)
    except Exception:
        pass

    consumer = "bb_auditor"
    log.debug("BB_FEED_AUDITOR запущен: аудит 12ч по событиям вставки в PG (с отсечкой created_at)")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=50, block=2000)
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    symbol = data.get("symbol")
                    interval = data.get("interval")
                    ts_ms = data.get("timestamp")

                    try:
                        if not symbol or not interval or not ts_ms:
                            to_ack.append(msg_id)
                            continue

                        end_ts = datetime.utcfromtimestamp(int(ts_ms) / 1000).replace(tzinfo=timezone.utc)
                        log.debug(f"BB_AUDIT: {symbol} [{interval}] @ {end_ts.isoformat()}")

                        missing_count = await audit_db_12h_bb(pg_pool, symbol, interval, end_ts)
                        log.debug(f"BB_AUDIT done: {symbol} [{interval}] — пропусков {missing_count}")

                    except Exception as e:
                        log.warning(f"BB_AUDIT ошибка {symbol}/{interval}/{ts_ms}: {e}", exc_info=True)
                    finally:
                        to_ack.append(msg_id)

            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as e:
            log.error(f"BB_FEED_AUDITOR ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)