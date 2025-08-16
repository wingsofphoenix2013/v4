# feed_auditor.py — аудит БД на целостность баров и фиксация пропусков за 12 часов

import asyncio
import logging
from datetime import datetime, timedelta

log = logging.getLogger("FEED_AUDITOR")

# 🔸 Соответствие интервалов таблицам и шагам
TABLE_MAP = {
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}

STEP_MIN = {
    "m5": 5,
    "m15": 15,
    "h1": 60,
}

# 🔸 Выравнивание времени к началу шага (UTC-наивное)
def align_start(ts, step_min):
    ts = ts.replace(second=0, microsecond=0)
    rem = ts.minute % step_min
    if rem:
        ts = ts - timedelta(minutes=rem)
    return ts

# 🔸 Аудит за 12 часов по одному символу/интервалу: записать пропуски в ohlcv4_gap
async def audit_db_12h(pg, symbol, interval, end_ts):
    table = TABLE_MAP.get(interval)
    if not table:
        return

    step_min = STEP_MIN[interval]
    start_ts = align_start(end_ts - timedelta(hours=12), step_min)
    end_ts = end_ts.replace(second=0, microsecond=0)
    step_literal = f"{step_min} minutes"

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            WITH gs AS (
              SELECT generate_series($1::timestamp, $2::timestamp, $3::interval) AS open_time
            )
            SELECT gs.open_time
            FROM gs
            LEFT JOIN {table} t
              ON t.symbol = $4 AND t.open_time = gs.open_time
            WHERE t.open_time IS NULL
            """,
            start_ts, end_ts, step_literal, symbol
        )

        if not rows:
            log.debug(f"[{symbol}] [{interval}] Нет пропусков за окно {start_ts}..{end_ts}")
            return

        await conn.executemany(
            """
            INSERT INTO ohlcv4_gap (symbol, interval, open_time, status)
            VALUES ($1, $2, $3, 'found')
            ON CONFLICT (symbol, interval, open_time) DO NOTHING
            """,
            [(symbol, interval, r["open_time"]) for r in rows]
        )

    log.info(f"[{symbol}] [{interval}] Пропуски зафиксированы: {len(rows)} шт (12ч окно)")

# 🔸 Основной воркер аудитора: слушаем триггеры вставок из PG и запускаем аудит
async def run_feed_auditor(pg, redis):
    group = "auditor_group"
    stream = "pg_candle_inserted"

    try:
        await redis.xgroup_create(stream, group, id='0', mkstream=True)
    except Exception:
        pass

    consumer = "auditor"

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

                        end_ts = datetime.utcfromtimestamp(int(ts_ms) / 1000)
                        await audit_db_12h(pg, symbol, interval, end_ts)

                    except Exception as e:
                        log.warning(f"Ошибка аудита {symbol}/{interval}/{ts_ms}: {e}", exc_info=True)
                    finally:
                        to_ack.append(msg_id)

            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as e:
            log.error(f"Ошибка FEED_AUDITOR: {e}", exc_info=True)
            await asyncio.sleep(2)