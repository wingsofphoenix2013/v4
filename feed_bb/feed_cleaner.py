# feed_cleaner.py — триггерная чистка по BTCUSDT h1: XTRIM старше 24ч в Streams + DELETE старых healed_ts в PG

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta

log = logging.getLogger("BB_FEED_CLEANER")

# 🔸 Конфиг/ENV
STREAM_OHLCV = "bb:ohlcv_stream"
STREAM_PG_INS = "bb:pg_candle_inserted"
CLEAN_SYMBOL = os.getenv("BB_CLEANER_SYMBOL", "BTCUSDT")
CLEAN_INTERVAL = os.getenv("BB_CLEANER_INTERVAL", "h1")
GROUP_NAME = os.getenv("BB_CLEANER_GROUP", "bb_cleaner_group")
CONSUMER_NAME = os.getenv("BB_CLEANER_CONSUMER", "bb_cleaner")
BLOCK_MS = int(os.getenv("BB_CLEANER_BLOCK_MS", "5000"))
RETENTION_HOURS = int(os.getenv("BB_CLEANER_RETENTION_HOURS", "24"))

# 🔸 Вспомогательное: получить текущее время Redis (UTC) и порог отсечки (dt, ms)
async def _get_cutoff_utc(redis) -> tuple[datetime, int]:
    # Redis TIME → [seconds, microseconds]
    sec, usec = await redis.execute_command("TIME")
    now_utc = datetime.fromtimestamp(int(sec) + int(usec) / 1_000_000, tz=timezone.utc)
    cutoff_utc = now_utc - timedelta(hours=RETENTION_HOURS)
    cutoff_ms = int(cutoff_utc.timestamp() * 1000)
    return cutoff_utc, cutoff_ms

# 🔸 Трим стримов по порогу MINID
async def _trim_streams(redis, cutoff_ms: int) -> tuple[int, int]:
    cutoff_id = f"{cutoff_ms}-0"
    # приблизительная обрезка для скорости: MINID ~ <cutoff_id>
    try:
        trimmed_ohlcv = await redis.execute_command("XTRIM", STREAM_OHLCV, "MINID", "~", cutoff_id)
    except Exception as e:
        log.warning(f"[CLEAN] XTRIM MINID {STREAM_OHLCV} ошибка: {e}")
        trimmed_ohlcv = 0
    try:
        trimmed_pgins = await redis.execute_command("XTRIM", STREAM_PG_INS, "MINID", "~", cutoff_id)
    except Exception as e:
        log.warning(f"[CLEAN] XTRIM MINID {STREAM_PG_INS} ошибка: {e}")
        trimmed_pgins = 0
    return int(trimmed_ohlcv or 0), int(trimmed_pgins or 0)

# 🔸 Чистка БД: удалить healed_ts старше порога
async def _delete_old_gaps(pg_pool, cutoff_dt_utc: datetime) -> int:
    # удаляем только те, что реально дозалиты в TS (healed_ts_at IS NOT NULL)
    try:
        async with pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM ohlcv_bb_gap
                    WHERE healed_ts_at IS NOT NULL
                      AND healed_ts_at < %s
                    """,
                    (cutoff_dt_utc.replace(tzinfo=None),)  # в БД timestamp без TZ, контракт — UTC-naive
                )
                # cursor.rowcount может быть -1 до завершения; после execute должен быть числом
                deleted = cur.rowcount if cur.rowcount is not None else 0
        return int(deleted or 0)
    except Exception as e:
        log.warning(f"[CLEAN] DELETE ohlcv_bb_gap ошибка: {e}", exc_info=True)
        return 0

# 🔸 Основной воркер: ждёт триггер (BTCUSDT/h1) и выполняет чистку
async def run_feed_cleaner_bb(pg_pool, redis):
    log.info(f"BB_FEED_CLEANER запущен: trigger={CLEAN_SYMBOL}/{CLEAN_INTERVAL}, retention={RETENTION_HOURS}h")

    # создать группу для ohlcv_stream (читать только новые сообщения)
    try:
        await redis.xgroup_create(STREAM_OHLCV, GROUP_NAME, id="$", mkstream=True)
    except Exception:
        # группа могла существовать — это нормально
        pass

    while True:
        try:
            # читаем новые сообщения как отдельный потребитель
            resp = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams={STREAM_OHLCV: ">"},
                count=100,
                block=BLOCK_MS
            )
            if not resp:
                continue

            to_ack = []
            trigger = False

            # перебираем сообщения batched
            for _stream_key, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    # условия достаточности
                    if not isinstance(data, dict):
                        continue
                    sym = data.get("symbol")
                    iv = data.get("interval")
                    if sym == CLEAN_SYMBOL and iv == CLEAN_INTERVAL:
                        trigger = True

            # если триггер пойман — чистим
            if trigger:
                cutoff_dt_utc, cutoff_ms = await _get_cutoff_utc(redis)
                trimmed_ohlcv, trimmed_pgins = await _trim_streams(redis, cutoff_ms)
                deleted_rows = await _delete_old_gaps(pg_pool, cutoff_dt_utc)

                # лог результата
                log.info(
                    "CLEAN OK: trigger=%s/%s at=%s cutoff=%s → XTRIM(%s)=%d, XTRIM(%s)=%d, DELETE(gap)=%d",
                    CLEAN_SYMBOL, CLEAN_INTERVAL,
                    datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                    cutoff_dt_utc.isoformat(),
                    STREAM_OHLCV, trimmed_ohlcv,
                    STREAM_PG_INS, trimmed_pgins,
                    deleted_rows
                )

            # ACK всех прочитанных (чтоб не копить PEL)
            if to_ack:
                try:
                    await redis.xack(STREAM_OHLCV, GROUP_NAME, *to_ack)
                except Exception as e:
                    log.warning(f"[CLEAN] XACK ошибка: {e}")

        except Exception as e:
            log.error(f"BB_FEED_CLEANER ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)