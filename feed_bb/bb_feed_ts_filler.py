# bb_feed_ts_filler.py — дозаполнение Redis TS по ohlcv_bb_gap (idle)

# 🔸 Импорты и зависимости
import asyncio
import logging

log = logging.getLogger("BB_TS_FILLER")

# 🔸 Основной воркер (idle)
async def run_feed_ts_filler_bb(pg_pool, redis):
    log.debug("BB_TS_FILLER запущен (idle): слежу за ohlcv_bb_gap")

    while True:
        try:
            async with pg_pool.connection() as conn:
                rows = await conn.execute("SELECT COUNT(*) FROM ohlcv_bb_gap WHERE status = 'healed_db'")
                log.debug(f"BB_TS_FILLER: найдено healed_db={rows}")
        except Exception as e:
            log.error(f"BB_TS_FILLER ошибка: {e}", exc_info=True)

        await asyncio.sleep(10)