# bb_feed_healer.py — лечение пропусков (idle): проверяет ohlcv_bb_gap и логирует

# 🔸 Импорты и зависимости
import asyncio
import logging

log = logging.getLogger("BB_FEED_HEALER")

# 🔸 Основной воркер (idle)
async def run_feed_healer_bb(pg_pool, redis):
    log.info("BB_FEED_HEALER запущен (idle): слежу за ohlcv_bb_gap")

    while True:
        try:
            async with pg_pool.connection() as conn:
                rows = await conn.execute("SELECT COUNT(*) FROM ohlcv_bb_gap WHERE status = 'found'")
                log.debug(f"BB_FEED_HEALER: найдено found={rows}")
        except Exception as e:
            log.error(f"BB_FEED_HEALER ошибка: {e}", exc_info=True)

        await asyncio.sleep(10)