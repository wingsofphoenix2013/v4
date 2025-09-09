# bb_stream_maintenance.py — регулярная чистка Redis Streams (bb:*)

# 🔸 Импорты и зависимости
import asyncio
import logging

log = logging.getLogger("BB_STREAM_MAINT")

# 🔸 Лимиты хвостов стримов (можешь менять позже через код/ENV)
STREAM_LIMITS = {
    "bb:ohlcv_stream":        20_000,  # закрытые бары
    "bb:pg_candle_inserted":  10_000,  # триггеры аудитора
    "bb:tickers_status_stream": 5_000, # события включения/выключения тикеров (если используешь)
}

INTERVAL_SECONDS = 60  # периодичность чистки

# 🔸 Воркер XTRIM по стримам bb:*
async def run_stream_maintenance_bb(redis):
    log.info("BB_STREAM_MAINT запущен (XTRIM bb:*)")
    while True:
        try:
            for key, maxlen in STREAM_LIMITS.items():
                try:
                    trimmed = await redis.execute_command("XTRIM", key, "MAXLEN", "~", maxlen)
                    log.debug(f"[{key}] XTRIM до ~{maxlen}, удалено ~{trimmed}")
                except Exception as e:
                    log.warning(f"[{key}] XTRIM ошибка: {e}")
            await asyncio.sleep(INTERVAL_SECONDS)
        except Exception as e:
            log.error(f"BB_STREAM_MAINT ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)