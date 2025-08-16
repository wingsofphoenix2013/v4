# feed_stream_maintenance.py — регулярная чистка Redis Streams

import asyncio
import logging

log = logging.getLogger("STREAM_MAINT")

# 🔸 чистим только используемые в модуле стримы + разумные лимиты хвоста
STREAM_LIMITS = {
    "ohlcv_stream": 20_000,         # ~1.5 суток буфера при текущем трафике
    "pg_candle_inserted": 10_000,   # триггеры аудитора
    "tickers_status_stream": 5_000, # события включения/выключения тикеров
}

INTERVAL_SECONDS = 60  # периодичность чистки

async def run_stream_maintenance(redis):
    log.info("STREAM_MAINT запущен (XTRIM по стримам)")
    while True:
        try:
            for key, maxlen in STREAM_LIMITS.items():
                try:
                    # MAXLEN ~ N — приблизительная обрезка (быстрая)
                    trimmed = await redis.execute_command("XTRIM", key, "MAXLEN", "~", maxlen)
                    log.debug(f"[{key}] XTRIM до ~{maxlen}, удалено ~{trimmed}")
                except Exception as e:
                    log.warning(f"[{key}] XTRIM ошибка: {e}")
            await asyncio.sleep(INTERVAL_SECONDS)
        except Exception as e:
            log.error(f"STREAM_MAINT ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)