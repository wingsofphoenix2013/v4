# bb_core_io.py — чтение bb:ohlcv_stream (idle): логируем новые сообщения, без записи в PG

# 🔸 Импорты и зависимости
import asyncio
import logging

log = logging.getLogger("BB_CORE_IO")

# 🔸 Основной воркер (idle)
async def run_core_io_bb(pg_pool, redis):
    stream_key = "bb:ohlcv_stream"
    last_id = "$"  # читаем только новые записи

    log.info("BB_CORE_IO запущен (idle): слушаю bb:ohlcv_stream")

    while True:
        try:
            resp = await redis.xread({stream_key: last_id}, count=10, block=5000)
            if not resp:
                log.debug("BB_CORE_IO: нет сообщений (idle)")
                continue

            for _stream, messages in resp:
                last_id = messages[-1][0]
                log.info(f"BB_CORE_IO: получено {len(messages)} новых сообщений (idle)")
                # idle-режим: ничего не пишем в PG, просто подтверждаем обработку циклом

        except Exception as e:
            log.error(f"BB_CORE_IO ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)