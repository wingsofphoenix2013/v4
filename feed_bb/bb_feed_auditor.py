# bb_feed_auditor.py — аудит bb:pg_candle_inserted (idle): только слушаем и логируем

# 🔸 Импорты и зависимости
import asyncio
import logging

log = logging.getLogger("BB_FEED_AUDITOR")

# 🔸 Основной воркер (idle)
async def run_feed_auditor_bb(pg_pool, redis):
    group = "bb_auditor_group"
    stream = "bb:pg_candle_inserted"

    try:
        await redis.xgroup_create(stream, group, id='0', mkstream=True)
    except Exception:
        pass

    consumer = "bb_auditor"
    log.info("BB_FEED_AUDITOR запущен (idle): слушаю bb:pg_candle_inserted")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=10, block=5000)
            if not resp:
                log.debug("BB_FEED_AUDITOR: нет сообщений (idle)")
                continue

            for _stream, messages in resp:
                log.info(f"BB_FEED_AUDITOR: получено {len(messages)} сообщений (idle)")
                # idle-режим: ничего не делаем, просто ack
                ids = [msg_id for msg_id, _ in messages]
                if ids:
                    await redis.xack(stream, group, *ids)

        except Exception as e:
            log.error(f"BB_FEED_AUDITOR ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)