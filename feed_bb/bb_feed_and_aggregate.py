# bb_feed_and_aggregate.py — формирование групп тикеров (dry), без WebSocket

# 🔸 Импорты и зависимости
import os
import asyncio
import logging
from itertools import islice

log = logging.getLogger("BB_FEED_AGGR")
GROUP_SIZE = int(os.getenv("BB_WS_GROUP_SIZE", "1"))

# 🔸 Разбиение на группы
def chunked(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk

# 🔸 Главный воркер (dry)
async def run_feed_and_aggregator_bb(pg_pool, redis):
    log.info(f"BB_FEED_AGGR (dry) запущен, GROUP_SIZE={GROUP_SIZE}")

    while True:
        try:
            async with pg_pool.connection() as conn:
                rows = await conn.execute(
                    "SELECT symbol FROM tickers_bb WHERE status = 'enabled' AND is_active = true ORDER BY symbol"
                )
                # psycopg3 execute() возвращает Cursor; забираем все строки:
                syms = [r[0] for r in await rows.fetchall()] if hasattr(rows, "fetchall") else []
        except Exception as e:
            log.error(f"BB_FEED_AGGR: ошибка загрузки тикеров: {e}", exc_info=True)
            syms = []

        if not syms:
            log.info("BB_FEED_AGGR: активных тикеров нет")
        else:
            groups = list(chunked(syms, GROUP_SIZE))
            log.info(f"BB_FEED_AGGR: активных={len(syms)}, групп={len(groups)} → {groups}")

        await asyncio.sleep(10)