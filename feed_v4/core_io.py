# feed_v4_main.py — управляющий модуль системы v4
import asyncio
import logging

from infra import init_pg_pool, init_redis_client, run_safe_loop, setup_logging
from core_io import run_core_io
from markprice_watcher import run_markprice_watcher
from feed_and_aggregate import (
    run_feed_and_aggregator,
    run_feed_and_aggregator_m5,
    run_feed_and_aggregator_m15,
    run_feed_and_aggregator_h1,
    load_all_tickers,
    handle_ticker_events
)

# 🔸 Главная точка запуска
async def main():
    setup_logging()

    pg = await init_pg_pool()
    redis = await init_redis_client()

    tickers, active, activated_at = await load_all_tickers(pg)

    state = {
        "tickers": tickers,
        "active": active,
        "activated_at": activated_at,
        "markprice_tasks": {},
        "kline_tasks": {},
        "m5_tasks": {},
        "m15_tasks": {},
        "h1_tasks": {},
    }

    # 🔸 Независимые очереди для каждого интервала
    refresh_queue_m1 = asyncio.Queue()
    refresh_queue_m5 = asyncio.Queue()
    refresh_queue_m15 = asyncio.Queue()
    refresh_queue_h1 = asyncio.Queue()

    # 🔸 Запуск всех воркеров с защитой
    await asyncio.gather(
        run_safe_loop(lambda: handle_ticker_events(redis, state, pg, refresh_queue_m1, refresh_queue_m5, refresh_queue_m15, refresh_queue_h1), "TICKER_EVENTS"),
        run_safe_loop(lambda: run_feed_and_aggregator(state, redis, pg, refresh_queue_m1), "FEED+AGGREGATOR"),
        run_safe_loop(lambda: run_feed_and_aggregator_m5(state, redis, pg, refresh_queue_m5), "FEED+AGGREGATOR:M5"),
        run_safe_loop(lambda: run_feed_and_aggregator_m15(state, redis, pg, refresh_queue_m15), "FEED+AGGREGATOR:M15"),
        run_safe_loop(lambda: run_feed_and_aggregator_h1(state, redis, pg, refresh_queue_h1), "FEED+AGGREGATOR:H1"),
        run_safe_loop(lambda: run_core_io(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_markprice_watcher(state, redis), "MARKPRICE")
    )

if __name__ == "__main__":
    asyncio.run(main())render@srv-d0lk27je5dus73clm6vg-55fcfb775-9jqhl:~/project/src/feed_v4$ 
render@srv-d0lk27je5dus73clm6vg-55fcfb775-9jqhl:~/project/src/feed_v4$ cat core_io.py
# 🔸 Импорты и зависимости
import asyncio
from decimal import Decimal
from datetime import datetime, timezone
import logging

# 🔸 Сопоставление интервалов с таблицами
TABLE_MAP = {
    "m1": "ohlcv4_m1",
    "m5": "ohlcv4_m5",
    "m15": "ohlcv4_m15",
    "h1": "ohlcv4_h1",
}

# 🔸 Основной воркер: чтение из Redis и запись в PostgreSQL
async def run_core_io(pg, redis):
    log = logging.getLogger("CORE_IO")
    stream_key = "ohlcv_stream"
    last_id = "$"  # можно заменить на "$" для чтения только новых

    semaphore = asyncio.Semaphore(20)  # Ограничение на кол-во одновременных вставок

    async def process_message(data):
        async with semaphore:
            interval = data["interval"]
            table = TABLE_MAP.get(interval)
            if not table:
                return

            symbol = data["symbol"]

            try:
                ts_int = int(data["timestamp"]) // 1000
                open_time = datetime.utcfromtimestamp(ts_int)
            except Exception as e:
                log.warning(f"Ошибка преобразования timestamp: {data.get('timestamp')} → {e}")
                return

            try:
                o = Decimal(data["o"])
                h = Decimal(data["h"])
                l = Decimal(data["l"])
                c = Decimal(data["c"])
                v = Decimal(data["v"])

                async with pg.acquire() as conn:
                    async with conn.transaction():
                        inserted = await conn.execute(f"""
                            INSERT INTO {table} (symbol, open_time, open, high, low, close, volume, source)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, 'stream')
                            ON CONFLICT (symbol, open_time) DO NOTHING
                        """, symbol, open_time, o, h, l, c, v)

                        log.debug(
                            f"Вставлена запись в {table}: {symbol} @ {open_time.isoformat()} "
                            f"[{interval.upper()}] вставлено={datetime.utcnow().isoformat()}"
                        )

                        deleted = await conn.execute(f"""
                            DELETE FROM {table}
                            WHERE open_time < (NOW() - INTERVAL '30 days')
                        """)
                        log.debug(f"Удалено старых записей из {table}: {deleted}")
            except Exception as e:
                log.exception(f"Ошибка вставки в PG для {symbol}: {e}")

    while True:
        try:
            response = await redis.xread({stream_key: last_id}, count=10, block=5000)

            if not response:
                continue  # таймаут

            for stream, messages in response:
                last_id = messages[-1][0]  # Обновить идентификатор
                tasks = [process_message(data) for _, data in messages]
                await asyncio.gather(*tasks)

        except Exception as e:
            # 🔸 Обработка исключений и логирование
            log.error(f"Ошибка: {e}", exc_info=True)
            await asyncio.sleep(2)