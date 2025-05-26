# indicators_v4_main.py — управляющий модуль расчёта индикаторов v4
import asyncio
import json
import logging
import pandas as pd
from collections import defaultdict
from infra import init_pg_pool, init_redis_client, setup_logging
from core_io import run_core_io

from indicators.compute_and_store import compute_and_store

active_tickers = {}         # symbol -> precision_price
indicator_instances = {}    # instance_id -> dict(indicator, timeframe, stream_publish, params)
required_candles = defaultdict(lambda: 200)  # tf -> сколько свечей загружать

# 🔸 Загрузка тикеров из PostgreSQL при старте
async def load_initial_tickers(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        for row in rows:
            active_tickers[row["symbol"]] = int(row["precision_price"])
            log.debug(f"[DEBUG] Loaded ticker: {row['symbol']} → precision={row['precision_price']}")

# 🔸 Загрузка расчётов индикаторов и параметров
async def load_initial_indicators(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        instances = await conn.fetch("""
            SELECT id, indicator, timeframe, stream_publish
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        for inst in instances:
            params = await conn.fetch("""
                SELECT param, value FROM indicator_parameters_v4
                WHERE instance_id = $1
            """, inst["id"])
            param_map = {p["param"]: p["value"] for p in params}
            indicator_instances[inst["id"]] = {
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "stream_publish": inst["stream_publish"],
                "params": param_map
            }
            log.debug(f"[DEBUG] Loaded instance id={inst['id']} → {inst['indicator']} {param_map}")

# 🔸 Подписка на обновления тикеров
async def watch_ticker_updates(pg, redis):
    log = logging.getLogger("TICKER_UPDATES")
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data["symbol"]
            field = data["type"]
            action = data["action"]

            if field in ("status", "tradepermission"):
                if action == "enabled":
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4
                            WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
                        """, symbol)
                        if row:
                            active_tickers[symbol] = int(row["precision_price"])
                            log.info(f"✅ Тикер включён: {symbol} → precision = {row['precision_price']}")
                else:
                    active_tickers.pop(symbol, None)
                    log.info(f"⛔️ Тикер отключён: {symbol}")
        except Exception as e:
            log.warning(f"Ошибка в ticker event: {e}")

# 🔸 Подписка на обновления расчётов индикаторов
async def watch_indicator_updates(pg, redis):
    log = logging.getLogger("INDICATOR_UPDATES")
    pubsub = redis.pubsub()
    await pubsub.subscribe("indicators_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            iid = int(data["id"])
            field = data["type"]
            action = data["action"]

            if field == "enabled":
                if action == "true":
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT id, indicator, timeframe, stream_publish
                            FROM indicator_instances_v4 WHERE id = $1
                        """, iid)
                        if row:
                            params = await conn.fetch("""
                                SELECT param, value FROM indicator_parameters_v4
                                WHERE instance_id = $1
                            """, iid)
                            param_map = {p["param"]: p["value"] for p in params}
                            indicator_instances[iid] = {
                                "indicator": row["indicator"],
                                "timeframe": row["timeframe"],
                                "stream_publish": row["stream_publish"],
                                "params": param_map
                            }
                            log.info(f"✅ Индикатор включён: id={iid} {row['indicator']} {param_map}")
                else:
                    indicator_instances.pop(iid, None)
                    log.info(f"⛔️ Индикатор отключён: id={iid}")

            elif field == "stream_publish" and iid in indicator_instances:
                indicator_instances[iid]["stream_publish"] = (action == "true")
                log.info(f"🔁 stream_publish обновлён: id={iid} → {action}")
        except Exception as e:
            log.warning(f"Ошибка в indicator event: {e}")
# 🔸 Загрузка свечей через параллельные TS.RANGE по каждому ключу
async def load_ohlcv_from_redis(redis, symbol: str, interval: str, end_ts: int, count: int):
    log = logging.getLogger("REDIS_LOAD")

    step_ms = {
        "m1": 60_000,
        "m5": 300_000,
        "m15": 900_000
    }[interval]
    start_ts = end_ts - (count - 1) * step_ms

    fields = ["o", "h", "l", "c", "v"]
    keys = {field: f"ts:{symbol}:{interval}:{field}" for field in fields}

    log.debug(f"🔍 Запрос TS.RANGE по ключам: {list(keys.values())}, from={start_ts}, to={end_ts}")

    # Параллельная отправка запросов
    tasks = {
        field: redis.execute_command("TS.RANGE", key, start_ts, end_ts)
        for field, key in keys.items()
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for field, result in zip(tasks.keys(), results):
        if isinstance(result, Exception):
            log.warning(f"Ошибка чтения {keys[field]}: {result}")
            continue
        log.debug(f"▶️ {keys[field]} — {len(result)} точек")
        try:
            if result:
                series[field] = {
                    int(ts): float(val)
                    for ts, val in result if val is not None
                }
        except Exception as e:
            log.warning(f"Ошибка при обработке значений {keys[field]}: {e}")

    df = None
    for field, values in series.items():
        s = pd.Series(values)
        s.index = pd.to_datetime(s.index, unit='ms')
        s.name = field
        if df is None:
            df = s.to_frame()
        else:
            df = df.join(s, how="outer")

    if df is None or df.empty:
        log.warning(f"⛔ DataFrame пустой — расчёт пропущен")
        return None

    df.index.name = "open_time"
    df = df.sort_index()

    if "c" in df:
        log.debug(f"Пример значений 'c': {df['c'].dropna().head().tolist()}")

    if len(df) < count:
        log.warning(f"⛔ Недостаточно данных для {symbol}/{interval}: {len(df)} из {count} требуемых — расчёт пропущен")
        return None
    else:
        log.debug(f"🔹 Загружено {len(df)} свечей для {symbol}/{interval} (ожидалось {count})")

    return df 
# 🔸 Подписка на ohlcv_channel — восстановленный расчёт с фильтром по активным индикаторам
async def watch_ohlcv_events(pg, redis):
    log = logging.getLogger("OHLCV_EVENTS")
    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_channel")
    log.info("Подписка на канал ohlcv_channel")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data.get("symbol")
            interval = data.get("interval")
            timestamp = data.get("timestamp")

            if symbol not in active_tickers:
                log.debug(f"Пропущено: неактивный тикер {symbol}")
                continue

            # 🔸 фильтрация по активным индикаторам
            relevant_instances = [
                iid for iid, inst in indicator_instances.items()
                if inst["timeframe"] == interval
            ]
            if not relevant_instances:
                log.debug(f"Пропущено: нет активных индикаторов для {symbol}/{interval}")
                continue

            precision = active_tickers.get(symbol, 8)
            log.debug(f"[TRACE] preparing compute for {symbol} → precision={precision}")

            # 🔸 загрузка свечей
            depth = required_candles.get(interval, 200)
            df = await load_ohlcv_from_redis(redis, symbol, interval, int(timestamp), depth)
            if df is None:
                log.warning(f"Пропуск расчёта: нет данных для {symbol} / {interval}")
                continue

            for iid in relevant_instances:
                inst = indicator_instances[iid]
                await compute_and_store(iid, inst, symbol, df, int(timestamp), pg, redis, precision)

        except Exception as e:
            log.warning(f"Ошибка в ohlcv_channel: {e}")
# 🔸 Точка входа
async def main():
    setup_logging()
    pg = await init_pg_pool()
    redis = await init_redis_client()

    await load_initial_tickers(pg)
    await load_initial_indicators(pg)

    await asyncio.gather(
        watch_ticker_updates(pg, redis),
        watch_indicator_updates(pg, redis),
        watch_ohlcv_events(pg, redis),
        run_core_io(pg, redis) 
    )

if __name__ == "__main__":
    asyncio.run(main())