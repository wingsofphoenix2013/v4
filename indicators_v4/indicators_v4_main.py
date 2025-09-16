# indicators_v4_main.py — управляющий модуль расчёта индикаторов v4 (переключён на Bybit/feed_bb)

import asyncio
import json
import logging
import pandas as pd
from collections import defaultdict
import math
from datetime import datetime, timedelta

# 🔸 Инфраструктура и воркеры
from infra import init_pg_pool, init_redis_client, setup_logging, run_safe_loop
from indicator_auditor import run_indicator_auditor
from indicator_healer import run_indicator_healer
from indicator_ts_filler import run_indicator_ts_filler
from core_io import run_core_io
from indicators.compute_and_store import compute_and_store, compute_snapshot_values_async
from cleanup_worker import run_indicators_cleanup

# 🔸 Глобальные переменные
active_tickers = {}         # symbol -> precision_price
indicator_instances = {}    # instance_id -> dict(indicator, timeframe, stream_publish, params, enabled_at)
required_candles = {
    "m5": 800,
    "m15": 800,
    "h1": 800,
}
# 🔸 Кэш стратегий (id -> market_watcher)
active_strategies = {}

AUDIT_WINDOW_HOURS = 12

# 🔸 Константы источника данных (Bybit/feed_bb)
BB_TS_PREFIX = "bb:ts"                  # bb:ts:{symbol}:{interval}:{field}
BB_OHLCV_CHANNEL = "bb:ohlcv_channel"   # pub/sub канал закрытых баров
BB_TICKERS_TABLE = "tickers_bb"         # таблица тикеров Bybit
BB_TICKERS_STREAM = "bb:tickers_status_stream"  # stream со статусами тикеров


# 🔸 Вспомогательные геттеры для воркеров
def get_instances_by_tf(tf: str):
    return [
        {
            "id": iid,
            "indicator": inst["indicator"],
            "timeframe": inst["timeframe"],
            "enabled_at": inst.get("enabled_at"),
            "params": inst["params"],
        }
        for iid, inst in indicator_instances.items()
        if inst["timeframe"] == tf
    ]

def get_precision(symbol: str) -> int:
    return active_tickers.get(symbol, 8)

def get_active_symbols():
    return list(active_tickers.keys())

# 🔸 Признак market_watcher по стратегии
def get_strategy_mw(strategy_id: int) -> bool:
    return bool(active_strategies.get(int(strategy_id), False))


# 🔸 Загрузка тикеров из PostgreSQL при старте (Bybit)
async def load_initial_tickers(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol, precision_price
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        for row in rows:
            active_tickers[row["symbol"]] = int(row["precision_price"])
            log.debug(f"Loaded ticker: {row['symbol']} → precision={row['precision_price']}")
    log.info(f"INIT: активных тикеров загружено: {len(active_tickers)}")


# 🔸 Загрузка расчётов индикаторов и параметров
async def load_initial_indicators(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        instances = await conn.fetch("""
            SELECT id, indicator, timeframe, stream_publish, enabled_at
            FROM indicator_instances_v4
            WHERE enabled = true
              AND timeframe IN ('m5','m15','h1')
        """)
        for inst in instances:
            params = await conn.fetch("""
                SELECT param, value
                FROM indicator_parameters_v4
                WHERE instance_id = $1
            """, inst["id"])
            param_map = {p["param"]: p["value"] for p in params}

            indicator_instances[inst["id"]] = {
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "stream_publish": inst["stream_publish"],
                "params": param_map,
                "enabled_at": inst["enabled_at"],
            }
            log.debug(f"Loaded instance id={inst['id']} → {inst['indicator']} {param_map}, enabled_at={inst['enabled_at']}")
    log.info(f"INIT: активных инстансов индикаторов загружено: {len(indicator_instances)}")


# 🔸 Загрузка стратегий (market_watcher) при старте
async def load_initial_strategies(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, market_watcher
            FROM strategies_v4
            WHERE enabled = true AND archived = false
        """)
        for row in rows:
            active_strategies[int(row["id"])] = bool(row["market_watcher"])
            log.debug(f"Loaded strategy: id={row['id']} → market_watcher={row['market_watcher']}")
    log.info(f"INIT: стратегий (enabled & not archived) загружено: {len(active_strategies)}")


# 🔸 Подписка на обновления тикеров (Bybit stream)
async def watch_ticker_updates(pg, redis):
    log = logging.getLogger("TICKER_UPDATES")
    stream = BB_TICKERS_STREAM
    group = "iv4_tickers_group"
    consumer = "iv4_tickers_1"

    # создать consumer-group
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=100, block=2000)
            if not resp:
                continue

            to_ack = []
            updated = 0
            removed = 0

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        status = data.get("status")
                        tradepermission = data.get("tradepermission")
                        if not symbol:
                            continue

                        # условия достаточности
                        if status == "enabled" and tradepermission == "enabled":
                            async with pg.acquire() as conn:
                                row = await conn.fetchrow(f"""
                                    SELECT precision_price
                                    FROM {BB_TICKERS_TABLE}
                                    WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
                                """, symbol)
                            if row:
                                active_tickers[symbol] = int(row["precision_price"])
                                updated += 1
                                log.debug(f"Тикер включён/обновлён: {symbol} → precision = {row['precision_price']}")
                        else:
                            if active_tickers.pop(symbol, None) is not None:
                                removed += 1
                                log.debug(f"Тикер отключён: {symbol}")
                    except Exception as e:
                        log.warning(f"ticker status parse error: {e}")

            if to_ack:
                await redis.xack(stream, group, *to_ack)

            # итог логирования
            if updated or removed:
                log.info(f"TICKER_UPDATES: обновлено={updated}, отключено={removed}, активных сейчас={len(active_tickers)}")

        except Exception as e:
            log.error(f"TICKER_UPDATES loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Подписка на обновления расчётов индикаторов (pub/sub без изменений)
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
                            FROM indicator_instances_v4
                            WHERE id = $1
                        """, iid)
                        if row:
                            # страховка: m1 не поддерживаем
                            if row["timeframe"] == "m1":
                                indicator_instances.pop(iid, None)
                                log.info(f"Индикатор m1 проигнорирован: id={iid}")
                                continue

                            # фиксируем момент активации
                            await conn.execute(
                                "UPDATE indicator_instances_v4 SET enabled_at = NOW() WHERE id = $1",
                                iid
                            )

                            params = await conn.fetch("""
                                SELECT param, value
                                FROM indicator_parameters_v4
                                WHERE instance_id = $1
                            """, iid)
                            param_map = {p["param"]: p["value"] for p in params}

                            indicator_instances[iid] = {
                                "indicator": row["indicator"],
                                "timeframe": row["timeframe"],
                                "stream_publish": row["stream_publish"],
                                "params": param_map,
                                # кладём enabled_at в память для будущего аудита/лечения
                                "enabled_at": None,  # прочитаем фактическое значение на следующем шаге загрузкой
                            }
                            log.info(f"Индикатор включён: id={iid} {row['indicator']} {param_map}")
                else:
                    indicator_instances.pop(iid, None)
                    log.info(f"Индикатор отключён: id={iid}")

            elif field == "stream_publish" and iid in indicator_instances:
                indicator_instances[iid]["stream_publish"] = (action == "true")
                log.info(f"stream_publish обновлён: id={iid} → {action}")

        except Exception as e:
            log.warning(f"Ошибка в indicator event: {e}")


# 🔸 Загрузка свечей из Redis TimeSeries (Bybit TS префикс)
async def load_ohlcv_from_redis(redis, symbol: str, interval: str, end_ts: int, count: int):
    log = logging.getLogger("REDIS_LOAD")

    # страховка от m1 и любых неподдерживаемых ТФ
    if interval == "m1":
        return None

    step_ms_map = {
        "m5": 300_000,
        "m15": 900_000,
        "h1": 3_600_000
    }
    step_ms = step_ms_map.get(interval)
    if step_ms is None:
        return None

    start_ts = end_ts - (count - 1) * step_ms

    fields = ["o", "h", "l", "c", "v"]
    keys = {field: f"{BB_TS_PREFIX}:{symbol}:{interval}:{field}" for field in fields}
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
        df = s.to_frame() if df is None else df.join(s, how="outer")

    if df is None or df.empty or len(df) < count:
        return None

    df.index.name = "open_time"
    df = df.sort_index()
    return df


# 🔸 Обработка событий из канала OHLCV (Bybit pub/sub)
async def watch_ohlcv_events(pg, redis):
    log = logging.getLogger("OHLCV_EVENTS")
    pubsub = redis.pubsub()
    await pubsub.subscribe(BB_OHLCV_CHANNEL)

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data.get("symbol")
            interval = data.get("interval")
            timestamp = data.get("timestamp")

            # страховка: m1 не поддерживаем
            if interval == "m1":
                continue

            if symbol not in active_tickers:
                continue

            relevant_instances = [
                iid for iid, inst in indicator_instances.items()
                if inst["timeframe"] == interval
            ]
            if not relevant_instances:
                continue

            precision = active_tickers.get(symbol, 8)
            depth = required_candles.get(interval, 200)

            ts = int(timestamp)
            df = await load_ohlcv_from_redis(redis, symbol, interval, ts, depth)
            if df is None:
                continue

            await asyncio.gather(*[
                compute_and_store(iid, indicator_instances[iid], symbol, df, ts, pg, redis, precision)
                for iid in relevant_instances
            ])
        except Exception as e:
            log.warning(f"Ошибка в {BB_OHLCV_CHANNEL}: {e}")


# 🔸 On-demand расчёт индикаторов: indicator_request → indicator_response
async def watch_indicator_requests(pg, redis):
    log = logging.getLogger("IND_ONDEMAND")

    step_min = {"m5": 5, "m15": 15, "h1": 60}
    stream = "indicator_request"
    group = "ind_req_group"
    consumer = "ind_req_1"

    # helper: floor к началу бара
    def floor_to_bar(ts_ms: int, tf: str) -> int:
        step = step_min[tf] * 60_000
        return (ts_ms // step) * step

    # создать consumer-group
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=50, block=2000)
            if not resp:
                continue

            to_ack = []
            ok_count = 0
            err_count = 0

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("timeframe") or data.get("interval")
                        iid_raw = data.get("instance_id")
                        ts_raw = data.get("timestamp_ms")

                        if not symbol or interval not in step_min or not iid_raw or not ts_raw:
                            await redis.xadd("indicator_response", {
                                "req_id": msg_id, "status": "error", "error": "bad_request"
                            })
                            err_count += 1
                            continue

                        instance_id = int(iid_raw)
                        ts_ms = int(ts_raw)

                        inst = indicator_instances.get(instance_id)
                        if not inst or inst.get("timeframe") != interval:
                            await redis.xadd("indicator_response", {
                                "req_id": msg_id, "status": "error", "error": "instance_not_active"
                            })
                            err_count += 1
                            continue

                        enabled_at = inst.get("enabled_at")
                        bar_open_ms = floor_to_bar(ts_ms, interval)

                        if enabled_at:
                            enabled_ms = int(enabled_at.replace(tzinfo=None).timestamp() * 1000)
                            if bar_open_ms < enabled_ms:
                                await redis.xadd("indicator_response", {
                                    "req_id": msg_id, "status": "error", "error": "before_enabled_at"
                                })
                                err_count += 1
                                continue

                        precision = active_tickers.get(symbol)
                        if precision is None:
                            await redis.xadd("indicator_response", {
                                "req_id": msg_id, "status": "error", "error": "symbol_not_active"
                            })
                            err_count += 1
                            continue

                        depth = required_candles.get(interval, 800)
                        df = await load_ohlcv_from_redis(redis, symbol, interval, bar_open_ms, depth)
                        if df is None or df.empty:
                            await redis.xadd("indicator_response", {
                                "req_id": msg_id, "status": "error", "error": "no_ohlcv"
                            })
                            err_count += 1
                            continue

                        values = await compute_snapshot_values_async(inst, symbol, df, precision)
                        if not values:
                            await redis.xadd("indicator_response", {
                                "req_id": msg_id, "status": "error", "error": "no_values"
                            })
                            err_count += 1
                            continue

                        await redis.xadd("indicator_response", {
                            "req_id": msg_id,
                            "status": "ok",
                            "symbol": symbol,
                            "timeframe": interval,
                            "instance_id": str(instance_id),
                            "open_time": datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat(),
                            "using_current_bar": "true",
                            "is_final": "false",
                            "results": json.dumps(values),
                        })
                        ok_count += 1

                    except Exception as e:
                        log.warning(f"request error: {e}", exc_info=True)
                        await redis.xadd("indicator_response", {
                            "req_id": msg_id, "status": "error", "error": "exception"
                        })
                        err_count += 1

            if to_ack:
                await redis.xack(stream, group, *to_ack)

            # итог логирования
            if ok_count or err_count:
                log.info(f"IND_ONDEMAND: ok={ok_count}, errors={err_count}")

        except Exception as e:
            logging.getLogger("IND_ONDEMAND").error(f"loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Точка входа
async def main():
    setup_logging()
    pg = await init_pg_pool()
    redis = await init_redis_client()

    await load_initial_tickers(pg)
    await load_initial_indicators(pg)
    await load_initial_strategies(pg)

    # инструкции по включению в общий оркестратор:
    # - этот модуль запускается как часть общего процесса (через run_safe_loop в oracle_v4_main.py / indicators_v4_main.py)
    # - все воркеры ниже самостоятельны и перезапускаются при ошибках
    await asyncio.gather(
        run_safe_loop(lambda: watch_ticker_updates(pg, redis), "TICKER_UPDATES"),
        run_safe_loop(lambda: watch_indicator_updates(pg, redis), "INDICATOR_UPDATES"),
        run_safe_loop(lambda: watch_ohlcv_events(pg, redis), "OHLCV_EVENTS"),
        run_safe_loop(lambda: run_core_io(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_indicator_auditor(pg, redis, window_hours=AUDIT_WINDOW_HOURS), "IND_AUDITOR"),
        run_safe_loop(lambda: run_indicator_healer(pg, redis), "IND_HEALER"),
        run_safe_loop(lambda: run_indicator_ts_filler(pg, redis), "IND_TS_FILLER"),
        run_safe_loop(lambda: watch_indicator_requests(pg, redis), "IND_ONDEMAND"),
        run_safe_loop(lambda: run_indicators_cleanup(pg, redis), "IND_CLEANUP"),
    )


if __name__ == "__main__":
    asyncio.run(main())