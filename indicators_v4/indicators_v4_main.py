# indicators_v4_main.py — управляющий модуль расчёта индикаторов v4 (приоритет m5, worker-пул OHLCV, защита Redis semaphore, понятные логи)

# 🔸 Импорты и зависимости
import os
import asyncio
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

# 🔸 Инфраструктура и воркеры
from infra import init_pg_pool, init_redis_client, setup_logging, run_safe_loop
from indicator_auditor import run_indicator_auditor
from indicator_healer import run_indicator_healer
from indicator_ts_filler import run_indicator_ts_filler
from core_io import run_core_io
from indicators.compute_and_store import compute_indicator_values, append_writes_to_pipeline
from cleanup_worker import run_indicators_cleanup
from indicator_pack import run_indicator_pack
from packs_config.pack_io import run_pack_io
from packs_config.pack_ready import run_pack_ready

# 🔸 Глобальные переменные
active_tickers = {}         # symbol -> precision_price
indicator_instances = {}    # instance_id -> {indicator, timeframe, stream_publish, params, enabled_at}
required_candles = {
    "m5": 800,
    "m15": 800,
    "h1": 800,
}

AUDIT_WINDOW_HOURS = 672

# 🔸 Константы источника данных (Bybit/feed_bb)
BB_TS_PREFIX = "bb:ts"                  # bb:ts:{symbol}:{interval}:{field}
BB_OHLCV_CHANNEL = "bb:ohlcv_channel"   # pub/sub канал закрытых баров
BB_TICKERS_TABLE = "tickers_bb"         # таблица тикеров Bybit
BB_TICKERS_STREAM = "bb:tickers_status_stream"  # stream со статусами тикеров

# 🔸 Таймшаги TF (мс)
TF_STEP_MS = {
    "m5": 300_000,
    "m15": 900_000,
    "h1": 3_600_000,
}

# 🔸 Приоритеты обработки TF (m5 важнее всего)
TF_PRIORITY = {
    "m5": 0,
    "m15": 1,
    "h1": 2,
}

# 🔸 Конфиг воркеров OHLCV (параллельная обработка событий закрытия свечи)
OHLCV_WORKERS = int(os.getenv("IV4_OHLCV_WORKERS", "6"))
OHLCV_QUEUE_MAXSIZE = int(os.getenv("IV4_OHLCV_QUEUE_MAXSIZE", "5000"))
OHLCV_STATS_PERIOD_SEC = int(os.getenv("IV4_OHLCV_STATS_PERIOD_SEC", "60"))

# 🔸 Ограничение конкурентности записи индикаторов в Redis (защита от connect timeouts)
OHLCV_CALC_CONCURRENCY = int(os.getenv("IV4_CALC_CONCURRENCY", "32"))


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

# 🔸 Построение базового имени (base) — как в системе v4
def build_base_name(indicator: str, params: dict) -> str:
    if indicator == "macd":
        return f"{indicator}{params['fast']}"
    if "length" in params:
        return f"{indicator}{params['length']}"
    return indicator


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

# 🔸 Подписка на обновления тикеров (Bybit stream) + периодический рефреш precision из tickers_bb
async def watch_ticker_updates(pg, redis):
    log = logging.getLogger("TICKER_UPDATES")
    stream = BB_TICKERS_STREAM
    group = "iv4_tickers_group"
    consumer = "iv4_tickers_1"

    # параметры периодического рефреша точности
    REFRESH_PERIOD_SEC = 900  # 15 минут
    next_refresh_at = datetime.utcnow()

    # создать consumer-group
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            # читаем события статусов тикеров
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=100, block=2000)

            to_ack = []
            updated = 0
            removed = 0

            if resp:
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

            # итог логирования по событиям стрима
            if updated or removed:
                log.info(f"TICKER_UPDATES: обновлено={updated}, отключено={removed}, активных сейчас={len(active_tickers)}")

            # периодический рефреш точности из БД (даже без событий стрима)
            now = datetime.utcnow()
            if now >= next_refresh_at:
                try:
                    async with pg.acquire() as conn:
                        rows = await conn.fetch(f"""
                            SELECT symbol, precision_price
                            FROM {BB_TICKERS_TABLE}
                            WHERE status = 'enabled' AND tradepermission = 'enabled'
                        """)
                    # соберём множество актуальных символов и применим точности
                    current = set(active_tickers.keys())
                    fresh = set()
                    refresh_updates = 0
                    new_symbols = 0

                    for r in rows:
                        sym = r["symbol"]
                        prec = int(r["precision_price"]) if r["precision_price"] is not None else 8
                        fresh.add(sym)
                        if sym not in active_tickers:
                            new_symbols += 1
                        elif active_tickers[sym] != prec:
                            refresh_updates += 1
                        active_tickers[sym] = prec

                    # удалим те, кто больше не активен в БД
                    removed_by_refresh = 0
                    for sym in list(current - fresh):
                        active_tickers.pop(sym, None)
                        removed_by_refresh += 1

                    # лог результата рефреша
                    log.debug(
                        f"PRECISIONS_REFRESH: updated={refresh_updates}, new={new_symbols}, "
                        f"removed={removed_by_refresh}, active={len(active_tickers)}"
                    )
                except Exception as e:
                    log.warning(f"PRECISIONS_REFRESH error: {e}", exc_info=True)
                finally:
                    next_refresh_at = now + timedelta(seconds=REFRESH_PERIOD_SEC)

        except Exception as e:
            log.error(f"TICKER_UPDATES loop error: {e}", exc_info=True)
            await asyncio.sleep(2)


# 🔸 Подписка на обновления расчётов индикаторов (pub/sub; фикс: не теряем enabled_at)
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
                        # читаем базовые поля инстанса
                        row = await conn.fetchrow("""
                            SELECT id, indicator, timeframe, stream_publish
                            FROM indicator_instances_v4
                            WHERE id = $1
                        """, iid)
                        if not row:
                            continue

                        # страховка: m1 не поддерживаем
                        if row["timeframe"] == "m1":
                            indicator_instances.pop(iid, None)
                            log.info(f"Индикатор m1 проигнорирован: id={iid}")
                            continue

                        # фиксируем момент активации и получаем фактический enabled_at
                        upd = await conn.fetchrow(
                            "UPDATE indicator_instances_v4 SET enabled_at = NOW() WHERE id = $1 RETURNING enabled_at",
                            iid
                        )
                        enabled_at = upd["enabled_at"] if upd else None

                        # параметры инстанса
                        params = await conn.fetch("""
                            SELECT param, value
                            FROM indicator_parameters_v4
                            WHERE instance_id = $1
                        """, iid)
                        param_map = {p["param"]: p["value"] for p in params}

                    # обновляем карту инстансов в памяти
                    indicator_instances[iid] = {
                        "indicator": row["indicator"],
                        "timeframe": row["timeframe"],
                        "stream_publish": row["stream_publish"],
                        "params": param_map,
                        "enabled_at": enabled_at,
                    }
                    log.info(f"Индикатор включён: id={iid} {row['indicator']} {param_map}, enabled_at={enabled_at}")

                else:
                    indicator_instances.pop(iid, None)
                    log.info(f"Индикатор отключён: id={iid}")

            elif field == "stream_publish" and iid in indicator_instances:
                indicator_instances[iid]["stream_publish"] = (action == "true")
                log.info(f"stream_publish обновлён: id={iid} → {action}")

        except Exception as e:
            log.warning(f"Ошибка в indicator event: {e}")


# 🔸 Загрузка свечей из Redis TimeSeries (Bybit TS префикс) — pipeline TS.RANGE
async def load_ohlcv_from_redis(redis, symbol: str, interval: str, end_ts: int, count: int):
    log = logging.getLogger("REDIS_LOAD")

    # страховка от m1 и любых неподдерживаемых ТФ
    if interval == "m1":
        return None

    step_ms = TF_STEP_MS.get(interval)
    if step_ms is None:
        return None

    start_ts = end_ts - (count - 1) * step_ms

    fields = ["o", "h", "l", "c", "v"]
    keys = {field: f"{BB_TS_PREFIX}:{symbol}:{interval}:{field}" for field in fields}

    # pipeline: один execute на все TS.RANGE
    pipe = redis.pipeline(transaction=False)
    for field in fields:
        pipe.execute_command("TS.RANGE", keys[field], start_ts, end_ts)

    results = await pipe.execute(raise_on_error=False)

    series = {}
    for field, result in zip(fields, results):
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


# 🔸 Worker: обработка одного OHLCV-события (загрузка TS → compute_and_store)
async def _ohlcv_event_worker(worker_id: int, queue: asyncio.PriorityQueue, pg, redis, stats: dict, calc_sem: asyncio.Semaphore):
    log = logging.getLogger(f"OHLCV_WORKER:{worker_id}")

    while True:
        prio, close_ts_ms, symbol, interval, ts = await queue.get()
        try:
            # стартовые метрики
            now_ms = int(datetime.utcnow().timestamp() * 1000)
            lag_ms = max(0, now_ms - int(close_ts_ms))

            # базовые фильтры безопасности
            if interval == "m1":
                stats["skipped_m1"] += 1
                continue

            if symbol not in active_tickers:
                stats["skipped_inactive"] += 1
                continue

            relevant_instances = [
                iid for iid, inst in indicator_instances.items()
                if inst["timeframe"] == interval
            ]
            if not relevant_instances:
                stats["skipped_no_instances"] += 1
                continue

            precision = active_tickers.get(symbol, 8)
            depth = required_candles.get(interval, 200)

            # загрузка данных для расчёта
            start_s = asyncio.get_event_loop().time()
            df = await load_ohlcv_from_redis(redis, symbol, interval, int(ts), depth)
            if df is None:
                stats["skipped_no_data"] += 1
                continue

            # подготовка единого pipeline на (symbol, tf, ts) — один execute вместо N
            open_time_iso = datetime.utcfromtimestamp(int(ts) / 1000).isoformat()
            pipe = redis.pipeline(transaction=False)
            kv_map = {}

            total_core = 0
            total_kv = 0
            total_ts = 0
            total_ready = 0
            total_instances = 0

            for iid in relevant_instances:
                inst = indicator_instances[iid]

                # расчёт значений (compute-only)
                values = compute_indicator_values(inst, symbol, df, precision)
                if not values:
                    continue

                base = build_base_name(inst["indicator"], inst["params"])

                # добавление команд записи в общий pipeline (без execute)
                counts = append_writes_to_pipeline(
                    pipe=pipe,
                    kv_map=kv_map,
                    instance_id=iid,
                    instance=inst,
                    symbol=symbol,
                    timeframe=interval,
                    ts=int(ts),
                    open_time_iso=open_time_iso,
                    base=base,
                    values=values,
                    precision=precision,
                )

                total_core += counts["core"]
                total_kv += counts["kv"]
                total_ts += counts["ts"]
                total_ready += counts["ready"]
                total_instances += 1

            # MSET — одной командой в конце
            if kv_map:
                pipe.mset(kv_map)

            # execute под семафором (защита от Redis connect timeouts)
            async with calc_sem:
                pipe_results = await pipe.execute(raise_on_error=False)

            # суммирование ошибок Redis (суммирующий log.info при наличии)
            redis_errors = 0
            for r in pipe_results:
                if isinstance(r, Exception):
                    redis_errors += 1

            if redis_errors:
                stats["errors"] += 1
                log.info(
                    "REDIS_WRITE: errors=%s symbol=%s tf=%s ts=%s instances=%s core=%s kv=%s ts=%s ready=%s",
                    redis_errors,
                    symbol,
                    interval,
                    int(ts),
                    total_instances,
                    total_core,
                    total_kv,
                    total_ts,
                    total_ready,
                )
            # обновление статистики
            proc_ms = (asyncio.get_event_loop().time() - start_s) * 1000.0
            stats["processed"] += 1
            stats[f"processed_{interval}"] += 1
            stats["lag_ms_sum"] += lag_ms
            stats["proc_ms_sum"] += proc_ms
            stats["lag_ms_max_period"] = max(stats["lag_ms_max_period"], lag_ms)
            stats["proc_ms_max_period"] = max(stats["proc_ms_max_period"], proc_ms)
            stats["lag_ms_max_life"] = max(stats["lag_ms_max_life"], lag_ms)
            stats["proc_ms_max_life"] = max(stats["proc_ms_max_life"], proc_ms)

        except Exception as e:
            stats["errors"] += 1
            log.warning(f"worker error: {e}", exc_info=True)
        finally:
            queue.task_done()


# 🔸 Reporter: суммирующий log.info (только когда есть активность) + ready метрики
async def _ohlcv_stats_reporter(queue: asyncio.PriorityQueue, stats: dict):
    log = logging.getLogger("OHLCV_STATS")

    prev = {
        "received": 0,
        "enqueued": 0,
        "queue_blocked": 0,
        "processed": 0,
        "processed_m5": 0,
        "processed_m15": 0,
        "processed_h1": 0,
        "skipped_inactive": 0,
        "skipped_no_instances": 0,
        "skipped_no_data": 0,
        "skipped_m1": 0,
        "errors": 0,
        "lag_ms_sum": 0,
        "proc_ms_sum": 0.0,
    }

    while True:
        await asyncio.sleep(OHLCV_STATS_PERIOD_SEC)

        # снимок
        rec = stats["received"]
        enq = stats["enqueued"]
        qblk = stats["queue_blocked"]
        prc = stats["processed"]
        ski = stats["skipped_inactive"]
        skn = stats["skipped_no_instances"]
        skd = stats["skipped_no_data"]
        skm = stats["skipped_m1"]
        err = stats["errors"]

        lag_sum = stats["lag_ms_sum"]
        proc_sum = stats["proc_ms_sum"]

        # дельты за период
        d_rec = rec - prev["received"]
        d_enq = enq - prev["enqueued"]
        d_qbk = qblk - prev["queue_blocked"]
        d_prc = prc - prev["processed"]
        d_ski = ski - prev["skipped_inactive"]
        d_skn = skn - prev["skipped_no_instances"]
        d_skd = skd - prev["skipped_no_data"]
        d_skm = skm - prev["skipped_m1"]
        d_err = err - prev["errors"]

        d_lag_sum = lag_sum - prev["lag_ms_sum"]
        d_proc_sum = proc_sum - prev["proc_ms_sum"]

        # ничего не произошло за период — не шумим логами
        if (d_rec + d_enq + d_qbk + d_prc + d_ski + d_skn + d_skd + d_skm + d_err) == 0:
            continue

        # средние за период
        lag_avg_s = (d_lag_sum / max(1, d_prc)) / 1000.0
        proc_avg_ms = d_proc_sum / max(1, d_prc)

        # per-tf за период
        d_m5 = stats["processed_m5"] - prev["processed_m5"]
        d_m15 = stats["processed_m15"] - prev["processed_m15"]
        d_h1 = stats["processed_h1"] - prev["processed_h1"]

        # max за период (сбросим после логирования)
        lag_max_period_s = stats["lag_ms_max_period"] / 1000.0
        proc_max_period_ms = stats["proc_ms_max_period"]
        stats["lag_ms_max_period"] = 0
        stats["proc_ms_max_period"] = 0.0

        # готовность индикаторов (простая и понятная метрика)
        ready_avg_s = lag_avg_s + (proc_avg_ms / 1000.0)
        ready_max_s = lag_max_period_s + (proc_max_period_ms / 1000.0)

        log.debug(
            "OHLCV: qsize=%s recv=%s enq=%s blocked=%s proc=%s (m5=%s,m15=%s,h1=%s) "
            "skip[inactive=%s,noinst=%s,nodata=%s,m1=%s] err=%s "
            "lag_avg=%.2fs lag_max=%.2fs proc_avg=%.1fms proc_max=%.1fms "
            "ready_avg=%.2fs ready_max=%.2fs "
            "life[lag_max=%.2fs proc_max=%.1fms]",
            queue.qsize(),
            d_rec,
            d_enq,
            d_qbk,
            d_prc,
            d_m5,
            d_m15,
            d_h1,
            d_ski,
            d_skn,
            d_skd,
            d_skm,
            d_err,
            lag_avg_s,
            lag_max_period_s,
            proc_avg_ms,
            proc_max_period_ms,
            ready_avg_s,
            ready_max_s,
            stats["lag_ms_max_life"] / 1000.0,
            stats["proc_ms_max_life"],
        )

        # обновляем prev
        prev["received"] = rec
        prev["enqueued"] = enq
        prev["queue_blocked"] = qblk
        prev["processed"] = prc
        prev["processed_m5"] = stats["processed_m5"]
        prev["processed_m15"] = stats["processed_m15"]
        prev["processed_h1"] = stats["processed_h1"]
        prev["skipped_inactive"] = ski
        prev["skipped_no_instances"] = skn
        prev["skipped_no_data"] = skd
        prev["skipped_m1"] = skm
        prev["errors"] = err
        prev["lag_ms_sum"] = lag_sum
        prev["proc_ms_sum"] = proc_sum


# 🔸 Обработка событий из канала OHLCV (Bybit pub/sub) — listener + PriorityQueue + worker-пул
async def watch_ohlcv_events(pg, redis):
    log = logging.getLogger("OHLCV_EVENTS")
    pubsub = redis.pubsub()
    await pubsub.subscribe(BB_OHLCV_CHANNEL)

    queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=OHLCV_QUEUE_MAXSIZE)

    # общая статистика (на аптайм воркера)
    stats = {
        "received": 0,
        "enqueued": 0,
        "queue_blocked": 0,
        "processed": 0,
        "processed_m5": 0,
        "processed_m15": 0,
        "processed_h1": 0,
        "skipped_inactive": 0,
        "skipped_no_instances": 0,
        "skipped_no_data": 0,
        "skipped_m1": 0,
        "errors": 0,
        "lag_ms_sum": 0,
        "proc_ms_sum": 0.0,
        "lag_ms_max_period": 0,
        "proc_ms_max_period": 0.0,
        "lag_ms_max_life": 0,
        "proc_ms_max_life": 0.0,
    }

    calc_sem = asyncio.Semaphore(OHLCV_CALC_CONCURRENCY)

    # стартуем воркеры и репортинг
    workers = [
        asyncio.create_task(_ohlcv_event_worker(i + 1, queue, pg, redis, stats, calc_sem))
        for i in range(OHLCV_WORKERS)
    ]
    reporter = asyncio.create_task(_ohlcv_stats_reporter(queue, stats))

    log.info(
        "OHLCV_EVENTS: started (workers=%s, queue_max=%s, calc_concurrency=%s, stats_period=%ss, priority=m5>m15>h1)",
        OHLCV_WORKERS,
        OHLCV_QUEUE_MAXSIZE,
        OHLCV_CALC_CONCURRENCY,
        OHLCV_STATS_PERIOD_SEC,
    )

    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue

            stats["received"] += 1

            try:
                data = json.loads(msg["data"])
                symbol = data.get("symbol")
                interval = data.get("interval")
                timestamp = data.get("timestamp")

                # быстрые фильтры (на стороне listener)
                if not symbol or not interval or timestamp is None:
                    continue

                # страховка: m1 не поддерживаем
                if interval == "m1":
                    stats["skipped_m1"] += 1
                    continue

                if interval not in TF_STEP_MS:
                    continue

                # фильтруем по активным тикерам уже на входе, чтобы не раздувать очередь
                if symbol not in active_tickers:
                    stats["skipped_inactive"] += 1
                    continue

                # если вообще нет инстансов на TF — тоже не кладём
                if not any(inst["timeframe"] == interval for inst in indicator_instances.values()):
                    stats["skipped_no_instances"] += 1
                    continue

                ts = int(timestamp)

                # приоритет и close_ts_ms для сортировки задач
                prio = TF_PRIORITY.get(interval, 9)
                close_ts_ms = ts + TF_STEP_MS.get(interval, 0)

                item = (prio, close_ts_ms, symbol, interval, ts)

                # кладём событие в очередь; закрытые бары не дропаем → при переполнении ждём
                if queue.full():
                    stats["queue_blocked"] += 1
                    await queue.put(item)
                else:
                    queue.put_nowait(item)

                stats["enqueued"] += 1

            except Exception as e:
                stats["errors"] += 1
                log.warning(f"Ошибка в {BB_OHLCV_CHANNEL}: {e}", exc_info=True)

    finally:
        # корректное завершение воркеров и pubsub
        try:
            await pubsub.unsubscribe(BB_OHLCV_CHANNEL)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass

        reporter.cancel()
        for t in workers:
            t.cancel()

        try:
            await reporter
        except Exception:
            pass

        for t in workers:
            try:
                await t
            except Exception:
                pass

        log.info("OHLCV_EVENTS: stopped")


# 🔸 Точка входа
async def main():
    setup_logging()
    pg = await init_pg_pool()
    redis = await init_redis_client()

    await load_initial_tickers(pg)
    await load_initial_indicators(pg)

    await asyncio.gather(
        run_safe_loop(lambda: watch_ticker_updates(pg, redis), "TICKER_UPDATES"),
        run_safe_loop(lambda: watch_indicator_updates(pg, redis), "INDICATOR_UPDATES"),
        run_safe_loop(lambda: watch_ohlcv_events(pg, redis), "OHLCV_EVENTS"),
        run_safe_loop(lambda: run_core_io(pg, redis), "CORE_IO"),
        run_safe_loop(lambda: run_indicator_auditor(pg, redis, window_hours=AUDIT_WINDOW_HOURS), "IND_AUDITOR"),
        run_safe_loop(lambda: run_indicator_healer(pg, redis), "IND_HEALER"),
        run_safe_loop(lambda: run_indicator_ts_filler(pg, redis), "IND_TS_FILLER"),
        run_safe_loop(lambda: run_indicators_cleanup(pg, redis), "IND_CLEANUP"),
        run_safe_loop(lambda: run_indicator_pack(pg, redis), "IND_PACK"),
        run_safe_loop(lambda: run_pack_io(pg, redis), "PACK_IO"),
        run_safe_loop(lambda: run_pack_ready(pg, redis), "PACK_READY"),
    )


if __name__ == "__main__":
    asyncio.run(main())