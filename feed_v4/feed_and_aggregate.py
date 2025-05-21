# feed_and_aggregate.py — приём и агрегация рыночных данных - 20/05/2025

import logging
import asyncio
import websockets
import json
import aiohttp
from infra import info_log
from decimal import Decimal, ROUND_DOWN
import time
from datetime import datetime, timedelta

# 🔸 Загрузка всех тикеров, точности и статуса из PostgreSQL
async def load_all_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price, status FROM tickers_v4
        """)
        tickers = {}
        active = set()
        for row in rows:
            tickers[row['symbol']] = row['precision_price']
            if row['status'] == 'enabled':
                active.add(row['symbol'].lower())
        return tickers, active
# 🔸 Обработка событий включения/отключения тикеров через Redis Stream
async def handle_ticker_events(redis, state, pg, refresh_queue):
    group = "aggregator_group"
    stream = "tickers_status_stream"
    logger = logging.getLogger("TICKER_STREAM")

    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        pass  # группа уже существует

    while True:
        resp = await redis.xreadgroup(group, "aggregator", streams={stream: ">"}, count=50, block=1000)
        for _, messages in resp:
            for msg_id, data in messages:
                symbol = data.get("symbol")
                action = data.get("action")
                if not symbol or not action:
                    continue

                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow("""
                            SELECT precision_price FROM tickers_v4 WHERE symbol = $1
                        """, symbol)
                        if row:
                            state["tickers"][symbol] = row["precision_price"]
                            info_log("TICKER_STREAM", f"Добавлен тикер из БД: {symbol}")

                if action == "enabled" and symbol in state["tickers"]:
                    logger.info(f"Активирован тикер: {symbol}")
                    state["active"].add(symbol.lower())
                    await refresh_queue.put("refresh")

                    # запуск потока markPrice и отслеживание задачи
                    precision = state["tickers"][symbol]
                    task = asyncio.create_task(watch_mark_price(symbol, redis, precision))
                    state["markprice_tasks"][symbol] = task

                elif action == "disabled" and symbol in state["tickers"]:
                    logger.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())
                    await refresh_queue.put("refresh")

                    # отмена потока markPrice, если он есть
                    task = state["markprice_tasks"].pop(symbol, None)
                    if task:
                        task.cancel()

                await redis.xack(stream, group, msg_id)

# 🔸 Сохранение полной свечи M1 в RedisJSON + Stream + Pub/Sub
async def store_and_publish_m1(redis, symbol, open_time, kline, precision):

    logger = logging.getLogger("KLINE")
    timestamp = int(open_time.timestamp() * 1000)
    json_key = f"ohlcv:{symbol.lower()}:m1:{timestamp}"

    def r(val):
        return float(Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

    candle = {
        "o": r(kline["o"]),
        "h": r(kline["h"]),
        "l": r(kline["l"]),
        "c": r(kline["c"]),
        "v": r(kline["v"]),
        "ts": timestamp
    }

    # Сохраняем свечу в Redis JSON
    await redis.execute_command("JSON.SET", json_key, "$", json.dumps(candle))

    info_log("KLINE", f"[{symbol}] M1 сохранена и опубликована: {open_time} → C={candle['c']}")

    # Формируем событие
    event = {
        "symbol": symbol,
        "interval": "m1",
        "timestamp": str(timestamp)
    }

    # Публикация в Redis Stream (для core_io.py)
    await redis.xadd("ohlcv_stream", event)

    # Публикация в Redis Pub/Sub (для realtime-подписчиков)
    await redis.publish("ohlcv_channel", json.dumps(event))

    # Агрегация M5 и M15
    await try_aggregate_m5(redis, symbol, open_time)
    await try_aggregate_m15(redis, symbol, open_time)
# 🔸 Агрегация M5 на основе RedisJSON M1-свечей (только при minute % 5 == 4)
async def try_aggregate_m5(redis, symbol, open_time):
    import logging
    import json

    logger = logging.getLogger("KLINE")

    if open_time.minute % 5 != 4:
        return

    end_ts = int(open_time.timestamp() * 1000)
    ts_list = [end_ts - 60_000 * i for i in reversed(range(5))]
    candles = []

    for ts in ts_list:
        key = f"ohlcv:{symbol.lower()}:m1:{ts}"
        try:
            data = await redis.execute_command("JSON.GET", key, "$")
            if not data:
                logger.warning(f"[{symbol}] M5: пропущена свеча {ts}")
                return
            parsed = json.loads(data)[0]
            candles.append(parsed)
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка чтения JSON для M5: {e}")
            return

    o = candles[0]["o"]
    h = max(c["h"] for c in candles)
    l = min(c["l"] for c in candles)
    c = candles[-1]["c"]
    v = sum(c["v"] for c in candles)
    m5_ts = ts_list[0]

    key = f"ohlcv:{symbol.lower()}:m5:{m5_ts}"
    candle = {
        "o": o,
        "h": h,
        "l": l,
        "c": c,
        "v": v,
        "ts": m5_ts
    }

    exists = await redis.exists(key)
    if exists:
        return

    await redis.execute_command("JSON.SET", key, "$", json.dumps(candle))
    info_log("KLINE", f"[{symbol}] Построена M5: {open_time.replace(second=0)} → O:{o} H:{h} L:{l} C:{c}")

    # 📤 Redis Stream (для core_io.py)
    await redis.xadd("ohlcv_stream", {
        "symbol": symbol,
        "interval": "m5",
        "timestamp": str(m5_ts)
    })

    # 📢 Redis Pub/Sub (для realtime подписчиков)
    await redis.publish("ohlcv_channel", json.dumps({
        "symbol": symbol,
        "interval": "m5",
        "timestamp": m5_ts
    }))
# 🔸 Агрегация M15 на основе RedisJSON M1-свечей (только при minute % 15 == 14)
async def try_aggregate_m15(redis, symbol, open_time):
    import logging
    import json

    logger = logging.getLogger("KLINE")

    if open_time.minute % 15 != 14:
        return

    end_ts = int(open_time.timestamp() * 1000)
    ts_list = [end_ts - 60_000 * i for i in reversed(range(15))]
    candles = []

    for ts in ts_list:
        key = f"ohlcv:{symbol.lower()}:m1:{ts}"
        try:
            data = await redis.execute_command("JSON.GET", key, "$")
            if not data:
                logger.warning(f"[{symbol}] M15: пропущена свеча {ts}")
                return
            parsed = json.loads(data)[0]
            candles.append(parsed)
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка чтения JSON для M15: {e}")
            return

    o = candles[0]["o"]
    h = max(c["h"] for c in candles)
    l = min(c["l"] for c in candles)
    c = candles[-1]["c"]
    v = sum(c["v"] for c in candles)
    m15_ts = ts_list[0]

    key = f"ohlcv:{symbol.lower()}:m15:{m15_ts}"
    candle = {
        "o": o,
        "h": h,
        "l": l,
        "c": c,
        "v": v,
        "ts": m15_ts
    }

    exists = await redis.exists(key)
    if exists:
        return

    await redis.execute_command("JSON.SET", key, "$", json.dumps(candle))
    info_log("KLINE", f"[{symbol}] Построена M15: {open_time.replace(second=0)} → O:{o} H:{h} L:{l} C:{c}")

    # 📤 Redis Stream (для core_io.py)
    await redis.xadd("ohlcv_stream", {
        "symbol": symbol,
        "interval": "m15",
        "timestamp": str(m15_ts)
    })

    # 📢 Redis Pub/Sub (для realtime подписчиков)
    await redis.publish("ohlcv_channel", json.dumps({
        "symbol": symbol,
        "interval": "m15",
        "timestamp": m15_ts
    }))
# 🔸 Поиск пропущенных M1 и запись в missing_m1_log_v4 + system_log_v4
async def detect_missing_m1(redis, pg, symbol, now_ts):
    logger = logging.getLogger("RECOVERY")
    missing = []

    for i in range(1, 5):  # последние 4 минуты
        ts = now_ts - i * 60 * 1000
        key = f"ohlcv:{symbol.lower()}:m1:{ts}"
        exists = await redis.exists(key)
        if not exists:
            missing.append(ts)

    async with pg.acquire() as conn:
        for ts in missing:
            open_time = datetime.utcfromtimestamp(ts / 1000)
            try:
                await conn.execute(
                    "INSERT INTO missing_m1_log_v4 (symbol, open_time) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    symbol, open_time
                )
                await conn.execute(
                    "INSERT INTO system_log_v4 (module, level, message, details) VALUES ($1, $2, $3, $4)",
                    "AGGREGATOR", "WARNING", "M1 missing", 
                    json.dumps({"symbol": symbol, "open_time": str(open_time)})
                )
                logger.warning(f"[{symbol}] Пропущена свеча: {open_time}")
            except Exception as e:
                logger.error(f"[{symbol}] Ошибка записи пропуска: {e}")
# 🔸 Восстановление одной M1 свечи через Binance API
async def restore_missing_m1(symbol, open_time, redis, pg, precision):
    import logging
    import aiohttp
    import json
    from decimal import Decimal, ROUND_DOWN

    logger = logging.getLogger("RECOVERY")
    ts = int(open_time.timestamp() * 1000)
    end_ts = ts + 60_000

    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": "1m",
        "startTime": ts,
        "endTime": end_ts,
        "limit": 1
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"[{symbol}] Binance API error: {resp.status}")
                    return False

                raw = await resp.json()
                if not raw:
                    logger.warning(f"[{symbol}] Binance API пустой результат")
                    return False

                data = raw[0]
                o, h, l, c, v = data[1:6]
                timestamp = int(data[0])

                def r(val):
                    return float(Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

                candle = {
                    "o": r(o),
                    "h": r(h),
                    "l": r(l),
                    "c": r(c),
                    "v": r(v),
                    "ts": timestamp,
                    "fixed": True
                }

                key = f"ohlcv:{symbol.lower()}:m1:{timestamp}"
                await redis.execute_command("JSON.SET", key, "$", json.dumps(candle))

                async with pg.acquire() as conn:
                    await conn.execute(
                        "UPDATE missing_m1_log_v4 SET fixed = true, fixed_at = NOW() WHERE symbol = $1 AND open_time = $2",
                        symbol, open_time
                    )
                    await conn.execute(
                        "INSERT INTO system_log_v4 (module, level, message, details) VALUES ($1, $2, $3, $4)",
                        "AGGREGATOR", "INFO", "M1 restored", 
                        json.dumps({"symbol": symbol, "open_time": str(open_time)})
                    )

                logger.info(f"[{symbol}] Восстановлена M1: {open_time}")

                # 🔸 Попытка построить M5 и M15 по нужным интервалам
                m5_minute = (open_time.minute // 5) * 5
                m5_time = open_time.replace(minute=m5_minute, second=0, microsecond=0)
                await try_aggregate_m5(redis, symbol, m5_time)

                m15_minute = (open_time.minute // 15) * 15
                m15_time = open_time.replace(minute=m15_minute, second=0, microsecond=0)
                await try_aggregate_m15(redis, symbol, m15_time)

                return True

    except Exception as e:
        logger.error(f"[{symbol}] Ошибка восстановления через API: {e}", exc_info=True)
        return False
# 🔸 Циклическое восстановление всех пропущенных свечей из missing_m1_log_v4
async def restore_missing_m1_loop(redis, pg, state):
    logger = logging.getLogger("RECOVERY")

    while True:
        async with pg.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, open_time FROM missing_m1_log_v4 WHERE fixed IS NOT true ORDER BY open_time ASC"
            )

        for row in rows:
            symbol = row["symbol"]
            open_time = row["open_time"]
            precision = state["tickers"].get(symbol)
            if not precision:
                continue

            success = await restore_missing_m1(symbol, open_time, redis, pg, precision)

            if success:
                minute = open_time.minute
                if minute % 5 == 4:
                    await try_aggregate_m5(redis, symbol, open_time)
        await asyncio.sleep(60)
# 🔸 Проверка: 4 и более подряд невосстановленных свечей → отключение тикера
async def check_consecutive_m1_failures(pg, symbol):
    logger = logging.getLogger("RECOVERY")

    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT open_time FROM missing_m1_log_v4
            WHERE symbol = $1 AND fixed IS NOT true
            ORDER BY open_time ASC
            LIMIT 10
        """, symbol)

        if len(rows) < 4:
            return

        timestamps = [row["open_time"] for row in rows]
        deltas = [(timestamps[i+1] - timestamps[i]).total_seconds() for i in range(len(timestamps)-1)]

        # Проверяем подряд 4 интервала по 60 секунд (разброс ±1с)
        count = 1
        for i in range(len(deltas)):
            if 59 <= deltas[i] <= 61:
                count += 1
                if count >= 4:
                    break
            else:
                count = 1

        if count >= 4:
            await conn.execute("""
                UPDATE tickers_v4 SET tradepermission = 'disabled'
                WHERE symbol = $1 AND tradepermission = 'enabled'
            """, symbol)

            await conn.execute("""
                INSERT INTO system_log_v4 (module, level, message, details)
                VALUES ($1, $2, $3, $4)
            """, "AGGREGATOR", "CRITICAL", "M1 permanently degraded",
                json.dumps({"symbol": symbol, "reason": "4+ consecutive missing M1"}))

            logger.critical(f"[{symbol}] Отключён — 4+ подряд пропущенных свечей")
# 🔸 Слушает WebSocket Binance и переподключается при изменении тикеров
async def listen_kline_stream(redis, state, refresh_queue):
    logger = logging.getLogger("KLINE")

    while True:
        if not state["active"]:
            info_log("KLINE", "Нет активных тикеров для подписки")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@kline_1m" for s in symbols]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        try:
            async with websockets.connect(stream_url) as ws:
                logger.info(f"Подключено к WebSocket Binance: {len(symbols)} тикеров")

                async def reader():
                    try:
                        async for msg in ws:
                            data = json.loads(msg)
                            if "data" not in data or "k" not in data["data"]:
                                continue
                            kline = data["data"]["k"]
                            if not kline["x"]:
                                continue
                            symbol = kline["s"]
                            open_time = datetime.utcfromtimestamp(kline["t"] / 1000)

                            await store_and_publish_m1(
                                redis,
                                symbol,
                                open_time,
                                kline,
                                state["tickers"][symbol]
                            )

                    except Exception as e:
                        logger.error(f"Ошибка чтения WebSocket: {e}", exc_info=True)

                async def watcher():
                    await refresh_queue.get()
                    logger.info("Получен сигнал переподключения WebSocket")
                    await ws.close()

                reader_task = asyncio.create_task(reader())
                watcher_task = asyncio.create_task(watcher())
                await asyncio.wait([reader_task, watcher_task], return_when=asyncio.FIRST_COMPLETED)

        except Exception as e:
            logger.error(f"Ошибка WebSocket: {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Поток markPrice для одного тикера с fstream.binance.com
async def watch_mark_price(symbol, redis, precision):

    logger = logging.getLogger("KLINE")

    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s"
    last_update = 0

    while True:
        try:
            async with websockets.connect(url) as ws:
                logger.info(f"[{symbol}] Подключение к потоку markPrice (futures)")
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        price = data.get("p")
                        if not price:
                            continue

                        now = time.time()
                        if now - last_update < 1:
                            continue

                        last_update = now
                        rounded = str(Decimal(price).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
                        await redis.set(f"price:{symbol}", rounded)
                        info_log("KLINE", f"[{symbol}] Обновление markPrice (futures): {rounded}")
                    except Exception as e:
                        logger.warning(f"[{symbol}] Ошибка обработки markPrice: {e}")
        except Exception as e:
            logger.error(f"[{symbol}] Ошибка WebSocket markPrice (futures): {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Основной запуск компонента с управлением тасками
async def run_feed_and_aggregator(pg, redis):

    log = logging.getLogger("FEED+AGGREGATOR")

    # Загрузка всех тикеров (enabled + disabled)
    tickers, active = await load_all_tickers(pg)
    log.info(f"Загружено тикеров: {len(tickers)} → {list(tickers.keys())}")

    for s in tickers:
        if s.lower() in active:
            log.info(f"Активен по умолчанию: {s}")
        else:
            log.info(f"Выключен по умолчанию: {s}")

    # Общее состояние
    state = {
        "tickers": tickers,            # symbol -> precision_price
        "active": active,              # set of lowercase symbols
        "markprice_tasks": {},         # symbol -> asyncio.Task
        "tasks": set()                 # все фоновые таски
    }

    # Очередь сигналов на переподключение WebSocket
    refresh_queue = asyncio.Queue()

    # Хелпер для создания отслеживаемых тасок
    def create_tracked_task(coro, name):
        task = asyncio.create_task(coro)
        state["tasks"].add(task)
        def on_done(t):
            try:
                t.result()
            except Exception as e:
                log.exception(f"[{name}] Task crashed: {e}")
            finally:
                state["tasks"].discard(t)
        task.add_done_callback(on_done)
        return task

    # Запуск подписки на Redis Stream
    create_tracked_task(handle_ticker_events(redis, state, pg, refresh_queue), "ticker_events")

    # Запуск потоков markPrice для каждого активного тикера (фьючерсный рынок)
    for symbol in state["active"]:
        upper_symbol = symbol.upper()
        precision = state["tickers"].get(upper_symbol)
        if precision is not None:
            task = create_tracked_task(watch_mark_price(upper_symbol, redis, precision), f"markprice_{upper_symbol}")
            state["markprice_tasks"][upper_symbol] = task

    # 🔸 Фоновая проверка отсутствующих M1
    async def recovery_loop():
        while True:
            now_ts = int(datetime.utcnow().replace(second=0, microsecond=0).timestamp() * 1000)
            for symbol in state["active"]:
                await detect_missing_m1(redis, pg, symbol.upper(), now_ts)
            await asyncio.sleep(60)

    create_tracked_task(recovery_loop(), "recovery_loop")

    # 🔸 Фоновое восстановление всех пропущенных свечей
    async def restore_loop():
        while True:
            async with pg.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT symbol, open_time FROM missing_m1_log_v4 WHERE fixed IS NOT true ORDER BY open_time ASC"
                )

            for row in rows:
                symbol = row["symbol"]
                open_time = row["open_time"]
                precision = state["tickers"].get(symbol)
                if not precision:
                    continue

                await check_consecutive_m1_failures(pg, symbol)

                success = await restore_missing_m1(symbol, open_time, redis, pg, precision)

                if success and open_time.minute % 5 == 4:
                    await try_aggregate_m5(redis, symbol, open_time)

            await asyncio.sleep(60)

    create_tracked_task(restore_loop(), "restore_loop")

    # Постоянный перезапуск слушателя WebSocket
    async def loop_listen():
        while True:
            await listen_kline_stream(redis, state, refresh_queue)

    create_tracked_task(loop_listen(), "listen_kline")

    # Цикл ожидания (можно будет использовать как watchdog или точка завершения)
    try:
        while True:
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        log.info("Aggregator shutdown requested, cancelling tasks...")
        for t in list(state["tasks"]):
            t.cancel()
        await asyncio.gather(*state["tasks"], return_exceptions=True)
        log.info("All tasks shut down cleanly.")