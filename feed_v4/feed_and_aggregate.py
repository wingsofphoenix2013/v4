# feed_and_aggregate.py — приём и агрегация рыночных данных - 22/05/2025 (новый)

import logging
import asyncio
import websockets
import json
import aiohttp
from infra import setup_logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import time
from datetime import datetime, timedelta

# Получаем логгер для модуля
log = logging.getLogger("FEED+AGGREGATOR")

# 🔸 Загрузка всех тикеров, точности и статуса из PostgreSQL
async def load_all_tickers(pg_pool):
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, precision_price, precision_qty, status FROM tickers_v4
        """)
        tickers = {}
        active = set()
        for row in rows:
            tickers[row['symbol']] = {
                "precision_price": row["precision_price"],
                "precision_qty": row["precision_qty"]
            }
            if row['status'] == 'enabled':
                active.add(row['symbol'].lower())
        return tickers, active
# 🔸 Обработка событий включения/отключения тикеров через Redis Stream
async def handle_ticker_events(redis, state, pg, refresh_queue):
    group = "aggregator_group"
    stream = "tickers_status_stream"

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
                            SELECT precision_price, precision_qty FROM tickers_v4 WHERE symbol = $1
                        """, symbol)
                        if row:
                            state["tickers"][symbol] = {
                                "precision_price": row["precision_price"],
                                "precision_qty": row["precision_qty"]
                            }
                            log.info(f"Добавлен тикер из БД: {symbol}")

                if action == "enabled" and symbol in state["tickers"]:
                    log.info(f"Активирован тикер: {symbol}")
                    state["active"].add(symbol.lower())
                    state["activated_at"][symbol] = datetime.utcnow()
                    await refresh_queue.put("refresh")

                    # запуск потока markPrice и отслеживание задачи
                    task = asyncio.create_task(watch_mark_price(symbol, redis, state))
                    state["markprice_tasks"][symbol] = task

                elif action == "disabled" and symbol in state["tickers"]:
                    log.info(f"Отключён тикер: {symbol}")
                    state["active"].discard(symbol.lower())
                    await refresh_queue.put("refresh")

                    # отмена потока markPrice, если он есть
                    task = state["markprice_tasks"].pop(symbol, None)
                    if task:
                        task.cancel()

                await redis.xack(stream, group, msg_id)
# 🔸 Сохранение полной свечи M1 в RedisTimeSeries + Stream + Pub/Sub
async def store_and_publish_m1(redis, symbol, open_time, kline, precision_price, precision_qty):
    ts = int(open_time.timestamp() * 1000)

    def r(val, precision):
        return float(Decimal(val).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))

    o = r(kline["o"], precision_price)
    h = r(kline["h"], precision_price)
    l = r(kline["l"], precision_price)
    c = r(kline["c"], precision_price)
    v = r(kline["v"], precision_qty)

    async def safe_ts_add(field_key, value, field_name):
        try:
            await redis.execute_command("TS.ADD", field_key, ts, value)
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                await redis.execute_command(
                    "TS.CREATE", field_key,
                    "RETENTION", 2592000000,  # 30 дней
                    "DUPLICATE_POLICY", "last",
                    "LABELS",
                    "symbol", symbol,
                    "interval", "m1",
                    "field", field_name
                )
                await redis.execute_command("TS.ADD", field_key, ts, value)
            else:
                raise

    await safe_ts_add(f"ts:{symbol}:m1:o", o, "o")
    await safe_ts_add(f"ts:{symbol}:m1:h", h, "h")
    await safe_ts_add(f"ts:{symbol}:m1:l", l, "l")
    await safe_ts_add(f"ts:{symbol}:m1:c", c, "c")
    await safe_ts_add(f"ts:{symbol}:m1:v", v, "v")

    log.info(f"[{symbol}] M1 TS сохранена: ts={ts} O={o} H={h} L={l} C={c} V={v}")

    event = {
        "symbol": symbol,
        "interval": "m1",
        "timestamp": str(ts)
    }

    await redis.xadd("ohlcv_stream", event)
    await redis.publish("ohlcv_channel", json.dumps(event))
# 🔸 Попытка построить свечу M5 из 5 M1 в RedisTimeSeries
async def try_aggregate_m5(redis, symbol, base_time, state):
    end_ts = int(base_time.timestamp() * 1000) + 4 * 60_000
    ts_list = [end_ts - i * 60_000 for i in reversed(range(5))]
    candles = []

    for ts in ts_list:
        try:
            o = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:o", ts)
            h = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:h", ts)
            l = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:l", ts)
            c = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:c", ts)
            v = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:v", ts)

            if not all([o, h, l, c, v]):
                return

            candles.append({
                "o": float(o[1]),
                "h": float(h[1]),
                "l": float(l[1]),
                "c": float(c[1]),
                "v": float(v[1]),
                "ts": ts
            })
        except Exception:
            return

    if not candles:
        return

    m5_ts = ts_list[0]
    o = candles[0]["o"]
    h = max(c["h"] for c in candles)
    l = min(c["l"] for c in candles)
    c = candles[-1]["c"]
    v = sum(c["v"] for c in candles)

    # Округление по точности
    precision_price = state["tickers"][symbol]["precision_price"]
    precision_qty = state["tickers"][symbol]["precision_qty"]

    def r(val, p):
        return float(Decimal(val).quantize(Decimal(f"1e-{p}"), rounding=ROUND_DOWN))

    o, h, l, c = map(lambda x: r(x, precision_price), [o, h, l, c])
    v = r(v, precision_qty)

    async def safe_ts_add(key, value, field):
        try:
            await redis.execute_command("TS.ADD", key, m5_ts, value)
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                await redis.execute_command(
                    "TS.CREATE", key,
                    "RETENTION", 2592000000,
                    "DUPLICATE_POLICY", "last",
                    "LABELS",
                    "symbol", symbol,
                    "interval", "m5",
                    "field", field
                )
                await redis.execute_command("TS.ADD", key, m5_ts, value)
            else:
                raise

    await safe_ts_add(f"ts:{symbol}:m5:o", o, "o")
    await safe_ts_add(f"ts:{symbol}:m5:h", h, "h")
    await safe_ts_add(f"ts:{symbol}:m5:l", l, "l")
    await safe_ts_add(f"ts:{symbol}:m5:c", c, "c")
    await safe_ts_add(f"ts:{symbol}:m5:v", v, "v")

    log.info(f"[{symbol}] Построена M5: {datetime.utcfromtimestamp(m5_ts / 1000)} O:{o} H:{h} L:{l} C:{c}")
# 🔸 Фоновая агрегация M5
async def aggregate_m5_loop(redis, state):
    while True:
        now = datetime.utcnow()
        if now.minute % 5 == 0:
            now -= timedelta(minutes=1)

        base_minute = (now.minute // 5) * 5
        base_time = now.replace(minute=base_minute, second=0, microsecond=0)

        for symbol in state["active"]:
            await try_aggregate_m5(redis, symbol.upper(), base_time, state)

        await asyncio.sleep(60)
# 🔸 Попытка построить свечу M15 из 15 M1 в RedisTimeSeries
async def try_aggregate_m15(redis, symbol, base_time, state):
    end_ts = int(base_time.timestamp() * 1000) + 14 * 60_000
    ts_list = [end_ts - i * 60_000 for i in reversed(range(15))]
    candles = []

    for ts in ts_list:
        try:
            o = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:o", ts)
            h = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:h", ts)
            l = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:l", ts)
            c = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:c", ts)
            v = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:v", ts)

            if not all([o, h, l, c, v]):
                return

            candles.append({
                "o": float(o[1]),
                "h": float(h[1]),
                "l": float(l[1]),
                "c": float(c[1]),
                "v": float(v[1]),
                "ts": ts
            })
        except Exception:
            return

    if not candles:
        return

    m15_ts = ts_list[0]
    o = candles[0]["o"]
    h = max(c["h"] for c in candles)
    l = min(c["l"] for c in candles)
    c = candles[-1]["c"]
    v = sum(c["v"] for c in candles)

    # Округление
    precision_price = state["tickers"][symbol]["precision_price"]
    precision_qty = state["tickers"][symbol]["precision_qty"]

    def r(val, p):
        return float(Decimal(val).quantize(Decimal(f"1e-{p}"), rounding=ROUND_DOWN))

    o, h, l, c = map(lambda x: r(x, precision_price), [o, h, l, c])
    v = r(v, precision_qty)

    async def safe_ts_add(key, value, field):
        try:
            await redis.execute_command("TS.ADD", key, m15_ts, value)
        except Exception as e:
            if "TSDB: the key does not exist" in str(e):
                await redis.execute_command(
                    "TS.CREATE", key,
                    "RETENTION", 2592000000,
                    "DUPLICATE_POLICY", "last",
                    "LABELS",
                    "symbol", symbol,
                    "interval", "m15",
                    "field", field
                )
                await redis.execute_command("TS.ADD", key, m15_ts, value)
            else:
                raise

    await safe_ts_add(f"ts:{symbol}:m15:o", o, "o")
    await safe_ts_add(f"ts:{symbol}:m15:h", h, "h")
    await safe_ts_add(f"ts:{symbol}:m15:l", l, "l")
    await safe_ts_add(f"ts:{symbol}:m15:c", c, "c")
    await safe_ts_add(f"ts:{symbol}:m15:v", v, "v")

    log.info(f"[{symbol}] Построена M15: {datetime.utcfromtimestamp(m15_ts / 1000)} O:{o} H:{h} L:{l} C:{c}")
# 🔸 Фоновая агрегация M15
async def aggregate_m15_loop(redis, state):
    while True:
        now = datetime.utcnow()
        if now.minute % 15 == 0:
            now -= timedelta(minutes=1)

        base_minute = (now.minute // 15) * 15
        base_time = now.replace(minute=base_minute, second=0, microsecond=0)

        for symbol in state["active"]:
            await try_aggregate_m15(redis, symbol.upper(), base_time, state)

        await asyncio.sleep(60)
# 🔸 Поиск пропущенных M1 и запись в missing_m1_log_v4 + system_log_v4
async def detect_missing_m1(redis, pg, symbol, now_ts, state):
    missing = []

    for i in range(1, 16):  # проверка последних 15 минут
        ts = now_ts - i * 60 * 1000
        open_time = datetime.utcfromtimestamp(ts / 1000)

        # Пропустить, если свеча раньше момента включения тикера
        if open_time < state["activated_at"].get(symbol, datetime.min):
            continue

        try:
            exists = await redis.execute_command("TS.GET", f"ts:{symbol}:m1:o", ts)
            if not exists:
                missing.append(ts)
        except Exception:
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
                log.warning(f"[{symbol}] Пропущена свеча: {open_time}")
            except Exception as e:
                log.error(f"[{symbol}] Ошибка записи пропуска: {e}")
# 🔸 Восстановление одной M1 свечи через Binance API + Redis TS + отметка в БД
async def restore_missing_m1(symbol, open_time, redis, pg, precision_price, precision_qty):
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
                    log.error(f"[{symbol}] Binance API error: {resp.status}")
                    return False

                raw = await resp.json()
                if not raw:
                    log.warning(f"[{symbol}] Binance API пустой результат")
                    return False

                data = raw[0]
                o, h, l, c, v = data[1:6]
                timestamp = int(data[0])

                def r(val, p):
                    return float(Decimal(val).quantize(Decimal(f"1e-{p}"), rounding=ROUND_DOWN))

                await redis.execute_command("TS.ADD", f"ts:{symbol}:m1:o", timestamp, r(o, precision_price))
                await redis.execute_command("TS.ADD", f"ts:{symbol}:m1:h", timestamp, r(h, precision_price))
                await redis.execute_command("TS.ADD", f"ts:{symbol}:m1:l", timestamp, r(l, precision_price))
                await redis.execute_command("TS.ADD", f"ts:{symbol}:m1:c", timestamp, r(c, precision_price))
                await redis.execute_command("TS.ADD", f"ts:{symbol}:m1:v", timestamp, r(v, precision_qty))

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

                log.info(f"[{symbol}] Восстановлена M1: {open_time}")
                return True

    except Exception as e:
        log.error(f"[{symbol}] Ошибка восстановления через API: {e}", exc_info=True)
        return False
# 🔸 Слушает WebSocket Binance и переподключается при изменении тикеров
async def listen_kline_stream(redis, state, refresh_queue):

    while True:
        if not state["active"]:
            log.info("Нет активных тикеров для подписки")
            await asyncio.sleep(10)
            continue

        symbols = sorted(state["active"])
        streams = [f"{s}@kline_1m" for s in symbols]
        stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        try:
            async with websockets.connect(stream_url) as ws:
                log.info(f"Подключено к WebSocket Binance: {len(symbols)} тикеров")

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
                                state["tickers"][symbol]["precision_price"],
                                state["tickers"][symbol]["precision_qty"]
                            )

                    except Exception as e:
                        log.error(f"Ошибка чтения WebSocket: {e}", exc_info=True)

                async def watcher():
                    await refresh_queue.get()
                    log.info("Получен сигнал переподключения WebSocket")
                    await ws.close()

                reader_task = asyncio.create_task(reader())
                watcher_task = asyncio.create_task(watcher())
                await asyncio.wait([reader_task, watcher_task], return_when=asyncio.FIRST_COMPLETED)

        except Exception as e:
            log.error(f"Ошибка WebSocket: {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Поток markPrice для одного тикера с fstream.binance.com
async def watch_mark_price(symbol, redis, state):
    log.debug(f"[{symbol}] DEBUG | type(state): {type(state)}, keys: {list(state.keys())}")

    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@markPrice@1s"
    last_update = 0

    while True:
        try:
            async with websockets.connect(url) as ws:
                log.info(f"[{symbol}] Подключение к потоку markPrice (futures)")
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
                        precision = state["tickers"][symbol]["precision_price"]
                        rounded = str(
                            Decimal(price).quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)
                        )
                        await redis.set(f"price:{symbol}", rounded)
                        log.debug(f"[{symbol}] Обновление markPrice: {rounded}")

                    except (InvalidOperation, ValueError, TypeError) as e:
                        log.warning(f"[{symbol}] Ошибка обработки markPrice: {type(e)}")

        except Exception as e:
            log.error(f"[{symbol}] Ошибка WebSocket markPrice (futures): {e}", exc_info=True)
            await asyncio.sleep(5)
# 🔸 Основной запуск компонента с управлением тасками
async def run_feed_and_aggregator(pg, redis):

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
        "tickers": tickers,            # symbol -> {precision_price, precision_qty}
        "active": active,              # set of lowercase symbols
        "markprice_tasks": {},         # symbol -> asyncio.Task
        "tasks": set(),                # все фоновые таски
        "activated_at": {}             # symbol -> datetime.utcnow()
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
            except asyncio.CancelledError:
                log.info(f"[{name}] Task cancelled")
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
        if upper_symbol in state["tickers"]:
            task = create_tracked_task(
                watch_mark_price(upper_symbol, redis, state),
                f"markprice_{upper_symbol}"
            )
            state["markprice_tasks"][upper_symbol] = task

    # 🔸 Фоновая проверка отсутствующих M1-свечей
    async def recovery_loop():
        while True:
            now_ts = int((datetime.utcnow() - timedelta(minutes=1)).replace(second=0, microsecond=0).timestamp() * 1000)
            for symbol in state["active"]:
                await detect_missing_m1(redis, pg, symbol.upper(), now_ts, state)
            await asyncio.sleep(60)

    create_tracked_task(recovery_loop(), "recovery_loop")
    
    # 🔸 Фоновая генерация М5
    create_tracked_task(aggregate_m5_loop(redis, state), "aggregate_m5_loop")
    
    # 🔸 Фоновая генерация М15
    create_tracked_task(aggregate_m15_loop(redis, state), "aggregate_m15_loop")
    
    # 🔸 Фоновое восстановление пропущенных M1 через Binance API
    async def restore_loop():
        while True:
            async with pg.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT symbol, open_time FROM missing_m1_log_v4
                    WHERE fixed IS NOT true
                    ORDER BY open_time ASC
                """)

            for row in rows:
                symbol = row["symbol"]
                open_time = row["open_time"]
                try:
                    precision_price = state["tickers"][symbol]["precision_price"]
                    precision_qty   = state["tickers"][symbol]["precision_qty"]
                except KeyError:
                    continue  # символ не в state — пропускаем

                await restore_missing_m1(symbol, open_time, redis, pg, precision_price, precision_qty)

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