# feed_and_aggregate.py — приём и логирование M1 и M5 свечей с реактивным управлением
import asyncio
import logging
import time
import json
from asyncpg import Pool
from redis.asyncio import Redis
from datetime import datetime
from websockets import connect
from itertools import islice
from decimal import Decimal, ROUND_DOWN

log = logging.getLogger("FEED+AGGREGATOR")

# 🔸 Загрузка всех тикеров из БД
async def load_all_tickers(pg: Pool):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, precision_price, precision_qty, status FROM tickers_v4")

    now = time.time()
    tickers = {}
    active = set()
    activated_at = {}

    for row in rows:
        symbol = row["symbol"]
        tickers[symbol] = {
            "precision_price": row["precision_price"],
            "precision_qty": row["precision_qty"]
        }

        if row["status"] == "enabled":
            active.add(symbol)
            activated_at[symbol] = now
            log.info(f"[{symbol}] Активен по умолчанию — activated_at={datetime.utcfromtimestamp(now).isoformat()}Z")
        else:
            log.info(f"[{symbol}] Неактивен по умолчанию")

    return tickers, active, activated_at

# 🔸 Отслеживание включения/выключения тикеров через Redis Stream
async def handle_ticker_events(
    redis: Redis,
    state: dict,
    pg: Pool,
    refresh_queue_m1: asyncio.Queue,
    refresh_queue_m5: asyncio.Queue,
    refresh_queue_m15: asyncio.Queue,
    refresh_queue_h1: asyncio.Queue
):
    group = "aggregator_group"
    stream = "tickers_status_stream"

    try:
        await redis.xgroup_create(stream, group, id='0', mkstream=True)
    except Exception:
        pass

    while True:
        resp = await redis.xreadgroup(group, "aggregator", streams={stream: '>'}, count=50, block=1000)
        for _, messages in resp:
            for msg_id, data in messages:
                symbol = data.get("symbol")
                action = data.get("action")

                if not symbol or not action:
                    continue

                if symbol not in state["tickers"]:
                    async with pg.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT precision_price, precision_qty FROM tickers_v4 WHERE symbol = $1",
                            symbol
                        )
                        if row:
                            state["tickers"][symbol] = {
                                "precision_price": row["precision_price"],
                                "precision_qty": row["precision_qty"]
                            }
                            log.debug(f"[{symbol}] Добавлен тикер из БД")

                if action == "enabled" and symbol in state["tickers"]:
                    state["active"].add(symbol)
                    ts = time.time()
                    state["activated_at"][symbol] = ts
                    log.info(f"[{symbol}] Активирован тикер — activated_at={datetime.utcfromtimestamp(ts).isoformat()}Z")
                    await refresh_queue_m1.put("refresh")
                    await refresh_queue_m5.put("refresh")
                    await refresh_queue_m15.put("refresh")
                    await refresh_queue_h1.put("refresh")

                elif action == "disabled" and symbol in state["active"]:
                    state["active"].remove(symbol)
                    log.info(f"[{symbol}] Деактивирован тикер")
                    await refresh_queue_m1.put("refresh")
                    await refresh_queue_m5.put("refresh")
                    await refresh_queue_m15.put("refresh")
                    await refresh_queue_h1.put("refresh")

# 🔸 Разбиение тикеров на группы
def chunked(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk

# 🔸 Подключение к WebSocket Binance по группе тикеров
async def listen_kline_stream(group_key, symbols, queue, interval="1m"):
    stream_names = [f"{s.lower()}@kline_{interval}" for s in symbols]
    stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    while True:
        try:
            async with connect(
                stream_url,
                ping_interval=None,  # отключаем встроенные ping/pong
                close_timeout=5
            ) as ws:
                log.info(f"[KLINE:{group_key}] Подключено к WebSocket: {stream_url}")

                # 🟡 Keep-alive через ручной pong
                async def keep_alive():
                    try:
                        while True:
                            await ws.pong()
                            log.debug(f"[KLINE:{group_key}] → pong (keepalive)")
                            await asyncio.sleep(180)
                    except asyncio.CancelledError:
                        log.debug(f"[KLINE:{group_key}] keep_alive завершён")
                    except Exception as e:
                        log.warning(f"[KLINE:{group_key}] Ошибка keep_alive: {e}")

                pong_task = asyncio.create_task(keep_alive())

                try:
                    async for msg in ws:
                        data = json.loads(msg)
                        kline = data.get("data", {}).get("k")
                        if not kline or not kline.get("x"):
                            continue
                        await queue.put(kline)
                finally:
                    pong_task.cancel()

        except Exception as e:
            log.error(f"[KLINE:{group_key}] Ошибка WebSocket: {e}", exc_info=True)
            log.info(f"[KLINE:{group_key}] Переподключение через 5 секунд...")
            await asyncio.sleep(5)

# 🔸 Воркер для обработки свечей
async def kline_worker(queue, state, redis, interval="M1"):
    while True:
        kline = await queue.get()
        try:
            symbol = kline["s"]
            open_time = datetime.utcfromtimestamp(kline["t"] / 1000)

            precision_price = state["tickers"][symbol]["precision_price"]
            precision_qty = state["tickers"][symbol]["precision_qty"]

            await store_and_publish_kline(
                redis=redis,
                symbol=symbol,
                open_time=open_time,
                kline=kline,
                interval=interval.lower(),
                precision_price=precision_price,
                precision_qty=precision_qty
            )
        except Exception as e:
            log.warning(f"[{interval}] Ошибка обработки свечи {symbol}: {e}", exc_info=True)

# 🔸 Универсальная функция сохранения в Redis
async def store_and_publish_kline(redis, symbol, open_time, kline, interval, precision_price, precision_qty):
    ts = int(open_time.timestamp() * 1000)

    o = float(kline["o"])
    h = float(kline["h"])
    l = float(kline["l"])
    c = float(kline["c"])
    v = float(Decimal(kline["v"]).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN))

    async def safe_ts_add(field_key, value, field_name):
        try:
            # Проверяем: существует ли ключ
            try:
                await redis.execute_command("TS.INFO", field_key)
            except Exception:
                # Если нет — создаём
                await redis.execute_command(
                    "TS.CREATE", field_key,
                    "RETENTION", 5184000000,  # 2 месяца
                    "DUPLICATE_POLICY", "last",
                    "LABELS",
                    "symbol", symbol,
                    "interval", interval,
                    "field", field_name
                )

            # Добавление значения
            await redis.execute_command("TS.ADD", field_key, ts, value)

        except Exception as e:
            log.warning(f"[{symbol}] Ошибка записи TS.ADD {field_key}: {e}")

    # Параллельная запись всех полей
    await asyncio.gather(
        safe_ts_add(f"ts:{symbol}:{interval}:o", o, "o"),
        safe_ts_add(f"ts:{symbol}:{interval}:h", h, "h"),
        safe_ts_add(f"ts:{symbol}:{interval}:l", l, "l"),
        safe_ts_add(f"ts:{symbol}:{interval}:c", c, "c"),
        safe_ts_add(f"ts:{symbol}:{interval}:v", v, "v"),
    )

    log.debug(f"[{symbol}] {interval.upper()} TS записана: open_time={open_time}, завершено={datetime.utcnow()}")

    # Публикация в Redis Stream
    await redis.xadd("ohlcv_stream", {
        "symbol": symbol,
        "interval": interval,
        "timestamp": str(ts),
        "o": str(o),
        "h": str(h),
        "l": str(l),
        "c": str(c),
        "v": str(v),
    })
    log.debug(f"[{symbol}] {interval.upper()} отправлена в Redis Stream: open_time={open_time}, отправлено={datetime.utcnow()}")

    # Уведомление через Pub/Sub
    await redis.publish("ohlcv_channel", json.dumps({
        "symbol": symbol,
        "interval": interval,
        "timestamp": str(ts)
    }))
# 🔸 M1: Реактивный запуск и управление WebSocket-группами
async def run_feed_and_aggregator(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.debug("🔸 Запуск приёма M1 свечей с реактивным управлением")
    queue = asyncio.Queue()
    state["kline_tasks"] = {}

    for _ in range(5):
        asyncio.create_task(kline_worker(queue, state, redis, interval="M1"))

    await refresh_queue.put("initial")

    while True:
        await refresh_queue.get()
        log.debug("🔁 Пересборка групп WebSocket после изменения активных тикеров")

        active_symbols = sorted(state["active"])
        log.info(f"[M1] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")
        new_groups = {
            f"M1:{','.join(group)}": group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["kline_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="1m"))
            state["kline_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["kline_tasks"].pop(group_key)
            task.cancel()
            log.debug(f"[KLINE:{group_key}] Поток остановлен — тикеры неактивны")

# 🔸 M5: Реактивный запуск и логирование M5 свечей
async def run_feed_and_aggregator_m5(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.debug("🔸 Запуск приёма M5 свечей")
    queue = asyncio.Queue()
    state["m5_tasks"] = {}

    for _ in range(2):
        asyncio.create_task(kline_worker(queue, state, redis, interval="M5"))

    await refresh_queue.put("initial-m5")

    while True:
        await refresh_queue.get()
        log.debug("🔁 [M5] Пересборка групп WebSocket")

        active_symbols = sorted(state["active"])
        log.info(f"[M5] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")
        new_groups = {
            f"M5:{','.join(group)}": group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["m5_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="5m"))
            state["m5_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["m5_tasks"].pop(group_key)
            task.cancel()
            log.debug(f"[KLINE:M5:{group_key}] Поток остановлен — тикеры неактивны")

# 🔸 M15: Реактивный запуск и логирование M15 свечей
async def run_feed_and_aggregator_m15(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.debug("🔸 Запуск приёма M15 свечей")
    queue = asyncio.Queue()
    state["m15_tasks"] = {}

    for _ in range(2):
        asyncio.create_task(kline_worker(queue, state, redis, interval="M15"))

    await refresh_queue.put("initial-m15")

    while True:
        await refresh_queue.get()
        log.debug("🔁 [M15] Пересборка групп WebSocket")

        active_symbols = sorted(state["active"])
        log.info(f"[M15] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")

        new_groups = {
            f"M15:{','.join(group)}": group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["m15_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="15m"))
            state["m15_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["m15_tasks"].pop(group_key)
            task.cancel()
            log.debug(f"[KLINE:M15:{group_key}] Поток остановлен — тикеры неактивны")
# 🔸 H1: Реактивный запуск и логирование H1 свечей
async def run_feed_and_aggregator_h1(state, redis: Redis, pg: Pool, refresh_queue: asyncio.Queue):
    log.debug("🔸 Запуск приёма H1 свечей")
    queue = asyncio.Queue()
    state["h1_tasks"] = {}

    for _ in range(2):
        asyncio.create_task(kline_worker(queue, state, redis, interval="H1"))

    await refresh_queue.put("initial-h1")

    while True:
        await refresh_queue.get()
        log.debug("🔁 [H1] Пересборка групп WebSocket")

        active_symbols = sorted(state["active"])
        log.info(f"[H1] Всего активных тикеров: {len(active_symbols)} → {active_symbols}")

        new_groups = {
            f"H1:{','.join(group)}": group
            for group in chunked(active_symbols, 3)
        }
        current_groups = set(state["h1_tasks"].keys())
        desired_groups = set(new_groups.keys())

        for group_key in desired_groups - current_groups:
            group_symbols = new_groups[group_key]
            task = asyncio.create_task(listen_kline_stream(group_key, group_symbols, queue, interval="1h"))
            state["h1_tasks"][group_key] = task

        for group_key in current_groups - desired_groups:
            task = state["h1_tasks"].pop(group_key)
            task.cancel()
            log.debug(f"[KLINE:H1:{group_key}] Поток остановлен — тикеры неактивны")
            