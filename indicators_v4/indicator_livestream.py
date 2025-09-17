# indicator_livestream.py — воркер «живых» значений индикаторов (ind_live:*) по (symbol, TF), тик раз в 60с, TTL=90с

import asyncio
import json
import logging
import time
from datetime import datetime
import pandas as pd

# 🔸 Импорты вычислителя снапшотов
from indicators.compute_and_store import compute_snapshot_values_async

# 🔸 Логгер модуля
log = logging.getLogger("IND_LIVESTREAM")

# 🔸 Константы TF, шаги и параметры live-тикера
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
STEP_MS  = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

LIVE_TICK_SEC   = 60        # период live-обновления
LIVE_TTL_SEC    = 90        # TTL ind_live:* (с запасом > периода)
LIVE_DEPTH_BARS = 800       # запас истории для расчёта «живых» индикаторов
LIVE_CONCURRENCY = 10       # семафор параллельных вычислений по инстансам TF

# 🔸 Префиксы хранилищ
BB_TS_PREFIX     = "bb:ts"       # bb:ts:{symbol}:{interval}:{field}
IND_LIVE_PREFIX  = "ind_live"    # ind_live:{symbol}:{tf}:{param}


# 🔸 Вспомогательные: вычисление начала бара и текущее время в мс (UTC)
def floor_to_bar(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step


# 🔸 Загрузка OHLCV из Redis TS (одним батчем) и сборка DataFrame
async def load_ohlcv_df(redis, symbol: str, tf: str, end_ts_ms: int, bars: int):
    if tf not in STEP_MS:
        return None

    step = STEP_MS[tf]
    start_ts = end_ts_ms - (bars - 1) * step

    fields = ["o", "h", "l", "c", "v"]
    keys = {f: f"{BB_TS_PREFIX}:{symbol}:{tf}:{f}" for f in fields}

    # один батч на 5 TS.RANGE
    tasks = {
        f: redis.execute_command("TS.RANGE", keys[f], start_ts, end_ts_ms)
        for f in fields
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for f, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            log.warning(f"[TS] RANGE {keys[f]} error: {res}")
            continue
        if res:
            try:
                series[f] = {int(ts): float(val) for ts, val in res if val is not None}
            except Exception as e:
                log.warning(f"[TS] parse {keys[f]} error: {e}")

    if not series or "c" not in series or not series["c"]:
        return None

    # общий индекс по меткам времени
    idx = sorted(series["c"].keys())
    df = None
    for f in fields:
        col_map = series.get(f, {})
        s = pd.Series({ts: col_map.get(ts) for ts in idx})
        s.index = pd.to_datetime(s.index, unit="ms")
        s.name = f
        df = s.to_frame() if df is None else df.join(s, how="outer")

    if df is None or df.empty:
        return None

    df.index.name = "open_time"
    return df.sort_index()


# 🔸 Менеджер циклов по (symbol, timeframe)
class TFManager:
    def __init__(self, pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
        self.pg = pg
        self.redis = redis
        self.get_instances_by_tf = get_instances_by_tf
        self.get_precision = get_precision
        self.get_active_symbols = get_active_symbols

        # (symbol, tf) -> asyncio.Task
        self.tasks = {}

    def _key(self, symbol: str, tf: str):
        return (symbol, tf)

    def is_running(self, symbol: str, tf: str) -> bool:
        t = self.tasks.get(self._key(symbol, tf))
        return t is not None and not t.done()

    async def start_or_restart(self, symbol: str, tf: str):
        if tf not in STEP_MIN:
            return
        key = self._key(symbol, tf)

        # перезапуск цикла, чтобы «фаза» шла от закрытия свечи (+60с)
        prev = self.tasks.get(key)
        if prev and not prev.done():
            prev.cancel()
            try:
                await prev
            except Exception:
                pass

        task = asyncio.create_task(self._tf_loop(symbol, tf))
        self.tasks[key] = task
        log.info(f"[START] {symbol}/{tf}: live-цикл запущен (первый тик через {LIVE_TICK_SEC}s)")

    async def stop_symbol(self, symbol: str):
        # остановить все TF по символу
        to_stop = [k for k in self.tasks.keys() if k[0] == symbol]
        for key in to_stop:
            t = self.tasks.pop(key, None)
            if t:
                t.cancel()
                try:
                    await t
                except Exception:
                    pass
                log.info(f"[STOP] {key[0]}/{key[1]}: live-цикл остановлен")

    async def _tf_loop(self, symbol: str, tf: str):
        # начальная задержка: 60с после «закрытого» (привязка фазы к событию)
        try:
            await asyncio.sleep(LIVE_TICK_SEC)
            sem = asyncio.Semaphore(LIVE_CONCURRENCY)

            while True:
                t0 = time.monotonic()

                try:
                    # проверка активности символа (если у нас есть такой список)
                    if symbol not in set(self.get_active_symbols()):
                        log.info(f"[SKIP] {symbol}/{tf}: символ неактивен")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 1) загрузка OHLCV
                    now_ms = int(datetime.utcnow().timestamp() * 1000)
                    bar_open_ms = floor_to_bar(now_ms, tf)

                    t_fetch0 = time.monotonic()
                    df = await load_ohlcv_df(self.redis, symbol, tf, bar_open_ms, LIVE_DEPTH_BARS)
                    t_fetch1 = time.monotonic()

                    if df is None or df.empty:
                        log.info(f"[SKIP] {symbol}/{tf}: нет данных OHLCV для live-тика")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 2) список инстансов TF (из in-memory кэша)
                    instances = self.get_instances_by_tf(tf)
                    if not instances:
                        log.info(f"[SKIP] {symbol}/{tf}: нет активных инстансов TF")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 3) расчёт значений по всем инстансам TF (параллельно, но с семафором)
                    precision = self.get_precision(symbol) or 8

                    async def run_one(inst):
                        # вычисление снапшота индикатора по текущему df
                        async with sem:
                            return inst["id"], await compute_snapshot_values_async(inst, symbol, df, precision)

                    t_comp0 = time.monotonic()
                    results = await asyncio.gather(*[run_one(inst) for inst in instances], return_exceptions=True)
                    t_comp1 = time.monotonic()

                    # 4) запись ind_live:* (одним pipeline)
                    pipe = self.redis.pipeline()
                    params_written = 0

                    for item in results:
                        if isinstance(item, Exception):
                            continue
                        iid, values = item
                        if not values:
                            continue
                        for pname, sval in values.items():
                            rkey = f"{IND_LIVE_PREFIX}:{symbol}:{tf}:{pname}"
                            # запись значения с TTL=90с
                            pipe.set(rkey, sval, ex=LIVE_TTL_SEC)
                            params_written += 1

                    t_write0 = time.monotonic()
                    if params_written > 0:
                        await pipe.execute()
                    t_write1 = time.monotonic()

                    # 5) лог итога тика
                    fetch_ms  = int((t_fetch1 - t_fetch0) * 1000)
                    comp_ms   = int((t_comp1  - t_comp0) * 1000)
                    write_ms  = int((t_write1 - t_write0) * 1000) if params_written > 0 else 0
                    total_ms  = int((time.monotonic() - t0) * 1000)

                    log.info(
                        f"[LIVE] {symbol}/{tf}: instances={len(instances)}, params_written={params_written}, "
                        f"fetch_ms={fetch_ms}, compute_ms={comp_ms}, write_ms={write_ms}, total_ms={total_ms}"
                    )

                except asyncio.CancelledError:
                    log.info(f"[CANCEL] {symbol}/{tf}: live-цикл остановлен")
                    return
                except Exception as e:
                    log.warning(f"[ERR] {symbol}/{tf}: ошибка live-обновления: {e}", exc_info=True)

                # пауза до следующего live-тика
                await asyncio.sleep(LIVE_TICK_SEC)

        except asyncio.CancelledError:
            log.info(f"[CANCEL:init] {symbol}/{tf}: live-цикл не запущен/остановлен на старте")
            return


# 🔸 Основной воркер: слушает iv4_inserted и управляет циклами по (symbol, TF)
async def run_indicator_livestream(pg, redis, get_instances_by_tf, get_precision, get_active_symbols):
    log.debug("IND_LIVESTREAM: воркер запущен")

    mgr = TFManager(pg, redis, get_instances_by_tf, get_precision, get_active_symbols)

    stream = "iv4_inserted"
    group = "ind_live_tf_group"
    consumer = "ind_live_tf_1"

    # создать consumer-group для iv4_inserted
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=200, block=2000)
            if not resp:
                continue

            to_ack = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")  # m5/m15/h1

                        # минимальная валидация
                        if not symbol or interval not in STEP_MIN:
                            continue

                        # запуск/перезапуск цикла по (symbol, TF)
                        await mgr.start_or_restart(symbol, interval)

                    except Exception as e:
                        log.warning(f"[STREAM] parse iv4_inserted error: {e}")

            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as e:
            log.error(f"[STREAM] iv4_inserted loop error: {e}", exc_info=True)
            await asyncio.sleep(2)