# indicator_livestream.py — воркер «живых» значений индикаторов: вычисляет по (symbol, TF) раз в 60с, публикует батч в Stream и (опционально) пишет KV

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

LIVE_TICK_SEC    = 30       # период live-обновления
LIVE_TTL_SEC     = 90       # TTL ind_live:* (если включено KV)
LIVE_DEPTH_BARS  = 800      # глубина истории для live-расчётов
LIVE_CONCURRENCY = 30       # семафор параллельных вычислений по инстансам TF

# 🔸 Публикация live в Stream и/или в KV
LIVE_PUBLISH_STREAM = True                # XADD в indicator_live_stream (для всех последующих постпроцессоров)
LIVE_WRITE_KV       = False               # писать ind_live:* (обычно False, чтобы не дублировать хранение)

# 🔸 Имена и префиксы хранилищ
BB_TS_PREFIX      = "bb:ts"               # bb:ts:{symbol}:{interval}:{field}
IND_LIVE_PREFIX   = "ind_live"            # ind_live:{symbol}:{tf}:{param}
LIVE_STREAM_NAME  = "indicator_live_stream"  # Stream с батч-событиями по (symbol, TF)

# 🔸 Вспомогательные: вычисление начала бара (UTC, мс)
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
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, end_ts_ms) for f in fields}
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
            except asyncio.CancelledError:
                # ожидаемая отмена — не считаем ошибкой
                pass
            except Exception:
                pass

        task = asyncio.create_task(self._tf_loop(symbol, tf))
        self.tasks[key] = task
        log.debug(f"[START] {symbol}/{tf}: live-цикл запущен (первый тик через {LIVE_TICK_SEC}s)")

    async def stop_symbol(self, symbol: str):
        # остановить все TF по символу
        to_stop = [k for k in self.tasks.keys() if k[0] == symbol]
        for key in to_stop:
            t = self.tasks.pop(key, None)
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                log.debug(f"[STOP] {key[0]}/{key[1]}: live-цикл остановлен")

    async def _tf_loop(self, symbol: str, tf: str):
        # без начальной задержки: первый live-тик выполняем сразу после перезапуска по iv4_inserted
        try:
            sem = asyncio.Semaphore(LIVE_CONCURRENCY)

            while True:
                t0 = time.monotonic()

                try:
                    # проверка активности символа
                    if symbol not in set(self.get_active_symbols()):
                        log.debug(f"[SKIP] {symbol}/{tf}: символ неактивен")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 1) загрузка OHLCV
                    now_ms = int(datetime.utcnow().timestamp() * 1000)
                    bar_open_ms = floor_to_bar(now_ms, tf)

                    t_fetch0 = time.monotonic()
                    df = await load_ohlcv_df(self.redis, symbol, tf, bar_open_ms, LIVE_DEPTH_BARS)
                    t_fetch1 = time.monotonic()

                    if df is None or df.empty:
                        log.debug(f"[SKIP] {symbol}/{tf}: нет данных OHLCV для live-тика")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 2) список инстансов TF (из in-memory кэша)
                    instances = self.get_instances_by_tf(tf)
                    if not instances:
                        log.debug(f"[SKIP] {symbol}/{tf}: нет активных инстансов TF")
                        await asyncio.sleep(LIVE_TICK_SEC)
                        continue

                    # 3) расчёт значений по всем инстансам TF (параллельно, но с семафором)
                    precision = self.get_precision(symbol) or 8

                    async def run_one(inst):
                        # вычисление снапшота индикатора по текущему df
                        async with sem:
                            return inst["id"], inst["indicator"], inst["params"], await compute_snapshot_values_async(inst, symbol, df, precision)

                    t_comp0 = time.monotonic()
                    results = await asyncio.gather(*[run_one(inst) for inst in instances], return_exceptions=True)
                    t_comp1 = time.monotonic()

                    # собрать батч для Stream: только валидные результаты
                    instances_payload = []
                    params_written = 0  # будет >0 только если включим KV

                    for res in results:
                        if isinstance(res, Exception):
                            continue
                        iid, indicator, params, values = res
                        if not values:
                            continue
                        instances_payload.append({
                            "instance_id": iid,
                            "indicator": indicator,
                            "params": params,
                            "values": values,  # dict param_name -> строковое значение
                        })

                    # 4a) публикация в Stream (один XADD на тик по (symbol, TF))
                    t_pub0 = time.monotonic()
                    if LIVE_PUBLISH_STREAM and instances_payload:
                        try:
                            await self.redis.xadd(LIVE_STREAM_NAME, {
                                "symbol": symbol,
                                "timeframe": tf,
                                "tick_open_time": datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat(),
                                "instances": json.dumps(instances_payload),
                                "precision": str(precision),
                            })
                        except Exception as e:
                            log.warning(f"[STREAM] XADD {LIVE_STREAM_NAME} error for {symbol}/{tf}: {e}")
                    t_pub1 = time.monotonic()

                    # 4b) (опционально) запись ind_live:* (одним pipeline), если включено LIVE_WRITE_KV
                    t_write0 = t_write1 = time.monotonic()
                    if LIVE_WRITE_KV and instances_payload:
                        pipe = self.redis.pipeline()
                        for item in instances_payload:
                            values = item.get("values") or {}
                            for pname, sval in values.items():
                                rkey = f"{IND_LIVE_PREFIX}:{symbol}:{tf}:{pname}"
                                pipe.set(rkey, sval, ex=LIVE_TTL_SEC)
                                params_written += 1
                        t_write0 = time.monotonic()
                        if params_written > 0:
                            await pipe.execute()
                        t_write1 = time.monotonic()

                    # 5) лог итога тика
                    fetch_ms  = int((t_fetch1 - t_fetch0) * 1000)
                    comp_ms   = int((t_comp1  - t_comp0) * 1000)
                    pub_ms    = int((t_pub1   - t_pub0) * 1000) if LIVE_PUBLISH_STREAM and instances_payload else 0
                    write_ms  = int((t_write1 - t_write0) * 1000) if params_written > 0 else 0
                    total_ms  = int((time.monotonic() - t0) * 1000)

                    log.debug(
                        f"[LIVE] {symbol}/{tf}: instances={len(instances_payload)}/{len(instances)}, "
                        f"stream_ms={pub_ms}, kv_params={params_written}, fetch_ms={fetch_ms}, "
                        f"compute_ms={comp_ms}, write_ms={write_ms}, total_ms={total_ms}"
                    )

                except asyncio.CancelledError:
                    log.debug(f"[CANCEL] {symbol}/{tf}: live-цикл остановлен")
                    return
                except Exception as e:
                    log.warning(f"[ERR] {symbol}/{tf}: ошибка live-обновления: {e}", exc_info=True)

                # пауза до следующего live-тика
                await asyncio.sleep(LIVE_TICK_SEC)

        except asyncio.CancelledError:
            log.debug(f"[CANCEL:init] {symbol}/{tf}: live-цикл не запущен/остановлен на старте")
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

    # 🔸 БУТСТРАП: стартуем live-циклы для всех активных символов и TF с инстансами (первый тик через 60с)
    try:
        symbols = get_active_symbols()
        tfs_with_instances = [tf for tf in ("m5", "m15", "h1") if get_instances_by_tf(tf)]
        started = 0
        for tf in tfs_with_instances:
            for sym in symbols:
                try:
                    await mgr.start_or_restart(sym, tf)  # _tf_loop сам ждёт 60с перед первым тиком
                    started += 1
                except asyncio.CancelledError:
                    # ожидаемая отмена при перезапуске — игнорируем
                    pass
        log.debug(f"[BOOT] live-циклы запущены: symbols={len(symbols)}, tfs={tfs_with_instances}, tasks={started}")
    except Exception as e:
        log.warning(f"[BOOT] ошибка инициализации live-циклов: {e}", exc_info=True)

    # 🔸 Антидребезг: запоминаем последний open_time по (symbol, TF), чтобы не перезапускать цикл на тот же бар
    last_open_time: dict[tuple[str, str], datetime] = {}

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={stream: ">"}, count=200, block=2000)
            if not resp:
                continue

            to_ack = []
            latest: dict[tuple[str, str], datetime | None] = {}  # (symbol, interval) -> max(open_time) или None

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        symbol = data.get("symbol")
                        interval = data.get("interval")  # m5/m15/h1
                        open_time_iso = data.get("open_time")

                        # минимальная валидация
                        if not symbol or interval not in STEP_MIN:
                            continue

                        # берём самый свежий open_time в батче для каждой пары (symbol, TF)
                        key = (symbol, interval)
                        ot = None
                        if open_time_iso:
                            try:
                                ot = datetime.fromisoformat(open_time_iso)
                            except Exception:
                                ot = None

                        if key not in latest:
                            latest[key] = ot
                        else:
                            if ot is not None:
                                if latest[key] is None or ot > latest[key]:
                                    latest[key] = ot

                    except Exception as e:
                        log.warning(f"[STREAM] parse iv4_inserted error: {e}")

            # запуск/перезапуск не чаще одного раза на бар и одну пару (symbol, TF)
            for (symbol, interval), ot in latest.items():
                try:
                    # если это тот же бар, который уже перезапускали — пропускаем
                    if ot is not None and last_open_time.get((symbol, interval)) == ot:
                        continue
                    if ot is not None:
                        last_open_time[(symbol, interval)] = ot

                    try:
                        await mgr.start_or_restart(symbol, interval)
                    except asyncio.CancelledError:
                        pass

                except Exception as e:
                    log.warning(f"[STREAM] start_or_restart error for {symbol}/{interval}: {e}")

            if to_ack:
                await redis.xack(stream, group, *to_ack)

        except Exception as e:
            log.error(f"[STREAM] iv4_inserted loop error: {e}", exc_info=True)
            await asyncio.sleep(2)