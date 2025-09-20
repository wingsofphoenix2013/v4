# indicator_position_snapshot.py — воркер снимка индикаторов на момент открытия позиции (этап 2: m5+m15+h1, param_type=indicator)

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

# 🔸 Константы стримов и настроек
POSITIONS_OPEN_STREAM = "positions_open_stream"      # входной стрим внешнего модуля (открытие позиции)

IND_REQ_STREAM   = "indicator_request"               # on-demand запросы индикаторов
IND_RESP_STREAM  = "indicator_response"              # on-demand ответы индикаторов (общий стрим системы)
RESP_GROUP       = "ips_resp_group"                  # наша consumer-group на ответах (только этот воркер)
RESP_CONSUMER    = "ips_resp_consumer_1"

IPS_GROUP        = "ips_group"                       # consumer-group для позиций
IPS_CONSUMER     = "ips_consumer_1"

# 🔸 Тайминги/ограничения
READ_BLOCK_MS             = 1500                     # блокирующее чтение из стримов (мс)
REQ_RESPONSE_TIMEOUT_MS   = 5000                     # общий таймаут ожидания ответа (мс) на один запрос индикатора
SECOND_TRY_TIMEOUT_MS     = 3000                     # таймаут второй попытки (мс) при первичном timeout
PARALLEL_REQUESTS_LIMIT   = 24                       # одновременные запросы on-demand
BATCH_INSERT_MAX          = 500                      # макс. размер пачки для INSERT

# 🔸 Таймфреймы и таймшаги (этап 2 — m5, m15, h1)
TF_ORDER = ["m5", "m15", "h1"]
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
STEP_MS  = {k: v * 60_000 for k, v in STEP_MIN.items()}

# 🔸 Параметры роутера ответов
PENDING_TTL_SEC           = 15                       # TTL для «ожидающих» сообщений (в памяти)
XAUTOCLAIM_MIN_IDLE_MS    = 2000                     # сообщения старше этого idle считаем «протухшими» и забираем
XAUTOCLAIM_BATCH          = 200                      # за раз подбирать не больше N сообщений
PENDING_MAX_IN_MEMORY     = 20000                    # мягкий лимит на буфер pending

# 🔸 Логгер
log = logging.getLogger("IND_POSSTAT")


# 🔸 Утилиты времени/парсинга
def floor_to_bar(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step

def parse_iso_to_ms(iso_str: str) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(iso_str)  # UTC-naive ISO ожидается системой
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def to_float_safe(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None

# 🔸 Нормализация базового имени для param_type='indicator'
def indicator_base_from_param_name(param_name: str) -> str:
    """
    В param_base для indicators пишем укороченный тип без длины:
      ema21 -> ema, rsi14 -> rsi, atr14 -> atr, kama30 -> kama,
      bb20_2_0_upper -> bb, macd12_macd -> macd, adx_dmi21_plus_di -> adx_dmi, lr50_angle -> lr.
    """
    if param_name.startswith("adx_dmi"):
        return "adx_dmi"
    for prefix in ("ema", "rsi", "mfi", "atr", "kama", "macd", "bb", "lr"):
        if param_name.startswith(prefix):
            return prefix
    return param_name.split("_", 1)[0]

def indicator_base_from_instance(inst: Dict[str, Any]) -> str:
    ind = str(inst.get("indicator", "indicator"))
    return "adx_dmi" if ind.startswith("adx_dmi") else ind


# 🔸 Роутер ответов indicator_response (гарантированная доставка без потерь)
class IndicatorResponseRouter:
    """
    Один фоновой читатель XREADGROUP по IND_RESP_STREAM/RESP_GROUP.
    Диспетчеризует сообщения по req_id в asyncio.Queue.
    «Неизвестные» req_id не ACK-аем; складываем в pending и отдаём при register(req_id), после чего ACK.
    Периодически забираем «протухшие» сообщения через XAUTOCLAIM.
    """
    def __init__(self, redis):
        self.redis = redis
        self._queues: Dict[str, asyncio.Queue] = {}                 # req_id -> Queue
        self._pending: Dict[str, Tuple[str, Dict[str, Any], float]] = {}  # req_id -> (msg_id, data, put_ts)
        self._reader_task: Optional[asyncio.Task] = None
        self._reclaimer_task: Optional[asyncio.Task] = None
        self._janitor_task: Optional[asyncio.Task] = None
        self._started = False
        self._lock = asyncio.Lock()

    async def start(self):
        if self._started:
            return
        # создать группу (идемпотентно)
        try:
            await self.redis.xgroup_create(IND_RESP_STREAM, RESP_GROUP, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                log.warning(f"xgroup_create (resp) error: {e}")
        self._reader_task   = asyncio.create_task(self._reader_loop())
        self._reclaimer_task= asyncio.create_task(self._reclaim_loop())
        self._janitor_task  = asyncio.create_task(self._janitor_loop())
        self._started = True
        log.debug("IND_POSSTAT: response router started")

    async def _reader_loop(self):
        while True:
            try:
                resp = await self.redis.xreadgroup(
                    groupname=RESP_GROUP,
                    consumername=RESP_CONSUMER,
                    streams={IND_RESP_STREAM: ">"},
                    count=200,
                    block=READ_BLOCK_MS
                )
                if not resp:
                    continue

                ack_ids: List[str] = []
                now_mono = asyncio.get_event_loop().time()

                for _, messages in resp:
                    for msg_id, data in messages:
                        req_id = data.get("req_id")
                        if not req_id:
                            ack_ids.append(msg_id)
                            continue

                        q = self._queues.get(req_id)
                        if q is not None:
                            try:
                                q.put_nowait((msg_id, data))
                                ack_ids.append(msg_id)
                            except Exception:
                                self._pending[req_id] = (msg_id, data, now_mono)
                        else:
                            if len(self._pending) < PENDING_MAX_IN_MEMORY:
                                self._pending[req_id] = (msg_id, data, now_mono)
                            else:
                                log.warning("IND_POSSTAT: pending buffer full, buffered without ACK; size=%d", len(self._pending))
                                self._pending[req_id] = (msg_id, data, now_mono)

                if ack_ids:
                    await self.redis.xack(IND_RESP_STREAM, RESP_GROUP, *ack_ids)

            except Exception as e:
                log.error(f"IND_POSSTAT: resp router read error: {e}", exc_info=True)
                await asyncio.sleep(0.3)

    async def _reclaim_loop(self):
        last_id = "0-0"
        while True:
            try:
                claimed = await self.redis.execute_command(
                    "XAUTOCLAIM", IND_RESP_STREAM, RESP_GROUP, RESP_CONSUMER,
                    XAUTOCLAIM_MIN_IDLE_MS, last_id, "COUNT", XAUTOCLAIM_BATCH
                )
                next_id, entries = claimed[0], claimed[1]
                now_mono = asyncio.get_event_loop().time()
                for msg_id, data in entries:
                    req_id = data.get("req_id")
                    if not req_id:
                        await self.redis.xack(IND_RESP_STREAM, RESP_GROUP, msg_id)
                        continue
                    if req_id in self._queues:
                        try:
                            self._queues[req_id].put_nowait((msg_id, data))
                            await self.redis.xack(IND_RESP_STREAM, RESP_GROUP, msg_id)
                        except Exception:
                            self._pending[req_id] = (msg_id, data, now_mono)
                    else:
                        self._pending[req_id] = (msg_id, data, now_mono)

                last_id = next_id or last_id
            except Exception as e:
                log.warning(f"IND_POSSTAT: resp router reclaim error: {e}", exc_info=True)
            finally:
                await asyncio.sleep(1.0)

    async def _janitor_loop(self):
        while True:
            try:
                now_mono = asyncio.get_event_loop().time()
                stale = [rid for rid, (_, _, put_ts) in self._pending.items()
                         if now_mono - put_ts > PENDING_TTL_SEC]
                if stale:
                    log.warning("IND_POSSTAT: pending entries stale=%d (kept unacked, waiting for register)", len(stale))
            except Exception:
                pass
            finally:
                await asyncio.sleep(5.0)

    async def register(self, req_id: str) -> asyncio.Queue:
        async with self._lock:
            q = asyncio.Queue(maxsize=1)
            self._queues[req_id] = q
            pending = self._pending.pop(req_id, None)
            if pending:
                msg_id, data, _ = pending
                try:
                    q.put_nowait((msg_id, data))
                    await self.redis.xack(IND_RESP_STREAM, RESP_GROUP, msg_id)
                except Exception:
                    self._pending[req_id] = (msg_id, data, asyncio.get_event_loop().time())
            return q

    async def unregister(self, req_id: str):
        async with self._lock:
            self._queues.pop(req_id, None)


# 🔸 Отправка on-demand запроса и получение ответа через общий роутер (с 1 ретраем)
async def request_indicator_snapshot(
    redis,
    router: IndicatorResponseRouter,
    *,
    symbol: str,
    timeframe: str,
    instance_id: int,
    timestamp_ms: int
) -> Tuple[str, Dict[str, Any]]:
    async def one_try(wait_ms: int) -> Tuple[str, Dict[str, Any]]:
        fields = {
            "symbol": symbol,
            "timeframe": timeframe,
            "instance_id": str(instance_id),
            "timestamp_ms": str(timestamp_ms),
        }
        try:
            req_id = await redis.xadd(IND_REQ_STREAM, fields)
        except Exception:
            log.warning("stream_error: XADD indicator_request failed", exc_info=True)
            return "stream_error", {}

        q = await router.register(req_id)
        try:
            try:
                msg_id, data = await asyncio.wait_for(q.get(), timeout=wait_ms / 1000.0)
            except asyncio.TimeoutError:
                return "timeout", {}

            status = data.get("status", "error")
            if status != "ok":
                return data.get("error", "exception"), {}

            try:
                open_time = data.get("open_time") or ""
                results_raw = data.get("results") or "{}"
                results = json.loads(results_raw)
                return "ok", {"open_time": open_time, "results": results}
            except Exception:
                return "exception", {}
        finally:
            await router.unregister(req_id)

    st, pl = await one_try(REQ_RESPONSE_TIMEOUT_MS)
    if st != "timeout":
        return st, pl
    log.info("IND_POSSTAT: retry on timeout for instance_id=%s symbol=%s tf=%s", instance_id, symbol, timeframe)
    return await one_try(SECOND_TRY_TIMEOUT_MS)


# 🔸 Сбор строк для одного инстанса индикатора (общая логика для всех TF)
async def build_rows_for_instance(
    redis,
    router: IndicatorResponseRouter,
    *,
    symbol: str,
    tf: str,
    instance: Dict[str, Any],
    bar_open_ms: int,
    strategy_id: int,
    position_uid: str
) -> List[Tuple]:
    instance_id = int(instance["id"])
    status, payload = await request_indicator_snapshot(
        redis,
        router,
        symbol=symbol,
        timeframe=tf,
        instance_id=instance_id,
        timestamp_ms=bar_open_ms
    )

    rows: List[Tuple] = []

    if status != "ok":
        base_short = indicator_base_from_instance(instance)
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "indicator", base_short, base_short,
            None, status,
            open_time_dt, "error", status
        ))
        return rows

    ot_raw = payload.get("open_time")
    try:
        open_time_dt = datetime.fromisoformat(ot_raw) if ot_raw else datetime.utcfromtimestamp(bar_open_ms / 1000)
    except Exception:
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)

    results: Dict[str, str] = payload.get("results", {})
    for param_name, str_value in results.items():
        base_short = indicator_base_from_param_name(param_name)
        fval = to_float_safe(str_value)
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "indicator", base_short, param_name,
            fval if fval is not None else None,
            None if fval is not None else str_value,
            open_time_dt, "ok", None
        ))
    return rows


# 🔸 Пакетная запись в PostgreSQL
async def run_insert_batch(pg, rows: List[Tuple]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO indicator_position_stat
        (position_uid, strategy_id, symbol, timeframe, param_type, param_base, param_name, value_num, value_text, open_time, status, error_code)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
        DO UPDATE SET
          value_num   = EXCLUDED.value_num,
          value_text  = EXCLUDED.value_text,
          open_time   = EXCLUDED.open_time,
          status      = EXCLUDED.status,
          error_code  = EXCLUDED.error_code,
          captured_at = NOW()
    """
    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(sql, rows)


# 🔸 Основной воркер снимка (этап 2: m5 → m15/h1, все param_type=indicator)
async def run_indicator_position_snapshot(pg, redis, get_instances_by_tf):
    log.info("IND_POSSTAT: воркер запущен (phase=2 indicators m5+m15+h1)")

    # создать consumer-group для позиций (идемпотентно)
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, IPS_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create (positions) error: {e}")

    # поднять роутер ответов и его consumer-group
    router = IndicatorResponseRouter(redis)
    await router.start()

    sem = asyncio.Semaphore(PARALLEL_REQUESTS_LIMIT)

    async def process_tf(tf: str, position_uid: str, strategy_id: int, symbol: str, bar_open_ms: int) -> int:
        # собрать инстансы по TF
        instances = [i for i in get_instances_by_tf(tf)]
        if not instances:
            log.info(f"IND_POSSTAT: no_instances_{tf} symbol={symbol}")
            return 0

        # параллельная обработка инстансов с лимитом
        rows_all: List[Tuple] = []

        async def run_one(inst):
            async with sem:
                try:
                    r = await build_rows_for_instance(
                        redis, router,
                        symbol=symbol, tf=tf, instance=inst,
                        bar_open_ms=bar_open_ms,
                        strategy_id=strategy_id,
                        position_uid=position_uid
                    )
                    return r
                except Exception:
                    log.warning(f"IND_POSSTAT: exception in build_rows_for_instance tf={tf}", exc_info=True)
                    base_short = indicator_base_from_instance(inst)
                    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                    return [(
                        position_uid, strategy_id, symbol, tf,
                        "indicator", base_short, base_short,
                        None, "exception",
                        open_time_dt, "error", "exception"
                    )]

        tasks = [asyncio.create_task(run_one(inst)) for inst in instances]
        results_batches = await asyncio.gather(*tasks, return_exceptions=False)
        for batch in results_batches:
            rows_all.extend(batch)

        # запись пачками
        for i in range(0, len(rows_all), BATCH_INSERT_MAX):
            await run_insert_batch(pg, rows_all[i:i + BATCH_INSERT_MAX])

        return len(rows_all)

    async def process_position(msg_id: str, data: Dict[str, Any]) -> Optional[str]:
        # условия достаточности
        position_uid = data.get("position_uid")
        strategy_id_s = data.get("strategy_id")
        symbol = data.get("symbol")
        created_at_iso = data.get("created_at")

        if not position_uid or not strategy_id_s or not symbol or not created_at_iso:
            log.info(f"IND_POSSTAT: bad_event msg_id={msg_id} data_keys={list(data.keys())}")
            return msg_id

        try:
            strategy_id = int(strategy_id_s)
        except Exception:
            strategy_id = 0

        ts_ms = parse_iso_to_ms(created_at_iso)
        if ts_ms is None:
            log.info(f"IND_POSSTAT: bad_event_time position_uid={position_uid}")
            return msg_id

        total_rows = 0

        # m5 — сначала (приоритет)
        tf = "m5"
        t0 = asyncio.get_event_loop().time()
        rows_m5 = await process_tf(tf, position_uid, strategy_id, symbol, floor_to_bar(ts_ms, tf))
        total_rows += rows_m5
        t1 = asyncio.get_event_loop().time()
        log.info(f"IND_POSSTAT: {tf} indicators done position_uid={position_uid} symbol={symbol} rows={rows_m5} elapsed_ms={int((t1-t0)*1000)}")

        # m15 и h1 — потом (параллельно)
        async def run_tf(tf2: str):
            t_start = asyncio.get_event_loop().time()
            rows = await process_tf(tf2, position_uid, strategy_id, symbol, floor_to_bar(ts_ms, tf2))
            t_end = asyncio.get_event_loop().time()
            log.info(f"IND_POSSTAT: {tf2} indicators done position_uid={position_uid} symbol={symbol} rows={rows} elapsed_ms={int((t_end-t_start)*1000)}")
            return rows

        rows_m15, rows_h1 = await asyncio.gather(run_tf("m15"), run_tf("h1"))
        total_rows += rows_m15 + rows_h1

        # итог по позиции
        log.info(f"IND_POSSTAT: indicators all TF done position_uid={position_uid} symbol={symbol} total_rows={total_rows}")

        return msg_id

    # 🔸 Основной цикл чтения позиций: пачкой + параллельная обработка + батч-ACK
    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=IPS_GROUP,
                consumername=IPS_CONSUMER,
                streams={POSITIONS_OPEN_STREAM: ">"},
                count=100,
                block=2000
            )
        except Exception as e:
            log.error(f"IND_POSSTAT: read error: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            continue

        if not resp:
            continue

        try:
            tasks = []
            for _, messages in resp:
                for msg_id, data in messages:
                    tasks.append(asyncio.create_task(process_position(msg_id, data)))

            done_ids = await asyncio.gather(*tasks, return_exceptions=False)
            ack_ids = [mid for mid in done_ids if mid]
            if ack_ids:
                await redis.xack(POSITIONS_OPEN_STREAM, IPS_GROUP, *ack_ids)
        except Exception as e:
            log.error(f"IND_POSSTAT: batch error: {e}", exc_info=True)
            await asyncio.sleep(0.5)