# indicator_position_snapshot.py — воркер снимка индикаторов на момент открытия позиции (этап 1: m5, param_type=indicator)

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
REQ_RESPONSE_TIMEOUT_MS   = 3500                     # общий таймаут ожидания ответа (мс) на один запрос индикатора
PARALLEL_REQUESTS_LIMIT   = 20                       # одновременные запросы on-demand
BATCH_INSERT_MAX          = 500                      # макс. размер пачки для INSERT

# 🔸 Таймшаги TF (этап 1 — только m5)
STEP_MIN = {"m5": 5}
STEP_MS  = {k: v * 60_000 for k, v in STEP_MIN.items()}

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
    Правило: в param_base для indicators пишем укороченный тип без длины:
      ema21 -> ema, rsi14 -> rsi, atr14 -> atr, kama30 -> kama,
      bb20_2_0_upper -> bb, macd12_macd -> macd, adx_dmi21_plus_di -> adx_dmi, lr50_angle -> lr.
    """
    if param_name.startswith("adx_dmi"):
        return "adx_dmi"
    for prefix in ("ema", "rsi", "mfi", "atr", "kama", "macd", "bb", "lr"):
        if param_name.startswith(prefix):
            return prefix
    # fallback: до первого '_' или целиком
    return param_name.split("_", 1)[0]

def indicator_base_from_instance(inst: Dict[str, Any]) -> str:
    # аналогично — без длины; специальное имя для adx_dmi
    ind = str(inst.get("indicator", "indicator"))
    return "adx_dmi" if ind.startswith("adx_dmi") else ind


# 🔸 Роутер ответов indicator_response (исключаем таймауты и гонки)
class IndicatorResponseRouter:
    """
    Один фоновой читатель XREADGROUP по IND_RESP_STREAM/RESP_GROUP.
    Диспетчеризует сообщения по req_id в asyncio.Queue. Мы ACK-аем только обслуженные сообщения.
    Предполагается, что в этой группе читаем ответы только на собственные запросы воркера.
    """
    def __init__(self, redis):
        self.redis = redis
        self._queues: Dict[str, asyncio.Queue] = {}
        self._task: Optional[asyncio.Task] = None
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
        self._task = asyncio.create_task(self._reader_loop())
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
                for _, messages in resp:
                    for msg_id, data in messages:
                        req_id = data.get("req_id")
                        if not req_id:
                            # нет корреляции — ACK и пропустить
                            ack_ids.append(msg_id)
                            continue
                        q = self._queues.get(req_id)
                        if q is None:
                            # запрос уже отождался/отменился — ACK и пропустить
                            ack_ids.append(msg_id)
                            continue
                        try:
                            q.put_nowait((msg_id, data))
                        except Exception:
                            # если очередь переполнена/закрыта — ACK и убрать регистрацию
                            ack_ids.append(msg_id)
                            self._queues.pop(req_id, None)

                if ack_ids:
                    await self.redis.xack(IND_RESP_STREAM, RESP_GROUP, *ack_ids)

            except Exception as e:
                log.error(f"IND_POSSTAT: resp router read error: {e}", exc_info=True)
                await asyncio.sleep(0.3)

    async def register(self, req_id: str) -> asyncio.Queue:
        # регистрируем очередь ожидания под конкретный req_id
        async with self._lock:
            q = asyncio.Queue(maxsize=1)
            self._queues[req_id] = q
            return q

    async def unregister(self, req_id: str):
        async with self._lock:
            self._queues.pop(req_id, None)


# 🔸 Отправка on-demand запроса и получение ответа через общий роутер
async def request_indicator_snapshot(redis, router: IndicatorResponseRouter, *, symbol: str, timeframe: str, instance_id: int, timestamp_ms: int) -> Tuple[str, Dict[str, Any]]:
    """
    Возвращает (status, payload), где:
      - status: "ok" или узкий код ошибки ("timeout","no_ohlcv","instance_not_active",
                "symbol_not_active","before_enabled_at","no_values","bad_request","exception","stream_error")
      - payload: при ok → {"open_time": <iso>, "results": {param_name: str_value, ...}}
    """
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

    # регистрируем свою очередь и ждём конкретный ответ
    q = await router.register(req_id)
    try:
        try:
            # общий таймаут на весь цикл ожидания
            msg_id, data = await asyncio.wait_for(q.get(), timeout=REQ_RESPONSE_TIMEOUT_MS / 1000.0)
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


# 🔸 Сбор строк для одного инстанса индикатора (этап 1: только m5)
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
    """
    Возвращает список кортежей для вставки в indicator_position_stat.
    Порядок полей кортежа соответствует VALUES ($1..$12) в run_insert_batch().
    """
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
        # при ошибке пишем одну строку с укороченной базой (без длины)
        base_short = indicator_base_from_instance(instance)
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
        rows.append((
            position_uid,                 # position_uid
            strategy_id,                  # strategy_id
            symbol,                       # symbol
            tf,                           # timeframe
            "indicator",                  # param_type
            base_short,                   # param_base (без длины)
            base_short,                   # param_name (на ошибке можно дублировать базу)
            None,                         # value_num
            status,                       # value_text (код ошибки)
            open_time_dt,                 # open_time (datetime)
            "error",                      # status
            status                        # error_code
        ))
        return rows

    # успех: open_time → datetime, разложить все пары {param_name: str_value}
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
            position_uid,                                  # position_uid
            strategy_id,                                   # strategy_id
            symbol,                                        # symbol
            tf,                                            # timeframe
            "indicator",                                   # param_type
            base_short,                                    # param_base (без длины)
            param_name,                                    # param_name (полный канон, с длиной)
            fval if fval is not None else None,            # value_num
            None if fval is not None else str_value,       # value_text
            open_time_dt,                                  # open_time (datetime)
            "ok",                                          # status
            None                                           # error_code
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


# 🔸 Основной воркер снимка (этап 1: только m5 + indicators, без таймаутов на ответах)
async def run_indicator_position_snapshot(pg, redis, get_instances_by_tf):
    log.info("IND_POSSTAT: воркер запущен (phase=1 m5/indicator)")

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

    async def process_position(msg_id: str, data: Dict[str, Any]) -> Optional[str]:
        # валидация входа
        position_uid = data.get("position_uid")
        strategy_id_s = data.get("strategy_id")
        symbol = data.get("symbol")
        created_at_iso = data.get("created_at")

        if not position_uid or not strategy_id_s or not symbol or not created_at_iso:
            log.info(f"IND_POSSTAT: bad_event msg_id={msg_id} data_keys={list(data.keys())}")
            return msg_id  # ACK мусора

        try:
            strategy_id = int(strategy_id_s)
        except Exception:
            strategy_id = 0

        ts_ms = parse_iso_to_ms(created_at_iso)
        if ts_ms is None:
            log.info(f"IND_POSSTAT: bad_event_time position_uid={position_uid}")
            return msg_id

        # bar_open для m5
        tf = "m5"
        bar_open_ms = floor_to_bar(ts_ms, tf)

        # собрать инстансы для m5
        instances = [i for i in get_instances_by_tf(tf)]
        if not instances:
            log.info(f"IND_POSSTAT: no_instances_m5 symbol={symbol}")
            return msg_id

        # параллельная обработка инстансов с лимитом
        t0 = asyncio.get_event_loop().time()
        rows_all: List[Tuple] = []

        async def run_one(inst):
            async with sem:
                try:
                    r = await build_rows_for_instance(
                        redis,
                        router,
                        symbol=symbol,
                        tf=tf,
                        instance=inst,
                        bar_open_ms=bar_open_ms,
                        strategy_id=strategy_id,
                        position_uid=position_uid
                    )
                    return r
                except Exception:
                    log.warning("IND_POSSTAT: exception in build_rows_for_instance", exc_info=True)
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

        # запись пачкой (частями, если очень много)
        for i in range(0, len(rows_all), BATCH_INSERT_MAX):
            await run_insert_batch(pg, rows_all[i:i + BATCH_INSERT_MAX])

        t1 = asyncio.get_event_loop().time()
        log.info(
            f"IND_POSSTAT: m5 indicators done position_uid={position_uid} "
            f"symbol={symbol} rows={len(rows_all)} elapsed_ms={int((t1 - t0) * 1000)}"
        )

        return msg_id

    # основной цикл чтения позиций: пачкой + параллельная обработка + батч-ACK
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