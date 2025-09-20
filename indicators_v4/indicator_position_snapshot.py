# indicator_position_snapshot.py — воркер снимка индикаторов на момент открытия позиции (этап 1: m5, param_type=indicator)

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

# 🔸 Импорты инфраструктуры
# (воркер встраивается в общий оркестратор через oracle_v4_main.py / indicators_v4_main.py)
# from infra import run_safe_loop  # не требуется в этом модуле напрямую

# 🔸 Константы стримов и настроек
POSITIONS_OPEN_STREAM = "positions_open_stream"   # входной стрим внешнего модуля
IND_REQ_STREAM        = "indicator_request"       # on-demand запросы индикаторов
IND_RESP_STREAM       = "indicator_response"      # on-demand ответы индикаторов

IPS_GROUP    = "ips_group"                        # consumer-group для позиций
IPS_CONSUMER = "ips_consumer_1"

# 🔸 Тайминги/ограничения
READ_BLOCK_MS            = 1500                   # ожидание в XREAD (мс)
REQ_RESPONSE_TIMEOUT_MS  = 2000                   # общий таймаут ожидания ответа (мс) на один запрос индикатора
PARALLEL_REQUESTS_LIMIT  = 20                     # одновременные запросы on-demand
BATCH_INSERT_MAX         = 500                    # макс. размер пачки для INSERT

# 🔸 Таймшаги TF (этап 1 — только m5)
STEP_MIN = {"m5": 5}
STEP_MS  = {k: v * 60_000 for k, v in STEP_MIN.items()}

# 🔸 Логгер
log = logging.getLogger("IND_POSSTAT")


# 🔸 Вспомогательные функции времени/парсинга
def floor_to_bar(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step

def parse_iso_to_ms(iso_str: str) -> Optional[int]:
    try:
        # ожидается UTC-naive ISO; парсим как naive и считаем, что это UTC
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def derive_base_from_param_name(param_name: str) -> str:
    # если есть '_' → base = всё до последнего '_', иначе base = param_name
    # подходит для bb20_2_0_{center,upper,lower}, macd12_{macd,macd_signal,macd_hist},
    # adx_dmi14_{adx,plus_di,minus_di}, lr50_{angle,center,upper,lower}
    if "_" in param_name:
        return param_name.rsplit("_", 1)[0]
    return param_name

def to_float_safe(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None


# 🔸 Отправка on-demand запроса и ожидание ответа по req_id
async def request_indicator_snapshot(redis, *, symbol: str, timeframe: str, instance_id: int, timestamp_ms: int) -> Tuple[str, Dict[str, Any]]:
    """
    Возвращает (status, payload), где:
      - status: "ok" или код ошибки ("timeout","no_ohlcv","instance_not_active",
                "symbol_not_active","before_enabled_at","no_values","bad_request","exception","stream_error")
      - payload: при ok → {"open_time": <iso>, "results": {param_name: str_value, ...}}
    """
    # подготовка и отправка запроса
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

    # ожидание ответа по req_id
    deadline = asyncio.get_event_loop().time() + (REQ_RESPONSE_TIMEOUT_MS / 1000.0)
    last_id = "0-0"

    while True:
        timeout = max(0.0, deadline - asyncio.get_event_loop().time())
        if timeout == 0.0:
            return "timeout", {}

        try:
            resp = await redis.xread({IND_RESP_STREAM: last_id}, block=min(int(timeout * 1000), READ_BLOCK_MS), count=200)
        except Exception:
            log.warning("stream_error: XREAD indicator_response failed", exc_info=True)
            return "stream_error", {}

        if not resp:
            # таймаут блока; цикл продолжится, если общий дедлайн не вышел
            continue

        # перебираем записи, ищем нашу по req_id
        for _, messages in resp:
            for msg_id, data in messages:
                last_id = msg_id
                if data.get("req_id") != req_id:
                    continue
                status = data.get("status", "error")
                if status != "ok":
                    return data.get("error", "exception"), {}
                # успех: собираем open_time и results
                try:
                    open_time = data.get("open_time") or ""
                    results_raw = data.get("results") or "{}"
                    results = json.loads(results_raw)
                    return "ok", {"open_time": open_time, "results": results}
                except Exception:
                    return "exception", {}

        # если своя запись не найдена — продолжаем до дедлайна


# 🔸 Сбор строк для одного инстанса индикатора (этап 1: только m5)
async def build_rows_for_instance(
    redis,
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
    Структура кортежа соответствует INSERT в run_insert_batch().
    """
    instance_id = int(instance["id"])
    status, payload = await request_indicator_snapshot(
        redis,
        symbol=symbol,
        timeframe=tf,
        instance_id=instance_id,
        timestamp_ms=bar_open_ms
    )

    rows: List[Tuple] = []

    if status != "ok":
        # одна строка об ошибке по базовому имени
        # базу восстанавливаем из типа индикатора и его params
        try:
            indicator = instance["indicator"]
            params = instance.get("params", {})
            if indicator == "macd":
                base = f"macd{params.get('fast')}"
            elif indicator == "bb":
                std_raw = str(round(float(params.get("std", 2.0)), 2)).replace(".", "_")
                base = f"bb{int(params.get('length', 20))}_{std_raw}"
            elif "length" in params:
                base = f"{indicator}{int(params['length'])}"
            else:
                base = indicator
        except Exception:
            base = "unknown"

        # открытие бара — передаём в БД именно datetime, а не ISO-строку
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)

        rows.append((
            position_uid,                # position_uid
            strategy_id,                 # strategy_id
            symbol,                      # symbol
            tf,                          # timeframe
            "indicator",                 # param_type
            base,                        # param_base
            base,                        # param_name (унифицируем на base при ошибке)
            None,                        # value_num
            status,                      # value_text (код ошибки как значение)
            open_time_dt,                # open_time (datetime)
            "error",                     # status
            status                       # error_code
        ))
        return rows

    # успех: разложить все пары {param_name: str_value} в строки
    ot_raw = payload.get("open_time")
    try:
        open_time_dt = datetime.fromisoformat(ot_raw) if ot_raw else datetime.utcfromtimestamp(bar_open_ms / 1000)
    except Exception:
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)

    results: Dict[str, str] = payload.get("results", {})

    for param_name, str_value in results.items():
        base = derive_base_from_param_name(param_name)
        fval = to_float_safe(str_value)

        rows.append((
            position_uid,                                  # position_uid
            strategy_id,                                   # strategy_id
            symbol,                                        # symbol
            tf,                                            # timeframe
            "indicator",                                   # param_type
            base,                                          # param_base
            param_name,                                    # param_name
            fval if fval is not None else None,            # value_num
            None if fval is not None else str_value,       # value_text (на случай нечисловых)
            open_time_dt,                                  # open_time (datetime)
            "ok",                                          # status
            None                                           # error_code
        ))

    return rows


# 🔸 Пакетная запись в PostgreSQL
async def run_insert_batch(pg, rows: List[Tuple]) -> None:
    if not rows:
        return
    # порядок параметров должен соответствовать VALUES ($1..$12)
    sql = """
        INSERT INTO indicator_position_stat
        (position_uid, strategy_id, symbol, timeframe, param_type, param_base, param_name, value_num, value_text, open_time, status, error_code)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
        DO UPDATE SET
          value_num = EXCLUDED.value_num,
          value_text = EXCLUDED.value_text,
          open_time = EXCLUDED.open_time,
          status = EXCLUDED.status,
          error_code = EXCLUDED.error_code,
          captured_at = NOW()
    """
    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(sql, rows)


# 🔸 Основной воркер снимка (этап 1: только m5 + indicators)
async def run_indicator_position_snapshot(pg, redis, get_instances_by_tf):
    log.info("IND_POSSTAT: воркер запущен (phase=1 m5/indicator)")

    # создать consumer-group для позиций (идемпотентно)
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, IPS_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    sem = asyncio.Semaphore(PARALLEL_REQUESTS_LIMIT)

    async def process_position(msg_id: str, data: Dict[str, Any]) -> Optional[str]:
        # условия достаточности
        position_uid = data.get("position_uid")
        strategy_id_s = data.get("strategy_id")
        symbol = data.get("symbol")
        created_at_iso = data.get("created_at")

        if not position_uid or not strategy_id_s or not symbol or not created_at_iso:
            log.info(f"IND_POSSTAT: bad_event msg_id={msg_id} data_keys={list(data.keys())}")
            return msg_id  # ACK, чтобы не застревать; событийный мусор не держим

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

        # собрать инстансы на m5
        instances = [i for i in get_instances_by_tf(tf)]
        if not instances:
            log.info(f"IND_POSSTAT: no_instances_m5 symbol={symbol}")
            return msg_id

        # параллельная обработка инстансов с лимитом
        t0 = asyncio.get_event_loop().time()
        rows_all: List[Tuple] = []

        async def run_one(inst):
            # обработка одного инстанса под семафором
            async with sem:
                try:
                    r = await build_rows_for_instance(
                        redis,
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
                    # формируем единственную ошибочную строку на base инстанса
                    indicator = inst.get("indicator", "unknown")
                    params = inst.get("params", {})
                    if indicator == "macd":
                        base = f"macd{params.get('fast')}"
                    elif indicator == "bb":
                        try:
                            std_raw = str(round(float(params.get("std", 2.0)), 2)).replace(".", "_")
                        except Exception:
                            std_raw = "2_0"
                        try:
                            length_i = int(params.get("length", 20))
                        except Exception:
                            length_i = 20
                        base = f"bb{length_i}_{std_raw}"
                    elif "length" in params:
                        try:
                            base = f"{indicator}{int(params['length'])}"
                        except Exception:
                            base = indicator
                    else:
                        base = indicator
                    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                    return [(
                        position_uid, strategy_id, symbol, tf,
                        "indicator", base, base,
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

    # 🔸 Основной цикл чтения стрима: пачкой + параллельная обработка + батч-ACK
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