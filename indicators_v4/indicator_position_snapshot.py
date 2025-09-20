# indicator_position_snapshot.py — воркер снимка признаков при открытии позиции (этап 4: m5+m15+h1; param_type=indicator + pack + marketwatch)

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

# 🔸 Константы стримов и настроек
POSITIONS_OPEN_STREAM = "positions_open_stream"          # входной стрим внешнего модуля (открытие позиции)

# on-demand indicators
IND_REQ_STREAM   = "indicator_request"
IND_RESP_STREAM  = "indicator_response"

# on-demand packs (gateway)
GW_REQ_STREAM    = "indicator_gateway_request"
GW_RESP_STREAM   = "indicator_gateway_response"

IPS_GROUP        = "ips_group"
IPS_CONSUMER     = "ips_consumer_1"

# 🔸 Тайминги/ограничения
READ_BLOCK_MS             = 1500       # блокирующее чтение XREAD (мс)
REQ_RESPONSE_TIMEOUT_MS   = 5000       # таймаут ожидания ответа (мс)
SECOND_TRY_TIMEOUT_MS     = 3000       # таймаут второй попытки (мс) при первичном timeout
PARALLEL_REQUESTS_LIMIT   = 30         # одновременные запросы on-demand (индикаторы/пакеты суммарно)
BATCH_INSERT_MAX          = 500        # макс. размер пачки для INSERT

# 🔸 Таймфреймы и шаги
TF_ORDER = ["m5", "m15", "h1"]
STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}
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
    Для indicators в param_base пишем укороченный тип без длины:
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

# 🔸 Отправка on-demand запроса индикатора и ожидание ответа (бездедлайново)
async def request_indicator_snapshot(redis, *, symbol: str, timeframe: str, instance_id: int, timestamp_ms: int) -> Tuple[str, Dict[str, Any]]:
    # читаем только новые ответы (снимок хвоста до запроса)
    start_id = "$"
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

    last_id = start_id
    while True:
        try:
            resp = await redis.xread({IND_RESP_STREAM: last_id}, block=READ_BLOCK_MS, count=200)
        except Exception:
            log.warning("stream_error: XREAD indicator_response failed", exc_info=True)
            continue  # просто продолжаем ждать

        if not resp:
            continue

        for _, messages in resp:
            for msg_id, data in messages:
                last_id = msg_id
                if data.get("req_id") != req_id:
                    continue
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

# 🔸 Отправка on-demand запроса pack (gateway) и ожидание ответа (бездедлайново)
async def request_pack(redis, *, symbol: str, timeframe: str, indicator: str, timestamp_ms: int, length: Optional[int] = None, std: Optional[str] = None) -> Tuple[str, List[Dict[str, Any]]]:
    start_id = "$"
    fields = {
        "symbol": symbol,
        "timeframe": timeframe,
        "indicator": indicator,
        "timestamp_ms": str(timestamp_ms),
    }
    if length is not None:
        fields["length"] = str(length)
    if std is not None:
        fields["std"] = std

    try:
        req_id = await redis.xadd(GW_REQ_STREAM, fields)
    except Exception:
        log.warning("stream_error: XADD indicator_gateway_request failed", exc_info=True)
        return "stream_error", []

    last_id = start_id
    while True:
        try:
            resp = await redis.xread({GW_RESP_STREAM: last_id}, block=READ_BLOCK_MS, count=200)
        except Exception:
            log.warning("stream_error: XREAD indicator_gateway_response failed", exc_info=True)
            continue  # просто ждём дальше

        if not resp:
            continue

        for _, messages in resp:
            for msg_id, data in messages:
                last_id = msg_id
                if data.get("req_id") != req_id:
                    continue
                status = data.get("status", "error")
                if status != "ok":
                    return data.get("error", "exception"), []
                try:
                    results_raw = data.get("results") or "[]"
                    results = json.loads(results_raw)
                    return "ok", results if isinstance(results, list) else []
                except Exception:
                    return "exception", []

# 🔸 Словарь result-полей для packs (только результат, без служебного)
PACK_FIELDS: Dict[str, List[str]] = {
    "rsi":       ["value", "bucket_low", "trend"],
    "mfi":       ["value", "bucket_low", "trend"],
    "ema":       ["dist_pct", "side", "dynamic"],
    "bb":        ["bucket", "bucket_delta", "bw_trend_smooth"],
    "lr":        ["bucket", "bucket_delta", "angle_trend", "angle"],
    "atr":       ["value_pct", "bucket", "bucket_delta"],
    "adx_dmi":   ["adx_bucket_low", "adx_dynamic_smooth", "gap_bucket_low", "gap_dynamic_smooth"],
    "macd":      ["mode", "cross", "zero_side", "hist_bucket_low_pct", "hist_trend_smooth"],
}

# 🔸 MarketWatch виды (пишем только итоговое состояние state)
MARKETWATCH_KINDS = ["trend", "volatility", "momentum", "extremes"]


# 🔸 Сбор строк для одного инстанса индикатора (indicator)
async def build_rows_for_indicator_instance(
    redis,
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


# 🔸 Сбор строк для одного вида pack (gateway)
async def build_rows_for_pack_kind(
    redis,
    *,
    symbol: str,
    tf: str,
    kind: str,                # rsi|mfi|ema|bb|lr|atr|adx_dmi|macd
    bar_open_ms: int,
    strategy_id: int,
    position_uid: str
) -> List[Tuple]:
    status, results = await request_pack(
        redis,
        symbol=symbol,
        timeframe=tf,
        indicator=kind,
        timestamp_ms=bar_open_ms
    )

    rows: List[Tuple] = []
    fields = PACK_FIELDS.get(kind, [])

    if status != "ok":
        open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "pack", kind, kind,
            None, status,
            open_time_dt, "error", status
        ))
        return rows

    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
    for item in results:
        base = str(item.get("base") or kind)   # packs: полный base (ema21, bb20_2_0, macd12, ...)
        pack = item.get("pack") or {}
        if not isinstance(pack, dict):
            continue
        for name in fields:
            if name not in pack:
                continue
            sval = str(pack[name])
            fval = to_float_safe(sval)
            rows.append((
                position_uid, strategy_id, symbol, tf,
                "pack", base, name,
                fval if fval is not None else None,
                None if fval is not None else sval,
                open_time_dt, "ok", None
            ))

    if not rows:
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "pack", kind, kind,
            None, "no_results",
            open_time_dt, "error", "no_results"
        ))
    return rows


# 🔸 Сбор строк для одного вида MarketWatch (gateway; пишем только state)
async def build_rows_for_mw_kind(
    redis,
    *,
    symbol: str,
    tf: str,
    kind: str,                # trend|volatility|momentum|extremes
    bar_open_ms: int,
    strategy_id: int,
    position_uid: str
) -> List[Tuple]:
    status, results = await request_pack(
        redis,
        symbol=symbol,
        timeframe=tf,
        indicator=kind,
        timestamp_ms=bar_open_ms
    )

    rows: List[Tuple] = []
    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)

    if status != "ok":
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "marketwatch", kind, "state",
            None, status,
            open_time_dt, "error", status
        ))
        return rows

    # ожидаем results как список из одного элемента {"base": kind, "pack": {"state": "...", ...}}
    wrote = False
    for item in results:
        base = str(item.get("base") or kind)
        pack = item.get("pack") or {}
        if isinstance(pack, dict) and "state" in pack:
            state_val = str(pack["state"])
            rows.append((
                position_uid, strategy_id, symbol, tf,
                "marketwatch", base, "state",
                None, state_val,
                open_time_dt, "ok", None
            ))
            wrote = True

    if not wrote:
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "marketwatch", kind, "state",
            None, "no_results",
            open_time_dt, "error", "no_results"
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


# 🔸 Основной воркер снимка (этап 4: indicators + packs + marketwatch по всем TF; m5 приоритет)
async def run_indicator_position_snapshot(pg, redis, get_instances_by_tf):
    log.info("IND_POSSTAT: воркер запущен (phase=4 indicators+packs+marketwatch m5+m15+h1)")

    # создать consumer-group для позиций (идемпотентно)
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, IPS_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create (positions) error: {e}")

    sem = asyncio.Semaphore(PARALLEL_REQUESTS_LIMIT)

    async def process_indicators_tf(tf: str, position_uid: str, strategy_id: int, symbol: str, bar_open_ms: int) -> int:
        instances = [i for i in get_instances_by_tf(tf)]
        if not instances:
            log.info(f"IND_POSSTAT: no_instances_{tf} symbol={symbol}")
            return 0

        rows_all: List[Tuple] = []

        async def run_one(inst):
            async with sem:
                try:
                    return await build_rows_for_indicator_instance(
                        redis,
                        symbol=symbol, tf=tf, instance=inst,
                        bar_open_ms=bar_open_ms,
                        strategy_id=strategy_id,
                        position_uid=position_uid
                    )
                except Exception:
                    log.warning(f"IND_POSSTAT: exception in build_rows_for_indicator_instance tf={tf}", exc_info=True)
                    base_short = indicator_base_from_instance(inst)
                    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                    return [(
                        position_uid, strategy_id, symbol, tf,
                        "indicator", base_short, base_short,
                        None, "exception",
                        open_time_dt, "error", "exception"
                    )]

        tasks = [asyncio.create_task(run_one(inst)) for inst in instances]
        for batch in await asyncio.gather(*tasks, return_exceptions=False):
            rows_all.extend(batch)

        for i in range(0, len(rows_all), BATCH_INSERT_MAX):
            await run_insert_batch(pg, rows_all[i:i + BATCH_INSERT_MAX])

        return len(rows_all)

    async def process_packs_tf(tf: str, position_uid: str, strategy_id: int, symbol: str, bar_open_ms: int) -> int:
        kinds = ["rsi", "mfi", "ema", "bb", "lr", "atr", "adx_dmi", "macd"]
        rows_all: List[Tuple] = []

        async def run_one(kind: str):
            async with sem:
                try:
                    return await build_rows_for_pack_kind(
                        redis,
                        symbol=symbol, tf=tf, kind=kind,
                        bar_open_ms=bar_open_ms,
                        strategy_id=strategy_id,
                        position_uid=position_uid
                    )
                except Exception:
                    log.warning(f"IND_POSSTAT: exception in build_rows_for_pack_kind tf={tf} kind={kind}", exc_info=True)
                    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                    return [(
                        position_uid, strategy_id, symbol, tf,
                        "pack", kind, kind,
                        None, "exception",
                        open_time_dt, "error", "exception"
                    )]

        tasks = [asyncio.create_task(run_one(k)) for k in kinds]
        for batch in await asyncio.gather(*tasks, return_exceptions=False):
            rows_all.extend(batch)

        for i in range(0, len(rows_all), BATCH_INSERT_MAX):
            await run_insert_batch(pg, rows_all[i:i + BATCH_INSERT_MAX])

        return len(rows_all)

    async def process_mw_tf(tf: str, position_uid: str, strategy_id: int, symbol: str, bar_open_ms: int) -> int:
        kinds = MARKETWATCH_KINDS
        rows_all: List[Tuple] = []

        async def run_one(kind: str):
            async with sem:
                try:
                    return await build_rows_for_mw_kind(
                        redis,
                        symbol=symbol, tf=tf, kind=kind,
                        bar_open_ms=bar_open_ms,
                        strategy_id=strategy_id,
                        position_uid=position_uid
                    )
                except Exception:
                    log.warning(f"IND_POSSTAT: exception in build_rows_for_mw_kind tf={tf} kind={kind}", exc_info=True)
                    open_time_dt = datetime.utcfromtimestamp(bar_open_ms / 1000)
                    return [(
                        position_uid, strategy_id, symbol, tf,
                        "marketwatch", kind, "state",
                        None, "exception",
                        open_time_dt, "error", "exception"
                    )]

        tasks = [asyncio.create_task(run_one(k)) for k in kinds]
        for batch in await asyncio.gather(*tasks, return_exceptions=False):
            rows_all.extend(batch)

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

        # m5 — приоритет: indicators + packs + marketwatch
        tf = "m5"
        t0 = asyncio.get_event_loop().time()
        b_m5 = floor_to_bar(ts_ms, tf)
        rows_m5_ind = await process_indicators_tf(tf, position_uid, strategy_id, symbol, b_m5)
        rows_m5_pack = await process_packs_tf(tf, position_uid, strategy_id, symbol, b_m5)
        rows_m5_mw  = await process_mw_tf(tf, position_uid, strategy_id, symbol, b_m5)
        total_rows += rows_m5_ind + rows_m5_pack + rows_m5_mw
        t1 = asyncio.get_event_loop().time()
        log.info(f"IND_POSSTAT: {tf} indicators+packs+mw done position_uid={position_uid} symbol={symbol} rows={rows_m5_ind + rows_m5_pack + rows_m5_mw} elapsed_ms={int((t1-t0)*1000)}")

        # m15 и h1 — затем (параллельно; для каждого TF считаем indicators + packs + mw)
        async def run_tf(tf2: str):
            t_start = asyncio.get_event_loop().time()
            b_tf = floor_to_bar(ts_ms, tf2)
            rows_ind = await process_indicators_tf(tf2, position_uid, strategy_id, symbol, b_tf)
            rows_pack = await process_packs_tf(tf2, position_uid, strategy_id, symbol, b_tf)
            rows_mw  = await process_mw_tf(tf2, position_uid, strategy_id, symbol, b_tf)
            t_end = asyncio.get_event_loop().time()
            rows_sum = rows_ind + rows_pack + rows_mw
            log.info(f"IND_POSSTAT: {tf2} indicators+packs+mw done position_uid={position_uid} symbol={symbol} rows={rows_sum} elapsed_ms={int((t_end-t_start)*1000)}")
            return rows_sum

        rows_m15, rows_h1 = await asyncio.gather(run_tf("m15"), run_tf("h1"))
        total_rows += rows_m15 + rows_h1

        log.info(f"IND_POSSTAT: all TF indicators+packs+mw done position_uid={position_uid} symbol={symbol} total_rows={total_rows}")
        return msg_id

    # 🔸 Основной цикл чтения позиций: пачкой + параллельная обработка + батч-ACK
    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=IPS_GROUP,
                consumername=IPS_CONSUMER,
                streams={POSITIONS_OPEN_STREAM: ">"},
                count=100,
                block=1000
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