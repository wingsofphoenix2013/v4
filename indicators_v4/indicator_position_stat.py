# indicator_position_stat.py — воркер on-demand снимка при открытии позиции (этап 2+3+4: m5+m15+h1; индикаторы + packs + marketwatch; параллельные позиции; пер-TF лог)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Время бара (floor к началу)
from packs.pack_utils import floor_to_bar

# 🔸 Константы стримов и таблиц
POSITIONS_OPEN_STREAM = "positions_open_stream"
INDICATOR_REQ_STREAM  = "indicator_request"
INDICATOR_RESP_STREAM = "indicator_response"
GW_REQ_STREAM         = "indicator_gateway_request"
GW_RESP_STREAM        = "indicator_gateway_response"
TARGET_TABLE          = "indicator_position_stat"

# 🔸 Параметры воркера / параллелизм
REQUIRED_TFS           = ("m5", "m15", "h1")  # теперь все 3 ТФ
POLL_INTERVAL_SEC      = 1                    # частота ретраев
RESP_BLOCK_MS          = 300                  # короткий блок на чтение ответов
GLOBAL_TIMEOUT_SEC     = 600                  # 10 минут на позицию
BATCH_SIZE_POS_OPEN    = 20                   # чтение позиций
BATCH_SIZE_RESP        = 200                  # чтение ответов (indicator/gateway)
CONCURRENCY_PER_TF     = {"m5": 80, "m15": 40, "h1": 30}  # лимиты on-demand per TF (m5 приоритетнее)
POSITIONS_CONCURRENCY  = 16                   # одновременно обрабатываемых позиций

# 🔸 Пакеты и MW
PACK_INDS = ("ema", "rsi", "mfi", "bb", "lr", "atr", "adx_dmi", "macd")
MW_KINDS  = ("trend", "volatility", "momentum", "extremes")

# 🔸 Белые списки полей паков (строго как задано)
PACK_FIELD_WHITELIST = {
    "rsi":     ["bucket_low", "trend"],
    "mfi":     ["bucket_low", "trend"],
    "bb":      ["bucket", "bucket_delta", "bw_trend_strict", "bw_trend_smooth"],
    "lr":      ["bucket", "bucket_delta", "angle_trend"],
    "atr":     ["bucket", "bucket_delta"],
    "adx_dmi": ["adx_bucket_low", "adx_dynamic_strict", "adx_dynamic_smooth",
                "gap_bucket_low", "gap_dynamic_strict", "gap_dynamic_smooth"],
    "ema":     ["side", "dynamic", "dynamic_strict", "dynamic_smooth"],
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct",
                "hist_trend_strict", "hist_trend_smooth"],
}

# 🔸 Логгер
log = logging.getLogger("IND_POS_STAT")


# 🔸 Нормализация created_at → open_time бара (ms)
def to_bar_open_ms(created_at_iso: str, tf: str) -> int:
    dt = datetime.fromisoformat(created_at_iso)
    return floor_to_bar(int(dt.timestamp() * 1000), tf)


# 🔸 Парс ISO → datetime
def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


# 🔸 Подготовка строк для индикаторов (param_type='indicator')
def build_rows_for_indicator_response(position_uid: str,
                                      strategy_id: int,
                                      symbol: str,
                                      tf: str,
                                      indicator_name: str,
                                      open_time_iso: str,
                                      results_json: str) -> list[tuple]:
    rows = []
    try:
        results = json.loads(results_json)
    except Exception:
        results = {}
    if not isinstance(results, dict) or not results:
        return rows
    open_time = parse_iso(open_time_iso)
    for param_name, str_val in results.items():
        try:
            value_num = float(str_val)
        except Exception:
            continue
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "indicator", indicator_name, param_name,
            value_num, None,
            open_time,
            "ok", None
        ))
    return rows


# 🔸 Вставка пачки строк в PG (UPSERT); возвращает upsert_count
async def insert_rows_pg(pg, rows: list[tuple]) -> int:
    if not rows:
        return 0
    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(f"""
                INSERT INTO {TARGET_TABLE}
                (position_uid, strategy_id, symbol, timeframe, param_type, param_base, param_name,
                 value_num, value_text, open_time, status, error_code, captured_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, NOW())
                ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
                DO UPDATE SET
                    value_num = EXCLUDED.value_num,
                    value_text = EXCLUDED.value_text,
                    open_time = EXCLUDED.open_time,
                    status = EXCLUDED.status,
                    error_code = EXCLUDED.error_code,
                    captured_at = NOW()
            """, rows)
    return len(rows)


# 🔸 Подсчёт уникальных строк по позиции и/или ТФ
async def count_unique_rows(pg, position_uid: str, tf: str | None = None) -> int:
    async with pg.acquire() as conn:
        if tf is None:
            rec = await conn.fetchrow(
                f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE} WHERE position_uid = $1", position_uid
            )
        else:
            rec = await conn.fetchrow(
                f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE} WHERE position_uid = $1 AND timeframe = $2",
                position_uid, tf
            )
        return int(rec["cnt"]) if rec else 0


# 🔸 Антидубли: локальная дедупликация кортежей по уникальному ключу
def dedup_rows(rows: list[tuple]) -> list[tuple]:
    seen = set()
    out = []
    for r in rows:
        key = (r[0], r[3], r[4], r[5], r[6])  # position_uid,timeframe,param_type,param_base,param_name
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# 🔸 Определить тип базы по префиксу (ema50 → ema, bb20_2_0 → bb, ...)
def base_kind(base: str) -> str | None:
    for k in PACK_FIELD_WHITELIST.keys():
        if base.startswith(k):
            return k
    return None


# 🔸 Утилита: плоский обход pack['pack'] (фильтр мета-полей)
def flatten_pack_dict(d: dict):
    for k, v in d.items():
        if k in ("open_time", "ref", "used_bases", "prev_state", "raw_state",
                 "streak_preview", "strong", "direction", "max_adx", "deltas"):
            continue
        yield (k, v)


# 🔸 Подготовка строк для паков (param_type='pack') по whitelist
def build_rows_for_pack_response(position_uid: str,
                                 strategy_id: int,
                                 symbol: str,
                                 tf: str,
                                 base: str,
                                 open_time_iso: str,
                                 pack_payload: dict) -> list[tuple]:
    rows = []
    kind = base_kind(base)
    if not kind:
        return rows
    allowed = set(PACK_FIELD_WHITELIST.get(kind, []))
    if not allowed:
        return rows
    open_time = parse_iso(open_time_iso)
    for pname, pval in flatten_pack_dict(pack_payload):
        if pname not in allowed:
            continue
        val_num = None
        val_text = None
        if isinstance(pval, (int, float)):
            val_num = float(pval)
        else:
            try:
                val_num = float(pval)
            except Exception:
                if pval is None:
                    continue
                if isinstance(pval, bool):
                    val_text = "true" if pval else "false"
                else:
                    val_text = str(pval)
        rows.append((
            position_uid, strategy_id, symbol, tf,
            "pack", base, pname,
            val_num, val_text,
            open_time,
            "ok", None
        ))
    return rows


# 🔸 Построить ожидаемые базы паков из списка инстансов (по TF)
def build_expected_pack_bases(instances: list[dict]) -> dict[str, set[str]]:
    expected: dict[str, set[str]] = {ind: set() for ind in PACK_INDS}
    for inst in instances:
        ind = inst["indicator"]
        params = inst["params"]
        if ind not in expected:
            continue
        if ind in ("ema", "rsi", "mfi", "atr", "adx_dmi", "lr"):
            try:
                L = int(params["length"])
                expected[ind].add(f"{ind}{L}")
            except Exception:
                pass
        elif ind == "macd":
            try:
                F = int(params["fast"])
                expected[ind].add(f"macd{F}")
            except Exception:
                pass
        elif ind == "bb":
            try:
                L = int(params["length"])
                S = round(float(params["std"]), 2)
                std_str = str(S).replace(".", "_")
                expected[ind].add(f"bb{L}_{std_str}")
            except Exception:
                pass
    return expected


# 🔸 Роутер ответов: читает indicator_response и gateway_response, доставляет по req_id
async def run_response_router(redis, req_routes: dict, req_lock: asyncio.Lock,
                              stop_event: asyncio.Event):
    last_ind_id = "0-0"
    last_gw_id  = "0-0"

    async def drain(stream_key: str, last_id: str):
        try:
            got = await redis.xread(streams={stream_key: last_id},
                                    count=BATCH_SIZE_RESP, block=RESP_BLOCK_MS)
        except Exception:
            return last_id, []
        if not got:
            return last_id, []
        out = []
        for _, msgs in got:
            for rid, payload in msgs:
                out.append((rid, payload))
                last_id = rid
        return last_id, out

    while not stop_event.is_set():
        last_ind_id, ind_items = await drain(INDICATOR_RESP_STREAM, last_ind_id)
        for rid, payload in ind_items:
            req_id = payload.get("req_id")
            if not req_id:
                continue
            queue = None
            async with req_lock:
                queue = req_routes.get(req_id)
            if queue:
                try:
                    await queue.put(("indicator", payload))
                except Exception:
                    pass

        last_gw_id, gw_items = await drain(GW_RESP_STREAM, last_gw_id)
        for rid, payload in gw_items:
            req_id = payload.get("req_id")
            if not req_id:
                continue
            queue = None
            async with req_lock:
                queue = req_routes.get(req_id)
            if queue:
                try:
                    await queue.put(("gateway", payload))
                except Exception:
                    pass

        await asyncio.sleep(0.01)


# 🔸 Обработчик одной позиции (отдельная задача; m5 приоритет, m15/h1 параллельно)
async def handle_position(pg, redis, get_instances_by_tf,
                          position_uid: str, strategy_id: int, symbol: str, created_at_iso: str,
                          req_routes: dict, req_lock: asyncio.Lock,
                          tf_semaphores: dict[str, asyncio.Semaphore]):

    instances_by_tf = {tf: [i for i in get_instances_by_tf(tf)] for tf in REQUIRED_TFS}
    expected_bases_by_tf = {tf: build_expected_pack_bases(instances_by_tf[tf]) for tf in REQUIRED_TFS}
    bar_open_ms_by_tf = {tf: to_bar_open_ms(created_at_iso, tf) for tf in REQUIRED_TFS}

    # состояния по TF
    start_ts_global = datetime.utcnow()
    tf_start_ts = {tf: None for tf in REQUIRED_TFS}
    tf_done = {tf: False for tf in REQUIRED_TFS}
    deadline = start_ts_global + timedelta(seconds=GLOBAL_TIMEOUT_SEC)

    # индикаторы
    ind_ctx = {
        tf: {
            inst["id"]: {"inflight": False, "state": "pending", "last_err": None, "req_ids": set(), "indicator": inst["indicator"]}
            for inst in instances_by_tf[tf]
        } for tf in REQUIRED_TFS
    }
    # паки
    pack_ctx = {
        tf: {
            ind: {"inflight": False, "state": "pending", "last_err": None,
                  "req_ids": set(), "done_bases": set(), "expected_bases": set(expected_bases_by_tf[tf].get(ind, set()))}
            for ind in PACK_INDS if expected_bases_by_tf[tf].get(ind)
        } for tf in REQUIRED_TFS
    }
    # marketwatch
    mw_ctx = {
        tf: {kind: {"inflight": False, "state": "pending", "last_err": None, "req_ids": set()}
             for kind in MW_KINDS} for tf in REQUIRED_TFS
    }

    # очередь ответов для этой позиции
    resp_queue: asyncio.Queue = asyncio.Queue()

    # ретраибельная ошибка?
    def is_retriable(err: str) -> bool:
        return err not in ("instance_not_active", "exception")

    # отправка indicator_request
    async def send_indicator(tf: str, inst_id: int):
        s = ind_ctx[tf][inst_id]
        if s["inflight"] or s["state"] != "pending":
            return
        if tf_start_ts[tf] is None:
            tf_start_ts[tf] = datetime.utcnow()
        async with tf_semaphores[tf]:
            rid = await redis.xadd(INDICATOR_REQ_STREAM, {
                "symbol": symbol,
                "timeframe": tf,
                "instance_id": str(inst_id),
                "timestamp_ms": str(bar_open_ms_by_tf[tf])
            })
        async with req_lock:
            req_routes[rid] = resp_queue
        s["inflight"] = True
        s["req_ids"].add(rid)

    # отправка gateway_request (pack или mw)
    async def send_gateway(tf: str, indicator_or_kind: str):
        if indicator_or_kind in pack_ctx[tf]:
            s = pack_ctx[tf][indicator_or_kind]
            ok_states = ("pending", "ok_part")
        else:
            s = mw_ctx[tf][indicator_or_kind]
            ok_states = ("pending",)
        if s["inflight"] or s["state"] not in ok_states:
            return
        if tf_start_ts[tf] is None:
            tf_start_ts[tf] = datetime.utcnow()
        async with tf_semaphores[tf]:
            rid = await redis.xadd(GW_REQ_STREAM, {
                "symbol": symbol,
                "timeframe": tf,
                "indicator": indicator_or_kind,
                "timestamp_ms": str(bar_open_ms_by_tf[tf])
            })
        async with req_lock:
            req_routes[rid] = resp_queue
        s["inflight"] = True
        s["req_ids"].add(rid)

    # первая волна: m5 сначала, затем m15+h1 (почти сразу, но после m5)
    if "m5" in REQUIRED_TFS:
        await asyncio.gather(*[send_indicator("m5", inst["id"]) for inst in instances_by_tf["m5"]])
        await asyncio.gather(*[send_gateway("m5", ind) for ind in pack_ctx["m5"].keys()])
        await asyncio.gather(*[send_gateway("m5", kind) for kind in MW_KINDS])

    for tf in ("m15", "h1"):
        if tf in REQUIRED_TFS:
            await asyncio.gather(*[send_indicator(tf, inst["id"]) for inst in instances_by_tf[tf]])
            await asyncio.gather(*[send_gateway(tf, ind) for ind in pack_ctx[tf].keys()])
            await asyncio.gather(*[send_gateway(tf, kind) for kind in MW_KINDS])

    total_upserts = 0
    upserts_by_tf = {tf: 0 for tf in REQUIRED_TFS}

    # главный цикл до готовности всех TF или таймаута
    while True:
        now = datetime.utcnow()
        if now >= deadline:
            # лог по таймауту
            for tf in REQUIRED_TFS:
                if tf_done[tf]:
                    continue
                for inst_id, s in ind_ctx[tf].items():
                    if s["state"] == "pending":
                        log.debug(f"[TIMEOUT] IND {symbol}/{tf} inst_id={inst_id} {s['indicator']} last_err={s['last_err']}")
                for ind, s in pack_ctx[tf].items():
                    if s["state"] in ("pending", "ok_part"):
                        missing = sorted(list(s["expected_bases"] - s["done_bases"]))
                        log.debug(f"[TIMEOUT] PACK {symbol}/{tf} {ind} missing_bases={missing} last_err={s['last_err']}")
                for kind, s in mw_ctx[tf].items():
                    if s["state"] == "pending":
                        log.debug(f"[TIMEOUT] MW {symbol}/{tf} {kind} last_err={s['last_err']}")
            break

        # окно POLL_INTERVAL_SEC: собираем ответы, пишем в БД порциями
        end_wait = now + timedelta(seconds=POLL_INTERVAL_SEC)
        collected_rows: list[tuple] = []

        while datetime.utcnow() < end_wait:
            try:
                src, payload = await asyncio.wait_for(resp_queue.get(), timeout=RESP_BLOCK_MS / 1000.0)
            except asyncio.TimeoutError:
                break

            status = payload.get("status")
            req_id = payload.get("req_id")
            r_symbol = payload.get("symbol")
            tf = payload.get("timeframe")

            if req_id:
                async with req_lock:
                    req_routes.pop(req_id, None)

            if r_symbol != symbol or tf not in REQUIRED_TFS:
                continue

            if src == "indicator":
                instance_id_raw = payload.get("instance_id")
                if not instance_id_raw:
                    continue
                iid = int(instance_id_raw)
                s = ind_ctx[tf].get(iid)
                if not s or req_id not in s["req_ids"]:
                    continue
                s["req_ids"].discard(req_id)
                s["inflight"] = False

                if status == "ok":
                    indicator_name = s["indicator"]
                    rows = build_rows_for_indicator_response(
                        position_uid=position_uid,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        tf=tf,
                        indicator_name=indicator_name,
                        open_time_iso=payload.get("open_time"),
                        results_json=payload.get("results", "{}"),
                    )
                    collected_rows.extend(rows)
                    s["state"] = "ok"
                    s["last_err"] = None
                else:
                    err = payload.get("error") or "unknown"
                    s["last_err"] = err
                    if not is_retriable(err):
                        s["state"] = "error"

            else:  # gateway (packs/MW)
                ind = payload.get("indicator")
                ctx_slot = None
                ctx_type = None
                if ind in pack_ctx[tf]:
                    ctx_slot = pack_ctx[tf][ind]
                    ctx_type = "pack"
                elif ind in mw_ctx[tf]:
                    ctx_slot = mw_ctx[tf][ind]
                    ctx_type = "mw"
                else:
                    continue
                if req_id not in ctx_slot["req_ids"]:
                    continue
                ctx_slot["req_ids"].discard(req_id)
                ctx_slot["inflight"] = False

                if status == "ok":
                    try:
                        results = json.loads(payload.get("results", "[]"))
                    except Exception:
                        results = []

                    if ctx_type == "pack":
                        if not isinstance(results, list):
                            ctx_slot["last_err"] = "bad_results"
                        else:
                            for item in results:
                                base = item.get("base")
                                p = item.get("pack", {})
                                if not base or not isinstance(p, dict):
                                    continue
                                open_time_iso = p.get("open_time") or datetime.utcfromtimestamp(bar_open_ms_by_tf[tf] / 1000).isoformat()
                                rows = build_rows_for_pack_response(position_uid, strategy_id, symbol, tf, base, open_time_iso, p)
                                if rows:
                                    collected_rows.extend(rows)
                                    ctx_slot["done_bases"].add(base)
                            ctx_slot["state"] = "ok" if (ctx_slot["expected_bases"] <= ctx_slot["done_bases"]) else "ok_part"
                            ctx_slot["last_err"] = None
                    else:
                        if not isinstance(results, list) or not results:
                            ctx_slot["last_err"] = "bad_results"
                        else:
                            item = results[0]
                            base = item.get("base", ind)  # trend|volatility|momentum|extremes
                            p = item.get("pack", {})
                            state_val = p.get("state")
                            if state_val is not None:
                                open_time_iso = p.get("open_time") or datetime.utcfromtimestamp(bar_open_ms_by_tf[tf] / 1000).isoformat()
                                rows = [(
                                    position_uid, strategy_id, symbol, tf,
                                    "marketwatch", base, "state",
                                    None, str(state_val),
                                    parse_iso(open_time_iso),
                                    "ok", None
                                )]
                                collected_rows.extend(rows)
                                ctx_slot["state"] = "ok"
                                ctx_slot["last_err"] = None
                            else:
                                ctx_slot["last_err"] = "no_state"
                else:
                    err = payload.get("error") or "unknown"
                    ctx_slot["last_err"] = err
                    if not is_retriable(err):
                        ctx_slot["state"] = "error"

        # запись в БД (локальная дедупликация)
        if collected_rows:
            deduped = dedup_rows(collected_rows)
            # разделим по TF, чтобы аккумулировать per-TF upserts
            by_tf = {}
            for r in deduped:
                by_tf.setdefault(r[3], []).append(r)  # r[3] = timeframe
            for tf, chunk in by_tf.items():
                n = await insert_rows_pg(pg, chunk)
                total_upserts += n
                upserts_by_tf[tf] += n

        # ретраи «раз в секунду»: отправляем тем, кто pending и не inflight (с учётом retriable)
        for tf in REQUIRED_TFS:
            # m5 приоритетнее: просто идём в порядке m5→m15→h1 (лимиты различаются в семафорах)
            for inst_id, s in ind_ctx[tf].items():
                if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                    await send_indicator(tf, inst_id)
            for ind, s in pack_ctx[tf].items():
                if s["state"] in ("pending", "ok_part") and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                    await send_gateway(tf, ind)
            for kind, s in mw_ctx[tf].items():
                if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                    await send_gateway(tf, kind)

        # проверка готовности по каждому TF → логируем пер-TF и отмечаем done
        def all_done_tf(tf: str) -> bool:
            if any(s["state"] in ("pending", "error") for s in ind_ctx[tf].values()):
                return False
            for ind, s in pack_ctx[tf].items():
                if s["state"] == "error":
                    return False
                if not (s["expected_bases"] <= s["done_bases"]):
                    return False
            if any(s["state"] in ("pending", "error") for s in mw_ctx[tf].values()):
                return False
            return True

        for tf in REQUIRED_TFS:
            if not tf_done[tf] and all_done_tf(tf):
                tf_done[tf] = True
                elapsed_ms_tf = int((datetime.utcnow() - (tf_start_ts[tf] or start_ts_global)).total_seconds() * 1000)
                ok_inst_tf = sum(1 for s in ind_ctx[tf].values() if s["state"] == "ok")
                ok_packs_tf = sum(len(s["done_bases"]) for s in pack_ctx[tf].values())
                ok_mw_tf = sum(1 for s in mw_ctx[tf].values() if s["state"] == "ok")
                unique_rows_tf = await count_unique_rows(pg, position_uid, tf)
                log.debug(
                    f"IND_POS_STAT: position={position_uid} {symbol} {tf} snapshot complete: "
                    f"ok_instances={ok_inst_tf}, ok_packs={ok_packs_tf}, ok_mw={ok_mw_tf}, "
                    f"rows_upserted_tf={upserts_by_tf[tf]}, unique_rows_tf={unique_rows_tf}, elapsed_ms={elapsed_ms_tf}"
                )

        # если все TF завершены — финальный лог и выход
        if all(tf_done.values()):
            elapsed_ms = int((datetime.utcnow() - start_ts_global).total_seconds() * 1000)
            unique_all = await count_unique_rows(pg, position_uid, None)
            total_ok_inst = sum(1 for tf in REQUIRED_TFS for s in ind_ctx[tf].values() if s["state"] == "ok")
            total_ok_packs = sum(len(s["done_bases"]) for tf in REQUIRED_TFS for s in pack_ctx[tf].values())
            total_ok_mw = sum(1 for tf in REQUIRED_TFS for s in mw_ctx[tf].values() if s["state"] == "ok")
            log.debug(
                f"IND_POS_STAT: position={position_uid} {symbol} ALL TF done: "
                f"ok_instances={total_ok_inst}, ok_packs={total_ok_packs}, ok_mw={total_ok_mw}, "
                f"rows_upserted_total={total_upserts}, unique_rows_total={unique_all}, elapsed_ms={elapsed_ms}"
            )
            break


# 🔸 Основной воркер: диспетчер позиций + глобальный роутер ответов
async def run_indicator_position_stat(pg, redis, get_instances_by_tf, get_precision):
    group = "iv4_possnap_group"
    consumer = "iv4_possnap_1"

    # создать consumer-group для входного стрима
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # роутинг req_id → очередь позиции
    req_routes: dict[str, asyncio.Queue] = {}
    req_lock = asyncio.Lock()

    # лимитер параллельных позиций
    pos_sema = asyncio.Semaphore(POSITIONS_CONCURRENCY)

    # семафоры TF для on-demand запросов
    tf_semaphores = {
        tf: asyncio.Semaphore(CONCURRENCY_PER_TF.get(tf, 30)) for tf in REQUIRED_TFS
    }

    # останов роутера при завершении процесса
    stop_event = asyncio.Event()
    router_task = asyncio.create_task(run_response_router(redis, req_routes, req_lock, stop_event))

    try:
        while True:
            try:
                resp = await redis.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={POSITIONS_OPEN_STREAM: ">"},
                    count=BATCH_SIZE_POS_OPEN,
                    block=2000
                )
            except Exception as e:
                log.error(f"positions read error: {e}", exc_info=True)
                await asyncio.sleep(0.5)
                continue

            if not resp:
                continue

            to_ack = []
            pos_tasks = []

            for _, messages in resp:
                for msg_id, data in messages:
                    try:
                        if (data.get("event_type") or "").lower() != "opened":
                            to_ack.append(msg_id)
                            continue

                        position_uid = data["position_uid"]
                        strategy_id = int(data["strategy_id"])
                        symbol = data["symbol"]
                        created_at_iso = data.get("created_at") or data.get("received_at")
                        if not created_at_iso:
                            log.debug(f"[SKIP] position {position_uid}: no created_at/received_at")
                            to_ack.append(msg_id)
                            continue

                        async def run_one():
                            async with pos_sema:
                                await handle_position(pg, redis, get_instances_by_tf,
                                                      position_uid, strategy_id, symbol, created_at_iso,
                                                      req_routes, req_lock, tf_semaphores)

                        task = asyncio.create_task(run_one())
                        pos_tasks.append((msg_id, task))

                    except Exception as e:
                        log.error(f"position spawn error: {e}", exc_info=True)

            # ждём завершения всех задач этой пачки и ACK-аем соответствующие сообщения
            for msg_id, task in pos_tasks:
                try:
                    await task
                    to_ack.append(msg_id)
                except Exception as e:
                    log.error(f"position task error: {e}", exc_info=True)

            if to_ack:
                await redis.xack(POSITIONS_OPEN_STREAM, group, *to_ack)

    finally:
        stop_event.set()
        try:
            await router_task
        except Exception:
            pass