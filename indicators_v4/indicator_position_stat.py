# indicator_position_stat.py — воркер on-demand снимка при открытии позиции (m5 only; consumer-groups, watchdog, неблокирующий диспетчер)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Время бара (floor к началу)
from packs.pack_utils import floor_to_bar

# 🔸 Константы стримов и таблиц
POSITIONS_OPEN_STREAM   = "positions_open_stream"
INDICATOR_REQ_STREAM    = "indicator_request"
INDICATOR_RESP_STREAM   = "indicator_response"
GW_REQ_STREAM           = "indicator_gateway_request"
GW_RESP_STREAM          = "indicator_gateway_response"
TARGET_TABLE            = "indicator_position_stat"

# 🔸 Параметры воркера / параллелизм
REQUIRED_TFS            = ("m5",)         # только m5
POLL_INTERVAL_SEC       = 1               # частота ретраев
RESP_BLOCK_MS           = 300             # короткий блок на чтение ответов
GLOBAL_TIMEOUT_SEC      = 600             # 10 минут на позицию
BATCH_SIZE_POS_OPEN     = 20              # чтение позиций
BATCH_SIZE_RESP         = 200             # чтение ответов (indicator/gateway)
CONCURRENCY_PER_M5      = 150             # лимит on-demand запросов по m5
POSITIONS_CONCURRENCY   = 16              # одновременно обрабатываемых позиций
LOST_REQ_SEC            = 12              # watchdog: через сколько секунд считаем req потерянным

# 🔸 Пакеты и MW (m5)
PACK_INDS = ("ema", "rsi", "mfi", "bb", "lr", "atr", "adx_dmi", "macd")
MW_KINDS  = ("trend", "volatility", "momentum", "extremes")

# 🔸 Белые списки полей паков (строго как согласовано)
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

# 🔸 Consumer-groups для ответов (гарантированная доставка)
IND_RESP_GROUP      = "iv4_possnap_indresp"
GW_RESP_GROUP       = "iv4_possnap_gwresp"
IND_RESP_CONSUMER   = "iv4_possnap_router_ind_1"
GW_RESP_CONSUMER    = "iv4_possnap_router_gw_1"

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

# 🔸 Подсчёт уникальных строк по позиции и TF
async def count_unique_rows(pg, position_uid: str, tf: str) -> int:
    async with pg.acquire() as conn:
        rec = await conn.fetchrow(
            f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE} WHERE position_uid = $1 AND timeframe = $2",
            position_uid, tf
        )
        return int(rec["cnt"]) if rec else 0

# 🔸 Антидубли: локальная дедупликация по ключу таблицы
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

# 🔸 Тип базы по префиксу (ema50 → ema, bb20_2_0 → bb, ...)
def base_kind(base: str) -> str | None:
    for k in PACK_FIELD_WHITELIST.keys():
        if base.startswith(k):
            return k
    return None

# 🔸 Плоский обход pack['pack'] (фильтр мета-полей)
def flatten_pack_dict(d: dict):
    for k, v in d.items():
        if k in ("open_time", "ref", "used_bases", "prev_state", "raw_state",
                 "streak_preview", "strong", "direction", "max_adx", "deltas"):
            continue
        yield (k, v)

# 🔸 Строки для паков (param_type='pack') по whitelist
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

# 🔸 Ожидаемые базы паков из активных инстансов (по m5)
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

# 🔸 Роутер ответов (consumer-groups): гарантированная доставка в очередь позиции
async def run_response_router(redis, req_routes: dict, req_lock: asyncio.Lock,
                              stop_event: asyncio.Event):

    # создать consumer-groups для ответных стримов
    try:
        await redis.xgroup_create(INDICATOR_RESP_STREAM, IND_RESP_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create ind_resp error: {e}")
    try:
        await redis.xgroup_create(GW_RESP_STREAM, GW_RESP_GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create gw_resp error: {e}")

    while not stop_event.is_set():
        try:
            tasks = [
                redis.xreadgroup(IND_RESP_GROUP, IND_RESP_CONSUMER, streams={INDICATOR_RESP_STREAM: ">"}, count=BATCH_SIZE_RESP, block=RESP_BLOCK_MS),
                redis.xreadgroup(GW_RESP_GROUP,  GW_RESP_CONSUMER,  streams={GW_RESP_STREAM: ">"},         count=BATCH_SIZE_RESP, block=RESP_BLOCK_MS),
            ]
            res_ind, res_gw = await asyncio.gather(*tasks, return_exceptions=True)

            # обработка INDICATOR_RESP_STREAM
            if isinstance(res_ind, list) and res_ind:
                to_ack = []
                for _, msgs in res_ind:
                    for msg_id, payload in msgs:
                        req_id = payload.get("req_id")
                        if not req_id:
                            to_ack.append(msg_id)
                            continue
                        queue = None
                        async with req_lock:
                            queue = req_routes.get(req_id)
                        if queue:
                            try:
                                await queue.put(("indicator", payload))
                                to_ack.append(msg_id)
                            except Exception:
                                pass
                        else:
                            to_ack.append(msg_id)
                if to_ack:
                    await redis.xack(INDICATOR_RESP_STREAM, IND_RESP_GROUP, *to_ack)

            # обработка GW_RESP_STREAM
            if isinstance(res_gw, list) and res_gw:
                to_ack = []
                for _, msgs in res_gw:
                    for msg_id, payload in msgs:
                        req_id = payload.get("req_id")
                        if not req_id:
                            to_ack.append(msg_id)
                            continue
                        queue = None
                        async with req_lock:
                            queue = req_routes.get(req_id)
                        if queue:
                            try:
                                await queue.put(("gateway", payload))
                                to_ack.append(msg_id)
                            except Exception:
                                pass
                        else:
                            to_ack.append(msg_id)
                if to_ack:
                    await redis.xack(GW_RESP_STREAM, GW_RESP_GROUP, *to_ack)

        except Exception as e:
            log.error(f"router loop error: {e}", exc_info=True)
            await asyncio.sleep(0.2)

# 🔸 Обработчик одной позиции (только m5; watchdog утерянных req)
async def handle_position_m5(pg, redis, get_instances_by_tf,
                             position_uid: str, strategy_id: int, symbol: str, created_at_iso: str,
                             req_routes: dict, req_lock: asyncio.Lock,
                             m5_semaphore: asyncio.Semaphore):

    instances = [i for i in get_instances_by_tf("m5")]
    expected_bases = build_expected_pack_bases(instances)
    bar_open_ms = to_bar_open_ms(created_at_iso, "m5")

    start_ts = datetime.utcnow()
    deadline = start_ts + timedelta(seconds=GLOBAL_TIMEOUT_SEC)

    # индикаторы per instance_id
    ind_ctx = {
        inst["id"]: {"inflight": False, "state": "pending", "last_err": None,
                     "req_ids": set(), "indicator": inst["indicator"], "sent_at": None}
        for inst in instances
    }

    # паки per indicator type
    pack_ctx = {
        ind: {"inflight": False, "state": "pending", "last_err": None,
              "req_ids": set(), "done_bases": set(),
              "expected_bases": set(expected_bases.get(ind, set())), "sent_at": None}
        for ind in PACK_INDS if expected_bases.get(ind)
    }

    # marketwatch
    mw_ctx = {
        kind: {"inflight": False, "state": "pending", "last_err": None,
               "req_ids": set(), "sent_at": None}
        for kind in MW_KINDS
    }

    # очередь ответов для этой позиции и набор req_id для быстрой очистки
    resp_queue: asyncio.Queue = asyncio.Queue()
    all_req_ids: set[str] = set()

    # ретраибельность
    def is_retriable(err: str) -> bool:
        return err not in ("instance_not_active", "exception")

    # отправка indicator_request
    async def send_indicator(inst_id: int):
        s = ind_ctx[inst_id]
        if s["inflight"] or s["state"] != "pending":
            return
        async with m5_semaphore:
            rid = await redis.xadd(INDICATOR_REQ_STREAM, {
                "symbol": symbol,
                "timeframe": "m5",
                "instance_id": str(inst_id),
                "timestamp_ms": str(bar_open_ms)
            })
        async with req_lock:
            req_routes[rid] = resp_queue
        s["inflight"] = True
        s["sent_at"] = datetime.utcnow()
        s["req_ids"].add(rid)
        all_req_ids.add(rid)

    # отправка gateway_request (pack или mw)
    async def send_gateway(kind_or_pack: str, is_pack: bool):
        s = pack_ctx[kind_or_pack] if is_pack else mw_ctx[kind_or_pack]
        ok_states = ("pending", "ok_part") if is_pack else ("pending",)
        if s["inflight"] or s["state"] not in ok_states:
            return
        async with m5_semaphore:
            rid = await redis.xadd(GW_REQ_STREAM, {
                "symbol": symbol,
                "timeframe": "m5",
                "indicator": kind_or_pack,          # имя пака или kind MW
                "timestamp_ms": str(bar_open_ms)
            })
        async with req_lock:
            req_routes[rid] = resp_queue
        s["inflight"] = True
        s["sent_at"] = datetime.utcnow()
        s["req_ids"].add(rid)
        all_req_ids.add(rid)

    # первая волна: все индикаторы, все паки, все MW
    await asyncio.gather(*[send_indicator(inst["id"]) for inst in instances])
    await asyncio.gather(*[send_gateway(ind, True) for ind in pack_ctx.keys()])
    await asyncio.gather(*[send_gateway(kind, False) for kind in MW_KINDS])

    total_upserts = 0

    # главный цикл
    while True:
        now = datetime.utcnow()
        if now >= deadline:
            # лог незакрытых
            for inst_id, s in ind_ctx.items():
                if s["state"] == "pending":
                    log.info(f"[TIMEOUT] IND {symbol}/m5 inst_id={inst_id} {s['indicator']} last_err={s['last_err']}")
            for ind, s in pack_ctx.items():
                if s["state"] in ("pending", "ok_part"):
                    missing = sorted(list(s["expected_bases"] - s["done_bases"]))
                    log.info(f"[TIMEOUT] PACK {symbol}/m5 {ind} missing_bases={missing} last_err={s['last_err']}")
            for kind, s in mw_ctx.items():
                if s["state"] == "pending":
                    log.info(f"[TIMEOUT] MW {symbol}/m5 {kind} last_err={s['last_err']}")
            break

        # окно сбора ответов
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
            if r_symbol != symbol or tf != "m5":
                continue

            # убрать req_id из маршрутов (больше не нужен)
            if req_id:
                async with req_lock:
                    req_routes.pop(req_id, None)

            if src == "indicator":
                instance_id_raw = payload.get("instance_id")
                if not instance_id_raw:
                    continue
                iid = int(instance_id_raw)
                s = ind_ctx.get(iid)
                if not s or req_id not in s["req_ids"]:
                    continue
                s["req_ids"].discard(req_id)
                s["inflight"] = False

                if status == "ok":
                    rows = build_rows_for_indicator_response(
                        position_uid=position_uid,
                        strategy_id=strategy_id,
                        symbol=symbol,
                        tf="m5",
                        indicator_name=s["indicator"],
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

            else:  # gateway
                kind = payload.get("indicator")
                # это pack или MW?
                if kind in pack_ctx:
                    ctx_slot = pack_ctx[kind]
                    is_pack = True
                elif kind in mw_ctx:
                    ctx_slot = mw_ctx[kind]
                    is_pack = False
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

                    if is_pack:
                        if not isinstance(results, list):
                            ctx_slot["last_err"] = "bad_results"
                        else:
                            for item in results:
                                base = item.get("base")
                                p = item.get("pack", {})
                                if not base or not isinstance(p, dict):
                                    continue
                                open_time_iso = p.get("open_time") or datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
                                rows = build_rows_for_pack_response(position_uid, strategy_id, symbol, "m5", base, open_time_iso, p)
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
                            base = item.get("base", kind)  # trend|volatility|momentum|extremes
                            p = item.get("pack", {})
                            state_val = p.get("state")
                            if state_val is not None:
                                open_time_iso = p.get("open_time") or datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()
                                rows = [(
                                    position_uid, strategy_id, symbol, "m5",
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

        # запись в БД
        if collected_rows:
            deduped = dedup_rows(collected_rows)
            n = await insert_rows_pg(pg, deduped)
            total_upserts += n

        # 🔸 watchdog утерянных запросов (через LOST_REQ_SEC снимаем inflight и перезапрашиваем)
        now = datetime.utcnow()
        for iid, s in ind_ctx.items():
            if s["state"] == "pending" and s["inflight"] and s["sent_at"] and (now - s["sent_at"]).total_seconds() > LOST_REQ_SEC:
                async with req_lock:
                    for rid in list(s["req_ids"]):
                        req_routes.pop(rid, None)
                s["req_ids"].clear()
                s["inflight"] = False
                s["sent_at"] = None
        for name, s in pack_ctx.items():
            if s["state"] in ("pending", "ok_part") and s["inflight"] and s["sent_at"] and (now - s["sent_at"]).total_seconds() > LOST_REQ_SEC:
                async with req_lock:
                    for rid in list(s["req_ids"]):
                        req_routes.pop(rid, None)
                s["req_ids"].clear()
                s["inflight"] = False
                s["sent_at"] = None
        for kind, s in mw_ctx.items():
            if s["state"] == "pending" and s["inflight"] and s["sent_at"] and (now - s["sent_at"]).total_seconds() > LOST_REQ_SEC:
                async with req_lock:
                    for rid in list(s["req_ids"]):
                        req_routes.pop(rid, None)
                s["req_ids"].clear()
                s["inflight"] = False
                s["sent_at"] = None

        # ретраи «раз в секунду»
        for iid, s in ind_ctx.items():
            if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                await send_indicator(iid)
        for ind, s in pack_ctx.items():
            if s["state"] in ("pending", "ok_part") and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                await send_gateway(ind, True)
        for kind, s in mw_ctx.items():
            if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                await send_gateway(kind, False)

        # готовность m5?
        def all_done_m5() -> bool:
            if any(s["state"] in ("pending", "error") for s in ind_ctx.values()):
                return False
            for s in pack_ctx.values():
                if s["state"] == "error":
                    return False
                if not (s["expected_bases"] <= s["done_bases"]):
                    return False
            if any(s["state"] in ("pending", "error") for s in mw_ctx.values()):
                return False
            return True

        if all_done_m5():
            elapsed_ms = int((datetime.utcnow() - start_ts).total_seconds() * 1000)
            ok_inst = sum(1 for s in ind_ctx.values() if s["state"] == "ok")
            ok_packs = sum(len(s["done_bases"]) for s in pack_ctx.values())
            ok_mw = sum(1 for s in mw_ctx.values() if s["state"] == "ok")
            unique_rows_tf = await count_unique_rows(pg, position_uid, "m5")
            log.info(
                f"IND_POS_STAT: position={position_uid} {symbol} m5 snapshot complete: "
                f"ok_instances={ok_inst}, ok_packs={ok_packs}, ok_mw={ok_mw}, "
                f"rows_upserted_tf={total_upserts}, unique_rows_tf={unique_rows_tf}, elapsed_ms={elapsed_ms}"
            )
            break

    # очистка всех req_id этой позиции из маршрутов (на всякий случай)
    async with req_lock:
        for rid in list(all_req_ids):
            req_routes.pop(rid, None)

# 🔸 Основной воркер: диспетчер позиций + глобальный роутер ответов (m5 only; неблокирующий)
async def run_indicator_position_stat(pg, redis, get_instances_by_tf, get_precision):
    group = "iv4_possnap_group"
    consumer = "iv4_possnap_1"

    # создать consumer-group для входного стрима позиций
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create pos_open error: {e}")

    # роутинг req_id → очередь позиции
    req_routes: dict[str, asyncio.Queue] = {}
    req_lock = asyncio.Lock()

    # лимитер параллельных позиций и запросов m5
    pos_sema = asyncio.Semaphore(POSITIONS_CONCURRENCY)
    m5_semaphore = asyncio.Semaphore(CONCURRENCY_PER_M5)

    # глобальный роутер ответов (consumer-groups)
    stop_event = asyncio.Event()
    router_task = asyncio.create_task(run_response_router(redis, req_routes, req_lock, stop_event))

    # пул активных задач позиций (для уборки завершённых)
    active_tasks: set[asyncio.Task] = set()

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
                # уборка завершённых задач, чтобы пул не рос
                for t in list(active_tasks):
                    if t.done():
                        active_tasks.discard(t)
                continue

            to_ack = []

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
                            log.info(f"[SKIP] position {position_uid}: no created_at/received_at")
                            to_ack.append(msg_id)
                            continue

                        # запускаем обработку позиции как отдельную задачу (не ждём завершения в диспетчере)
                        async def run_one():
                            async with pos_sema:
                                await handle_position_m5(pg, redis, get_instances_by_tf,
                                                         position_uid, strategy_id, symbol, created_at_iso,
                                                         req_routes, req_lock, m5_semaphore)

                        task = asyncio.create_task(run_one())
                        active_tasks.add(task)
                        # неблокирующий диспетчер: ACK СРАЗУ после успешного спауна задачи
                        to_ack.append(msg_id)

                    except Exception as e:
                        log.error(f"position spawn error: {e}", exc_info=True)
                        # не ACK — повторим позже

            if to_ack:
                await redis.xack(POSITIONS_OPEN_STREAM, group, *to_ack)

            # уборка завершённых задач
            for t in list(active_tasks):
                if t.done():
                    active_tasks.discard(t)

    finally:
        stop_event.set()
        try:
            await router_task
        except Exception:
            pass