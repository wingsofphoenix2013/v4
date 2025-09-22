# indicator_position_stat.py — воркер on-demand снимка при открытии позиции (этап 2: m5 индикаторы + packs + marketwatch, антидубли, быстрый сбор ответов)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Время бара и шаги TF
from packs.pack_utils import floor_to_bar, STEP_MS

# 🔸 Константы стримов и таблиц
POSITIONS_OPEN_STREAM = "positions_open_stream"
INDICATOR_REQ_STREAM = "indicator_request"
INDICATOR_RESP_STREAM = "indicator_response"
GW_REQ_STREAM = "indicator_gateway_request"
GW_RESP_STREAM = "indicator_gateway_response"
TARGET_TABLE = "indicator_position_stat"

# 🔸 Параметры воркера
REQUIRED_TFS = ("m5",)             # этап 2 — только m5
POLL_INTERVAL_SEC = 1              # частота ретраев недостающих сущностей
RESP_BLOCK_MS = 300                # внутренний короткий блок на чтение ответов (~0.3с)
GLOBAL_TIMEOUT_SEC = 600           # 10 минут на позицию
BATCH_SIZE_POS_OPEN = 20           # чтение событий позиций
BATCH_SIZE_RESP_READ = 200         # чтение ответов (indicator/gateway)
CONCURRENCY_PER_TF = 50            # лимит параллельных on-demand запросов по TF

# 🔸 Наборы паков и MW
PACK_INDS = ("ema", "rsi", "mfi", "lr", "atr", "adx_dmi", "macd", "bb")
MW_KINDS = ("trend", "volatility", "momentum", "extremes")

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
            "indicator",                   # param_type
            indicator_name,                # param_base (короткое имя индикатора: ema/lr/...)
            param_name,                    # param_name (каноника: ema9, lr50_angle, ...)
            value_num, None,               # value_num, value_text
            open_time,                     # open_time
            "ok", None                     # status, error_code
        ))
    return rows


# 🔸 Утилита: «плоский» обход словаря pack['pack']
def flatten_pack_dict(d: dict, prefix=""):
    for k, v in d.items():
        if k in ("open_time", "ref"):  # метаданные паков не пишем как параметры
            continue
        name = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict):
            # вложенные (например, deltas.*)
            yield from flatten_pack_dict(v, name)
        else:
            yield (name, v)


# 🔸 Подготовка строк для паков (param_type='pack')
def build_rows_for_pack_response(position_uid: str,
                                 strategy_id: int,
                                 symbol: str,
                                 tf: str,
                                 base: str,
                                 open_time_iso: str,
                                 pack_payload: dict) -> list[tuple]:
    rows = []
    open_time = parse_iso(open_time_iso)
    for pname, pval in flatten_pack_dict(pack_payload):
        # числа → value_num, строки/булевы → value_text
        val_num = None
        val_text = None
        if isinstance(pval, (int, float)):
            val_num = float(pval)
        else:
            # строки вида "0.04" тоже считаем числами
            try:
                val_num = float(pval)
            except Exception:
                if isinstance(pval, bool):
                    val_text = "true" if pval else "false"
                else:
                    # если список/None — сериализуем кратко в текст
                    if pval is None:
                        val_text = None  # пропустим такие
                        continue
                    if isinstance(pval, (list, tuple)):
                        try:
                            val_text = json.dumps(pval)
                        except Exception:
                            continue
                    else:
                        val_text = str(pval)

        rows.append((
            position_uid, strategy_id, symbol, tf,
            "pack",          # param_type
            base,            # param_base (например: ema50, bb20_2_0, macd12, trend ...)
            pname,           # param_name (например: dist_pct, dynamic_smooth, deltas.d_adx)
            val_num, val_text,
            open_time,
            "ok", None
        ))
    return rows


# 🔸 Вставка пачки строк в PG (UPSERT); возвращает (upsert_count, unique_count)
async def insert_rows_pg(pg, rows: list[tuple]) -> tuple[int, int]:
    if not rows:
        return 0, 0
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
            # реальное число уникальных строк по позиции
            sample = rows[0]
            position_uid = sample[0]
            rec = await conn.fetchrow(f"""
                SELECT COUNT(*) AS cnt
                FROM {TARGET_TABLE}
                WHERE position_uid = $1
            """, position_uid)
            unique_count = int(rec["cnt"]) if rec else 0
    return len(rows), unique_count


# 🔸 Антидубли: локальная дедупликация кортежей по уникальному ключу таблицы
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


# 🔸 Основной воркер: позиции → m5 индикаторы + packs + MW (ретраи 1с, быстрый сбор ответов)
async def run_indicator_position_stat(pg, redis, get_instances_by_tf, get_precision):
    group = "iv4_possnap_group"
    consumer = "iv4_possnap_1"

    # создать свою consumer-group для входного стрима
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # оффсеты для XREAD (без групп) по ответам
    last_ind_resp_id = "0-0"
    last_gw_resp_id = "0-0"

    # лимитер запросов по TF
    tf_semaphores = {tf: asyncio.Semaphore(CONCURRENCY_PER_TF) for tf in REQUIRED_TFS}

    # helper: ретраибельные ошибки on-demand
    def is_retriable(err: str) -> bool:
        return err not in ("instance_not_active", "exception")

    # helper: чтение ответов индикаторов
    async def drain_indicator_responses():
        nonlocal last_ind_resp_id
        try:
            got = await redis.xread(streams={INDICATOR_RESP_STREAM: last_ind_resp_id},
                                    count=BATCH_SIZE_RESP_READ, block=RESP_BLOCK_MS)
        except Exception:
            return []
        if not got:
            return []
        out = []
        for _, msgs in got:
            for rid, payload in msgs:
                out.append((rid, payload))
                last_ind_resp_id = rid
        return out

    # helper: чтение ответов gateway
    async def drain_gateway_responses():
        nonlocal last_gw_resp_id
        try:
            got = await redis.xread(streams={GW_RESP_STREAM: last_gw_resp_id},
                                    count=BATCH_SIZE_RESP_READ, block=RESP_BLOCK_MS)
        except Exception:
            return []
        if not got:
            return []
        out = []
        for _, msgs in got:
            for rid, payload in msgs:
                out.append((rid, payload))
                last_gw_resp_id = rid
        return out

    while True:
        try:
            resp = await redis.xreadgroup(groupname=group, consumername=consumer,
                                          streams={POSITIONS_OPEN_STREAM: ">"},
                                          count=BATCH_SIZE_POS_OPEN, block=2000)
            if not resp:
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

                        # подготовка контекста по TF (этап 2 — только m5)
                        instances_by_tf = {tf: [i for i in get_instances_by_tf(tf)] for tf in REQUIRED_TFS}
                        expected_bases_by_tf = {
                            tf: build_expected_pack_bases(instances_by_tf[tf]) for tf in REQUIRED_TFS
                        }
                        bar_open_ms_by_tf = {tf: to_bar_open_ms(created_at_iso, tf) for tf in REQUIRED_TFS}

                        # состояния
                        start_ts = datetime.utcnow()
                        deadline = start_ts + timedelta(seconds=GLOBAL_TIMEOUT_SEC)

                        # индикаторы (per instance)
                        ind_ctx = {
                            tf: {
                                inst["id"]: {
                                    "inflight": False,
                                    "req_ids": set(),
                                    "state": "pending",  # pending|ok|error
                                    "last_err": None
                                } for inst in instances_by_tf[tf]
                            } for tf in REQUIRED_TFS
                        }

                        # паки (per indicator base) — строим ожидаемые базы из активных инстансов
                        pack_ctx = {
                            tf: {
                                ind: {
                                    "inflight": False,
                                    "req_ids": set(),
                                    "state": "pending",      # pending|ok_part|ok|error
                                    "done_bases": set(),     # какие базы уже получили и записали
                                    "expected_bases": set(expected_bases_by_tf[tf].get(ind, set())),
                                    "last_err": None
                                } for ind in PACK_INDS if expected_bases_by_tf[tf].get(ind)
                            } for tf in REQUIRED_TFS
                        }

                        # marketwatch (four kinds)
                        mw_ctx = {
                            tf: {
                                kind: {
                                    "inflight": False,
                                    "req_ids": set(),
                                    "state": "pending",      # pending|ok|error
                                    "last_err": None
                                } for kind in MW_KINDS
                            } for tf in REQUIRED_TFS
                        }

                        # helpers: отправка запросов (инд/пак/мв)
                        async def request_indicator(tf: str, inst: dict):
                            s = ind_ctx[tf][inst["id"]]
                            if s["inflight"] or s["state"] != "pending":
                                return
                            async with tf_semaphores[tf]:
                                rid = await redis.xadd(INDICATOR_REQ_STREAM, {
                                    "symbol": symbol,
                                    "timeframe": tf,
                                    "instance_id": str(inst["id"]),
                                    "timestamp_ms": str(bar_open_ms_by_tf[tf])
                                })
                            s["inflight"] = True
                            s["req_ids"].add(rid)

                        async def request_pack(tf: str, ind: str):
                            s = pack_ctx[tf][ind]
                            if s["inflight"] or s["state"] not in ("pending", "ok_part"):
                                return
                            async with tf_semaphores[tf]:
                                rid = await redis.xadd(GW_REQ_STREAM, {
                                    "symbol": symbol,
                                    "timeframe": tf,
                                    "indicator": ind,
                                    # не указываем length/std/fast → gateway вернёт все активные базы
                                    "timestamp_ms": str(bar_open_ms_by_tf[tf])
                                })
                            s["inflight"] = True
                            s["req_ids"].add(rid)

                        async def request_mw(tf: str, kind: str):
                            s = mw_ctx[tf][kind]
                            if s["inflight"] or s["state"] != "pending":
                                return
                            async with tf_semaphores[tf]:
                                rid = await redis.xadd(GW_REQ_STREAM, {
                                    "symbol": symbol,
                                    "timeframe": tf,
                                    "indicator": kind,
                                    "timestamp_ms": str(bar_open_ms_by_tf[tf])
                                })
                            s["inflight"] = True
                            s["req_ids"].add(rid)

                        # цикл до полной готовности m5 или таймаута
                        total_upserts = 0
                        unique_after = 0
                        first_round = True

                        while True:
                            now = datetime.utcnow()
                            if now >= deadline:
                                # логируем, что не успели
                                for tf in REQUIRED_TFS:
                                    # индикаторы
                                    for inst in instances_by_tf[tf]:
                                        s = ind_ctx[tf][inst["id"]]
                                        if s["state"] == "pending":
                                            log.info(f"[TIMEOUT] IND {symbol}/{tf} inst_id={inst['id']} {inst['indicator']} last_err={s['last_err']}")
                                    # паки
                                    for ind in pack_ctx[tf]:
                                        s = pack_ctx[tf][ind]
                                        if s["state"] in ("pending", "ok_part"):
                                            missing = sorted(list(s["expected_bases"] - s["done_bases"]))
                                            log.info(f"[TIMEOUT] PACK {symbol}/{tf} {ind} missing_bases={missing} last_err={s['last_err']}")
                                    # MW
                                    for kind in mw_ctx[tf]:
                                        s = mw_ctx[tf][kind]
                                        if s["state"] == "pending":
                                            log.info(f"[TIMEOUT] MW {symbol}/{tf} {kind} last_err={s['last_err']}")
                                break

                            # 1) первая волна — отправляем сразу всё по m5
                            if first_round:
                                if "m5" in REQUIRED_TFS:
                                    # индикаторы
                                    await asyncio.gather(*[
                                        request_indicator("m5", inst) for inst in instances_by_tf["m5"]
                                    ])
                                    # паки (только те индикаторы, для которых есть ожидаемые базы)
                                    await asyncio.gather(*[
                                        request_pack("m5", ind) for ind in pack_ctx["m5"].keys()
                                    ])
                                    # MW
                                    await asyncio.gather(*[
                                        request_mw("m5", kind) for kind in MW_KINDS
                                    ])
                                first_round = False
                            else:
                                # 2) ретраи: только те, кто retriable и не inflight
                                for tf in REQUIRED_TFS:
                                    # индикаторы
                                    for inst in instances_by_tf[tf]:
                                        s = ind_ctx[tf][inst["id"]]
                                        if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                                            await request_indicator(tf, inst)
                                    # паки
                                    for ind in pack_ctx[tf]:
                                        s = pack_ctx[tf][ind]
                                        if s["state"] in ("pending", "ok_part") and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                                            await request_pack(tf, ind)
                                    # MW
                                    for kind in mw_ctx[tf]:
                                        s = mw_ctx[tf][kind]
                                        if s["state"] == "pending" and not s["inflight"] and (s["last_err"] is None or is_retriable(s["last_err"])):
                                            await request_mw(tf, kind)

                            # 3) собираем ответы ~RESP_BLOCK_MS, повторяем внутри окна POLL_INTERVAL_SEC
                            end_wait = now + timedelta(seconds=POLL_INTERVAL_SEC)
                            collected_rows = []

                            while datetime.utcnow() < end_wait:
                                # индикаторы
                                for rid, payload in await drain_indicator_responses():
                                    status = payload.get("status")
                                    req_id = payload.get("req_id")
                                    r_symbol = payload.get("symbol")
                                    tf = payload.get("timeframe")
                                    instance_id_raw = payload.get("instance_id")
                                    if tf not in ind_ctx or r_symbol != symbol or not instance_id_raw or not req_id:
                                        continue
                                    iid = int(instance_id_raw)
                                    if iid not in ind_ctx[tf]:
                                        continue
                                    s = ind_ctx[tf][iid]
                                    if req_id not in s["req_ids"]:
                                        continue
                                    # снять inflight
                                    s["req_ids"].discard(req_id)
                                    s["inflight"] = False
                                    if status == "ok":
                                        inst = next((i for i in instances_by_tf[tf] if i["id"] == iid), None)
                                        if not inst:
                                            continue
                                        rows = build_rows_for_indicator_response(
                                            position_uid=position_uid,
                                            strategy_id=strategy_id,
                                            symbol=symbol,
                                            tf=tf,
                                            indicator_name=inst["indicator"],
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

                                # gateway (packs + MW)
                                for rid, payload in await drain_gateway_responses():
                                    status = payload.get("status")
                                    req_id = payload.get("req_id")
                                    r_symbol = payload.get("symbol")
                                    tf = payload.get("timeframe")
                                    ind = payload.get("indicator")
                                    if tf not in pack_ctx or r_symbol != symbol or not ind or not req_id:
                                        continue

                                    # это может быть pack или mw
                                    # пробуем pack
                                    ctx_slot = None
                                    ctx_type = None
                                    if ind in pack_ctx[tf]:
                                        ctx_slot = pack_ctx[tf][ind]
                                        ctx_type = "pack"
                                    elif ind in MW_KINDS:
                                        ctx_slot = mw_ctx[tf][ind]
                                        ctx_type = "mw"
                                    else:
                                        continue

                                    if req_id not in ctx_slot["req_ids"]:
                                        continue
                                    # снять inflight
                                    ctx_slot["req_ids"].discard(req_id)
                                    ctx_slot["inflight"] = False

                                    if status == "ok":
                                        try:
                                            results = json.loads(payload.get("results", "[]"))
                                        except Exception:
                                            results = []

                                        # packs: results — список {base, pack}
                                        if ctx_type == "pack":
                                            if not isinstance(results, list):
                                                ctx_slot["last_err"] = "bad_results"
                                            else:
                                                for item in results:
                                                    try:
                                                        base = item.get("base")
                                                        p = item.get("pack", {})
                                                        if not base or not isinstance(p, dict):
                                                            continue
                                                        # open_time берём из самого пакета (ISO)
                                                        open_time_iso = p.get("open_time") or payload.get("open_time") \
                                                            or datetime.utcfromtimestamp(bar_open_ms_by_tf[tf] / 1000).isoformat()
                                                        rows = build_rows_for_pack_response(
                                                            position_uid, strategy_id, symbol, tf, base, open_time_iso, p
                                                        )
                                                        collected_rows.extend(rows)
                                                        ctx_slot["done_bases"].add(base)
                                                    except Exception:
                                                        continue
                                            # состояние: ok, если закрыли все ожидаемые базы
                                            if ctx_slot["expected_bases"] <= ctx_slot["done_bases"]:
                                                ctx_slot["state"] = "ok"
                                            else:
                                                ctx_slot["state"] = "ok_part"
                                            ctx_slot["last_err"] = None

                                        # mw: results — список из одного {base="trend", pack={...}}
                                        else:
                                            if not isinstance(results, list) or not results:
                                                ctx_slot["last_err"] = "bad_results"
                                            else:
                                                item = results[0]
                                                base = item.get("base", ind)
                                                p = item.get("pack", {})
                                                open_time_iso = p.get("open_time") or datetime.utcfromtimestamp(bar_open_ms_by_tf[tf] / 1000).isoformat()
                                                rows = build_rows_for_pack_response(
                                                    position_uid, strategy_id, symbol, tf, base, open_time_iso, p
                                                )
                                                collected_rows.extend(rows)
                                                ctx_slot["state"] = "ok"
                                                ctx_slot["last_err"] = None
                                    else:
                                        err = payload.get("error") or "unknown"
                                        ctx_slot["last_err"] = err
                                        if not is_retriable(err):
                                            ctx_slot["state"] = "error"

                                await asyncio.sleep(0.01)

                            # 4) запись в БД (с локальной дедупликацией)
                            if collected_rows:
                                deduped = dedup_rows(collected_rows)
                                upserts, unique_cnt = await insert_rows_pg(pg, deduped)
                                total_upserts += upserts
                                unique_after = unique_cnt

                            # 5) критерий готовности m5: все индикаторы, все базы паков, все 4 MW — без pending/error
                            def all_done_tf(tf: str) -> bool:
                                # индикаторы
                                if any(s["state"] in ("pending", "error") for s in ind_ctx[tf].values()):
                                    return False
                                # паки
                                for ind, s in pack_ctx[tf].items():
                                    if s["state"] == "error":
                                        return False
                                    # должны закрыть все ожидаемые базы
                                    if not (s["expected_bases"] <= s["done_bases"]):
                                        return False
                                # MW
                                if any(s["state"] in ("pending", "error") for s in mw_ctx[tf].values()):
                                    return False
                                return True

                            all_done = True
                            for tf in REQUIRED_TFS:
                                if not all_done_tf(tf):
                                    all_done = False

                            if all_done:
                                elapsed_ms = int((datetime.utcnow() - start_ts).total_seconds() * 1000)
                                ok_inst = sum(1 for tf in REQUIRED_TFS for s in ind_ctx[tf].values() if s["state"] == "ok")
                                ok_packs = sum(len(s["done_bases"]) for tf in REQUIRED_TFS for s in pack_ctx[tf].values())
                                ok_mw = sum(1 for tf in REQUIRED_TFS for s in mw_ctx[tf].values() if s["state"] == "ok")
                                log.info(
                                    f"IND_POS_STAT: position={position_uid} {symbol} m5 snapshot complete: "
                                    f"ok_instances={ok_inst}, ok_packs={ok_packs}, ok_mw={ok_mw}, "
                                    f"rows_upserted={total_upserts}, unique_rows={unique_after}, elapsed_ms={elapsed_ms}"
                                )
                                break

                        # позиция завершена для этапа 2
                        to_ack.append(msg_id)

                    except Exception as e:
                        log.error(f"position handling error: {e}", exc_info=True)
                        # не ACK — чтобы повторить

            if to_ack:
                await redis.xack(POSITIONS_OPEN_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"run loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)