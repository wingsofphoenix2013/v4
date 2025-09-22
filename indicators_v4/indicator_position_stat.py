# indicator_position_stat.py — воркер on-demand снимка индикаторов при открытии позиции (этап 1: только «сырые» индикаторы по m5; антидубли, привязка ответов по req_id)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Утилиты времени TF (floor к началу бара)
from packs.pack_utils import STEP_MS, floor_to_bar

# 🔸 Константы стримов и таблиц
POSITIONS_OPEN_STREAM = "positions_open_stream"
INDICATOR_REQ_STREAM = "indicator_request"
INDICATOR_RESP_STREAM = "indicator_response"
TARGET_TABLE = "indicator_position_stat"

# 🔸 Параметры воркера (этап 1)
REQUIRED_TFS = ("m5",)          # на этом этапе считаем только m5
POLL_INTERVAL_SEC = 1           # частота ретраев недостающих значений
GLOBAL_TIMEOUT_SEC = 600        # 10 минут на позицию
BATCH_SIZE_POS_OPEN = 20        # сколько сообщений позиций читаем за раз
BATCH_SIZE_RESP_READ = 200      # сколько ответов индикаторов читаем за раз
CONCURRENCY_PER_TF = 50         # локальный лимит параллельных запросов per TF

# 🔸 Логгер
log = logging.getLogger("IND_POS_STAT")


# 🔸 Вспомогательное: нормализация времени к open_time бара TF (ms)
def to_bar_open_ms(created_at_iso: str, tf: str) -> int:
    dt = datetime.fromisoformat(created_at_iso)
    ts_ms = int(dt.timestamp() * 1000)
    return floor_to_bar(ts_ms, tf)


# 🔸 Вспомогательное: разобрать ISO в datetime
def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


# 🔸 Вспомогательное: подготовка INSERT записей для одного ответа индикатора
def build_rows_for_indicator_response(position_uid: str,
                                      strategy_id: int,
                                      symbol: str,
                                      tf: str,
                                      indicator_name: str,
                                      open_time_iso: str,
                                      results_json: str) -> list[tuple]:
    """
    Возвращает список кортежей для executemany INSERT в indicator_position_stat.
    param_type='indicator', param_base=<короткое имя индикатора>, param_name=<канон. имя параметра>.
    value_num — float, value_text — NULL; status='ok'.
    """
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
            # на этапе 1 храним только числовые значения
            continue

        rows.append((
            position_uid,                 # position_uid
            strategy_id,                  # strategy_id
            symbol,                       # symbol
            tf,                           # timeframe
            "indicator",                  # param_type
            indicator_name,               # param_base (короткое имя индикатора)
            param_name,                   # param_name (канонический param)
            value_num,                    # value_num
            None,                         # value_text
            open_time,                    # open_time
            "ok",                         # status
            None                          # error_code
        ))
    return rows


# 🔸 Вспомогательное: INSERT пачки строк в PG (UPSERT по уникальному ключу)
async def insert_rows_pg(pg, rows: list[tuple]) -> tuple[int, int]:
    """
    Возвращает (upsert_count, unique_count).
    upsert_count — сколько строк отправили в executemany (после локальной дедупликации),
    unique_count — сколько уникальных (по ключу) строк реально хранится после операции.
    """
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

            # считаем реальное число уникальных строк по позиции
            # (это недорого: одна агрегация по нужной позиции; используем DISTINCT ON ключе)
            sample = rows[0]
            position_uid = sample[0]
            rec = await conn.fetchrow(f"""
                SELECT COUNT(*) AS cnt
                FROM {TARGET_TABLE}
                WHERE position_uid = $1
            """, position_uid)
            unique_count = int(rec["cnt"]) if rec else 0

    return len(rows), unique_count


# 🔸 Основной воркер: читаем открытия позиций и снимаем «сырые» индикаторы по m5 (с антидублированием запросов/ответов)
async def run_indicator_position_stat(pg, redis, get_instances_by_tf, get_precision):
    """
    Этап 1:
    - Подписываемся на positions_open_stream своей consumer-group.
    - На событие «opened»: для каждого TF из REQUIRED_TFS (пока только m5)
      отправляем indicator_request по всем активным инстансам TF.
    - Избегаем дублей: один активный запрос на instance; обрабатываем только ответы с нашими req_id.
    - Ретраим ответы каждые 1с до полной готовности либо до глобального таймаута 10 минут.
    - Пишем только param_type='indicator' строки в indicator_position_stat.
    - Логи по итогам — log.info.
    """
    group = "iv4_possnap_group"
    consumer = "iv4_possnap_1"

    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(POSITIONS_OPEN_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # локальный оффсет для чтения ответов indicator_response (XREAD без группы — не держим PEL и не мешаем другим)
    last_resp_id = "0-0"

    # 🔸 Пул семафоров по TF для ограничения параллелизма
    tf_semaphores = {tf: asyncio.Semaphore(CONCURRENCY_PER_TF) for tf in REQUIRED_TFS}

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={POSITIONS_OPEN_STREAM: ">"},
                count=BATCH_SIZE_POS_OPEN,
                block=2000
            )
            if not resp:
                continue

            to_ack: list[str] = []

            # обработка позиций последовательно (m5 приоритет внутри позиции)
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

                        # активные инстансы per TF (только m5 на этапе 1)
                        instances_by_tf = {tf: [i for i in get_instances_by_tf(tf)] for tf in REQUIRED_TFS}

                        # контекст ожидания: по каждому instance — inflight, req_ids, state, last_err, last_req_at
                        ctx = {
                            tf: {
                                inst["id"]: {
                                    "inflight": False,
                                    "req_ids": set(),
                                    "state": "pending",    # pending|ok|error
                                    "last_err": None,
                                    "last_req_at": None,
                                } for inst in instances
                            } for tf, instances in instances_by_tf.items()
                        }

                        start_ts = datetime.utcnow()
                        deadline = start_ts + timedelta(seconds=GLOBAL_TIMEOUT_SEC)
                        bar_open_ms_by_tf = {tf: to_bar_open_ms(created_at_iso, tf) for tf in REQUIRED_TFS}

                        # helper: ретраибельна ли ошибка
                        def is_retriable(err: str) -> bool:
                            return err not in ("instance_not_active", "exception")

                        # helper: отправить запрос для одного инстанса (если нет активного)
                        async def request_one(tf: str, inst: dict):
                            s = ctx[tf][inst["id"]]
                            if s["inflight"]:
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
                            s["last_req_at"] = datetime.utcnow()

                        # helper: отправить запросы для всех pending по TF
                        async def request_all_pending(tf: str, first_round: bool):
                            tasks = []
                            for inst in instances_by_tf[tf]:
                                s = ctx[tf][inst["id"]]
                                if s["state"] != "pending":
                                    continue
                                # на первой волне отправляем всем; далее — только если предыдущая попытка дала retriable ошибку и запрос не в полёте
                                if first_round or ((s["last_err"] is not None) and is_retriable(s["last_err"]) and not s["inflight"]):
                                    tasks.append(asyncio.create_task(request_one(tf, inst)))
                            if tasks:
                                await asyncio.gather(*tasks, return_exceptions=True)

                        # helper: чтение ответов indicator_response (только наши req_id), без захвата чужих
                        async def drain_indicator_responses():
                            nonlocal last_resp_id
                            try:
                                got = await redis.xread(
                                    streams={INDICATOR_RESP_STREAM: last_resp_id},
                                    count=BATCH_SIZE_RESP_READ,
                                    block=1000
                                )
                            except Exception:
                                return []
                            if not got:
                                return []
                            out = []
                            for _, msgs in got:
                                for rid, payload in msgs:
                                    out.append((rid, payload))
                                    last_resp_id = rid
                            return out

                        # helper: собрать уникальные строки для PG (локальная дедупликация по ключу)
                        def dedup_rows(rows: list[tuple]) -> list[tuple]:
                            seen = set()
                            out = []
                            for r in rows:
                                key = (r[0], r[3], r[4], r[5], r[6])  # (position_uid, timeframe, param_type, param_base, param_name)
                                if key in seen:
                                    continue
                                seen.add(key)
                                out.append(r)
                            return out

                        # главный цикл ретраев — до полной готовности m5 или таймаута
                        total_upserts = 0
                        unique_after = 0
                        first_round = True
                        while True:
                            now = datetime.utcnow()
                            if now >= deadline:
                                # таймаут — логируем незакрытые
                                for tf in REQUIRED_TFS:
                                    for inst in instances_by_tf[tf]:
                                        s = ctx[tf][inst["id"]]
                                        if s["state"] == "pending":
                                            log.info(f"[TIMEOUT] position {position_uid} {symbol}/{tf} inst_id={inst['id']} indicator={inst['indicator']} last_err={s['last_err']}")
                                break

                            # 1) отправка запросов: m5 сначала
                            if "m5" in REQUIRED_TFS:
                                await request_all_pending("m5", first_round)
                            # другие TF отсутствуют на этапе 1
                            first_round = False

                            # 2) собираем ответы в течение POLL_INTERVAL_SEC
                            end_wait = now + timedelta(seconds=POLL_INTERVAL_SEC)
                            collected_rows: list[tuple] = []

                            while datetime.utcnow() < end_wait:
                                batch = await drain_indicator_responses()
                                if not batch:
                                    await asyncio.sleep(0.05)
                                    continue

                                for resp_id, payload in batch:
                                    status = payload.get("status")
                                    req_id = payload.get("req_id")
                                    r_symbol = payload.get("symbol")
                                    tf = payload.get("timeframe")
                                    instance_id_raw = payload.get("instance_id")

                                    # фильтрация по символу/TF/instance и strict по нашему req_id
                                    if tf not in ctx or r_symbol != symbol or not instance_id_raw or not req_id:
                                        continue
                                    iid = int(instance_id_raw)
                                    if iid not in ctx[tf]:
                                        continue
                                    s = ctx[tf][iid]
                                    if req_id not in s["req_ids"]:
                                        # это не ответ на наш запрос — игнорируем
                                        continue

                                    # этот req_id обработан — снимаем inflight-флаг
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
                                    elif status == "error":
                                        err = payload.get("error") or "unknown"
                                        s["last_err"] = err
                                        if err in ("instance_not_active", "exception"):
                                            s["state"] = "error"
                                        # иначе остаётся pending — перезапросим на следующем тике

                                await asyncio.sleep(0.01)

                            # 3) локальная дедупликация и запись в PG
                            if collected_rows:
                                deduped = dedup_rows(collected_rows)
                                upserts, unique_cnt = await insert_rows_pg(pg, deduped)
                                total_upserts += upserts
                                unique_after = unique_cnt  # реальное уникальное кол-во строк по позиции после этой итерации

                            # 4) проверка готовности m5
                            all_done = True
                            for tf in REQUIRED_TFS:
                                states = [ctx[tf][inst["id"]]["state"] for inst in instances_by_tf[tf]]
                                # «нужно получить все»: нет pending и нет error
                                if any(s in ("pending", "error") for s in states):
                                    all_done = False
                            if all_done:
                                elapsed_ms = int((datetime.utcnow() - start_ts).total_seconds() * 1000)
                                ok_cnt = sum(1 for tf in REQUIRED_TFS for inst in instances_by_tf[tf] if ctx[tf][inst["id"]]["state"] == "ok")
                                log.info(
                                    f"IND_POS_STAT: position={position_uid} {symbol} m5 snapshot complete: "
                                    f"ok_instances={ok_cnt}, rows_upserted={total_upserts}, unique_rows={unique_after}, elapsed_ms={elapsed_ms}"
                                )
                                break

                        # позиция обработана на этапе 1 после готовности m5 или таймаута
                        to_ack.append(msg_id)

                    except Exception as e:
                        log.error(f"position handling error: {e}", exc_info=True)
                        # не ACK — чтобы можно было повторить обработку

            # батчевый ACK для обработанных
            if to_ack:
                await redis.xack(POSITIONS_OPEN_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"run loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)