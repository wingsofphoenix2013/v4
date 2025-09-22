# indicator_position_stat.py — воркер on-demand снимка индикаторов при открытии позиции (этап 1: только «сырые» индикаторы по m5)

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
            # если внезапно пришло ненаumeric значение — пропустим на этапе 1
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
async def insert_rows_pg(pg, rows: list[tuple]):
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


# 🔸 Основной воркер: читаем открытия позиций и снимаем «сырые» индикаторы по m5
async def run_indicator_position_stat(pg, redis, get_instances_by_tf, get_precision):
    """
    Этап 1:
    - Подписываемся на positions_open_stream своей consumer-group.
    - На событие «opened»: для каждого TF из REQUIRED_TFS (пока только m5)
      отправляем indicator_request по всем активным инстансам TF.
    - Ретраим ответы каждую 1с до полной готовности либо до глобального таймаута 10 минут.
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

    # локальный оффсет для чтения ответов indicator_response
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

            # вложенная обработка каждой позиции по очереди (m5 приоритет внутри позиции)
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
                            # нет валидного времени — ACK, но логируем
                            log.info(f"[SKIP] position {position_uid}: no created_at/received_at")
                            to_ack.append(msg_id)
                            continue

                        # соберём активные инстансы per TF (только m5 на этапе 1)
                        instances_by_tf = {tf: [i for i in get_instances_by_tf(tf)] for tf in REQUIRED_TFS}

                        # построим задания на indicator_request для всех инстансов m5
                        start_ts = datetime.utcnow()
                        deadline = start_ts + timedelta(seconds=GLOBAL_TIMEOUT_SEC)

                        # прогресс слежения: по каждому TF — map instance_id -> state ('pending'|'ok'|'error'), last_error
                        progress = {tf: {inst["id"]: {"state": "pending", "err": None} for inst in instances}
                                    for tf, instances in instances_by_tf.items()}

                        # предвычислим bar_open_ms на основе created_at
                        bar_open_ms_by_tf = {tf: to_bar_open_ms(created_at_iso, tf) for tf in REQUIRED_TFS}

                        # helper: отправить запрос для одного инстанса
                        async def request_one(tf: str, inst: dict):
                            async with tf_semaphores[tf]:
                                await redis.xadd(INDICATOR_REQ_STREAM, {
                                    "symbol": symbol,
                                    "timeframe": tf,
                                    "instance_id": str(inst["id"]),
                                    "timestamp_ms": str(bar_open_ms_by_tf[tf])
                                })

                        # helper: отправить запросы для всех pending по TF
                        async def request_all_pending(tf: str):
                            tasks = []
                            for inst in instances_by_tf[tf]:
                                if progress[tf][inst["id"]]["state"] == "pending":
                                    tasks.append(asyncio.create_task(request_one(tf, inst)))
                            if tasks:
                                await asyncio.gather(*tasks, return_exceptions=True)

                        # helper: обработать пакет ответов indicator_response
                        async def drain_indicator_responses():
                            nonlocal last_resp_id
                            try:
                                got = await redis.xread(streams={INDICATOR_RESP_STREAM: last_resp_id}, count=BATCH_SIZE_RESP_READ, block=1000)
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

                        # helper: записать в PG ответы по одному TF
                        async def persist_ok_rows(tf: str, collected: list[tuple]):
                            if not collected:
                                return 0
                            n = await insert_rows_pg(pg, collected)
                            return n

                        # главный цикл ретраев — до полной готовности m5 (и остальных TF в этой фазе) или таймаута
                        total_inserted = 0
                        first_round = True
                        while True:
                            now = datetime.utcnow()
                            if now >= deadline:
                                # пишем ошибки для всех оставшихся pending как 'error' c последним кодом (без значения)
                                err_rows = []
                                for tf in REQUIRED_TFS:
                                    for inst in instances_by_tf[tf]:
                                        st = progress[tf][inst["id"]]
                                        if st["state"] == "pending":
                                            # финализируем как error по каждому ожидаемому param (не знаем конкретный список param_name на этапе ожидания) — на этапе 1 фиксируем агрегированно по instance (пустой results не пишем)
                                            # Для наглядности — лог:
                                            log.info(f"[TIMEOUT] position {position_uid} {symbol}/{tf} inst_id={inst['id']} indicator={inst['indicator']} last_err={st['err']}")
                                # завершаем обработку позиции с таймаутом (без доп. вставок)
                                break

                            # 1) первая волна запросов (или перезапрос только pending)
                            if first_round:
                                # m5 приоритет — сначала m5
                                await request_all_pending("m5") if "m5" in REQUIRED_TFS else None
                                # остальные TF (на этапе 1 их нет) можно было бы запустить параллельно
                                first_round = False
                            else:
                                # повторные запросы только по тем, кто pending и давал ретраибельные ошибки
                                for tf in REQUIRED_TFS:
                                    await request_all_pending(tf)

                            # 2) собираем ответы из indicator_response до следующего тика
                            end_wait = now + timedelta(seconds=POLL_INTERVAL_SEC)
                            collected_rows_by_tf = {tf: [] for tf in REQUIRED_TFS}

                            # ждём внутри окна POLL_INTERVAL_SEC, чтобы собрать пачку ответов
                            while datetime.utcnow() < end_wait:
                                batch = await drain_indicator_responses()
                                if not batch:
                                    await asyncio.sleep(0.05)
                                    continue

                                for resp_id, payload in batch:
                                    req_id = payload.get("req_id")
                                    status = payload.get("status")
                                    r_symbol = payload.get("symbol")
                                    tf = payload.get("timeframe")
                                    instance_id_raw = payload.get("instance_id")
                                    if not tf or tf not in progress or r_symbol != symbol or not instance_id_raw:
                                        continue

                                    iid = int(instance_id_raw)
                                    if iid not in progress[tf]:
                                        continue

                                    if status == "ok":
                                        inst = next((i for i in instances_by_tf[tf] if i["id"] == iid), None)
                                        if not inst:
                                            continue
                                        open_time_iso = payload.get("open_time")
                                        results_json = payload.get("results", "{}")
                                        rows = build_rows_for_indicator_response(
                                            position_uid=position_uid,
                                            strategy_id=strategy_id,
                                            symbol=symbol,
                                            tf=tf,
                                            indicator_name=inst["indicator"],
                                            open_time_iso=open_time_iso,
                                            results_json=results_json
                                        )
                                        if rows:
                                            collected_rows_by_tf[tf].extend(rows)
                                        progress[tf][iid]["state"] = "ok"
                                        progress[tf][iid]["err"] = None

                                    elif status == "error":
                                        err = payload.get("error") or "unknown"
                                        progress[tf][iid]["err"] = err
                                        # финальные ошибки — помечаем как error (на этапе 1 не пишем отдельные строки, только меняем статус ожидания)
                                        if err in ("instance_not_active", "exception"):
                                            progress[tf][iid]["state"] = "error"
                                        # иначе оставляем pending — перезапросим на следующем тике

                                await asyncio.sleep(0.01)

                            # 3) сохраняем собранные ok-значения в PG пачками по TF
                            for tf in REQUIRED_TFS:
                                if collected_rows_by_tf[tf]:
                                    added = await persist_ok_rows(tf, collected_rows_by_tf[tf])
                                    total_inserted += added

                            # 4) проверка «готовности» всех TF (этап 1 — только m5)
                            all_done = True
                            for tf in REQUIRED_TFS:
                                # m5 готов тогда, когда все инстансы tf в состоянии ok/или финальная ошибка (но по правилам — ожидаем все, поэтому error тоже оставит all_done=True, если их нет; на этапе 1 следуем правилу "нужно получить все" → считаем done только если нет pending и нет error)
                                states = [v["state"] for v in progress[tf].values()]
                                if any(s == "pending" for s in states):
                                    all_done = False
                                if any(s == "error" for s in states):
                                    all_done = False
                            if all_done:
                                elapsed_ms = int((datetime.utcnow() - start_ts).total_seconds() * 1000)
                                ok_cnt = sum(1 for tf in REQUIRED_TFS for v in progress[tf].values() if v["state"] == "ok")
                                log.info(f"IND_POS_STAT: position={position_uid} {symbol} m5 snapshot complete: ok_instances={ok_cnt}, rows_inserted={total_inserted}, elapsed_ms={elapsed_ms}")
                                break

                        # на этапе 1 — считаем позицию обработанной полностью после готовности m5
                        to_ack.append(msg_id)

                    except Exception as e:
                        log.error(f"position handling error: {e}", exc_info=True)
                        # в случае исключения — не ACK, чтобы можно было повторить обработку

            # батчевый ACK для обработанных
            if to_ack:
                await redis.xack(POSITIONS_OPEN_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"run loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)