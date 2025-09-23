# position_snapshot_worker.py — воркер снапшотов индикаторов/паков/MW по открытым позициям (m5 приоритет; TF последовательно; гарантированная полнота с повторными сборами; UPSERT в indicator_position_stat с таймаутами)

import asyncio
import json
import logging
from datetime import datetime, timezone
import uuid


# 🔸 Логгер
log = logging.getLogger("POS_SNAPSHOT")

# 🔸 Константы/настройки воркера (без ENV)
REQ_STREAM_POSITIONS = "positions_open_stream"
GW_REQ_STREAM        = "indicator_gateway_request"
GW_RESP_STREAM       = "indicator_gateway_response"

POS_CONCURRENCY             = 5            # сколько позиций одновременно обрабатываем
BATCH_INSERT_SIZE           = 400          # батч вставки в PG
POS_TFS_ORDER               = ["m5", "m15", "h1"]  # порядок TF (m5 всегда первым)
POS_DRY_RUN                 = False        # True = не пишем в PG, только логи

# бюджет времени на ОДИН TF (сек): делаем несколько циклов "send-all → collect" до дедлайна
POS_REQ_TIMEOUT_SEC         = 60.0
RETRY_GAP_SEC               = 3.0          # пауза между циклами догрузки "missing"

# таймауты защиты от «залипания» слотов
POS_POSITION_TIMEOUT_SEC    = 120.0        # общий дедлайн на обработку одной позиции (все TF подряд)
DB_UPSERT_TIMEOUT_SEC       = 15.0         # таймаут на один батч UPSERT в БД (сек)

# 🔸 Пакеты: какие поля писать в indicator_position_stat (param_type='pack')
PACK_WHITELIST = {
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

# 🔸 Список типов на TF (RAW / PACK / MW)
RAW_TYPES  = ["rsi","mfi","ema","atr","lr","adx_dmi","macd","bb"]
PACK_TYPES = ["rsi","mfi","ema","atr","lr","adx_dmi","macd","bb"]
MW_TYPES   = ["trend","volatility","momentum","extremes"]


# 🔸 Вспомогательные: floor к началу бара по TF (ms → ms)
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_map = {"m5": 5, "m15": 15, "h1": 60}
    step = step_map.get(tf)
    if not step:
        raise ValueError(f"unsupported tf: {tf}")
    step_ms = step * 60_000
    return (ts_ms // step_ms) * step_ms

# 🔸 Вспомогательные: ISO → ms (UTC-naive input → treat as UTC)
def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

# 🔸 Построить один запрос в gateway (mode=raw|pack)
def make_gw_request(symbol: str, tf: str, indicator: str, now_ms: int, mode: str) -> dict:
    return {
        "symbol": symbol,
        "timeframe": tf,
        "indicator": indicator,
        "timestamp_ms": str(now_ms),
        "mode": mode,
    }

# 🔸 Сформировать полный список запросов для TF и «теги ожидания» (indicator, mode)
def build_tf_requests(symbol: str, tf: str, created_at_ms: int) -> tuple[list[dict], list[tuple[str,str]]]:
    reqs: list[dict] = []
    tags: list[tuple[str,str]] = []
    # сначала RAW (часть может фолбэкнуть на prev-bar — быстрее ответ)
    for ind in RAW_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="raw"))
        tags.append((ind, "raw"))
    # затем PACK
    for ind in PACK_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="pack"))
        tags.append((ind, "pack"))
    # и MW (как pack)
    for ind in MW_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="pack"))
        tags.append((ind, "pack"))
    return reqs, tags

# 🔸 Отправить ВСЕ запросы TF и собрать ответы через XREAD с baseline id (без consumer-group, без ACK)
async def gw_send_and_collect(redis, reqs: list[dict], time_left_sec: float) -> tuple[list[dict], set[str]]:
    # 1) взять базовую метку ДО отправки (последний id в стриме или "0-0")
    since_id = "0-0"
    try:
        tail = await redis.execute_command("XREVRANGE", GW_RESP_STREAM, "+", "-", "COUNT", 1)
        if tail and len(tail) > 0:
            since_id = tail[0][0]  # например "1716484845123-0"
    except Exception:
        pass

    # 2) отправка всех запросов, запомнить их req_id (msg_id запроса)
    req_ids = set()
    for payload in reqs:
        try:
            mid = await redis.xadd(GW_REQ_STREAM, payload)
            req_ids.add(mid)
        except Exception as e:
            log.warning(f"[GW] xadd req error: {e}")

    collected: dict[str, dict] = {}
    deadline = asyncio.get_event_loop().time() + max(0.0, time_left_sec)
    last_id = since_id  # курсор XREAD: читать сообщения с ID > last_id

    # 3) читать ответы до дедлайна, фильтровать по req_id
    while req_ids and asyncio.get_event_loop().time() < deadline:
        try:
            resp = await redis.xread(streams={GW_RESP_STREAM: last_id}, count=200, block=1000)
        except Exception as e:
            log.warning(f"[GW] xread resp error: {e}")
            await asyncio.sleep(0.2)
            continue

        if not resp:
            continue

        # resp: [(stream, [(msg_id, fields), ...])]
        for _, messages in resp:
            if messages:
                # продвигаем курсор на ПОСЛЕДНИЙ ID из пакета
                last_id = messages[-1][0]
            for msg_id, data in messages:
                rid = data.get("req_id")
                if rid in req_ids:
                    collected[rid] = data
                    req_ids.remove(rid)

    return list(collected.values()), req_ids
        
# 🔸 Преобразование ответа gateway (OK) → строки indicator_position_stat
def map_gateway_ok_to_rows(position_uid: str,
                           strategy_id: int,
                           symbol: str,
                           tf: str,
                           open_time_iso: str,
                           gw_resp: dict) -> tuple[list[tuple], list[tuple]]:
    ok_rows: list[tuple] = []
    err_rows: list[tuple] = []

    indicator = gw_resp.get("indicator")
    mode = gw_resp.get("mode")

    # парсинг json результатов
    try:
        results_json = gw_resp.get("results")
        items = json.loads(results_json) if results_json else []
    except Exception:
        # ошибки парсинга — value_text заполняем, чтобы пройти XOR
        err_rows.append((
            position_uid, strategy_id, symbol, tf,
            ("pack" if indicator in MW_TYPES or indicator in PACK_TYPES else "indicator"),
            indicator, "results_parse",
            None, "results_parse_error",
            datetime.fromisoformat(open_time_iso), "error", "results_parse_error"
        ))
        return ok_rows, err_rows

    # helper: пуш строки
    def push_row(param_type: str, pbase: str, pname: str, vnum, vtext, status: str = "ok", ecode: str | None = None):
        ok_rows.append((
            position_uid, strategy_id, symbol, tf,
            param_type, pbase, pname,
            vnum, vtext,
            datetime.fromisoformat(open_time_iso),
            status, ecode
        ))

    # режим PACK (включая MW)
    if mode == "pack":
        if indicator in MW_TYPES:
            # marketwatch: только state
            for it in items:
                pack = it.get("pack", {})
                state = pack.get("state")
                if state is not None:
                    push_row("marketwatch", indicator, "state", None, str(state))
                else:
                    err_rows.append((
                        position_uid, strategy_id, symbol, tf,
                        "marketwatch", indicator, "state",
                        None, "mw_no_state",
                        datetime.fromisoformat(open_time_iso), "error", "mw_no_state"
                    ))
            return ok_rows, err_rows

        # обычные packs
        for it in items:
            base = it.get("base")
            pack = it.get("pack", {})
            kind = indicator
            fields = PACK_WHITELIST.get(kind, [])
            for pname in fields:
                val = pack.get(pname)
                if val is None:
                    continue
                vnum, vtext = None, None
                try:
                    vnum = float(val)
                except Exception:
                    vtext = str(val)
                push_row("pack", str(base), pname, vnum, vtext)
        return ok_rows, err_rows

    # режим RAW (indicator)
    # items — массив элементов {"base": "...", "pack": {"results": {<k>:<v>, ...}}, "mode":"raw"}
    for it in items:
        pack = it.get("pack", {})
        results = pack.get("results", {})
        pbase = indicator  # тип без длины
        for k, v in results.items():
            pname = str(k)  # канонический ключ (со всеми суффиксами)
            vnum, vtext = None, None
            try:
                vnum = float(v)
            except Exception:
                vtext = str(v)
            ok_rows.append((
                position_uid, strategy_id, symbol, tf,
                "indicator", pbase, pname,
                vnum, vtext,
                datetime.fromisoformat(open_time_iso),
                "ok", None
            ))
    return ok_rows, err_rows

# 🔸 Преобразование ответа gateway (ERROR) → строки ошибок (value_text обязательно!)
def map_gateway_error_to_rows(position_uid: str,
                              strategy_id: int,
                              symbol: str,
                              tf: str,
                              open_time_iso: str,
                              gw_resp: dict) -> list[tuple]:
    indicator = gw_resp.get("indicator")
    mode = gw_resp.get("mode") or ("pack" if indicator in PACK_TYPES or indicator in MW_TYPES else "indicator")
    error = gw_resp.get("error") or "unknown"
    when = datetime.fromisoformat(open_time_iso)

    if mode == "pack":
        return [(
            position_uid, strategy_id, symbol, tf,
            ("marketwatch" if indicator in MW_TYPES else "pack"),
            indicator, "error",
            None, error,
            when, "error", error
        )]
    else:
        return [(
            position_uid, strategy_id, symbol, tf,
            "indicator", indicator, "error",
            None, error,
            when, "error", error
        )]

# 🔸 UPSERT строк в indicator_position_stat (батчами) с statement_timeout
async def upsert_rows(pg, rows: list[tuple]):
    if not rows:
        return
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
    # подготовим строковый timeout для PG, например '15s'
    pg_stmt_timeout = f"{int(DB_UPSERT_TIMEOUT_SEC)}s"

    i = 0
    total = len(rows)
    async with pg.acquire() as conn:
        while i < total:
            chunk = rows[i:i+BATCH_INSERT_SIZE]
            async with conn.transaction():
                # локальный statement_timeout только на эту транзакцию
                try:
                    await conn.execute(f"SET LOCAL statement_timeout = '{pg_stmt_timeout}'")
                except Exception:
                    pass
                await conn.executemany(sql, chunk)
            i += len(chunk)

# 🔸 Обработка одного TF (send-all → collect → retry missing до дедлайна)
async def process_tf(pg, redis, position_uid: str, strategy_id: int, symbol: str, tf: str, created_at_ms: int):
    tf_t0 = asyncio.get_event_loop().time()

    # фиксируем open_time TF по created_at
    bar_open_ms = floor_to_bar_ms(created_at_ms, tf)
    open_time_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

    # сформировать полный список запросов и ожидаемых тегов
    full_reqs, tags_expected = build_tf_requests(symbol, tf, created_at_ms)
    expected = len(full_reqs)

    # первый цикл: отправить все, собрать ответы
    resps, _ = await gw_send_and_collect(redis, full_reqs, time_left_sec=POS_REQ_TIMEOUT_SEC)

    # счётчики по тегам (indicator, mode)
    from collections import Counter

    def resp_tag(resp):
        ind = resp.get("indicator")
        mode = resp.get("mode") or ("pack" if ind in PACK_TYPES or ind in MW_TYPES else "indicator")
        return (ind, mode)

    got = Counter(resp_tag(r) for r in resps)
    exp = Counter(tags_expected)

    # цикл догрузки: пока есть missing и не вышли за дедлайн, шлём только недостающие
    while True:
        missing_counter = exp - got
        missing = sum(missing_counter.values())
        now = asyncio.get_event_loop().time()
        time_left = POS_REQ_TIMEOUT_SEC - (now - tf_t0)
        if missing == 0 or time_left <= 0:
            break

        # подготовить только «missing» запросы
        retry_reqs = []
        for (ind, mode), cnt in missing_counter.items():
            for _ in range(cnt):
                retry_reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode))

        # пауза между циклами
        await asyncio.sleep(min(RETRY_GAP_SEC, max(0.0, time_left)))
        # отправить и собрать повторно
        res_try, _ = await gw_send_and_collect(
            redis,
            retry_reqs,
            time_left_sec=max(0.0, POS_REQ_TIMEOUT_SEC - (asyncio.get_event_loop().time() - tf_t0))
        )
        # учесть полученные
        got.update(resp_tag(r) for r in res_try)
        resps.extend(res_try)

    # разбор всех полученных ответов → строки
    ok_rows_all: list[tuple] = []
    err_rows_all: list[tuple] = []

    for resp in resps:
        status = resp.get("status")
        if status == "ok":
            oks, errs = map_gateway_ok_to_rows(position_uid, strategy_id, symbol, tf, open_time_iso, resp)
            ok_rows_all.extend(oks)
            err_rows_all.extend(errs)
        else:
            err_rows_all.extend(map_gateway_error_to_rows(position_uid, strategy_id, symbol, tf, open_time_iso, resp))

    # финальная оценка missing
    got = Counter(resp_tag(r) for r in resps)
    missing_counter = Counter(tags_expected) - got
    missing = sum(missing_counter.values())

    # для оставшихся missing сгенерировать timeout-строки (value_text="timeout" для XOR)
    if missing:
        when = datetime.fromisoformat(open_time_iso)
        for (ind, mode), cnt in missing_counter.items():
            for _ in range(cnt):
                if mode == "pack":
                    err_rows_all.append((
                        position_uid, strategy_id, symbol, tf,
                        ("marketwatch" if ind in MW_TYPES else "pack"),
                        ind, "error",
                        None, "timeout",
                        when, "error", "timeout"
                    ))
                else:
                    err_rows_all.append((
                        position_uid, strategy_id, symbol, tf,
                        "indicator", ind, "error",
                        None, "timeout",
                        when, "error", "timeout"
                    ))

    tf_t1 = asyncio.get_event_loop().time()
    received = len(resps)
    log.info(f"[TF] {symbol}/{tf} ok={len(ok_rows_all)} err={len(err_rows_all)} expected={expected} received={received} missing={missing} elapsed_ms={int((tf_t1-tf_t0)*1000)}")

    return ok_rows_all, err_rows_all

# 🔸 Обработка одной позиции: TF последовательно (m5 → m15 → h1) + общий таймаут на позицию
async def process_position(pg, redis, get_strategy_mw, pos_payload: dict) -> None:
    pos_t0 = asyncio.get_event_loop().time()

    # извлекаем базовые поля
    position_uid = pos_payload.get("position_uid")
    symbol = pos_payload.get("symbol")
    strategy_id = int(pos_payload.get("strategy_id")) if pos_payload.get("strategy_id") is not None else None
    created_at_iso = pos_payload.get("created_at")

    if not position_uid or not symbol or strategy_id is None or not created_at_iso:
        log.info(f"[SKIP] bad position payload: {pos_payload}")
        return

    # фильтр по стратегиям: только те, где market_watcher=true
    try:
        if not get_strategy_mw(int(strategy_id)):
            log.info(f"[SKIP] strategy_id={strategy_id} market_watcher=false")
            return
    except Exception:
        log.info(f"[SKIP] strategy_id={strategy_id} (mw flag check failed)")
        return

    created_at_ms = iso_to_ms(created_at_iso)

    # последовательная обработка TF
    tfs = POS_TFS_ORDER[:] if POS_TFS_ORDER else ["m5","m15","h1"]
    if "m5" not in tfs:
        tfs.insert(0, "m5")

    total_ok = total_err = 0

    for tf in tfs:
        ok_rows, err_rows = await process_tf(pg, redis, position_uid, strategy_id, symbol, tf, created_at_ms)
        total_ok += len(ok_rows)
        total_err += len(err_rows)
        if not POS_DRY_RUN and (ok_rows or err_rows):
            try:
                await asyncio.wait_for(upsert_rows(pg, ok_rows + err_rows), timeout=DB_UPSERT_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                log.warning(f"[POS] db_timeout uid={position_uid} sym={symbol} tf={tf}")

    pos_t1 = asyncio.get_event_loop().time()
    log.info(f"POS_SNAPSHOT OK uid={position_uid} sym={symbol} ok_rows={total_ok} err_rows={total_err} elapsed_ms={int((pos_t1-pos_t0)*1000)}")

# 🔸 Основной воркер: подписка на positions_open_stream, TF последовательно, приоритет m5; общий таймаут на позицию
async def run_position_snapshot_worker(pg, redis, get_instances_by_tf, get_precision, get_strategy_mw):
    log.debug("POS_SNAPSHOT: воркер запущен")

    group = "possnap_group"
    consumer = "possnap_1"

    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(REQ_STREAM_POSITIONS, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    sem = asyncio.Semaphore(POS_CONCURRENCY)

    async def handle_one(msg_id: str, data: dict) -> str | None:
        # ограничение параллелизма по позициям
        async with sem:
            try:
                # общий таймаут на позицию
                await asyncio.wait_for(
                    process_position(pg, redis, get_strategy_mw, data),
                    timeout=POS_POSITION_TIMEOUT_SEC
                )
            except asyncio.TimeoutError:
                try:
                    position_uid = data.get("position_uid")
                    symbol = data.get("symbol")
                    log.warning(f"[POS] position_timeout uid={position_uid} sym={symbol}")
                except Exception:
                    log.warning(f"[POS] position_timeout (payload logging failed)")
            except Exception as e:
                log.warning(f"[POS] error {e}", exc_info=True)
            return msg_id

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={REQ_STREAM_POSITIONS: ">"},
                count=50,
                block=2000
            )
        except Exception as e:
            log.error(f"POS_SNAPSHOT read error: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            continue

        if not resp:
            continue

        try:
            tasks = []
            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    tasks.append(asyncio.create_task(handle_one(msg_id, data)))

            done_ids = await asyncio.gather(*tasks, return_exceptions=False)
            ack_ids = [mid for mid in done_ids if mid]
            if ack_ids:
                await redis.xack(REQ_STREAM_POSITIONS, group, *ack_ids)
        except Exception as e:
            log.error(f"POS_SNAPSHOT batch error: {e}", exc_info=True)
            await asyncio.sleep(0.5)