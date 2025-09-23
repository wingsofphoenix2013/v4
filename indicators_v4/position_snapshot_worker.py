# position_snapshot_worker.py — воркер снапшотов индикаторов/паков/MW по открытым позициям (m5 приоритет; m15→h1 последовательно; запросы через indicator_gateway; UPSERT в indicator_position_stat)

import asyncio
import json
import logging
from datetime import datetime, timezone

# 🔸 Логгер
log = logging.getLogger("POS_SNAPSHOT")

# 🔸 Константы/настройки воркера
REQ_STREAM_POSITIONS = "positions_open_stream"
GW_REQ_STREAM        = "indicator_gateway_request"
GW_RESP_STREAM       = "indicator_gateway_response"

POS_CONCURRENCY     = 6            # сколько позиций одновременно обрабатываем
BATCH_INSERT_SIZE   = 400          # батч вставки в PG
POS_TFS_ORDER       = ["m5", "m15", "h1"]  # порядок TF (m5 всегда первым)
POS_DRY_RUN         = False        # True = не пишем в PG, только логи
POS_REQ_TIMEOUT_SEC = 35.0         # общий таймаут ожидания ответов за один TF (секунды)
TF_WAVE_SIZE        = 10           # отправляем запросы в gateway «волнами» по N штук, чтобы не забивать очередь


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

# 🔸 Отправка «волны» запросов в gateway и сбор ответов в пределах оставшегося таймаута
async def gw_send_wave_and_collect(redis, reqs: list[dict], time_left_sec: float) -> list[dict]:
    group = "possnap_gw_group"
    consumer = "possnap_gw_1"

    # создать consumer-group для ответа (идемпотентно)
    try:
        await redis.xgroup_create(GW_RESP_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"[GW] xgroup_create resp error: {e}")

    # отправка запросов
    req_ids = set()
    for payload in reqs:
        try:
            mid = await redis.xadd(GW_REQ_STREAM, payload)
            req_ids.add(mid)
        except Exception as e:
            log.warning(f"[GW] xadd req error: {e}")

    # сбор ответов по req_id
    collected: dict[str, dict] = {}
    deadline = asyncio.get_event_loop().time() + max(0.0, time_left_sec)

    while req_ids and asyncio.get_event_loop().time() < deadline:
        try:
            resp = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={GW_RESP_STREAM: ">"},
                count=200,
                block=1000
            )
        except Exception as e:
            log.warning(f"[GW] read resp error: {e}")
            await asyncio.sleep(0.2)
            continue

        if not resp:
            continue

        to_ack = []
        for _, messages in resp:
            for msg_id, data in messages:
                to_ack.append(msg_id)
                rid = data.get("req_id")
                if rid in req_ids:
                    collected[rid] = data
                    req_ids.remove(rid)

        if to_ack:
            try:
                await redis.xack(GW_RESP_STREAM, group, *to_ack)
            except Exception:
                pass

    return list(collected.values())

# 🔸 Преобразование ответа gateway (OK) → список строк для indicator_position_stat
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
    try:
        results_json = gw_resp.get("results")
        items = json.loads(results_json) if results_json else []
    except Exception:
        err_rows.append((
            position_uid, strategy_id, symbol, tf,
            ("pack" if indicator in MW_TYPES or indicator in PACK_TYPES else "indicator"),
            indicator, "results_parse",
            None, None,
            datetime.fromisoformat(open_time_iso), "error", "results_parse_error"
        ))
        return ok_rows, err_rows

    def push_row(param_type: str, pbase: str, pname: str, vnum, vtext, status: str = "ok", ecode: str | None = None):
        ok_rows.append((
            position_uid, strategy_id, symbol, tf,
            param_type, pbase, pname,
            vnum, vtext,
            datetime.fromisoformat(open_time_iso),
            status, ecode
        ))

    if mode == "pack":
        if indicator in MW_TYPES:
            for it in items:
                pack = it.get("pack", {})
                state = pack.get("state")
                if state is not None:
                    # marketwatch: только state
                    push_row("marketwatch", indicator, "state", None, str(state))
                else:
                    err_rows.append((
                        position_uid, strategy_id, symbol, tf,
                        "marketwatch", indicator, "state",
                        None, None,
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

    # RAW (indicator)
    for it in items:
        pack = it.get("pack", {})
        results = pack.get("results", {})
        pbase = indicator  # тип без длины
        for k, v in results.items():
            pname = str(k)  # каноническое имя со строкой длины/суффиксом
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

# 🔸 Преобразование ответа gateway (ERROR) → строки ошибок
def map_gateway_error_to_rows(position_uid: str,
                              strategy_id: int,
                              symbol: str,
                              tf: str,
                              open_time_iso: str,
                              gw_resp: dict) -> list[tuple]:
    indicator = gw_resp.get("indicator")
    mode = gw_resp.get("mode") or ("pack" if indicator in PACK_TYPES or indicator in MW_TYPES else "indicator")
    error = gw_resp.get("error") or "unknown"
    if mode == "pack":
        return [(
            position_uid, strategy_id, symbol, tf,
            ("marketwatch" if indicator in MW_TYPES else "pack"),
            indicator, "error",
            None, None,
            datetime.fromisoformat(open_time_iso),
            "error", error
        )]
    else:
        return [(
            position_uid, strategy_id, symbol, tf,
            "indicator", indicator, "error",
            None, None,
            datetime.fromisoformat(open_time_iso),
            "error", error
        )]

# 🔸 Формирование пачки gateway-запросов для TF
def build_tf_requests(symbol: str, tf: str, created_at_ms: int) -> tuple[list[dict], list[tuple[str,str]]]:
    reqs: list[dict] = []
    tags: list[tuple[str,str]] = []  # (indicator, mode)
    # RAW сначала (они быстрее за счёт возможного фолбэка)
    for ind in RAW_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="raw"))
        tags.append((ind, "raw"))
    # затем PACK
    for ind in PACK_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="pack"))
        tags.append((ind, "pack"))
    # MW (как pack)
    for ind in MW_TYPES:
        reqs.append(make_gw_request(symbol, tf, ind, created_at_ms, mode="pack"))
        tags.append((ind, "pack"))
    return reqs, tags

# 🔸 Обработка одного TF (последовательные волны; маппинг ответов; генерация timeout-ошибок)
async def process_tf(pg, redis, position_uid: str, strategy_id: int, symbol: str, tf: str, created_at_ms: int):
    t0 = asyncio.get_event_loop().time()

    bar_open_ms = floor_to_bar_ms(created_at_ms, tf)
    open_time_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

    reqs, tags_expected = build_tf_requests(symbol, tf, created_at_ms)
    total_expected = len(reqs)

    # волнами отправляем и собираем
    waves = 0
    collected_resps: list[dict] = []
    deadline = t0 + POS_REQ_TIMEOUT_SEC
    i = 0
    while i < total_expected:
        time_left = max(0.0, deadline - asyncio.get_event_loop().time())
        if time_left <= 0:
            break
        wave = reqs[i:i+TF_WAVE_SIZE]
        resps = await gw_send_wave_and_collect(redis, wave, time_left)
        collected_resps.extend(resps)
        i += len(wave)
        waves += 1

    ok_rows_all: list[tuple] = []
    err_rows_all: list[tuple] = []

    # разобрать пришедшие ответы
    for resp in collected_resps:
        status = resp.get("status")
        if status == "ok":
            oks, errs = map_gateway_ok_to_rows(position_uid, strategy_id, symbol, tf, open_time_iso, resp)
            ok_rows_all.extend(oks)
            err_rows_all.extend(errs)
        else:
            err_rows_all.extend(map_gateway_error_to_rows(position_uid, strategy_id, symbol, tf, open_time_iso, resp))

    # посчитать «каких ещё не пришло» — по (indicator,mode)
    # соберём полученные теги
    from collections import Counter
    got = Counter()
    for resp in collected_resps:
        ind = resp.get("indicator")
        mode = resp.get("mode") or ("pack" if ind in PACK_TYPES or ind in MW_TYPES else "indicator")
        got[(ind, mode)] += 1
    exp = Counter(tags_expected)
    missing_counter = exp - got
    missing = sum(missing_counter.values())

    # сгенерировать timeout-строки для недошедших
    if missing:
        for (ind, mode), cnt in missing_counter.items():
            for _ in range(cnt):
                if mode == "pack":
                    err_rows_all.append((
                        position_uid, strategy_id, symbol, tf,
                        ("marketwatch" if ind in MW_TYPES else "pack"),
                        ind, "error",
                        None, None,
                        datetime.fromisoformat(open_time_iso),
                        "error", "timeout"
                    ))
                else:
                    err_rows_all.append((
                        position_uid, strategy_id, symbol, tf,
                        "indicator", ind, "error",
                        None, None,
                        datetime.fromisoformat(open_time_iso),
                        "error", "timeout"
                    ))

    t1 = asyncio.get_event_loop().time()
    log.info(f"[TF] {symbol}/{tf} ok={len(ok_rows_all)} err={len(err_rows_all)} "
             f"waves={waves} expected={total_expected} missing={missing} elapsed_ms={int((t1-t0)*1000)}")

    return ok_rows_all, err_rows_all

# 🔸 UPSERT строк в indicator_position_stat (батчами)
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
    i = 0
    total = len(rows)
    async with pg.acquire() as conn:
        while i < total:
            chunk = rows[i:i+BATCH_INSERT_SIZE]
            async with conn.transaction():
                await conn.executemany(sql, chunk)
            i += len(chunk)

# 🔸 Обработка одной позиции: m5 → затем m15 → затем h1 (последовательно после m5)
async def process_position(pg, redis, get_strategy_mw, pos_payload: dict) -> None:
    t0 = asyncio.get_event_loop().time()

    position_uid = pos_payload.get("position_uid")
    symbol = pos_payload.get("symbol")
    strategy_id = int(pos_payload.get("strategy_id")) if pos_payload.get("strategy_id") is not None else None
    created_at_iso = pos_payload.get("created_at")

    if not position_uid or not symbol or strategy_id is None or not created_at_iso:
        log.info(f"[SKIP] bad position payload: {pos_payload}")
        return

    try:
        if not get_strategy_mw(int(strategy_id)):
            log.info(f"[SKIP] strategy_id={strategy_id} market_watcher=false")
            return
    except Exception:
        log.info(f"[SKIP] strategy_id={strategy_id} (mw flag check failed)")
        return

    created_at_ms = iso_to_ms(created_at_iso)

    # порядок TF: m5 → m15 → h1 (stricly), чтобы не перегружать gateway
    tfs = POS_TFS_ORDER[:] if POS_TFS_ORDER else ["m5","m15","h1"]
    if "m5" not in tfs:
        tfs.insert(0, "m5")

    total_ok = total_err = 0

    for tf in tfs:
        ok_rows, err_rows = await process_tf(pg, redis, position_uid, strategy_id, symbol, tf, created_at_ms)
        total_ok += len(ok_rows)
        total_err += len(err_rows)
        if not POS_DRY_RUN and (ok_rows or err_rows):
            await upsert_rows(pg, ok_rows + err_rows)

    t1 = asyncio.get_event_loop().time()
    log.info(f"POS_SNAPSHOT OK uid={position_uid} sym={symbol} ok_rows={total_ok} err_rows={total_err} elapsed_ms={int((t1-t0)*1000)}")

# 🔸 Основной воркер: подписка на positions_open_stream, приоритет m5, затем m15→h1
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
                await process_position(pg, redis, get_strategy_mw, data)
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