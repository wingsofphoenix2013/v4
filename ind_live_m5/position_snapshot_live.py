# position_snapshot_live.py — live-снапшоты по открытым позициям: чтение ind_live / ind_mw_live / pack_live и запись в indicator_position_stat

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any, Set

from indicators.compute_and_store import get_expected_param_names
# 🔸 Общие пороги MW (для derived-флага high_vol)
from indicator_mw_shared import vol_thresholds

# 🔸 Логгер
log = logging.getLogger("POS_SNAPSHOT_LIVE")

# 🔸 Константы/настройки
REQ_STREAM_POSITIONS = "positions_open_stream"   # входной стрим открытых позиций
GROUP       = "possnap_live_group"               # consumer-group
CONSUMER    = "possnap_live_1"

POS_CONCURRENCY  = 10                            # одновременно обрабатываем позиций
READ_COUNT       = 50                            # сколько сообщений за раз читаем из стрима
READ_BLOCK_MS    = 2000                          # блокировка чтения (мс)

# 🔸 Таймфреймы для live-снимка
TFS = ("m5", "m15", "h1")

# 🔸 Whitelist полей PACK (как в position_snapshot_worker.py)
PACK_WHITELIST: Dict[str, List[str]] = {
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

# 🔸 Типы индикаторов
RAW_TYPES  = ("rsi","mfi","ema","atr","lr","adx_dmi","macd","bb","kama")
PACK_TYPES = ("rsi","mfi","ema","atr","lr","adx_dmi","macd","bb")
MW_TYPES   = ("trend","volatility","momentum","extremes")

# 🔸 Константы БД для записи
TABLE_LIVE = "indicator_position_stat"
DB_UPSERT_TIMEOUT_SEC = 15
BATCH_INSERT_SIZE = 400


# 🔸 Утилиты времени (ISO → ms и floor к началу бара TF)
def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step_map = {"m5": 5, "m15": 15, "h1": 60}
    step = step_map.get(tf)
    if not step:
        raise ValueError(f"unsupported tf: {tf}")
    step_ms = step * 60_000
    return (ts_ms // step_ms) * step_ms

def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# 🔸 MGET-помощник (pipeline)
async def mget_map(redis, keys: List[str]) -> Dict[str, Optional[str]]:
    if not keys:
        return {}
    try:
        pipe = redis.pipeline(transaction=False)
        for k in keys:
            pipe.get(k)
        vals = await pipe.execute()
        out: Dict[str, Optional[str]] = {}
        for k, v in zip(keys, vals):
            out[k] = v if isinstance(v, str) or v is None else (str(v) if v is not None else None)
        return out
    except Exception as e:
        log.debug(f"[MGET] pipeline error: {e}", exc_info=False)
        out: Dict[str, Optional[str]] = {}
        for k in keys:
            try:
                out[k] = await redis.get(k)
            except Exception:
                out[k] = None
        return out


# 🔸 Сбор ожидаемых RAW-параметров и PACK-баз по активным инстансам TF
def collect_expectations(instances_all_tf: List[Dict[str, Any]], tf: str) -> Tuple[Dict[str, str], Dict[str, Set[Any]]]:
    raw_expect: Dict[str, str] = {}
    pack_bases: Dict[str, Set[Any]] = {k: set() for k in PACK_TYPES}

    for inst in instances_all_tf:
        if inst.get("timeframe") != tf:
            continue
        ind = inst.get("indicator")
        params = inst.get("params") or {}

        # RAW ожидаемые имена
        try:
            for pname in get_expected_param_names(ind, params):
                raw_expect[pname] = ind
        except Exception:
            pass

        # PACK базы
        try:
            if ind in ("rsi","mfi","ema","atr","kama","lr","adx_dmi"):
                L = int(params.get("length"))
                if ind in pack_bases:
                    pack_bases[ind].add(L)
            elif ind == "macd":
                F = int(params.get("fast"))
                pack_bases["macd"].add(F)
            elif ind == "bb":
                L = int(params.get("length"))
                S = round(float(params.get("std")), 2)
                pack_bases["bb"].add((L, S))
        except Exception:
            pass

    return raw_expect, pack_bases


# 🔸 Формирование строки для БД
def make_row(position_uid: str, strategy_id: int, symbol: str, tf: str,
             param_type: str, param_base: str, param_name: str,
             value: Optional[str], open_time_iso: str,
             status: str = "ok", error_code: Optional[str] = None) -> Tuple:
    vnum = None
    vtext = None
    if status == "ok" and value is not None:
        try:
            vnum = float(value)
        except Exception:
            vtext = value
    else:
        vtext = error_code or "error"
    return (
        position_uid, strategy_id, symbol, tf,
        param_type, param_base, param_name,
        vnum, vtext,
        datetime.fromisoformat(open_time_iso),
        status, error_code
    )


# 🔸 UPSERT в indicator_position_stat (батчами, с statement_timeout)
async def upsert_rows_live(pg, rows: List[Tuple]):
    if not rows:
        return
    sql = f"""
    INSERT INTO {TABLE_LIVE}
      (position_uid, strategy_id, symbol, timeframe, param_type, param_base, param_name,
       value_num, value_text, open_time, status, error_code)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    ON CONFLICT (position_uid, timeframe, param_type, param_base, param_name)
    DO UPDATE SET
      value_num  = EXCLUDED.value_num,
      value_text = EXCLUDED.value_text,
      open_time  = EXCLUDED.open_time,
      status     = EXCLUDED.status,
      error_code = EXCLUDED.error_code,
      captured_at= NOW()
    """
    timeout_txt = f"{int(DB_UPSERT_TIMEOUT_SEC)}s"

    i = 0
    total = len(rows)
    async with pg.acquire() as conn:
        while i < total:
            chunk = rows[i:i+BATCH_INSERT_SIZE]
            async with conn.transaction():
                try:
                    await conn.execute(f"SET LOCAL statement_timeout = '{timeout_txt}'")
                except Exception:
                    pass
                await conn.executemany(sql, chunk)
            i += len(chunk)


# 🔸 Обработка одного TF: собрать строки RAW/PACK/MW из ключей
async def process_tf_live(redis,
                          get_instances_by_tf,
                          position_uid: str,
                          strategy_id: int,
                          symbol: str,
                          tf: str,
                          created_at_ms: int) -> Tuple[List[Tuple], List[Tuple]]:
    tf_t0 = time.monotonic()

    # фиксируем open_time для БД — привязка к времени открытия позиции
    bar_open_ms = floor_to_bar_ms(created_at_ms, tf)
    open_time_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

    rows_ok: List[Tuple] = []
    rows_err: List[Tuple] = []

    # ожидания по инстансам
    instances_tf = list(get_instances_by_tf(tf))
    raw_expect, pack_bases = collect_expectations(instances_tf, tf)

    # ----- RAW (ind_live) -----
    raw_keys = [f"ind_live:{symbol}:{tf}:{pname}" for pname in raw_expect.keys()]
    raw_map = await mget_map(redis, raw_keys)

    for pname in raw_expect.keys():
        key = f"ind_live:{symbol}:{tf}:{pname}"
        val = raw_map.get(key)
        if val is None:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "indicator", raw_expect[pname], pname,
                                     None, open_time_iso, status="error", error_code="missing_live"))
        else:
            rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                                    "indicator", raw_expect[pname], pname,
                                    val, open_time_iso, status="ok"))

    # ----- MW (ind_mw_live) -----
    mw_objs: Dict[str, Dict[str, Any]] = {}   # kind -> pack-dict
    mw_states: Dict[str, str] = {}            # kind -> state

    for kind in MW_TYPES:
        mw_key = f"ind_mw_live:{symbol}:{tf}:{kind}"
        try:
            js = await redis.get(mw_key)
        except Exception:
            js = None

        if not js:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "marketwatch", kind, "state",
                                     None, open_time_iso, status="error", error_code="missing_live"))
            continue

        try:
            obj = json.loads(js)
            pack = obj.get("pack") or {}
            state = pack.get("state")
        except Exception:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "marketwatch", kind, "state",
                                     None, open_time_iso, status="error", error_code="json_parse"))
            continue

        if state is None:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "marketwatch", kind, "state",
                                     None, open_time_iso, status="error", error_code="state_missing"))
            continue

        mw_states[kind] = str(state)
        mw_objs[kind] = pack

        # state по kind (как раньше)
        rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                                "marketwatch", kind, "state",
                                str(state), open_time_iso, status="ok"))

    # ----- MW-derived: pullback_flag / mom_align / high_vol -----
    # direction из trend.state
    trend_state = mw_states.get("trend")
    if trend_state:
        if trend_state.startswith("up_"):
            trend_dir = "up"
        elif trend_state.startswith("down_"):
            trend_dir = "down"
        else:
            trend_dir = "sideways"
    else:
        trend_dir = None

    # pullback_flag: сравнение направления тренда и знака смещения BB-корзины в extremes
    pullback_flag = "none"
    ext_pack = mw_objs.get("extremes") or {}
    try:
        bb_info = ext_pack.get("bb") or {}
        bdelta_raw = bb_info.get("bucket_delta")
        bdelta = int(bdelta_raw) if bdelta_raw is not None else None
    except Exception:
        bdelta = None

    if trend_dir in ("up", "down") and bdelta is not None:
        if trend_dir == "up":
            if bdelta <= -1: pullback_flag = "against"
            elif bdelta >= +1: pullback_flag = "with"
        elif trend_dir == "down":
            if bdelta >= +1: pullback_flag = "against"
            elif bdelta <= -1: pullback_flag = "with"

    rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                            "marketwatch", "pullback_flag", "state",
                            pullback_flag, open_time_iso, status="ok"))

    # mom_align: импульс из Momentum относительно направления тренда
    mom_state = mw_states.get("momentum")
    if mom_state in ("bull_impulse", "bear_impulse") and trend_dir in ("up","down"):
        if (mom_state == "bull_impulse" and trend_dir == "up") or (mom_state == "bear_impulse" and trend_dir == "down"):
            mom_align = "aligned"
        else:
            mom_align = "countertrend"
    else:
        mom_align = "flat"

    rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                            "marketwatch", "mom_align", "state",
                            mom_align, open_time_iso, status="ok"))

    # high_vol: ATR% из volatility.pack против порога
    vol_pack = mw_objs.get("volatility") or {}
    atr_pct_str = None
    try:
        atr_pct_str = vol_pack.get("atr_pct") or (vol_pack.get("atr") or {}).get("pct")
    except Exception:
        atr_pct_str = None

    atr_pct_val = None
    try:
        atr_pct_val = float(atr_pct_str) if atr_pct_str is not None else None
    except Exception:
        atr_pct_val = None

    high_thr = vol_thresholds(tf)["atr_high"]
    high_vol = "yes" if (atr_pct_val is not None and atr_pct_val >= high_thr) else "no"

    rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                            "marketwatch", "high_vol", "state",
                            high_vol, open_time_iso, status="ok"))

    # ----- PACK (pack_live) -----
    for ind in ("rsi","mfi","ema","atr","lr","adx_dmi"):  # без 'kama'
        for L in sorted(pack_bases.get(ind, set())):
            base = f"{ind}{int(L)}"
            pkey = f"pack_live:{ind}:{symbol}:{tf}:{base}"
            try:
                pjson = await redis.get(pkey)
            except Exception:
                pjson = None
            if not pjson:
                rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                         "pack", base, "error",
                                         None, open_time_iso, status="error", error_code="missing_live"))
                continue
            try:
                pobj = json.loads(pjson)
                pack = pobj.get("pack") or {}
            except Exception:
                rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                         "pack", base, "error",
                                         None, open_time_iso, status="error", error_code="json_parse"))
                continue

            fields = PACK_WHITELIST.get(ind, [])
            for pname in fields:
                val = pack.get(pname)
                if val is None:
                    continue
                rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                                        "pack", base, pname,
                                        str(val), open_time_iso, status="ok"))

    # macd: базы по fast
    for F in sorted(pack_bases.get("macd", set())):
        base = f"macd{int(F)}"
        pkey = f"pack_live:macd:{symbol}:{tf}:{base}"
        try:
            pjson = await redis.get(pkey)
        except Exception:
            pjson = None
        if not pjson:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "pack", base, "error",
                                     None, open_time_iso, status="error", error_code="missing_live"))
        else:
            try:
                pobj = json.loads(pjson)
                pack = pobj.get("pack") or {}
                for pname in PACK_WHITELIST.get("macd", []):
                    val = pack.get(pname)
                    if val is None:
                        continue
                    rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                                            "pack", base, pname,
                                            str(val), open_time_iso, status="ok"))
            except Exception:
                rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                         "pack", base, "error",
                                         None, open_time_iso, status="error", error_code="json_parse"))

    # bb: базы по (length,std)
    for (L, S) in sorted(pack_bases.get("bb", set())):
        std_str = str(round(float(S), 2)).replace(".", "_")
        base = f"bb{int(L)}_{std_str}"
        pkey = f"pack_live:bb:{symbol}:{tf}:{base}"
        try:
            pjson = await redis.get(pkey)
        except Exception:
            pjson = None
        if not pjson:
            rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                     "pack", base, "error",
                                     None, open_time_iso, status="error", error_code="missing_live"))
        else:
            try:
                pobj = json.loads(pjson)
                pack = pobj.get("pack") or {}
                for pname in PACK_WHITELIST.get("bb", []):
                    val = pack.get(pname)
                    if val is None:
                        continue
                    rows_ok.append(make_row(position_uid, strategy_id, symbol, tf,
                                            "pack", base, pname,
                                            str(val), open_time_iso, status="ok"))
            except Exception:
                rows_err.append(make_row(position_uid, strategy_id, symbol, tf,
                                         "pack", base, "error",
                                         None, open_time_iso, status="error", error_code="json_parse"))

    tf_ms = int((time.monotonic() - tf_t0) * 1000)
    log.info(
        f"POS_SNAPSHOT_LIVE uid={position_uid} sym={symbol} tf={tf} "
        f"raw_ok={len([r for r in rows_ok if r[4]=='indicator'])} pack_ok={len([r for r in rows_ok if r[4]=='pack'])} mw_ok={len([r for r in rows_ok if r[4]=='marketwatch'])} "
        f"raw_err={len([r for r in rows_err if r[4]=='indicator'])} pack_err={len([r for r in rows_err if r[4]=='pack'])} mw_err={len([r for r in rows_err if r[4]=='marketwatch'])} "
        f"elapsed_ms={tf_ms}"
    )

    return rows_ok, rows_err


# 🔸 Обработка одной позиции: TF последовательно (m5 → m15 → h1)
async def process_position_live(redis,
                                get_instances_by_tf,
                                get_strategy_mw,
                                pos_payload: Dict[str, Any],
                                pg=None) -> None:
    pos_t0 = time.monotonic()

    # извлекаем базовые поля
    position_uid = pos_payload.get("position_uid")
    symbol = pos_payload.get("symbol")
    strategy_id_raw = pos_payload.get("strategy_id")
    created_at_iso = pos_payload.get("created_at")

    # валидация
    try:
        strategy_id = int(strategy_id_raw) if strategy_id_raw is not None else None
    except Exception:
        strategy_id = None

    if not position_uid or not symbol or strategy_id is None or not created_at_iso:
        log.debug(f"[SKIP] bad position payload: {pos_payload}")
        return

    # фильтр по стратегиям (market_watcher=true)
    try:
        if not get_strategy_mw(int(strategy_id)):
            log.debug(f"[SKIP] strategy_id={strategy_id} market_watcher=false")
            return
    except Exception:
        log.debug(f"[SKIP] strategy_id={strategy_id} (mw flag check failed)")
        return

    created_at_ms = iso_to_ms(created_at_iso)

    # аккумулируем строки со всех TF и пишем одним батчем
    all_rows_ok: List[Tuple] = []
    all_rows_err: List[Tuple] = []
    total_ok = total_err = 0

    for tf in TFS:
        ok_rows, err_rows = await process_tf_live(
            redis, get_instances_by_tf, position_uid, strategy_id, symbol, tf, created_at_ms
        )
        total_ok  += len(ok_rows)
        total_err += len(err_rows)
        all_rows_ok.extend(ok_rows)
        all_rows_err.extend(err_rows)

    # 🔸 запись в БД: одна попытка, при FK-ошибке — пауза 5 сек и повтор
    try:
        await upsert_rows_live(pg, all_rows_ok + all_rows_err)
    except Exception as e:
        msg = str(e)
        # короткий retry только на FK-ошибки по позициям/стратегиям
        if ("fk_ips_position" in msg) or ("fk_ips_strategy" in msg) or ("ForeignKeyViolationError" in msg):
            log.warning(f"[LIVE] FK missing on first write (uid={position_uid}). Retry in 5s…")
            await asyncio.sleep(5)
            try:
                await upsert_rows_live(pg, all_rows_ok + all_rows_err)
                log.info(f"[LIVE] retry write OK uid={position_uid}")
            except Exception as e2:
                log.warning(f"POS_SNAPSHOT_LIVE db_upsert error (retry) uid={position_uid} sym={symbol}: {e2}", exc_info=True)
        else:
            log.warning(f"POS_SNAPSHOT_LIVE db_upsert error uid={position_uid} sym={symbol}: {e}", exc_info=True)

    pos_ms = int((time.monotonic() - pos_t0) * 1000)
    log.info(
        f"POS_SNAPSHOT_LIVE WRITE uid={position_uid} sym={symbol} ok_rows={total_ok} err_rows={total_err} elapsed_ms={pos_ms}"
    )


# 🔸 Основной воркер: отдельная consumer-group
async def run_position_snapshot_live(pg,
                                     redis,
                                     get_instances_by_tf,
                                     get_strategy_mw):
    log.debug("POS_SNAPSHOT_LIVE: воркер запущен (чтение live-ключей → запись в indicator_position_stat)")

    # создаём consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(REQ_STREAM_POSITIONS, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    sem = asyncio.Semaphore(POS_CONCURRENCY)

    async def handle_one(msg_id: str, data: dict) -> str:
        async with sem:
            try:
                await process_position_live(redis, get_instances_by_tf, get_strategy_mw, data, pg=pg)
            except Exception as e:
                log.warning(f"[LIVE] error {e}", exc_info=True)
            return msg_id

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={REQ_STREAM_POSITIONS: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS
            )
        except Exception as e:
            log.error(f"POS_SNAPSHOT_LIVE read error: {e}", exc_info=True)
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
                await redis.xack(REQ_STREAM_POSITIONS, GROUP, *ack_ids)
        except Exception as e:
            log.error(f"POS_SNAPSHOT_LIVE batch error: {e}", exc_info=True)
            await asyncio.sleep(0.5)