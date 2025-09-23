# indicator_gateway.py — on-demand координатор (RAW + PACK: RSI/MFI/BB/LR/ATR/EMA/ADX-DMI/MACD + TREND/VOL/MOM/EXT) с параллелизмом, ожиданием бара, кэшем и дедупликацией

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta

# 🔸 Импорты pack-билдеров
from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.bb_pack  import build_bb_pack
from packs.lr_pack  import build_lr_pack
from packs.atr_pack import build_atr_pack
from packs.ema_pack import build_ema_pack
from packs.adx_dmi_pack import build_adx_dmi_pack
from packs.macd_pack import build_macd_pack
from packs.trend_pack import build_trend_pack
from packs.volatility_pack import build_volatility_pack
from packs.momentum_pack import build_momentum_pack
from packs.extremes_pack import build_extremes_pack
from packs.pack_utils import floor_to_bar, load_ohlcv_df

# 🔸 Логгер
log = logging.getLogger("IND_GATEWAY")

# 🔸 Streams
REQ_STREAM  = "indicator_gateway_request"
RESP_STREAM = "indicator_gateway_response"

# 🔸 Параметры параллелизма/батчинга и ожиданий
GATEWAY_CONCURRENCY   = 30
GATEWAY_BATCH_SIZE    = 100
GW_WAIT_FIRST_MS      = 5000   # первые 5 секунд ждём активно
GW_WAIT_RETRIES       = 10      # потом 5 попыток по 1 секунде
GW_WAIT_RETRY_GAP_MS  = 1000
GW_CACHE_TTL_SEC      = 30
GW_LOCK_TTL_SEC       = 15     # должен покрывать все ожидания
DF_MEMO_TTL_SEC       = 10

# 🔸 Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Префиксы Redis
TS_IND_PREFIX  = "ts_ind"            # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX   = "bb:ts"             # bb:ts:{symbol}:{tf}:{o|h|l|c|v}
RAW_CACHE_PREF = "gw_cache:raw"      # gw_cache:raw:{symbol}:{tf}:{instance_id}:{bar_open_ms}
PACK_CACHE_PREF = "gw_cache"         # gw_cache:{indicator}:{symbol}:{tf}:{base}:{bar_open_ms}
LOCK_RAW_PREF  = "lock:gw:raw"       # lock:gw:raw:{symbol}:{tf}:{instance_id}:{bar_open_ms}
LOCK_PACK_PREF = "lock:gw:pack"      # lock:gw:pack:{indicator}:{symbol}:{tf}:{base}:{bar_open_ms}

# 🔸 In-process DF memo (коалесcенс на всплесках)
_df_memo: dict[tuple[str, str, int], tuple[float, object]] = {}

# 🔸 Валидация индикаторов
PACK_INDICATORS = {"rsi","mfi","bb","lr","atr","ema","adx_dmi","macd","trend","volatility","momentum","extremes"}
RAW_INDICATORS  = {"rsi","mfi","bb","lr","atr","ema","adx_dmi","macd"}  # композиты в RAW не поддерживаем

# 🔸 Ключи кэша/локов (PACK)
def pack_cache_key(indicator: str, symbol: str, tf: str, base: str, bar_open_ms: int) -> str:
    return f"{PACK_CACHE_PREF}:{indicator}:{symbol}:{tf}:{base}:{bar_open_ms}"

def pack_public_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    if indicator == "bb":
        return f"bbpos_pack:{symbol}:{tf}:{base}"
    if indicator == "lr":
        return f"lrpos_pack:{symbol}:{tf}:{base}"
    if indicator == "atr":
        return f"atr_pack:{symbol}:{tf}:{base}"
    if indicator == "adx_dmi":
        return f"adx_dmi_pack:{symbol}:{tf}:{base}"
    if indicator == "macd":
        return f"macd_pack:{symbol}:{tf}:{base}"
    return f"{indicator}_pack:{symbol}:{tf}:{base}"

def pack_lock_key(indicator: str, symbol: str, tf: str, base: str, bar_open_ms: int) -> str:
    return f"{LOCK_PACK_PREF}:{indicator}:{symbol}:{tf}:{base}:{bar_open_ms}"

# 🔸 Ключи кэша/локов (RAW)
def raw_cache_key(symbol: str, tf: str, instance_id: int, bar_open_ms: int) -> str:
    return f"{RAW_CACHE_PREF}:{symbol}:{tf}:{instance_id}:{bar_open_ms}"

def raw_lock_key(symbol: str, tf: str, instance_id: int, bar_open_ms: int) -> str:
    return f"{LOCK_RAW_PREF}:{symbol}:{tf}:{instance_id}:{bar_open_ms}"

# 🔸 Парсинг std из строки (2 знака)
def parse_std(std_raw) -> float | None:
    try:
        return round(float(std_raw), 2)
    except Exception:
        return None

# 🔸 Проверка наличия точки close на exact open_time (bar-ready)
async def ts_has_close(redis, symbol: str, tf: str, open_ms: int) -> bool:
    try:
        res = await redis.execute_command("TS.RANGE", f"{BB_TS_PREFIX}:{symbol}:{tf}:c", open_ms, open_ms)
        return bool(res)
    except Exception:
        return False

# 🔸 Ожидание появления текущего бара (универсально для RAW/PACK)
async def wait_current_bar(redis, symbol: str, tf: str, open_ms: int) -> bool:
    # активная фаза ожидания (5 секунд, проверка ~каждые 200мс)
    deadline = time.monotonic() + GW_WAIT_FIRST_MS / 1000.0
    while time.monotonic() < deadline:
        if await ts_has_close(redis, symbol, tf, open_ms):
            return True
        await asyncio.sleep(0.2)
    # ещё 5 ретраев по 1 секунде
    for _ in range(GW_WAIT_RETRIES):
        if await ts_has_close(redis, symbol, tf, open_ms):
            return True
        await asyncio.sleep(GW_WAIT_RETRY_GAP_MS / 1000.0)
    return False

# 🔸 Получить/положить DF из in-process memo
def df_memo_get(symbol: str, tf: str, open_ms: int):
    key = (symbol, tf, open_ms)
    now = time.monotonic()
    rec = _df_memo.get(key)
    if not rec:
        return None
    exp_ts, df = rec
    if now > exp_ts:
        _df_memo.pop(key, None)
        return None
    return df

def df_memo_put(symbol: str, tf: str, open_ms: int, df):
    key = (symbol, tf, open_ms)
    _df_memo[key] = (time.monotonic() + DF_MEMO_TTL_SEC, df)

# 🔸 Канонизация base для RAW по инстансу
def raw_base_name(indicator: str, params: dict) -> str:
    if indicator == "macd":
        return f"macd{params['fast']}"
    if "length" in params:
        return f"{indicator}{params['length']}"
    return indicator

# 🔸 Единый формат ответа (элемент массива)
def make_response_item(base: str, mode: str, pack_payload: dict, open_iso: str, using_current: bool, ref: str | None = None) -> dict:
    item = {
        "base": base,
        "mode": mode,
        "pack": {
            **pack_payload,
            "open_time": open_iso,
            "using_current_bar": "true" if using_current else "false",
        },
    }
    if ref is not None:
        item["pack"]["ref"] = ref
    return item

# 🔸 Основной воркер gateway (RAW + PACK)
async def run_indicator_gateway(pg, redis, get_instances_by_tf, get_precision, compute_snapshot_values_async):
    log.debug("IND_GATEWAY: воркер запущен")

    group = "gw_group"
    consumer = "gw_consumer"

    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(REQ_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    sem = asyncio.Semaphore(GATEWAY_CONCURRENCY)

    async def process_one(msg_id: str, data: dict) -> str | None:
        # обработка одного сообщения с ограничением по семафору
        async with sem:
            t0 = time.monotonic()
            try:
                # базовые поля запроса
                symbol   = data.get("symbol")
                tf       = data.get("timeframe")
                ind      = data.get("indicator")
                mode     = (data.get("mode") or "pack").lower()
                length_s = data.get("length")      # для rsi/mfi/ema/lr/atr/adx_dmi/bb/macd
                std_s    = data.get("std")         # только для bb
                ts_raw   = data.get("timestamp_ms")

                # валидация общая
                if not symbol or tf not in ("m5","m15","h1") or ind not in PACK_INDICATORS or mode not in ("pack","raw"):
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_request"})
                    return msg_id

                # режим RAW не поддерживает композиты
                if mode == "raw" and ind not in RAW_INDICATORS:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_request"})
                    return msg_id

                # нормализуем время к началу бара (фиксируем таргет)
                now_ms = int(ts_raw) if ts_raw else int(datetime.utcnow().timestamp() * 1000)
                bar_open_ms = floor_to_bar(now_ms, tf)
                open_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

                # точность по символу
                precision = get_precision(symbol)
                if precision is None:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"symbol_not_active"})
                    return msg_id
                precision = int(precision)

                # ожидание текущего бара: для PACK обязательно, для RAW допускаем фолбэк
                ready = await wait_current_bar(redis, symbol, tf, bar_open_ms)
                if not ready and mode == "pack":
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"not_ready"})
                    log.debug(f"IND_GATEWAY PACK not_ready {ind} {symbol}/{tf} open={open_iso}")
                    return msg_id

                results: list[dict] = []
                cache_hits = 0

                # 🔸 PACK режим (унифицированный ответ)
                if mode == "pack":
                    # для индикаторов с инстансами — проверим, что есть активные; для композитов — пропускаем
                    instances = [] if ind in ("trend","volatility","momentum","extremes") else [i for i in get_instances_by_tf(tf) if i["indicator"] == ind]
                    if ind not in ("trend","volatility","momentum","extremes") and not instances:
                        await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                        return msg_id

                    # ветви по индикатору
                    if ind in ("rsi","mfi","ema","lr","atr","adx_dmi"):
                        # определить список length
                        if length_s:
                            try:
                                L = int(length_s)
                            except Exception:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                return msg_id
                            if not any(int(i["params"]["length"]) == L for i in instances):
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                            lengths = [L]
                        else:
                            lengths = sorted({int(i["params"]["length"]) for i in instances})

                        for L in lengths:
                            base = f"{ind}{L}"
                            ckey = pack_cache_key(ind, symbol, tf, base, bar_open_ms)
                            pkey = pack_public_key(ind, symbol, tf, base)

                            cached = await redis.get(ckey)
                            if cached:
                                try:
                                    pack_obj = json.loads(cached)
                                    # дополним унифицированными полями
                                    pack_obj["mode"] = "pack"
                                    pack_obj["pack"]["using_current_bar"] = "true"
                                    results.append(pack_obj)
                                    cache_hits += 1
                                    continue
                                except Exception:
                                    pass

                            # ожидание готовности уже выполнили; строим пакет
                            if ind == "rsi":
                                pack_obj = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "mfi":
                                pack_obj = await build_mfi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "ema":
                                pack_obj = await build_ema_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "lr":
                                pack_obj = await build_lr_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "atr":
                                pack_obj = await build_atr_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            else:  # adx_dmi
                                pack_obj = await build_adx_dmi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)

                            if pack_obj:
                                # выставим унифицированные поля
                                pack_obj["mode"] = "pack"
                                pack_obj["pack"]["using_current_bar"] = "true"
                                js = json.dumps(pack_obj)
                                await redis.set(ckey, js, ex=GW_CACHE_TTL_SEC)
                                await redis.set(pkey, js, ex=GW_CACHE_TTL_SEC)
                                results.append(pack_obj)

                    elif ind == "macd":
                        # длина трактуется как fast
                        if length_s:
                            try:
                                F = int(length_s)
                            except Exception:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                return msg_id
                            if not any(int(i["params"]["fast"]) == F for i in instances):
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                            fasts = [F]
                        else:
                            fasts = sorted({int(i["params"]["fast"]) for i in instances})

                        for F in fasts:
                            base = f"macd{F}"
                            ckey = pack_cache_key(ind, symbol, tf, base, bar_open_ms)
                            pkey = pack_public_key(ind, symbol, tf, base)

                            cached = await redis.get(ckey)
                            if cached:
                                try:
                                    pack_obj = json.loads(cached)
                                    pack_obj["mode"] = "pack"
                                    pack_obj["pack"]["using_current_bar"] = "true"
                                    results.append(pack_obj)
                                    cache_hits += 1
                                    continue
                                except Exception:
                                    pass

                            pack_obj = await build_macd_pack(symbol, tf, F, now_ms, precision, redis, compute_snapshot_values_async)
                            if pack_obj:
                                pack_obj["mode"] = "pack"
                                pack_obj["pack"]["using_current_bar"] = "true"
                                js = json.dumps(pack_obj)
                                await redis.set(ckey, js, ex=GW_CACHE_TTL_SEC)
                                await redis.set(pkey, js, ex=GW_CACHE_TTL_SEC)
                                results.append(pack_obj)

                    elif ind == "bb":
                        # собрать активные (length, std)
                        active_pairs = []
                        for i in instances:
                            try:
                                L = int(i["params"]["length"])
                                S = round(float(i["params"]["std"]), 2)
                                active_pairs.append((L, S))
                            except Exception:
                                pass

                        if length_s and std_s:
                            try:
                                L = int(length_s)
                                S = parse_std(std_s)
                            except Exception:
                                L, S = None, None
                            if (L, S) not in active_pairs:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                            pairs = [(L, S)]
                        elif length_s and not std_s:
                            try:
                                L = int(length_s)
                            except Exception:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                return msg_id
                            pairs = [(L, S) for (L2, S) in active_pairs if L2 == L]
                            if not pairs:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                        elif std_s and not length_s:
                            S = parse_std(std_s)
                            pairs = [(L, S) for (L, S2) in active_pairs if S2 == S]
                            if not pairs:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                        else:
                            pairs = sorted(set(active_pairs))

                        for (L, S) in pairs:
                            std_str = str(round(float(S), 2)).replace(".", "_")
                            base = f"bb{int(L)}_{std_str}"
                            ckey = pack_cache_key(ind, symbol, tf, base, bar_open_ms)
                            pkey = pack_public_key(ind, symbol, tf, base)

                            cached = await redis.get(ckey)
                            if cached:
                                try:
                                    pack_obj = json.loads(cached)
                                    pack_obj["mode"] = "pack"
                                    pack_obj["pack"]["using_current_bar"] = "true"
                                    results.append(pack_obj)
                                    cache_hits += 1
                                    continue
                                except Exception:
                                    pass

                            pack_obj = await build_bb_pack(symbol, tf, L, S, now_ms, precision, redis, compute_snapshot_values_async)
                            if pack_obj:
                                pack_obj["mode"] = "pack"
                                pack_obj["pack"]["using_current_bar"] = "true"
                                js = json.dumps(pack_obj)
                                await redis.set(ckey, js, ex=GW_CACHE_TTL_SEC)
                                await redis.set(pkey, js, ex=GW_CACHE_TTL_SEC)
                                results.append(pack_obj)

                    elif ind in ("trend","volatility","momentum","extremes"):
                        base = ind
                        ckey = pack_cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = pack_public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                pack_obj = json.loads(cached)
                                pack_obj["mode"] = "pack"
                                pack_obj["pack"]["using_current_bar"] = "true"
                                results.append(pack_obj)
                                cache_hits += 1
                            except Exception:
                                pass
                        else:
                            if ind == "trend":
                                pack_obj = await build_trend_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "volatility":
                                pack_obj = await build_volatility_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                            elif ind == "momentum":
                                pack_obj = await build_momentum_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                            else:  # extremes
                                pack_obj = await build_extremes_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)

                            if pack_obj:
                                pack_obj["mode"] = "pack"
                                pack_obj["pack"]["using_current_bar"] = "true"
                                js = json.dumps(pack_obj)
                                await redis.set(ckey, js, ex=GW_CACHE_TTL_SEC)
                                await redis.set(pkey, js, ex=GW_CACHE_TTL_SEC)
                                results.append(pack_obj)

                # 🔸 RAW режим (унифицированный ответ)
                else:
                    # соберём активные инстансы запрошенного индикатора на TF
                    instances = [i for i in get_instances_by_tf(tf) if i["indicator"] == ind]
                    if not instances:
                        await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                        return msg_id

                    # список целей (по длинам/fast)
                    targets = []
                    if ind == "macd":
                        if length_s:
                            try:
                                F = int(length_s)
                            except Exception:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                return msg_id
                            instances = [i for i in instances if int(i["params"]["fast"]) == F]
                            if not instances:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                        targets = instances
                    elif ind == "bb":
                        # bb для RAW тоже допустим: length+std в инстансах
                        if length_s and std_s:
                            try:
                                L = int(length_s)
                                S = parse_std(std_s)
                            except Exception:
                                L, S = None, None
                            instances = [i for i in instances if int(i["params"]["length"]) == L and round(float(i["params"]["std"]),2) == S]
                            if not instances:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                        targets = instances
                    else:
                        if length_s:
                            try:
                                L = int(length_s)
                            except Exception:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                return msg_id
                            instances = [i for i in instances if int(i["params"]["length"]) == L]
                            if not instances:
                                await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                return msg_id
                        targets = instances

                    # фолбэк на предыдущий бар, если current не готов
                    using_current = True
                    if not ready:
                        using_current = False
                        bar_open_ms = bar_open_ms - STEP_MS[tf]
                        open_iso = datetime.utcfromtimestamp(bar_open_ms / 1000).isoformat()

                    # коалесcенс DF (in-process)
                    df = df_memo_get(symbol, tf, bar_open_ms)
                    if df is None:
                        df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
                        if df is None or df.empty:
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"no_ohlcv"})
                            return msg_id
                        df_memo_put(symbol, tf, bar_open_ms, df)

                    # обрабатываем цели
                    for inst in targets:
                        instance_id = int(inst["id"])
                        enabled_at = inst.get("enabled_at")
                        # проверка enabled_at для бара
                        if enabled_at:
                            enabled_ms = int(enabled_at.replace(tzinfo=None).timestamp() * 1000)
                            if bar_open_ms < enabled_ms:
                                # бар раньше активации — пропускаем инстанс
                                continue

                        # кэш RAW
                        ckey = raw_cache_key(symbol, tf, instance_id, bar_open_ms)
                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                cached_dict = json.loads(cached)
                                # уже в унифицированном виде
                                results.append(cached_dict)
                                cache_hits += 1
                                continue
                            except Exception:
                                pass

                        # вычисление (без дублирования через лок)
                        lkey = raw_lock_key(symbol, tf, instance_id, bar_open_ms)
                        got_lock = False
                        try:
                            got_lock = await redis.set(lkey, "1", nx=True, ex=GW_LOCK_TTL_SEC)
                        except Exception:
                            pass

                        if not got_lock:
                            # кто-то уже считает — подождём кэш короткими попытками
                            for _ in range(15):  # ~1.5с
                                await asyncio.sleep(0.1)
                                cached = await redis.get(ckey)
                                if cached:
                                    try:
                                        cached_dict = json.loads(cached)
                                        results.append(cached_dict)
                                        cache_hits += 1
                                        break
                                    except Exception:
                                        break
                            else:
                                # не дождались — продолжаем считать сами (best-effort)
                                pass

                        if got_lock or not cached:
                            # расчёт RAW
                            base = raw_base_name(inst["indicator"], inst["params"])
                            values = await compute_snapshot_values_async(inst, symbol, df, precision)
                            if values:
                                payload = {"results": values}
                                item = make_response_item(
                                    base=base,
                                    mode="raw",
                                    pack_payload=payload,
                                    open_iso=open_iso,
                                    using_current=using_current,
                                    ref=("live" if using_current else "closed_prev")
                                )
                                js = json.dumps(item)
                                await redis.set(ckey, js, ex=GW_CACHE_TTL_SEC)
                                results.append(item)
                            # снимаем лок
                            try:
                                await redis.delete(lkey)
                            except Exception:
                                pass

                # 🔸 Ответ
                if results:
                    await redis.xadd(RESP_STREAM, {
                        "req_id": msg_id,
                        "status": "ok",
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": ind,
                        "mode": mode,
                        "results": json.dumps(results),
                    })
                    t1 = time.monotonic()
                    log.debug(
                        f"IND_GATEWAY OK mode={mode} ind={ind} {symbol}/{tf} "
                        f"open={open_iso} count={len(results)} cache_hits={cache_hits} "
                        f"elapsed_ms={int((t1-t0)*1000)}"
                    )
                else:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"no_results"})
                    t1 = time.monotonic()
                    log.debug(
                        f"IND_GATEWAY EMPTY mode={mode} ind={ind} {symbol}/{tf} "
                        f"open={open_iso} cache_hits={cache_hits} elapsed_ms={int((t1-t0)*1000)}"
                    )
                return msg_id

            except Exception as e:
                log.warning(f"[GW] error: {e}", exc_info=True)
                try:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"exception"})
                except Exception:
                    pass
                return msg_id

    # 🔸 Основной цикл чтения стрима: пачкой + параллельная обработка + батч-ACK
    while True:
        try:
            resp = await redis.xreadgroup(
                group, consumer,
                streams={REQ_STREAM: ">"},
                count=GATEWAY_BATCH_SIZE,
                block=2000
            )
        except Exception as e:
            log.error(f"IND_GATEWAY read error: {e}", exc_info=True)
            await asyncio.sleep(0.5)
            continue

        if not resp:
            continue

        try:
            tasks = []
            for _, messages in resp:
                for msg_id, data in messages:
                    tasks.append(asyncio.create_task(process_one(msg_id, data)))

            done_ids = await asyncio.gather(*tasks, return_exceptions=False)
            ack_ids = [mid for mid in done_ids if mid]
            if ack_ids:
                await redis.xack(REQ_STREAM, group, *ack_ids)
        except Exception as e:
            log.error(f"IND_GATEWAY batch error: {e}", exc_info=True)
            await asyncio.sleep(0.5)