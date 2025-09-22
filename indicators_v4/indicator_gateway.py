# indicator_gateway.py — on-demand координатор (RSI + MFI + BB + LR + ATR + EMA + ADX/DMI) c параллельной обработкой сообщений

import asyncio
import json
import logging
import time
from datetime import datetime

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
from packs.pack_utils import floor_to_bar

# 🔸 Логгер
log = logging.getLogger("IND_GATEWAY")

# 🔸 Streams
REQ_STREAM  = "indicator_gateway_request"
RESP_STREAM = "indicator_gateway_response"

# 🔸 TTL барного кэша/публичного пакета
LIVE_TTL_SEC = 30

# 🔸 Параллелизм и размер пачки
GATEWAY_CONCURRENCY = 20
GATEWAY_BATCH_SIZE  = 100

# 🔸 Ключи кэша/публичных пакетов
def cache_key(indicator: str, symbol: str, tf: str, base_or_len: str, bar_open_ms: int) -> str:
    return f"gw_cache:{indicator}:{symbol}:{tf}:{base_or_len}:{bar_open_ms}"

def public_key(indicator: str, symbol: str, tf: str, base: str) -> str:
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
    # ema/rsi/mfi → общий шаблон
    return f"{indicator}_pack:{symbol}:{tf}:{base}"

# 🔸 Разбор std из строки запроса (2 знака точности)
def parse_std(std_raw) -> float | None:
    try:
        return round(float(std_raw), 2)
    except Exception:
        return None

# 🔸 Основной воркер gateway (RSI + MFI + BB + LR + ATR + EMA + ADX/DMI)
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
                symbol   = data.get("symbol")
                tf       = data.get("timeframe")
                ind      = data.get("indicator")
                length_s = data.get("length")     # для rsi/mfi/ema/lr/atr/adx_dmi/bb
                std_s    = data.get("std")        # только для bb
                ts_raw   = data.get("timestamp_ms")

                # валидация
                valid_inds = ("rsi","mfi","bb","lr","atr","ema","adx_dmi","macd","trend","volatility","momentum","extremes")
                if not symbol or tf not in ("m5","m15","h1") or ind not in valid_inds:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_request"})
                    return msg_id

                now_ms = int(ts_raw) if ts_raw else int(datetime.utcnow().timestamp() * 1000)
                bar_open_ms = floor_to_bar(now_ms, tf)

                # активные инстансы
                if ind not in ("trend", "volatility", "momentum", "extremes"):
                    instances = [i for i in get_instances_by_tf(tf) if i["indicator"] == ind]
                    if not instances:
                        await redis.xadd(RESP_STREAM, {
                            "req_id": msg_id,
                            "status": "error",
                            "error": "instance_not_found"
                        })
                        return msg_id
                else:
                    # trend/volatility не имеют собственных инстансов — агрегируем из нескольких индикаторов
                    instances = []

                precision = get_precision(symbol) or 8
                results = []

                # 🔸 RSI / MFI / EMA
                if ind in ("rsi", "mfi", "ema"):
                    if length_s:
                        try:
                            L = int(length_s)
                        except Exception:
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "bad_length"})
                            return msg_id
                        if not any(int(i["params"]["length"]) == L for i in instances):
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "instance_not_found"})
                            return msg_id
                        lengths = [L]
                    else:
                        lengths = sorted({int(i["params"]["length"]) for i in instances})

                    for L in lengths:
                        base = f"{ind}{L}"
                        # для rsi/mfi кэш-ключ строим по длине, для ema — по base (единый стиль с публичным ключом)
                        ckey = cache_key(ind, symbol, tf, (str(L) if ind in ("rsi", "mfi") else base), bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        if ind == "rsi":
                            pack = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                        elif ind == "mfi":
                            pack = await build_mfi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                        else:  # ema
                            pack = await build_ema_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)

                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 MACD (base по fast)
                elif ind == "macd":
                    # длина в запросе трактуем как fast
                    if length_s:
                        try:
                            F = int(length_s)
                        except Exception:
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "bad_length"})
                            return msg_id
                        # в инстансах MACD fast лежит в params["fast"]
                        if not any(int(i["params"]["fast"]) == F for i in instances):
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "instance_not_found"})
                            return msg_id
                        fasts = [F]
                    else:
                        fasts = sorted({int(i["params"]["fast"]) for i in instances})

                    for F in fasts:
                        base = f"macd{F}"
                        ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        pack = await build_macd_pack(symbol, tf, F, now_ms, precision, redis, compute_snapshot_values_async)

                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 BB
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
                        ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        pack = await build_bb_pack(symbol, tf, L, S, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 LR
                elif ind == "lr":
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
                        base = f"lr{L}"
                        ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        pack = await build_lr_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 ATR
                elif ind == "atr":
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
                        base = f"atr{L}"
                        ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        pack = await build_atr_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 TREND (live на текущем баре)
                elif ind == "trend":
                    base = "trend"
                    ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                    pkey = public_key(ind, symbol, tf, base)

                    cached = await redis.get(ckey)
                    if cached:
                        try:
                            results.append(json.loads(cached))
                        except Exception:
                            pass
                    else:
                        pack = await build_trend_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)
                            
                # 🔸 VOLATILITY (live на текущем баре)
                elif ind == "volatility":
                    base = "volatility"
                    ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                    pkey = public_key(ind, symbol, tf, base)

                    cached = await redis.get(ckey)
                    if cached:
                        try:
                            results.append(json.loads(cached))
                        except Exception:
                            pass
                    else:
                        pack = await build_volatility_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 MOMENTUM (live на текущем баре)
                elif ind == "momentum":
                    base = "momentum"
                    ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                    pkey = public_key(ind, symbol, tf, base)

                    cached = await redis.get(ckey)
                    if cached:
                        try:
                            results.append(json.loads(cached))
                        except Exception:
                            pass
                    else:
                        pack = await build_momentum_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 EXTREMES (live на текущем баре)
                elif ind == "extremes":
                    base = "extremes"
                    ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                    pkey = public_key(ind, symbol, tf, base)

                    cached = await redis.get(ckey)
                    if cached:
                        try:
                            results.append(json.loads(cached))
                        except Exception:
                            pass
                    else:
                        pack = await build_extremes_pack(symbol, tf, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)
                                      
                # 🔸 ADX/DMI
                else:  # ind == "adx_dmi"
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
                        base = f"adx_dmi{L}"
                        ckey = cache_key(ind, symbol, tf, base, bar_open_ms)
                        pkey = public_key(ind, symbol, tf, base)

                        cached = await redis.get(ckey)
                        if cached:
                            try:
                                results.append(json.loads(cached))
                                continue
                            except Exception:
                                pass

                        pack = await build_adx_dmi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                        if pack:
                            js = json.dumps(pack)
                            await redis.set(ckey, js, ex=LIVE_TTL_SEC)
                            await redis.set(pkey, js, ex=LIVE_TTL_SEC)
                            results.append(pack)

                # 🔸 Ответ
                if results:
                    await redis.xadd(RESP_STREAM, {
                        "req_id": msg_id,
                        "status": "ok",
                        "symbol": symbol,
                        "timeframe": tf,
                        "indicator": ind,
                        "results": json.dumps(results),
                    })
                else:
                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"no_results"})

                t1 = time.monotonic()
                log.debug(f"[DONE] IND_GATEWAY {ind.upper()} {symbol}/{tf} elapsed_ms={int((t1-t0)*1000)} results={len(results)}")
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