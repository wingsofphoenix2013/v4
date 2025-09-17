# indicator_gateway.py — on-demand координатор (RSI + MFI + BB)

import asyncio
import json
import logging
import time
from datetime import datetime

from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.bb_pack  import build_bb_pack
from packs.pack_utils import floor_to_bar

log = logging.getLogger("IND_GATEWAY")

# 🔸 Streams
REQ_STREAM  = "indicator_gateway_request"
RESP_STREAM = "indicator_gateway_response"

# 🔸 TTL барного кэша/публичного пакета
LIVE_TTL_SEC = 30

# 🔸 Ключи кэша/публичных пакетов
def cache_key(indicator: str, symbol: str, tf: str, base_or_len: str, bar_open_ms: int) -> str:
    # для rsi/mfi base_or_len = "<length>"; для bb — "<base>"
    return f"gw_cache:{indicator}:{symbol}:{tf}:{base_or_len}:{bar_open_ms}"

def public_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    # rsi → rsi_pack:{symbol}:{tf}:rsi{L}
    # mfi → mfi_pack:{symbol}:{tf}:mfi{L}
    # bb  → bbpos_pack:{symbol}:{tf}:{base}
    if indicator == "bb":
        return f"bbpos_pack:{symbol}:{tf}:{base}"
    return f"{indicator}_pack:{symbol}:{tf}:{base}"


# 🔸 Разбор std из строки запроса (2 знака точности)
def parse_std(std_raw) -> float | None:
    try:
        return round(float(std_raw), 2)
    except Exception:
        return None


# 🔸 Основной воркер gateway (RSI + MFI + BB)
async def run_indicator_gateway(pg, redis, get_instances_by_tf, get_precision, compute_snapshot_values_async):
    log.debug("IND_GATEWAY: воркер запущен")

    group = "gw_group"
    consumer = "gw_consumer"

    # создать consumer-group
    try:
        await redis.xgroup_create(REQ_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(group, consumer, streams={REQ_STREAM: ">"}, count=50, block=2000)
            if not resp:
                continue

            to_ack = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    t0 = time.monotonic()
                    try:
                        symbol   = data.get("symbol")
                        tf       = data.get("timeframe")
                        ind      = data.get("indicator")
                        length_s = data.get("length")    # для rsi/mfi/bb
                        std_s    = data.get("std")       # только для bb
                        ts_raw   = data.get("timestamp_ms")

                        if not symbol or tf not in ("m5","m15","h1") or ind not in ("rsi","mfi","bb"):
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "bad_request"})
                            continue

                        now_ms = int(ts_raw) if ts_raw else int(datetime.utcnow().timestamp() * 1000)
                        bar_open_ms = floor_to_bar(now_ms, tf)

                        # активные инстансы по TF и типу
                        instances = [i for i in get_instances_by_tf(tf) if i["indicator"] == ind]
                        if not instances:
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                            continue

                        precision = get_precision(symbol) or 8
                        results = []

                        if ind in ("rsi", "mfi"):
                            # определить список длин
                            if length_s:
                                try:
                                    L = int(length_s)
                                except Exception:
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                    continue
                                if not any(int(i["params"]["length"]) == L for i in instances):
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                    continue
                                lengths = [L]
                            else:
                                lengths = sorted({int(i["params"]["length"]) for i in instances})

                            for L in lengths:
                                # кэш на бар (ключ по length)
                                ckey = cache_key(ind, symbol, tf, str(L), bar_open_ms)
                                pkey = public_key(ind, symbol, tf, f"{ind}{L}")

                                cached = await redis.get(ckey)
                                if cached:
                                    try:
                                        results.append(json.loads(cached))
                                        continue
                                    except Exception:
                                        pass

                                if ind == "rsi":
                                    pack = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                                else:
                                    pack = await build_mfi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)

                                if pack:
                                    data_json = json.dumps(pack)
                                    await redis.set(ckey, data_json, ex=LIVE_TTL_SEC)
                                    await redis.set(pkey, data_json, ex=LIVE_TTL_SEC)
                                    results.append(pack)

                        else:
                            # BB: требуется (length, std). Если не заданы — вернуть все активные BB по TF.
                            # Соберём набор (length, std) из активных инстансов; std нормализуем до 2 знаков.
                            active_pairs = []
                            for i in instances:
                                try:
                                    L = int(i["params"]["length"])
                                    S = round(float(i["params"]["std"]), 2)
                                    active_pairs.append((L, S))
                                except Exception:
                                    continue

                            pairs = []
                            if length_s and std_s:
                                try:
                                    L = int(length_s)
                                    S = parse_std(std_s)
                                except Exception:
                                    L, S = None, None
                                if (L, S) not in active_pairs:
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                    continue
                                pairs = [(L, S)]
                            elif length_s and not std_s:
                                try:
                                    L = int(length_s)
                                except Exception:
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"bad_length"})
                                    continue
                                pairs = [(L, S) for (L2, S) in active_pairs if L2 == L]
                                if not pairs:
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                    continue
                            elif std_s and not length_s:
                                S = parse_std(std_s)
                                pairs = [(L, S) for (L, S2) in active_pairs if S2 == S]
                                if not pairs:
                                    await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                                    continue
                            else:
                                # ни length, ни std → все активные BB
                                pairs = sorted(set(active_pairs))

                            for (L, S) in pairs:
                                # base вычислим внутри build_bb_pack; кэшом пользуемся по base
                                pack = None

                                # сначала проверка кэша по base (для этого нужен base → сгенерим так же, как в pack)
                                # формирование base дублируем здесь (чтобы не считать лишний раз):
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
                                    data_json = json.dumps(pack)
                                    await redis.set(ckey, data_json, ex=LIVE_TTL_SEC)
                                    await redis.set(pkey, data_json, ex=LIVE_TTL_SEC)
                                    results.append(pack)

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
                        log.info(f"[DONE] IND_GATEWAY {ind.upper()} {symbol}/{tf} elapsed_ms={int((t1-t0)*1000)} results={len(results)}")

                    except Exception as e:
                        log.warning(f"[GW] error {e}", exc_info=True)
                        await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "exception"})

            if to_ack:
                await redis.xack(REQ_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"IND_GATEWAY loop error: {e}", exc_info=True)
            await asyncio.sleep(2)