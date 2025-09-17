# indicator_gateway.py — on-demand координатор (RSI + MFI)

import asyncio
import json
import logging
import time
from datetime import datetime

from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.pack_utils import floor_to_bar

log = logging.getLogger("IND_GATEWAY")

# 🔸 Streams
REQ_STREAM  = "indicator_gateway_request"
RESP_STREAM = "indicator_gateway_response"

# 🔸 Шаблоны ключей кэша/публичных пакетов
CACHE_KEY_TPL  = "gw_cache:{indicator}:{symbol}:{tf}:{length}:{bar_open_ms}"
PUBLIC_KEY_TPL = "{indicator}_pack:{symbol}:{tf}:{indicator}{length}"

# 🔸 TTL барного кэша/публичного пакета
LIVE_TTL_SEC = 30

# 🔸 Основной воркер gateway (RSI + MFI)
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
                        length_s = data.get("length")
                        ts_raw   = data.get("timestamp_ms")

                        if not symbol or tf not in ("m5","m15","h1") or ind not in ("rsi","mfi"):
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "bad_request"})
                            continue

                        now_ms = int(ts_raw) if ts_raw else int(datetime.utcnow().timestamp() * 1000)
                        bar_open_ms = floor_to_bar(now_ms, tf)

                        # активные инстансы по TF и типу
                        instances = [i for i in get_instances_by_tf(tf) if i["indicator"] == ind]
                        if not instances:
                            await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status":"error", "error":"instance_not_found"})
                            continue

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

                        results = []
                        precision = get_precision(symbol) or 8

                        for L in lengths:
                            cache_key  = CACHE_KEY_TPL.format(indicator=ind, symbol=symbol, tf=tf, length=L, bar_open_ms=bar_open_ms)
                            public_key = PUBLIC_KEY_TPL.format(indicator=ind, symbol=symbol, tf=tf, length=L)

                            # 1) попытка взять из барного кэша
                            cached = await redis.get(cache_key)
                            if cached:
                                try:
                                    results.append(json.loads(cached))
                                    continue
                                except Exception:
                                    pass

                            # 2) посчитать on-demand
                            if ind == "rsi":
                                pack = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            else:  # mfi
                                pack = await build_mfi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)

                            if pack:
                                data_json = json.dumps(pack)
                                # кэш шлюза + публичный ключ
                                await redis.set(cache_key,  data_json, ex=LIVE_TTL_SEC)
                                await redis.set(public_key, data_json, ex=LIVE_TTL_SEC)
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
                        log.info(f"[DONE] IND_GATEWAY {ind.upper()} {symbol}/{tf} len={lengths} elapsed_ms={int((t1-t0)*1000)} results={len(results)}")

                    except Exception as e:
                        log.warning(f"[GW] error {e}", exc_info=True)
                        await redis.xadd(RESP_STREAM, {"req_id": msg_id, "status": "error", "error": "exception"})

            if to_ack:
                await redis.xack(REQ_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"IND_GATEWAY loop error: {e}", exc_info=True)
            await asyncio.sleep(2)