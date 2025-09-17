# indicator_gateway.py — on-demand координатор (пока только для RSI)

import asyncio
import json
import logging
import time
from datetime import datetime

from packs.rsi_pack import build_rsi_pack
from packs.pack_utils import floor_to_bar

log = logging.getLogger("IND_GATEWAY")

# 🔸 Конфигурация
REQ_STREAM  = "indicator_gateway_request"
RESP_STREAM = "indicator_gateway_response"

CACHE_KEY   = "gw_cache:rsi:{symbol}:{tf}:{length}:{bar_open_ms}"
PUBLIC_KEY  = "rsi_pack:{symbol}:{tf}:rsi{length}"

LIVE_TTL_SEC = 30


# 🔸 Основной воркер gateway (пока только rsi)
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
                        symbol = data.get("symbol")
                        tf = data.get("timeframe")
                        ind = data.get("indicator")
                        length_raw = data.get("length")
                        ts_raw = data.get("timestamp_ms")

                        if not symbol or tf not in ("m5","m15","h1") or ind != "rsi":
                            await redis.xadd(RESP_STREAM, {
                                "req_id": msg_id,
                                "status": "error",
                                "error": "bad_request"
                            })
                            continue

                        now_ms = int(ts_raw) if ts_raw else int(datetime.utcnow().timestamp()*1000)
                        bar_open_ms = floor_to_bar(now_ms, tf)

                        # какие RSI-инстансы активны на TF
                        instances = [i for i in get_instances_by_tf(tf) if i["indicator"]=="rsi"]
                        if not instances:
                            await redis.xadd(RESP_STREAM, {
                                "req_id": msg_id, "status":"error", "error":"instance_not_found"
                            })
                            continue

                        lengths = []
                        if length_raw:
                            try:
                                L = int(length_raw)
                                if not any(int(i["params"]["length"])==L for i in instances):
                                    await redis.xadd(RESP_STREAM, {
                                        "req_id": msg_id, "status":"error", "error":"instance_not_found"
                                    })
                                    continue
                                lengths = [L]
                            except Exception:
                                await redis.xadd(RESP_STREAM, {
                                    "req_id": msg_id, "status":"error", "error":"bad_length"
                                })
                                continue
                        else:
                            lengths = [int(i["params"]["length"]) for i in instances]

                        results = []
                        precision = get_precision(symbol) or 8

                        for L in lengths:
                            cache_key = CACHE_KEY.format(symbol=symbol, tf=tf, length=L, bar_open_ms=bar_open_ms)
                            public_key = PUBLIC_KEY.format(symbol=symbol, tf=tf, length=L)

                            cached = await redis.get(cache_key)
                            if cached:
                                try:
                                    pack = json.loads(cached)
                                    results.append(pack)
                                    continue
                                except Exception:
                                    pass

                            pack = await build_rsi_pack(symbol, tf, L, now_ms, precision, redis, compute_snapshot_values_async)
                            if pack:
                                await redis.set(cache_key, json.dumps(pack), ex=LIVE_TTL_SEC)
                                await redis.set(public_key, json.dumps(pack), ex=LIVE_TTL_SEC)
                                results.append(pack)

                        if results:
                            await redis.xadd(RESP_STREAM, {
                                "req_id": msg_id,
                                "status": "ok",
                                "symbol": symbol,
                                "timeframe": tf,
                                "indicator": "rsi",
                                "results": json.dumps(results),
                            })

                        t1 = time.monotonic()
                        log.info(f"[DONE] IND_GATEWAY RSI {symbol}/{tf} len={lengths} elapsed_ms={int((t1-t0)*1000)} results={len(results)}")

                    except Exception as e:
                        log.warning(f"[GW] error {e}", exc_info=True)
                        await redis.xadd(RESP_STREAM, {
                            "req_id": msg_id, "status": "error", "error": "exception"
                        })

            if to_ack:
                await redis.xack(REQ_STREAM, group, *to_ack)

        except Exception as e:
            log.error(f"IND_GATEWAY loop error: {e}", exc_info=True)
            await asyncio.sleep(2)