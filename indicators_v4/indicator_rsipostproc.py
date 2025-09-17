# indicator_rsipostproc.py — постпроцессинг live-RSI: корзина (шаг 5), тренд vs закрытый бар, композитный ключ в Redis

import asyncio
import json
import logging
from datetime import datetime

# 🔸 Логгер модуля
log = logging.getLogger("IND_RSI_POST")

# 🔸 Конфигурация
LIVE_STREAM_NAME = "indicator_live_stream"     # stream, который публикует indicator_livestream (batched)
GROUP_NAME       = "rsi_post_group"            # consumer-group для этого воркера
CONSUMER_NAME    = "rsi_post_1"

# 🔸 TTL для композитного live-ключа (сек)
LIVE_TTL_SEC = 90

# 🔸 Порог «мертвого коридора» (эпсилон) на шкале RSI (в пунктах RSI)
EPSILON_BY_TF = {
    "m5":  0.3,
    "m15": 0.4,
    "h1":  0.6,
}

# 🔸 Ключи Redis
def rsi_closed_key(symbol: str, tf: str, length: int) -> str:
    return f"ind:{symbol}:{tf}:rsi{length}"

def rsi_pack_key(symbol: str, tf: str, length: int) -> str:
    # композитный live-ключ (JSON): {"value": "…", "bucket_low": 20, "delta": "…", "trend": "up|flat|down", "ref": "closed"}
    return f"rsi_pack:{symbol}:{tf}:rsi{length}"


# 🔸 Утилиты
def clamp(v: float, lo: float, hi: float) -> float:
    return hi if v > hi else lo if v < lo else v

def to_bucket_low(rsi_value: float) -> int:
    # нижняя граница корзины кратная 5: 0,5,10,…,95
    x = clamp(rsi_value, 0.0, 99.9999)  # прижимаем 100 к 99.9999, чтобы не получить 100
    return int((int(x) // 5) * 5)

def classify_trend(delta: float, eps: float) -> str:
    if delta >= eps:
        return "up"
    if delta <= -eps:
        return "down"
    return "flat"


# 🔸 Основной воркер постпроцессинга RSI
async def run_indicator_rsipostproc(pg, redis):
    log.debug("IND_RSI_POST: воркер запущен")

    # создать consumer-group (идемпотентно)
    try:
        await redis.xgroup_create(LIVE_STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={LIVE_STREAM_NAME: ">"},
                count=100,
                block=2000
            )
            if not resp:
                continue

            to_ack = []
            total_processed = 0
            total_written = 0

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        # ожидаем поля от indicator_livestream: symbol, timeframe, tick_open_time, instances(JSON), precision
                        symbol = data.get("symbol")
                        tf = data.get("timeframe")
                        instances_raw = data.get("instances")
                        if not symbol or tf not in ("m5", "m15", "h1") or not instances_raw:
                            continue

                        try:
                            instances = json.loads(instances_raw) if isinstance(instances_raw, str) else instances_raw
                        except Exception:
                            continue

                        # фильтруем только RSI
                        rsi_items = [x for x in instances if str(x.get("indicator")) == "rsi"]
                        if not rsi_items:
                            continue

                        pipe = redis.pipeline()
                        writes = 0

                        eps = EPSILON_BY_TF.get(tf, 0.3)

                        for item in rsi_items:
                            params = item.get("params") or {}
                            values = item.get("values") or {}
                            # ожидаем имя param вида rsi{length}
                            try:
                                length = int(params.get("length"))
                            except Exception:
                                continue

                            param_name = f"rsi{length}"
                            sval = values.get(param_name)
                            if sval is None:
                                continue

                            try:
                                v_live = float(sval)
                            except Exception:
                                continue

                            # корзина
                            bucket = to_bucket_low(v_live)

                            # референс: закрытое значение (ind:{symbol}:{tf}:rsi{length})
                            k_closed = rsi_closed_key(symbol, tf, length)
                            s_closed = await redis.get(k_closed)
                            if s_closed is None:
                                # если закрытого нет — приравниваем дельту к 0 и ставим flat
                                delta = 0.0
                                trend = "flat"
                            else:
                                try:
                                    v_closed = float(s_closed)
                                except Exception:
                                    v_closed = v_live
                                delta = v_live - v_closed
                                trend = classify_trend(delta, eps)

                            # композитный ключ
                            pack = {
                                "value": f"{v_live:.2f}",          # фиксируем 2 знака для RSI
                                "bucket_low": bucket,              # целое 0..95
                                "delta": f"{delta:.2f}",           # дельта к закрытому
                                "trend": trend,                    # up/flat/down
                                "ref": "closed"                    # пометка, что сравнение с закрытым
                            }
                            pipe.set(rsi_pack_key(symbol, tf, length), json.dumps(pack), ex=LIVE_TTL_SEC)
                            writes += 1
                            total_processed += 1

                        if writes:
                            await pipe.execute()
                            total_written += writes

                    except Exception as e:
                        log.warning(f"[RSI_POST] обработка записи ошиблась: {e}", exc_info=True)

            if to_ack:
                await redis.xack(LIVE_STREAM_NAME, GROUP_NAME, *to_ack)

            if total_processed or total_written:
                log.info(f"IND_RSI_POST: processed={total_processed}, written={total_written}")

        except Exception as e:
            log.error(f"IND_RSI_POST loop error: {e}", exc_info=True)
            await asyncio.sleep(2)