# indicator_mfipostproc.py — постпроцессинг live-MFI: корзина (шаг 5), тренд vs закрытый бар, композитный ключ в Redis

import asyncio
import json
import logging
from datetime import datetime

# 🔸 Логгер модуля
log = logging.getLogger("IND_MFI_POST")

# 🔸 Конфигурация
LIVE_STREAM_NAME = "indicator_live_stream"   # batched-события от indicator_livestream
GROUP_NAME       = "mfi_post_group"          # consumer-group для этого воркера
CONSUMER_NAME    = "mfi_post_1"

# 🔸 TTL для композитного live-ключа (сек)
LIVE_TTL_SEC = 90

# 🔸 Порог «мертвого коридора» (эпсилон) на шкале MFI (в пунктах индекса)
# Для единообразия берём те же значения, что и для RSI; при желании потом подстроишь отдельно.
EPSILON_BY_TF = {
    "m5":  0.3,
    "m15": 0.4,
    "h1":  0.6,
}

# 🔸 Ключи Redis
def mfi_closed_key(symbol: str, tf: str, length: int) -> str:
    # закрытое (финальное) значение индикатора из общей системы
    return f"ind:{symbol}:{tf}:mfi{length}"

def mfi_pack_key(symbol: str, tf: str, length: int) -> str:
    # композитный live-ключ (JSON): {"value","bucket_low","delta","trend","ref"}
    return f"mfi_pack:{symbol}:{tf}:mfi{length}"


# 🔸 Утилиты
def clamp(v: float, lo: float, hi: float) -> float:
    return hi if v > hi else lo if v < lo else v

def to_bucket_low(x: float) -> int:
    # нижняя граница корзины кратная 5: 0,5,10,…,95 (прижимаем 100 к <100)
    y = clamp(x, 0.0, 99.9999)
    return int((int(y) // 5) * 5)

def classify_trend(delta: float, eps: float) -> str:
    if delta >= eps:
        return "up"
    if delta <= -eps:
        return "down"
    return "flat"


# 🔸 Основной воркер постпроцессинга MFI
async def run_indicator_mfipostproc(pg, redis):
    log.debug("IND_MFI_POST: воркер запущен")

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
                        # ожидаемые поля от indicator_livestream
                        symbol = data.get("symbol")
                        tf = data.get("timeframe")
                        instances_raw = data.get("instances")
                        if not symbol or tf not in ("m5", "m15", "h1") or not instances_raw:
                            continue

                        try:
                            instances = json.loads(instances_raw) if isinstance(instances_raw, str) else instances_raw
                        except Exception:
                            continue

                        # фильтруем только MFI
                        mfi_items = [x for x in instances if str(x.get("indicator")) == "mfi"]
                        if not mfi_items:
                            continue

                        pipe = redis.pipeline()
                        eps = EPSILON_BY_TF.get(tf, 0.4)
                        writes = 0

                        for item in mfi_items:
                            params = item.get("params") or {}
                            values = item.get("values") or {}

                            # имя параметра вида mfi{length}
                            try:
                                length = int(params.get("length"))
                            except Exception:
                                continue

                            param_name = f"mfi{length}"
                            sval = values.get(param_name)
                            if sval is None:
                                continue

                            try:
                                v_live = float(sval)
                            except Exception:
                                continue

                            # корзина
                            bucket = to_bucket_low(v_live)

                            # референс: закрытое значение (ind:{symbol}:{tf}:mfi{length})
                            k_closed = mfi_closed_key(symbol, tf, length)
                            s_closed = await redis.get(k_closed)
                            if s_closed is None:
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
                                "value": f"{v_live:.2f}",
                                "bucket_low": bucket,
                                "delta": f"{delta:.2f}",
                                "trend": trend,
                                "ref": "closed"
                            }
                            pipe.set(mfi_pack_key(symbol, tf, length), json.dumps(pack), ex=LIVE_TTL_SEC)
                            total_processed += 1
                            writes += 1

                        if writes:
                            await pipe.execute()
                            total_written += writes

                    except Exception as e:
                        log.warning(f"[MFI_POST] обработка записи ошиблась: {e}", exc_info=True)

            if to_ack:
                await redis.xack(LIVE_STREAM_NAME, GROUP_NAME, *to_ack)

            if total_processed or total_written:
                log.debug(f"IND_MFI_POST: processed={total_processed}, written={total_written}")

        except Exception as e:
            log.error(f"IND_MFI_POST loop error: {e}", exc_info=True)
            await asyncio.sleep(2)