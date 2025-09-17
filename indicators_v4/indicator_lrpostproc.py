# indicator_lrpostproc.py — постпроцессинг live-LR: 12-корзинная позиция внутри канала, bucket_delta, тренд угла (±5% от закрытого) → композитный ключ

import asyncio
import json
import logging
from datetime import datetime

# 🔸 Логгер модуля
log = logging.getLogger("IND_LR_POST")

# 🔸 Конфигурация
LIVE_STREAM_NAME = "indicator_live_stream"   # batched-события от indicator_livestream
GROUP_NAME       = "lr_post_group"           # consumer-group для этого воркера
CONSUMER_NAME    = "lr_post_1"

# 🔸 Параметры TF
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 TTL композитного live-ключа (сек)
LIVE_TTL_SEC = 30

# 🔸 Порог «такой же угол»: относительное изменение <= 5%
ANGLE_FLAT_REL = 0.05
ANGLE_ABS_EPS  = 1e-6  # защита от деления на ноль

# 🔸 Помощники по именам и ключам
def lr_base_from_params(params: dict) -> str:
    length = int(params["length"])
    return f"lr{length}"

def lr_pack_key(symbol: str, tf: str, base: str) -> str:
    # композитный live-ключ для LR
    return f"lrpos_pack:{symbol}:{tf}:{base}"

def ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"ind:{symbol}:{tf}:{param_name}"

def ts_ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"ts_ind:{symbol}:{tf}:{param_name}"

def ts_close_key(symbol: str, tf: str) -> str:
    return f"bb:ts:{symbol}:{tf}:c"

def mark_price_key(symbol: str) -> str:
    return f"bb:price:{symbol}"

# 🔸 Утилиты (корзины/динамики)
def clamp(v, lo, hi):
    return hi if v > hi else lo if v < lo else v

def classify_bucket_delta(d: int) -> str:
    if d == 0: return "no_change"
    if d == 1: return "up_1"
    if d == 2: return "up_2"
    if d >= 3: return "up_strong"
    if d == -1: return "down_1"
    if d == -2: return "down_2"
    if d <= -3: return "down_strong"
    return "no_change"

def compute_bucket_12(price: float, lower: float, upper: float) -> int | None:
    # нумерация 0..11 сверху вниз:
    # 0/1 — над, 2..9 — внутри (сверху→вниз), 10/11 — под
    width = upper - lower
    if width <= 0:
        return None
    seg = width / 8.0
    top1 = upper + seg
    top2 = upper + 2 * seg
    bot1 = lower - seg
    bot2 = lower - 2 * seg

    if price >= top2:
        return 0
    if price >= upper:  # [upper, top2)
        return 1

    if price >= lower:  # [lower, upper)
        k = int((upper - price) // seg)  # 0..7 (0 — самый верхний сегмент)
        k = clamp(k, 0, 7)
        return 2 + int(k)

    if price >= bot1:  # [bot1, lower)
        return 10
    return 11

def classify_angle_trend(angle_live: float, angle_closed: float) -> tuple[str, float]:
    # возвращает (trend, delta), где trend ∈ {"up","down","flat"} по правилу 5% от закрытого
    delta = angle_live - angle_closed
    denom = abs(angle_closed)
    if denom < ANGLE_ABS_EPS:
        # если закрытый угол ~0, считаем "flat", если абсолютная дельта очень мала
        if abs(delta) <= ANGLE_ABS_EPS:
            return "flat", delta
        # иначе знак дельты определяет направление
        return ("up" if delta > 0 else "down"), delta

    rel = abs(delta) / denom
    if rel <= ANGLE_FLAT_REL:
        return "flat", delta
    return ("up" if delta > 0 else "down"), delta

# 🔸 Доступ к ценам/закрытым значениям
async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    mp = await redis.get(mark_price_key(symbol))
    if mp is not None:
        try:
            return float(mp)
        except Exception:
            pass
    try:
        res = await redis.execute_command("TS.GET", ts_close_key(symbol, tf))
        if res and len(res) == 2:
            return float(res[1])
    except Exception:
        pass
    return None

async def fetch_closed_close(redis, symbol: str, tf: str, last_closed_ms: int) -> float | None:
    try:
        res = await redis.execute_command("TS.RANGE", ts_close_key(symbol, tf), last_closed_ms, last_closed_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

async def fetch_closed_lr(redis, symbol: str, tf: str, base: str) -> dict:
    out = {}
    try:
        up = await redis.get(ind_key(symbol, tf, f"{base}_upper"))
        lo = await redis.get(ind_key(symbol, tf, f"{base}_lower"))
        ct = await redis.get(ind_key(symbol, tf, f"{base}_center"))
        ang = await redis.get(ind_key(symbol, tf, f"{base}_angle"))
        if up is not None: out["upper"] = float(up)
        if lo is not None: out["lower"] = float(lo)
        if ct is not None: out["center"] = float(ct)
        if ang is not None: out["angle"] = float(ang)
    except Exception:
        pass
    return out

# 🔸 Основной воркер постпроцессинга LR
async def run_indicator_lrpostproc(pg, redis):
    log.debug("IND_LR_POST: воркер запущен")

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
                        symbol = data.get("symbol")
                        tf = data.get("timeframe")
                        tick_open_iso = data.get("tick_open_time")
                        instances_raw = data.get("instances")
                        if not symbol or tf not in STEP_MS or not instances_raw:
                            continue

                        try:
                            instances = json.loads(instances_raw) if isinstance(instances_raw, str) else instances_raw
                        except Exception:
                            continue

                        # берём только LR-инстансы
                        lr_items = [x for x in instances if str(x.get("indicator")) == "lr"]
                        if not lr_items:
                            continue

                        # время последнего закрытого бара
                        last_closed_ms = None
                        try:
                            if tick_open_iso:
                                t = datetime.fromisoformat(tick_open_iso)
                                step = STEP_MS[tf]
                                last_closed_ms = int((t.timestamp() * 1000) - step)
                        except Exception:
                            last_closed_ms = None

                        # live-цена
                        live_price = await fetch_mark_or_last_close(redis, symbol, tf)

                        pipe = redis.pipeline()
                        writes = 0

                        for item in lr_items:
                            params = item.get("params") or {}
                            values = item.get("values") or {}
                            try:
                                base = lr_base_from_params(params)
                            except Exception:
                                continue

                            # live LR-параметры
                            try:
                                up_live = float(values.get(f"{base}_upper"))
                                lo_live = float(values.get(f"{base}_lower"))
                                ct_live = float(values.get(f"{base}_center"))
                                ang_live = float(values.get(f"{base}_angle"))
                            except Exception:
                                continue

                            # корзины: live и закрытая
                            bucket_live = None
                            if live_price is not None:
                                bucket_live = compute_bucket_12(live_price, lo_live, up_live)

                            bucket_closed = None
                            closed = {}
                            if last_closed_ms is not None:
                                closed = await fetch_closed_lr(redis, symbol, tf, base)
                                if "upper" in closed and "lower" in closed:
                                    close_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms)
                                    if close_closed is not None:
                                        bucket_closed = compute_bucket_12(close_closed, closed["lower"], closed["upper"])

                            bucket_delta = "unknown"
                            if bucket_live is not None and bucket_closed is not None:
                                d = int(bucket_live - bucket_closed)
                                bucket_delta = classify_bucket_delta(d)

                            # динамика угла: сравнение live vs closed (5% правило)
                            angle_trend = "unknown"
                            angle_delta = None
                            if "angle" in closed:
                                trend, d_ang = classify_angle_trend(ang_live, closed["angle"])
                                angle_trend = trend
                                angle_delta = d_ang

                            # композитный пакет
                            pack = {
                                "bucket": bucket_live if bucket_live is not None else None,
                                "bucket_delta": bucket_delta,
                                "angle": f"{ang_live:.5f}",
                                "angle_delta": (f"{angle_delta:.5f}" if angle_delta is not None else None),
                                "angle_trend": angle_trend,
                                # полезные поля
                                "price": (f"{live_price:.8f}" if live_price is not None else None),
                                "lower": f"{lo_live:.8f}",
                                "upper": f"{up_live:.8f}",
                                "center": f"{ct_live:.8f}",
                            }

                            pipe.set(lr_pack_key(symbol, tf, base), json.dumps(pack), ex=LIVE_TTL_SEC)
                            writes += 1
                            total_processed += 1

                        if writes:
                            await pipe.execute()
                            total_written += writes

                    # условия достаточности и защита цикла
                    except Exception as e:
                        log.warning(f"[LR_POST] обработка записи ошиблась: {e}", exc_info=True)

            if to_ack:
                await redis.xack(LIVE_STREAM_NAME, GROUP_NAME, *to_ack)

            if total_processed or total_written:
                log.info(f"IND_LR_POST: processed={total_processed}, written={total_written}")

        except Exception as e:
            log.error(f"IND_LR_POST loop error: {e}", exc_info=True)
            await asyncio.sleep(2)