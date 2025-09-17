# indicator_bbpostproc.py — постпроцессинг live-BB: 12-корзинная позиция цены, bucket_delta, bw_trend_strict/smooth → композитный ключ в Redis

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Логгер модуля
log = logging.getLogger("IND_BB_POST")

# 🔸 Конфигурация
LIVE_STREAM_NAME = "indicator_live_stream"   # batched-события от indicator_livestream
GROUP_NAME       = "bb_post_group"           # consumer-group для этого воркера
CONSUMER_NAME    = "bb_post_1"

# 🔸 Параметры TF
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 TTL композитного live-ключа (сек)
LIVE_TTL_SEC = 90

# 🔸 Порог для относительной динамики ширины полосы (строгий/сглаженный используют одинаковый ε)
BW_EPS_REL = {
    "m5":  0.05,   # 5%
    "m15": 0.04,   # 4%
    "h1":  0.03,   # 3%
}

# 🔸 Глубина для сглаженного тренда ширины (N закрытых баров)
BW_SMOOTH_N = 5

# 🔸 Помощники по именам и ключам
def bb_base_from_params(params: dict) -> str:
    # base ровно как в compute_and_store.get_expected_param_names
    length = int(params["length"])
    std_raw = round(float(params["std"]), 2)
    std_str = str(std_raw).replace(".", "_")
    return f"bb{length}_{std_str}"

def bb_pack_key(symbol: str, tf: str, base: str) -> str:
    # композитный live-ключ для BB: JSON с bucket/bucket_delta/bw_trend_*
    return f"bbpos_pack:{symbol}:{tf}:{base}"

def ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"ind:{symbol}:{tf}:{param_name}"

def ts_ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"ts_ind:{symbol}:{tf}:{param_name}"

def ts_close_key(symbol: str, tf: str) -> str:
    return f"bb:ts:{symbol}:{tf}:c"

def mark_price_key(symbol: str) -> str:
    return f"bb:price:{symbol}"

# 🔸 Утилиты
def clamp(v: float, lo: float, hi: float) -> float:
    return hi if v > hi else lo if v < lo else v

def classify_bucket_delta(d: int) -> str:
    if d == 0:
        return "no_change"
    if d == 1:
        return "up_1"
    if d == 2:
        return "up_2"
    if d >= 3:
        return "up_strong"
    if d == -1:
        return "down_1"
    if d == -2:
        return "down_2"
    if d <= -3:
        return "down_strong"
    return "no_change"

def classify_bw_trend(rel_diff: float, eps: float) -> str:
    if rel_diff >= eps:
        return "expanding"
    if rel_diff <= -eps:
        return "contracting"
    return "stable"

def compute_bucket_12(price: float, lower: float, upper: float) -> int | None:
    width = upper - lower
    if width <= 0:
        return None

    seg = width / 8.0
    top1 = upper + seg
    top2 = upper + 2 * seg
    bot1 = lower - seg
    bot2 = lower - 2 * seg

    # над полосой: 0 (крайняя), 1 (обычная)
    if price >= top2:
        return 0
    if price >= upper:  # [upper, top2)
        return 1

    # внутри полосы: 8 сегментов сверху вниз → 2..9
    if price >= lower:  # [lower, upper)
        # 0..7: 0 — самый верхний внутренний сегмент (непосредственно под upper)
        k = int((upper - price) // seg)
        if k < 0:
            k = 0
        if k > 7:
            k = 7
        return 2 + k  # 2..9

    # под полосой: 10 (обычная), 11 (крайняя)
    if price >= bot1:  # [bot1, lower)
        return 10
    return 11

async def fetch_mark_or_last_close(redis, symbol: str, tf: str) -> float | None:
    # сначала markPrice, если нет — последняя close из TS
    mp = await redis.get(mark_price_key(symbol))
    if mp is not None:
        try:
            return float(mp)
        except Exception:
            pass
    try:
        # TS.GET даст последнюю точку серии; ок для фоллбэка
        res = await redis.execute_command("TS.GET", ts_close_key(symbol, tf))
        if res and len(res) == 2:
            return float(res[1])
    except Exception:
        pass
    return None

async def fetch_closed_close(redis, symbol: str, tf: str, last_closed_ms: int) -> float | None:
    # точный close последнего закрытого бара
    try:
        res = await redis.execute_command("TS.RANGE", ts_close_key(symbol, tf), last_closed_ms, last_closed_ms)
        if res:
            return float(res[0][1])
    except Exception:
        pass
    return None

async def fetch_closed_bb(redis, symbol: str, tf: str, base: str) -> tuple[float | None, float | None]:
    # закрытые upper/lower из KV ind:*
    try:
        up = await redis.get(ind_key(symbol, tf, f"{base}_upper"))
        lo = await redis.get(ind_key(symbol, tf, f"{base}_lower"))
        return (float(up) if up is not None else None,
                float(lo) if lo is not None else None)
    except Exception:
        return (None, None)

async def fetch_smooth_bw(redis, symbol: str, tf: str, base: str, last_closed_ms: int, n: int) -> float | None:
    # средняя ширина за N последних закрытых баров из TS индикаторов
    step = STEP_MS[tf]
    start = last_closed_ms - (n - 1) * step
    try:
        up_series = await redis.execute_command("TS.RANGE", ts_ind_key(symbol, tf, f"{base}_upper"), start, last_closed_ms)
        lo_series = await redis.execute_command("TS.RANGE", ts_ind_key(symbol, tf, f"{base}_lower"), start, last_closed_ms)
        if not up_series or not lo_series:
            return None
        # соберём по общему таймстампу
        up_map = {int(ts): float(v) for ts, v in up_series}
        lo_map = {int(ts): float(v) for ts, v in lo_series}
        xs = sorted(set(up_map.keys()) & set(lo_map.keys()))
        if not xs:
            return None
        widths = [up_map[t] - lo_map[t] for t in xs]
        if not widths:
            return None
        return sum(widths) / len(widths)
    except Exception:
        return None


# 🔸 Основной воркер постпроцессинга BB
async def run_indicator_bbpostproc(pg, redis):
    log.debug("IND_BB_POST: воркер запущен")

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
                        # ожидаемые поля от indicator_livestream: symbol, timeframe, tick_open_time, instances(JSON)
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

                        # берём только BB-инстансы
                        bb_items = [x for x in instances if str(x.get("indicator")) == "bb"]
                        if not bb_items:
                            continue

                        # вычислим last_closed_ms как (open текущего бара − шаг TF)
                        last_closed_ms = None
                        try:
                            if tick_open_iso:
                                t = datetime.fromisoformat(tick_open_iso)
                                step = STEP_MS[tf]
                                last_closed_ms = int((t.timestamp() * 1000) - step)
                        except Exception:
                            last_closed_ms = None

                        # цена live: markPrice, фоллбэк — последняя close
                        live_price = await fetch_mark_or_last_close(redis, symbol, tf)
                        if live_price is None:
                            # без цены нет корзины, но можно посчитать тренды ширины
                            pass

                        pipe = redis.pipeline()
                        writes = 0
                        eps_rel = BW_EPS_REL.get(tf, 0.04)

                        for item in bb_items:
                            params = item.get("params") or {}
                            values = item.get("values") or {}
                            try:
                                base = bb_base_from_params(params)
                            except Exception:
                                continue

                            # live верх/низ
                            try:
                                up_live = float(values.get(f"{base}_upper"))
                                lo_live = float(values.get(f"{base}_lower"))
                            except Exception:
                                continue

                            # bucket live
                            bucket_live = None
                            if live_price is not None:
                                bucket_live = compute_bucket_12(live_price, lo_live, up_live)

                            # закрытые BB + закрытый close → bucket_closed
                            bucket_closed = None
                            up_closed = lo_closed = None
                            if last_closed_ms is not None:
                                up_closed, lo_closed = await fetch_closed_bb(redis, symbol, tf, base)
                                if up_closed is not None and lo_closed is not None:
                                    close_closed = await fetch_closed_close(redis, symbol, tf, last_closed_ms)
                                    if close_closed is not None:
                                        bucket_closed = compute_bucket_12(close_closed, lo_closed, up_closed)

                            # delta по корзине (классификация)
                            if bucket_live is not None and bucket_closed is not None:
                                d = bucket_live - bucket_closed
                                bucket_delta = classify_bucket_delta(int(d))
                            else:
                                bucket_delta = "unknown"

                            # ширина полосы: strict vs closed
                            bw_strict = "stable"
                            width_live = up_live - lo_live
                            width_closed = None
                            if up_closed is not None and lo_closed is not None:
                                width_closed = up_closed - lo_closed
                                if width_closed and width_closed > 0:
                                    rel = (width_live - width_closed) / width_closed
                                    bw_strict = classify_bw_trend(rel, eps_rel)

                            # сглаженный тренд ширины vs среднее за N закрытых
                            bw_smooth = "stable"
                            if last_closed_ms is not None:
                                bw_mean = await fetch_smooth_bw(redis, symbol, tf, base, last_closed_ms, BW_SMOOTH_N)
                                if bw_mean and bw_mean > 0:
                                    rel2 = (width_live - bw_mean) / bw_mean
                                    bw_smooth = classify_bw_trend(rel2, eps_rel)

                            # сборка пакета
                            pack = {
                                "bucket": bucket_live if bucket_live is not None else None,
                                "bucket_delta": bucket_delta,
                                "bw_trend_strict": bw_strict,
                                "bw_trend_smooth": bw_smooth,
                                # полезные поля для отладки/визуализаций
                                "price": f"{live_price:.8f}" if live_price is not None else None,
                                "lower": f"{lo_live:.8f}",
                                "upper": f"{up_live:.8f}",
                            }

                            pipe.set(bb_pack_key(symbol, tf, base), json.dumps(pack), ex=LIVE_TTL_SEC)
                            writes += 1
                            total_processed += 1

                        if writes:
                            await pipe.execute()
                            total_written += writes

                    except Exception as e:
                        log.warning(f"[BB_POST] обработка записи ошиблась: {e}", exc_info=True)

            if to_ack:
                await redis.xack(LIVE_STREAM_NAME, GROUP_NAME, *to_ack)

            if total_processed or total_written:
                log.debug(f"IND_BB_POST: processed={total_processed}, written={total_written}")

        except Exception as e:
            log.error(f"IND_BB_POST loop error: {e}", exc_info=True)
            await asyncio.sleep(2)