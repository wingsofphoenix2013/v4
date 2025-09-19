# indicator_mw_extremes.py — воркер расчёта рыночного условия Extremes (overbought_extension / oversold_extension / pullback_in_uptrend / pullback_in_downtrend / none)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Константы и настройки (в стиле MW_TREND / MW_VOL / MW_MOM)
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (rsi/mfi/bb/lr)
GROUP       = "mw_ext_group"
CONSUMER    = "mw_ext_1"

# TS-барьер: ждём, пока в TS появятся все нужные точки @open_time
BARRIER_FAST_POLL_MS = 300
BARRIER_FAST_SECONDS = 15
BARRIER_SLOW_POLL_MS = 1200
BARRIER_MAX_WAIT_SEC = 90

# Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# Нужные базы (для триггера из indicator_stream)
EXPECTED_BASES = {"rsi14", "rsi21", "mfi14", "mfi21", "bb20_2_0", "lr50", "lr100"}

# Префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# Пороговые значения зон (TF-aware при необходимости)
RSI_OVERBOUGHT = {"m5": 70.0, "m15": 70.0, "h1": 70.0}
RSI_OVERSOLD   = {"m5": 30.0, "m15": 30.0, "h1": 30.0}
MFI_OVERBOUGHT = {"m5": 80.0, "m15": 80.0, "h1": 80.0}
MFI_OVERSOLD   = {"m5": 20.0, "m15": 20.0, "h1": 20.0}

# Критерии тренда по LR-углам для pullback
LR_UP_ANGLE_EPS   = {"m5": 1e-4, "m15": 8e-4, "h1": 2e-3}   # > eps → uptrend (по каналу)
LR_DOWN_ANGLE_EPS = {"m5": -1e-4, "m15": -8e-4, "h1": -2e-3}# < -eps → downtrend

# Параллелизм
MAX_CONCURRENCY = 30

# 🔸 Логгер
log = logging.getLogger("MW_EXT")


# 🔸 Утилиты времени
def iso_to_ms(iso: str) -> int:
    dt = datetime.fromisoformat(iso)
    return int(dt.timestamp() * 1000)

def prev_bar_ms(open_ms: int, tf: str) -> int:
    return open_ms - STEP_MS[tf]


# 🔸 Чтение одной точки TS по exact open_time (from=to)
async def ts_get_at(redis, key: str, ts_ms: int):
    try:
        res = await redis.execute_command("TS.RANGE", key, ts_ms, ts_ms)
        if res:
            return float(res[0][1])
    except Exception as e:
        log.warning(f"[TS] read error {key}@{ts_ms}: {e}")
    return None


# 🔸 Проверка полноты набора значений в TS на open_time
async def ts_has_all(redis, symbol: str, tf: str, open_ms: int) -> bool:
    keys = [
        f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi14",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi21",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi14",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi21",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle",
        f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
    ]
    vals = await asyncio.gather(*[ts_get_at(redis, k, open_ms) for k in keys], return_exceptions=False)
    return all(v is not None for v in vals)


# 🔸 Ожидание полного комплекта TS-точек (TS-барьер)
async def wait_for_ts_barrier(redis, symbol: str, tf: str, open_ms: int) -> bool:
    deadline = datetime.utcnow() + timedelta(seconds=BARRIER_MAX_WAIT_SEC)
    fast_until = datetime.utcnow() + timedelta(seconds=BARRIER_FAST_SECONDS)
    while datetime.utcnow() < deadline:
        if await ts_has_all(redis, symbol, tf, open_ms):
            return True
        now = datetime.utcnow()
        interval_ms = BARRIER_FAST_POLL_MS if now < fast_until else BARRIER_SLOW_POLL_MS
        await asyncio.sleep(interval_ms / 1000.0)
    return False


# 🔸 Подсчёт BB 12-корзин (как в bb_pack.py)
def bb_bucket_12(price: float, lower: float, upper: float) -> int | None:
    width = upper - lower
    if width <= 0:
        return None
    seg = width / 8.0
    top2 = upper + 2 * seg
    if price >= top2:
        return 0
    if price >= upper:
        return 1
    if price >= lower:
        k = int((upper - price) // seg)
        if k < 0: k = 0
        if k > 7: k = 7
        return 2 + k
    bot1 = lower - seg
    if price >= bot1:
        return 10
    return 11


# 🔸 Загрузка значений (cur/prev) из TS
async def load_ext_inputs(redis, symbol: str, tf: str, open_ms: int) -> dict:
    prev_ms = prev_bar_ms(open_ms, tf)
    keys = {
        "rsi14":      f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi14",
        "rsi21":      f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi21",
        "mfi14":      f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi14",
        "mfi21":      f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi21",
        "bb_up":      f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper",
        "bb_lo":      f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower",
        "lr50_ang":   f"{TS_IND_PREFIX}:{symbol}:{tf}:lr50_angle",
        "lr100_ang":  f"{TS_IND_PREFIX}:{symbol}:{tf}:lr100_angle",
        "close":      f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
    }

    async def read_pair(key: str):
        cur = await ts_get_at(redis, key, open_ms)
        prev = await ts_get_at(redis, key, prev_ms)
        return cur, prev

    tasks = {name: read_pair(key) for name, key in keys.items()}
    res = await asyncio.gather(*tasks.values(), return_exceptions=False)

    out = {}
    for (name, _), (cur, prev) in zip(tasks.items(), res):
        out[name] = {"cur": cur, "prev": prev}
    out["open_ms"] = open_ms
    out["prev_ms"] = prev_ms
    return out


# 🔸 Запись результата в Redis KV и PostgreSQL
async def persist_result(pg, redis, symbol: str, tf: str, open_time_iso: str,
                         state: str, details: dict,
                         source: str = "live", version: int = 1):
    # KV
    kv_key = f"{KV_MW_PREFIX}:{symbol}:{tf}:extremes"
    payload = {
        "state": state,
        "version": version,
        "open_time": open_time_iso,
        "computed_at": datetime.utcnow().isoformat(),
        "details": details,
    }
    try:
        await redis.set(kv_key, json.dumps(payload))
    except Exception as e:
        log.warning(f"[KV] set error {kv_key}: {e}")

    # PG upsert
    try:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO indicator_marketwatch_values
                  (symbol, timeframe, open_time, kind, state, status, details, version, source, computed_at, updated_at)
                VALUES ($1,$2,$3,'extremes',$4,'ok',$5,$6,$7,NOW(),NOW())
                ON CONFLICT (symbol, timeframe, open_time, kind)
                DO UPDATE SET
                  state = EXCLUDED.state,
                  details = EXCLUDED.details,
                  version = EXCLUDED.version,
                  source = EXCLUDED.source,
                  updated_at = NOW()
                """,
                symbol, tf, datetime.fromisoformat(open_time_iso),
                state, json.dumps(details), version, source
            )
    except Exception as e:
        log.error(f"[PG] upsert error extremes {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Расчёт Extremes по ключу (symbol, tf, open_time) — TS-барьер + cur/prev
async def compute_ext_for_bar(pg, redis, symbol: str, tf: str, open_iso: str):
    open_ms = iso_to_ms(open_iso)

    # ждём полный комплект в TS
    ready = await wait_for_ts_barrier(redis, symbol, tf, open_ms)
    if not ready:
        log.debug(f"MW_EXT GAP {symbol}/{tf}@{open_iso} (TS not ready)")
        return

    # читаем cur/prev
    x = await load_ext_inputs(redis, symbol, tf, open_ms)

    # RSI/MFI текущие
    rsi14, rsi21 = x["rsi14"]["cur"], x["rsi21"]["cur"]
    mfi14, mfi21 = x["mfi14"]["cur"], x["mfi21"]["cur"]

    # BB и цена → корзины 12-сегментов (cur/prev)
    up_cur, lo_cur, pr_cur = x["bb_up"]["cur"], x["bb_lo"]["cur"], x["close"]["cur"]
    up_prev, lo_prev, pr_prev = x["bb_up"]["prev"], x["bb_lo"]["prev"], x["close"]["prev"]

    bb_bucket_cur = None
    bb_bucket_prev = None
    if None not in (up_cur, lo_cur, pr_cur):
        bb_bucket_cur = bb_bucket_12(pr_cur, lo_cur, up_cur)
    if None not in (up_prev, lo_prev, pr_prev):
        bb_bucket_prev = bb_bucket_12(pr_prev, lo_prev, up_prev)

    bb_bucket_delta = None
    if bb_bucket_cur is not None and bb_bucket_prev is not None:
        bb_bucket_delta = bb_bucket_cur - bb_bucket_prev

    # LR-тренд (по углам)
    ang50, ang100 = x["lr50_ang"]["cur"], x["lr100_ang"]["cur"]
    up_eps  = LR_UP_ANGLE_EPS.get(tf, 1e-4)
    dn_eps  = LR_DOWN_ANGLE_EPS.get(tf, -1e-4)
    uptrend   = (ang50 is not None and ang50 > up_eps) or (ang100 is not None and ang100 > up_eps)
    downtrend = (ang50 is not None and ang50 < dn_eps) or (ang100 is not None and ang100 < dn_eps)

    # Флаги зон
    ob = ((rsi14 is not None and rsi14 >= RSI_OVERBOUGHT[tf]) or
          (rsi21 is not None and rsi21 >= RSI_OVERBOUGHT[tf]) or
          (mfi14 is not None and mfi14 >= MFI_OVERBOUGHT[tf]) or
          (mfi21 is not None and mfi21 >= MFI_OVERBOUGHT[tf]))
    os = ((rsi14 is not None and rsi14 <= RSI_OVERSOLD[tf]) or
          (rsi21 is not None and rsi21 <= RSI_OVERSOLD[tf]) or
          (mfi14 is not None and mfi14 <= MFI_OVERSOLD[tf]) or
          (mfi21 is not None and mfi21 <= MFI_OVERSOLD[tf]))

    # 🔸 Классификация (приоритет)
    state = "none"

    # overbought_extension: зона overbought + верхние корзины BB (0..3) и/или рост корзины
    if ob and bb_bucket_cur is not None and bb_bucket_cur <= 3:
        state = "overbought_extension"
    elif ob and (bb_bucket_delta is not None and bb_bucket_delta >= 1):
        state = "overbought_extension"

    # oversold_extension: зона oversold + нижние корзины BB (8..11) и/или падение корзины
    elif os and bb_bucket_cur is not None and bb_bucket_cur >= 8:
        state = "oversold_extension"
    elif os and (bb_bucket_delta is not None and bb_bucket_delta <= -1):
        state = "oversold_extension"

    # pullback в ап/даун-тренде (по LR): движение корзины BB против тренда
    elif uptrend and (bb_bucket_delta is not None and bb_bucket_delta <= -1):
        state = "pullback_in_uptrend"
    elif downtrend and (bb_bucket_delta is not None and bb_bucket_delta >= 1):
        state = "pullback_in_downtrend"

    # Детали (округление для красоты)
    def r2(x): return None if x is None else round(float(x), 2)
    def r5(x): return None if x is None else round(float(x), 5)

    details = {
        "rsi": {"rsi14": r2(rsi14), "rsi21": r2(rsi21)},
        "mfi": {"mfi14": r2(mfi14), "mfi21": r2(mfi21)},
        "bb": {
            "bucket_cur": bb_bucket_cur,
            "bucket_prev": bb_bucket_prev,
            "bucket_delta": bb_bucket_delta,
            "upper": r5(up_cur), "lower": r5(lo_cur)
        },
        "lr": {"ang50": r5(ang50), "ang100": r5(ang100), "uptrend": bool(uptrend), "downtrend": bool(downtrend)},
        "flags": {"overbought": bool(ob), "oversold": bool(os)},
        "used_bases": ["rsi14","rsi21","mfi14","mfi21","bb20_2_0_upper","bb20_2_0_lower","lr50_angle","lr100_angle","close"],
        "missing_bases": [],
        "open_time_iso": open_iso,
    }

    await persist_result(pg, redis, symbol, tf, open_iso, state, details)
    log.debug(
        f"MW_EXT OK {symbol}/{tf}@{open_iso} state={state} "
        f"bb_bkt={bb_bucket_cur} Δbkt={bb_bucket_delta} "
        f"OB={details['flags']['overbought']} OS={details['flags']['oversold']} "
        f"LR(up={uptrend},down={downtrend})"
    )


# 🔸 Основной воркер: слушает indicator_stream, запускает расчёт с TS-барьером
async def run_indicator_mw_extremes(pg, redis):
    log.debug("MW_EXT: воркер запущен")

    # создаём consumer-group на indicator_stream
    try:
        await redis.xgroup_create(STREAM_READY, GROUP, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create error: {e}")

    # семафор на параллельность
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    in_flight = set()

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_READY: ">"},
                count=200,
                block=1000
            )
            if not resp:
                continue

            to_ack = []
            tasks = []

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        if data.get("status") != "ready":
                            continue

                        symbol   = data["symbol"]
                        tf       = data.get("timeframe") or data.get("interval")
                        base     = data["indicator"]      # 'rsi14'|'mfi21'|'bb20_2_0'|'lr50'|...
                        open_iso = data["open_time"]

                        # интересуют только наши базы
                        if base not in EXPECTED_BASES:
                            continue

                        key = (symbol, tf, open_iso)
                        if key in in_flight:
                            continue

                        async def _run(key_tuple):
                            sym, tff, iso = key_tuple
                            async with sem:
                                try:
                                    await compute_ext_for_bar(pg, redis, sym, tff, iso)
                                except Exception as e:
                                    log.error(f"MW_EXT compute error {sym}/{tff}@{iso}: {e}", exc_info=True)
                                finally:
                                    in_flight.discard(key_tuple)

                        in_flight.add(key)
                        tasks.append(asyncio.create_task(_run(key)))

                    except Exception as e:
                        log.warning(f"MW_EXT message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(STREAM_READY, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_EXT ack error: {e}")

            if tasks:
                done, _ = await asyncio.wait(tasks, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                for _t in done:
                    pass

        except Exception as e:
            log.error(f"MW_EXT loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)