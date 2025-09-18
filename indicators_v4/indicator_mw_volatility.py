# indicator_mw_volatility.py — воркер расчёта рыночного условия Volatility (low_squeeze / normal / expanding / high)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Константы и настройки (синхронизированы по стилю с MW_TREND)
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (atr14, bb20_2_0)
GROUP       = "mw_vol_group"
CONSUMER    = "mw_vol_1"

# TS-барьер: ждём, пока в TS появятся все нужные точки @open_time
BARRIER_FAST_POLL_MS = 300
BARRIER_FAST_SECONDS = 15
BARRIER_SLOW_POLL_MS = 1200
BARRIER_MAX_WAIT_SEC = 90

# Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# Нужные базы (индикаторы, без суффиксов)
EXPECTED_BASES = {"atr14", "bb20_2_0"}  # для триггера; фактически читаем upper/lower/close/atr14 из TS

# Префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# Пороги ATR% (уровни волатильности) — стартовые, TF-aware
ATR_LOW_PCT   = {"m5": 0.30, "m15": 0.40, "h1": 0.60}
ATR_HIGH_PCT  = {"m5": 0.80, "m15": 1.00, "h1": 1.50}

# Относительное изменение ширины полос BB (bw = upper-lower)
BW_EXPAND_EPS = {"m5": 0.04, "m15": 0.03, "h1": 0.02}  # expanding, если (bw_cur - bw_prev) / bw_prev >= eps
BW_CONTR_EPS  = {"m5": -0.04, "m15": -0.03, "h1": -0.02}  # contracting, если <= eps

# Параллелизм
MAX_CONCURRENCY = 30

# 🔸 Логгер
log = logging.getLogger("MW_VOL")


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
        f"{TS_IND_PREFIX}:{symbol}:{tf}:atr14",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower",
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


# 🔸 Загрузка значений (cur/prev) из TS
async def load_vol_inputs(redis, symbol: str, tf: str, open_ms: int) -> dict:
    prev_ms = prev_bar_ms(open_ms, tf)
    keys = {
        "atr14":       f"{TS_IND_PREFIX}:{symbol}:{tf}:atr14",
        "bb_upper":    f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_upper",
        "bb_lower":    f"{TS_IND_PREFIX}:{symbol}:{tf}:bb20_2_0_lower",
        "close":       f"{BB_TS_PREFIX}:{symbol}:{tf}:c",
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


# 🔸 Вспомогательные расчёты
def atr_pct(atr: float | None, close: float | None) -> float | None:
    if atr is None or close is None or close == 0:
        return None
    return (atr / close) * 100.0

def atr_bucket(atr_pct_val: float | None) -> int | None:
    if atr_pct_val is None:
        return None
    # шаг 0.1% → 1,2,3,...
    return int(atr_pct_val / 0.1) + 1

def classify_bw_phase(tf: str, bw_cur: float | None, bw_prev: float | None) -> tuple[str, float | None]:
    if bw_cur is None or bw_prev is None or bw_prev == 0:
        return "unknown", None
    rel = (bw_cur - bw_prev) / bw_prev
    if rel >= BW_EXPAND_EPS.get(tf, 0.03):
        return "expanding", rel
    if rel <= BW_CONTR_EPS.get(tf, -0.03):
        return "contracting", rel
    return "stable", rel


# 🔸 Запись результата в Redis KV и PostgreSQL
async def persist_result(pg, redis, symbol: str, tf: str, open_time_iso: str,
                         state: str, status: str, details: dict,
                         source: str = "live", version: int = 1):
    # KV
    kv_key = f"{KV_MW_PREFIX}:{symbol}:{tf}:volatility"
    payload = {
        "state": state,
        "status": status,
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
                VALUES ($1,$2,$3,'volatility',$4,$5,$6,$7,$8,NOW(),NOW())
                ON CONFLICT (symbol, timeframe, open_time, kind)
                DO UPDATE SET
                  state = EXCLUDED.state,
                  status = EXCLUDED.status,
                  details = EXCLUDED.details,
                  version = EXCLUDED.version,
                  source = EXCLUDED.source,
                  updated_at = NOW()
                """,
                symbol, tf, datetime.fromisoformat(open_time_iso),
                state, status, json.dumps(details), version, source
            )
    except Exception as e:
        log.error(f"[PG] upsert error volatility {symbol}/{tf}@{open_time_iso}: {e}")


# 🔸 Расчёт Volatility по ключу (symbol, tf, open_time) — TS-барьер + дельты
async def compute_vol_for_bar(pg, redis, symbol: str, tf: str, open_iso: str):
    open_ms = iso_to_ms(open_iso)

    # ждём полный комплект точек в TS
    ready = await wait_for_ts_barrier(redis, symbol, tf, open_ms)
    if not ready:
        log.debug(f"MW_VOL GAP {symbol}/{tf}@{open_iso} (TS not ready)")
        return

    # читаем cur/prev
    data = await load_vol_inputs(redis, symbol, tf, open_ms)

    # метрики
    atr_pct_cur  = atr_pct(data["atr14"]["cur"],  data["close"]["cur"])
    atr_pct_prev = atr_pct(data["atr14"]["prev"], data["close"]["prev"])
    atr_b_cur    = atr_bucket(atr_pct_cur)
    atr_b_prev   = atr_bucket(atr_pct_prev)
    atr_b_delta  = None if (atr_b_cur is None or atr_b_prev is None) else (atr_b_cur - atr_b_prev)

    bw_cur = None
    if data["bb_upper"]["cur"] is not None and data["bb_lower"]["cur"] is not None:
        bw_cur = data["bb_upper"]["cur"] - data["bb_lower"]["cur"]
    bw_prev = None
    if data["bb_upper"]["prev"] is not None and data["bb_lower"]["prev"] is not None:
        bw_prev = data["bb_upper"]["prev"] - data["bb_lower"]["prev"]

    bw_phase, bw_rel = classify_bw_phase(tf, bw_cur, bw_prev)

    # классификация состояния (приоритеты)
    low_th  = ATR_LOW_PCT.get(tf, 0.3)
    high_th = ATR_HIGH_PCT.get(tf, 0.8)
    is_low  = (atr_pct_cur is not None and atr_pct_cur < low_th)
    is_high = (atr_pct_cur is not None and atr_pct_cur > high_th)

    if is_low and bw_phase == "contracting":
        state = "low_squeeze"
    elif is_high:
        state = "high"
    elif bw_phase == "expanding":
        state = "expanding"
    else:
        state = "normal"

    # детали (округлим для красоты)
    def r2(x): return None if x is None else round(float(x), 2)
    def r6(x): return None if x is None else round(float(x), 6)

    details = {
        "atr_pct": r2(atr_pct_cur),
        "atr_bucket": atr_b_cur,
        "atr_bucket_delta": atr_b_delta,
        "bw": {
            "cur": r6(bw_cur),
            "prev": r6(bw_prev),
            "rel_diff": r6(bw_rel),
            "phase": bw_phase  # expanding/contracting/stable/unknown
        },
        "flags": {
            "is_low": bool(is_low),
            "is_high": bool(is_high),
            "is_expanding": bw_phase == "expanding",
            "is_contracting": bw_phase == "contracting",
        },
        "used_bases": ["atr14", "bb20_2_0_upper", "bb20_2_0_lower", "close"],
        "missing_bases": [],
        "open_time_iso": open_iso,
    }

    await persist_result(pg, redis, symbol, tf, open_iso, state, "ok", details)
    log.debug(
        f"MW_VOL OK {symbol}/{tf}@{open_iso} state={state} "
        f"atr_pct={details['atr_pct']} atr_b={details['atr_bucket']} Δatr_b={details['atr_bucket_delta']} "
        f"bw_phase={bw_phase} bw_rel={details['bw']['rel_diff']}"
    )


# 🔸 Основной воркер: слушает indicator_stream, запускает расчёт с TS-барьером
async def run_indicator_mw_volatility(pg, redis):
    log.debug("MW_VOL: воркер запущен")

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
                        base     = data["indicator"]      # 'atr' / 'bb...'
                        open_iso = data["open_time"]

                        # интересуют только наши базы (atr14, bb20_2_0)
                        if base not in EXPECTED_BASES:
                            # для bb приходит 'bb20_2_0' — ок; для atr — 'atr' (в твоём ready base = indicator)
                            # на всякий случай допускаем индикатор 'atr' как триггер
                            if base != "atr":
                                continue

                        key = (symbol, tf, open_iso)
                        if key in in_flight:
                            continue

                        async def _run(key_tuple):
                            sym, tff, iso = key_tuple
                            async with sem:
                                try:
                                    await compute_vol_for_bar(pg, redis, sym, tff, iso)
                                except Exception as e:
                                    log.error(f"MW_VOL compute error {sym}/{tff}@{iso}: {e}", exc_info=True)
                                finally:
                                    in_flight.discard(key_tuple)

                        in_flight.add(key)
                        tasks.append(asyncio.create_task(_run(key)))

                    except Exception as e:
                        log.warning(f"MW_VOL message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(STREAM_READY, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_VOL ack error: {e}")

            if tasks:
                done, _ = await asyncio.wait(tasks, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                for _t in done:
                    pass

        except Exception as e:
            log.error(f"MW_VOL loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)