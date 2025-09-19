# indicator_mw_momentum.py — воркер расчёта рыночного условия Momentum (bull/bear impulse, overbought/oversold, divergence_flat)

import asyncio
import json
import logging
from datetime import datetime, timedelta

# 🔸 Общие правила MarketWatch (Momentum)
from indicator_mw_shared import (
    load_prev_state,
    mom_thresholds,
    apply_mom_hysteresis_and_dwell,
)

# 🔸 Константы и настройки (в стиле MW_TREND / MW_VOL)
STREAM_READY = "indicator_stream"          # вход: готовность инстансов (macd12/macd5, rsi14/21, mfi14/21)
GROUP       = "mw_mom_group"
CONSUMER    = "mw_mom_1"

# TS-барьер: ждём, пока в TS появятся все нужные точки @open_time
BARRIER_FAST_POLL_MS = 300
BARRIER_FAST_SECONDS = 15
BARRIER_SLOW_POLL_MS = 1200
BARRIER_MAX_WAIT_SEC = 90

# Таймшаги TF (мс)
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# Нужные базы (индикаторы, без суффиксов) — для триггера
EXPECTED_BASES = {"macd12", "macd5", "rsi14", "rsi21", "mfi14", "mfi21"}

# Префиксы Redis
TS_IND_PREFIX = "ts_ind"   # ts_ind:{symbol}:{tf}:{param}
BB_TS_PREFIX  = "bb:ts"    # bb:ts:{symbol}:{tf}:c
KV_MW_PREFIX  = "ind_mw"   # ind_mw:{symbol}:{tf}:{kind}

# Пороговые значения для Momentum (TF-aware при необходимости)
RSI_OVERBOUGHT = {"m5": 70.0, "m15": 70.0, "h1": 70.0}
RSI_OVERSOLD   = {"m5": 30.0, "m15": 30.0, "h1": 30.0}
MFI_OVERBOUGHT = {"m5": 80.0, "m15": 80.0, "h1": 80.0}
MFI_OVERSOLD   = {"m5": 20.0, "m15": 20.0, "h1": 20.0}

# MACD near-zero зона (нормировка к цене, в %)
MACD_ZERO_EPS_PCT = {"m5": 0.03, "m15": 0.05, "h1": 0.10}
# Антидребезг тренда гистограммы (в п.п.)
HIST_MOVE_EPS_PCT = {"m5": 0.03, "m15": 0.04, "h1": 0.05}

# Параллелизм
MAX_CONCURRENCY = 30

# 🔸 Логгер
log = logging.getLogger("MW_MOM")


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
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd_signal",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd_hist",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd_signal",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd_hist",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi14",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi21",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi14",
        f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi21",
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
async def load_mom_inputs(redis, symbol: str, tf: str, open_ms: int) -> dict:
    prev_ms = prev_bar_ms(open_ms, tf)
    keys = {
        # MACD12
        "m12_macd":   f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd",
        "m12_sig":    f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd_signal",
        "m12_hist":   f"{TS_IND_PREFIX}:{symbol}:{tf}:macd12_macd_hist",
        # MACD5
        "m5_macd":    f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd",
        "m5_sig":     f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd_signal",
        "m5_hist":    f"{TS_IND_PREFIX}:{symbol}:{tf}:macd5_macd_hist",
        # RSI / MFI
        "rsi14":      f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi14",
        "rsi21":      f"{TS_IND_PREFIX}:{symbol}:{tf}:rsi21",
        "mfi14":      f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi14",
        "mfi21":      f"{TS_IND_PREFIX}:{symbol}:{tf}:mfi21",
        # price
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


# 🔸 Вспомогательные расчёты (проценты/зоны)
def hist_pct(v_hist: float | None, price: float | None) -> float | None:
    if v_hist is None or price is None or price == 0:
        return None
    return (v_hist / price) * 100.0

def is_overbought(tf: str, rsi: float | None, mfi: float | None) -> bool:
    rsi_thr = RSI_OVERBOUGHT.get(tf, 70.0)
    mfi_thr = MFI_OVERBOUGHT.get(tf, 80.0)
    return (rsi is not None and rsi >= rsi_thr) or (mfi is not None and mfi >= mfi_thr)

def is_oversold(tf: str, rsi: float | None, mfi: float | None) -> bool:
    rsi_thr = RSI_OVERSOLD.get(tf, 30.0)
    mfi_thr = MFI_OVERSOLD.get(tf, 20.0)
    return (rsi is not None and rsi <= rsi_thr) or (mfi is not None and mfi <= mfi_thr)


# 🔸 Запись результата в Redis KV и PostgreSQL
async def persist_result(pg, redis, symbol: str, tf: str, open_time_iso: str,
                         state: str, details: dict,
                         source: str = "live", version: int = 1):
    # KV
    kv_key = f"{KV_MW_PREFIX}:{symbol}:{tf}:momentum"
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
                VALUES ($1,$2,$3,'momentum',$4,'ok',$5,$6,$7,NOW(),NOW())
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
        log.error(f"[PG] upsert error momentum {symbol}/{tf}@{open_time_iso}: {e}")

# 🔸 Расчёт Momentum по ключу (symbol, tf, open_time) — TS-барьер + dpp + hysteresis/dwell
async def compute_momentum_for_bar(pg, redis, symbol: str, tf: str, open_iso: str):
    open_ms = iso_to_ms(open_iso)

    # TS-барьер
    ready = await wait_for_ts_barrier(redis, symbol, tf, open_ms)
    if not ready:
        log.debug(f"MW_MOM GAP {symbol}/{tf}@{open_iso} (TS not ready)")
        return

    # читаем cur/prev
    x = await load_mom_inputs(redis, symbol, tf, open_ms)

    price_cur  = x["close"]["cur"]
    price_prev = x["close"]["prev"]

    # MACD hist в % (cur/prev)
    def hist_pct(v_hist, price):
        if v_hist is None or price is None or price == 0:
            return None
        return (v_hist / price) * 100.0

    m12_hist_pct_cur  = hist_pct(x["m12_hist"]["cur"], price_cur)
    m12_hist_pct_prev = hist_pct(x["m12_hist"]["prev"], price_prev)
    m5_hist_pct_cur   = hist_pct(x["m5_hist"]["cur"],  price_cur)
    m5_hist_pct_prev  = hist_pct(x["m5_hist"]["prev"], price_prev)

    # Δгистограммы (п.п.)
    def dpp(cur, prev):
        if cur is None or prev is None: return None
        return cur - prev

    d_m12_hist_pp = dpp(m12_hist_pct_cur, m12_hist_pct_prev)
    d_m5_hist_pp  = dpp(m5_hist_pct_cur,  m5_hist_pct_prev)

    # режимы MACD (спред macd-signal)
    m12_mode = None
    if x["m12_macd"]["cur"] is not None and x["m12_sig"]["cur"] is not None:
        m12_mode = "bull" if (x["m12_macd"]["cur"] - x["m12_sig"]["cur"]) >= 0 else "bear"
    m5_mode = None
    if x["m5_macd"]["cur"] is not None and x["m5_sig"]["cur"] is not None:
        m5_mode = "bull" if (x["m5_macd"]["cur"] - x["m5_sig"]["cur"]) >= 0 else "bear"

    # near-zero по MACD12 (в %)
    macd12_zero_pct = None
    if x["m12_macd"]["cur"] is not None and price_cur is not None and price_cur != 0:
        macd12_zero_pct = (x["m12_macd"]["cur"] / price_cur) * 100.0

    # RSI/MFI уровни и дельты
    rsi14, rsi21 = x["rsi14"]["cur"], x["rsi21"]["cur"]
    mfi14, mfi21 = x["mfi14"]["cur"], x["mfi21"]["cur"]
    drsi14 = None if (x["rsi14"]["prev"] is None or rsi14 is None) else (rsi14 - x["rsi14"]["prev"])
    dmfi14 = None if (x["mfi14"]["prev"] is None or mfi14 is None) else (mfi14 - x["mfi14"]["prev"])

    # флаги зон
    def is_overbought(rsi, mfi):
        return (rsi is not None and rsi >= RSI_OVERBOUGHT.get(tf,70.0)) or \
               (mfi is not None and mfi >= MFI_OVERBOUGHT.get(tf,80.0))
    def is_oversold(rsi, mfi):
        return (rsi is not None and rsi <= RSI_OVERSOLD.get(tf,30.0)) or \
               (mfi is not None and mfi <= MFI_OVERSOLD.get(tf,20.0))

    overbought = is_overbought(rsi14, mfi14) or is_overbought(rsi21, mfi21)
    oversold   = is_oversold(rsi14, mfi14)   or is_oversold(rsi21, mfi21)

    # импульсные признаки (антидребезг в решателе raw_state, а hysteresis — в shared)
    thr = mom_thresholds(tf)
    hist_in  = thr["hist_in"]
    # raw-решение
    if overbought:
        raw_state = "overbought"
    elif oversold:
        raw_state = "oversold"
    else:
        # импульс, если режимы согласованы и есть достаточная Δhist
        bull_ok = (m12_mode == "bull" and m5_mode == "bull") and \
                  ((d_m12_hist_pp is not None and d_m12_hist_pp > hist_in) or
                   (d_m5_hist_pp  is not None and d_m5_hist_pp  > hist_in))
        bear_ok = (m12_mode == "bear" and m5_mode == "bear") and \
                  ((d_m12_hist_pp is not None and d_m12_hist_pp < -hist_in) or
                   (d_m5_hist_pp  is not None and d_m5_hist_pp  < -hist_in))

        if bull_ok:
            raw_state = "bull_impulse"
        elif bear_ok:
            raw_state = "bear_impulse"
        else:
            raw_state = "divergence_flat"

    # hysteresis + dwell на основе прошлого state/streak
    prev_state, prev_streak = await load_prev_state(redis, kind="momentum", symbol=symbol, tf=tf)
    final_state, new_streak = apply_mom_hysteresis_and_dwell(
        prev_state=prev_state,
        raw_state=raw_state,
        features={"d12": d_m12_hist_pp, "d5": d_m5_hist_pp, "near_zero": None},
        thr=thr,
        prev_streak=prev_streak,
    )

    # детали (округление)
    def r2(x): return None if x is None else round(float(x), 2)

    details = {
        "macd": {
            "mode12": m12_mode,
            "mode5":  m5_mode,
            "hist12_pct": r2(m12_hist_pct_cur),
            "hist12_delta_pp": r2(d_m12_hist_pp),
            "hist5_pct":  r2(m5_hist_pct_cur),
            "hist5_delta_pp":  r2(d_m5_hist_pp),
            "near_zero_pct": r2(macd12_zero_pct),
        },
        "rsi": {"rsi14": r2(rsi14), "drsi14": r2(drsi14), "rsi21": r2(rsi21)},
        "mfi": {"mfi14": r2(mfi14), "dmfi14": r2(dmfi14), "mfi21": r2(mfi21)},
        "flags": {"overbought": bool(overbought), "oversold": bool(oversold)},
        "used_bases": ["macd12","macd5","rsi14","rsi21","mfi14","mfi21","close"],
        "missing_bases": [],
        "open_time_iso": open_iso,

        # синхронизация с on-demand
        "raw_state": raw_state,
        "prev_state": prev_state,
        "streak": new_streak,
    }

    await persist_result(pg, redis, symbol, tf, open_iso, final_state, details)
    log.debug(
        f"MW_MOM OK {symbol}/{tf}@{open_iso} state={final_state} "
        f"(raw={raw_state}, prev={prev_state}, streak={new_streak}) "
        f"m12_hist={details['macd']['hist12_pct']}pp Δ={details['macd']['hist12_delta_pp']}pp "
        f"RSI14={details['rsi']['rsi14']} Δ={details['rsi']['drsi14']} MFI14={details['mfi']['mfi14']} Δ={details['mfi']['dmfi14']}"
    )

# 🔸 Основной воркер: слушает indicator_stream, запускает расчёт с TS-барьером
async def run_indicator_mw_momentum(pg, redis):
    log.debug("MW_MOM: воркер запущен")

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
                        base     = data["indicator"]              # 'macd12'|'macd5'|'rsi14'|'rsi21'|'mfi14'|'mfi21'|...
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
                                    await compute_momentum_for_bar(pg, redis, sym, tff, iso)
                                except Exception as e:
                                    log.error(f"MW_MOM compute error {sym}/{tff}@{iso}: {e}", exc_info=True)
                                finally:
                                    in_flight.discard(key_tuple)

                        in_flight.add(key)
                        tasks.append(asyncio.create_task(_run(key)))

                    except Exception as e:
                        log.warning(f"MW_MOM message error: {e}", exc_info=True)

            if to_ack:
                try:
                    await redis.xack(STREAM_READY, GROUP, *to_ack)
                except Exception as e:
                    log.warning(f"MW_MOM ack error: {e}")

            if tasks:
                done, _ = await asyncio.wait(tasks, timeout=0, return_when=asyncio.FIRST_COMPLETED)
                for _t in done:
                    pass

        except Exception as e:
            log.error(f"MW_MOM loop error: {e}", exc_info=True)
            await asyncio.sleep(0.5)