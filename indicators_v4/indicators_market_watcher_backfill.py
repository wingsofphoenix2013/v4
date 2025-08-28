# 🔸 indicators_market_watcher_backfill.py — регулярный бэкофилл 14 суток (через 2 мин, затем каждые 96 часов)

import os
import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta
from statistics import median

from infra import init_pg_pool, init_redis_client

# 🔸 Константы и мягкие лимиты
BF_DAYS = int(os.getenv("MRW_BF_DAYS", "14"))
START_DELAY_SEC = int(os.getenv("MRW_BF_START_DELAY_SEC", "120"))     # 2 минуты после старта
SLEEP_AFTER_SEC  = int(os.getenv("MRW_BF_SLEEP_AFTER_SEC",  str(96*3600)))  # 96 часов между прогонами

BF_PG_BATCH = int(os.getenv("MRW_BF_PG_BATCH", "1000"))
BF_TS_BATCH = int(os.getenv("MRW_BF_TS_BATCH", "1000"))
BF_SLEEP_BETWEEN_BATCH_MS = int(os.getenv("MRW_BF_SLEEP_BETWEEN_BATCH_MS", "150"))

N_PCT = int(os.getenv("MRW_N_PCT", "200"))     # окно для p30/p70
N_ACC = int(os.getenv("MRW_N_ACC", "50"))      # окно для z-score Δhist
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5"))   # порог ускорения

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000     # 14 суток
REGIME_VERSION = 1
REGIME_PARAM = "regime9_code"

REQUIRED_TFS = ("h1", "m15", "m5")             # порядок обхода
TF_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

# 🔸 Утилиты статистики (без эмодзи внутри функций)
def _p30(vals):
    if not vals: return float("nan")
    k = max(0, int(0.30*(len(vals)-1)))
    return sorted(vals)[k]

def _p70(vals):
    if not vals: return float("nan")
    k = max(0, int(0.70*(len(vals)-1)))
    return sorted(vals)[k]

def _mad(vals):
    if not vals: return 0.0
    m = median(vals)
    dev = [abs(x-m) for x in vals]
    return median(dev) or 1e-9

def _zscore(x, vals):
    m = median(vals)
    s = _mad(vals)
    return (x - m) / s

async def _load_active_symbols(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    return [r["symbol"] for r in rows]

async def _ts_range_map(redis, key, start_ms, end_ms):
    try:
        res = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return {int(ts): float(v) for ts, v in (res or [])}
    except Exception:
        return {}

def _classify(tf, ema_t1, ema_t, macd_t1, macd_t, z_vals, adx_vals, bbw_vals, atr_vals):
    ema_slope = ema_t - ema_t1
    d_hist = macd_t - macd_t1
    z_d_hist = _zscore(d_hist, list(z_vals)) if z_vals else 0.0

    adx = adx_vals[-1]
    adx_low = max(_p30(list(adx_vals)), 15.0)
    adx_high = min(_p70(list(adx_vals)), 30.0)

    bb_width = bbw_vals[-1]
    bb_low = _p30(list(bbw_vals))
    bb_high = _p70(list(bbw_vals))

    atr = atr_low = atr_high = None
    if tf in {"m5","m15"} and atr_vals and len(atr_vals)>0:
        atr = atr_vals[-1]
        atr_low = _p30(list(atr_vals))
        atr_high = _p70(list(atr_vals))

    is_trend = adx >= adx_high
    is_flat  = adx <= adx_low

    if is_flat:
        if bb_width <= bb_low and (atr is None or atr <= atr_low):
            code = 0  # F_CONS
        elif bb_width >= bb_high:
            code = 1  # F_EXP
        else:
            code = 2  # F_DRIFT
    else:
        direction_up = ema_slope > 0.0
        if z_d_hist > +EPS_Z:   accel=0
        elif abs(z_d_hist) <= EPS_Z: accel=1
        else:                   accel=2
        code = (3+accel) if direction_up else (6+accel)

    diag = {
        "adx": adx, "adx_low": adx_low, "adx_high": adx_high,
        "bb_width": bb_width, "bb_low": bb_low, "bb_high": bb_high,
        "atr": atr, "atr_low": atr_low, "atr_high": atr_high,
        "ema_slope": ema_slope, "macd_hist": macd_t,
        "d_hist": d_hist, "z_d_hist": z_d_hist
    }
    return code, diag

async def _backfill_symbol_tf(pg, redis, symbol, tf, start_ms, end_ms):
    log = logging.getLogger("MRW_BF")
    step = TF_STEP_MS[tf]

    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5","m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key= f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bb_u    = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bb_l    = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bb_c    = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5","m15"} else None

    left_pad = max((N_PCT-1), N_ACC) * step
    span_start = start_ms - left_pad

    ema_map, macd_map, adx_map, bbu_map, bbl_map, bbc_map = await asyncio.gather(
        _ts_range_map(redis, ema_key,  span_start, end_ms),
        _ts_range_map(redis, macd_key, span_start, end_ms),
        _ts_range_map(redis, adx_key,  span_start, end_ms),
        _ts_range_map(redis, bb_u,     span_start, end_ms),
        _ts_range_map(redis, bb_l,     span_start, end_ms),
        _ts_range_map(redis, bb_c,     span_start, end_ms),
    )
    atr_map = await _ts_range_map(redis, atr_key, span_start, end_ms) if atr_key else {}

    ts_list = [ts for ts in sorted(ema_map.keys()) if start_ms <= ts <= end_ms]
    if not ts_list:
        log.info(f"[EMPTY] {symbol}/{tf} — нет данных за окно")
        return

    adx_win = deque(maxlen=N_PCT)
    bbw_win = deque(maxlen=N_PCT)
    atr_win = deque(maxlen=N_PCT) if atr_key else None
    dhist_win = deque(maxlen=N_ACC)

    pre_ts = [ts for ts in sorted(ema_map.keys()) if span_start <= ts < start_ms]
    for ts in pre_ts:
        if ts-step not in ema_map or ts not in macd_map or ts-step not in macd_map: continue
        if ts not in adx_map or ts not in bbu_map or ts not in bbl_map or ts not in bbc_map: continue
        d_hist = macd_map[ts] - macd_map[ts-step]
        bbw = 0.0 if bbc_map.get(ts,0.0)==0.0 else (bbu_map[ts]-bbl_map[ts])/bbc_map[ts]
        adx_win.append(adx_map[ts]); bbw_win.append(bbw); dhist_win.append(d_hist)
        if atr_win is not None and ts in atr_map: atr_win.append(atr_map[ts])

    pg_rows, ts_cmds = [], []
    rows_written = 0

    async def _flush():
        nonlocal pg_rows, ts_cmds, rows_written
        if pg_rows:
            async with pg.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany("""
                        INSERT INTO indicator_marketwatcher_v4
                          (symbol, timeframe, open_time, regime_code, version_id,
                           adx, adx_low, adx_high,
                           bb_width, bb_low, bb_high,
                           atr, atr_low, atr_high,
                           ema_slope, macd_hist, d_hist, z_d_hist)
                        VALUES
                          ($1,$2,$3,$4,$5,
                           $6,$7,$8,
                           $9,$10,$11,
                           $12,$13,$14,
                           $15,$16,$17,$18)
                        ON CONFLICT (symbol, timeframe, open_time)
                        DO UPDATE SET
                          regime_code = EXCLUDED.regime_code,
                          version_id  = EXCLUDED.version_id,
                          adx         = EXCLUDED.adx,
                          adx_low     = EXCLUDED.adx_low,
                          adx_high    = EXCLUDED.adx_high,
                          bb_width    = EXCLUDED.bb_width,
                          bb_low      = EXCLUDED.bb_low,
                          bb_high     = EXCLUDED.bb_high,
                          atr         = EXCLUDED.atr,
                          atr_low     = EXCLUDED.atr_low,
                          atr_high    = EXCLUDED.atr_high,
                          ema_slope   = EXCLUDED.ema_slope,
                          macd_hist   = EXCLUDED.macd_hist,
                          d_hist      = EXCLUDED.d_hist,
                          z_d_hist    = EXCLUDED.z_d_hist,
                          updated_at  = NOW()
                    """, pg_rows)
            rows_written += len(pg_rows)
            pg_rows = []
        if ts_cmds:
            await asyncio.gather(*ts_cmds, return_exceptions=True)
            ts_cmds = []
        await asyncio.sleep(BF_SLEEP_BETWEEN_BATCH_MS/1000)

    for ts in ts_list:
        if ts-step not in ema_map or ts not in macd_map or ts-step not in macd_map: continue
        if ts not in adx_map or ts not in bbu_map or ts not in bbl_map or ts not in bbc_map: continue

        ema_t1, ema_t = ema_map[ts-step], ema_map[ts]
        macd_t1, macd_t = macd_map[ts-step], macd_map[ts]
        d_hist = macd_t - macd_t1
        bbw = 0.0 if bbc_map.get(ts,0.0)==0.0 else (bbu_map[ts]-bbl_map[ts])/bbc_map[ts]

        adx_win.append(adx_map[ts]); bbw_win.append(bbw); dhist_win.append(d_hist)
        if atr_win is not None and ts in atr_map: atr_win.append(atr_map[ts])

        if not adx_win or not bbw_win or not dhist_win: continue

        code, diag = _classify(tf, ema_t1, ema_t, macd_t1, macd_t, dhist_win, adx_win, bbw_win, atr_win)
        open_time_dt = datetime.utcfromtimestamp(ts/1000)

        pg_rows.append((
            symbol, tf, open_time_dt, code, REGIME_VERSION,
            diag["adx"], diag["adx_low"], diag["adx_high"],
            diag["bb_width"], diag["bb_low"], diag["bb_high"],
            diag["atr"], diag["atr_low"], diag["atr_high"],
            diag["ema_slope"], diag["macd_hist"], diag["d_hist"], diag["z_d_hist"]
        ))

        ts_key = f"ts_ind:{symbol}:{tf}:{REGIME_PARAM}"
        ts_cmds.append(redis.execute_command(
            "TS.ADD", ts_key, ts, str(code),
            "RETENTION", RETENTION_TS_MS,
            "DUPLICATE_POLICY", "last"
        ))

        if len(pg_rows) >= BF_PG_BATCH or len(ts_cmds) >= BF_TS_BATCH:
            await _flush()

    await _flush()
    logging.getLogger("MRW_BF").info(f"[DONE] {symbol}/{tf}: rows={rows_written}")

# 🔸 Главный цикл: стартовая задержка → полный проход 14 суток → сон 96 часов → повтор
async def run_market_watcher_backfill_regular():
    log = logging.getLogger("MRW_BF")
    pg = await init_pg_pool()
    redis = await init_redis_client()

    while True:
        log.info(f"regular backfill: sleep {START_DELAY_SEC}s before run")
        await asyncio.sleep(START_DELAY_SEC)

        try:
            symbols = await _load_active_symbols(pg)
            if not symbols:
                log.info("no active symbols, nothing to do")
            else:
                end_dt = datetime.utcnow()
                start_dt = end_dt - timedelta(days=BF_DAYS)
                start_ms = int(start_dt.timestamp()*1000)
                end_ms   = int(end_dt.timestamp()*1000)

                log.info(f"regular backfill: start window [{start_dt.isoformat()} .. {end_dt.isoformat()}], symbols={len(symbols)}")

                # последовательный проход: символ → TF (h1→m15→m5)
                for sym in symbols:
                    for tf in REQUIRED_TFS:
                        await _backfill_symbol_tf(pg, redis, sym, tf, start_ms, end_ms)

                log.info("regular backfill: finished")

        except Exception as e:
            log.error(f"regular backfill error: {e}", exc_info=True)

        log.info(f"regular backfill: sleep {SLEEP_AFTER_SEC}s before next run")
        await asyncio.sleep(SLEEP_AFTER_SEC)