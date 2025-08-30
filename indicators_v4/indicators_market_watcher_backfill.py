# 🔸 indicators_market_watcher_backfill.py — регулярный бэкофилл 14 суток (regime9 v2, единое ядро + гистерезис)

import os
import asyncio
import logging
from datetime import datetime, timedelta
from statistics import median

from infra import init_pg_pool, init_redis_client
from regime9_core import RegimeState, RegimeParams, decide_regime_code

# 🔸 Константы и мягкие лимиты
BF_DAYS = int(os.getenv("MRW_BF_DAYS", "14"))
START_DELAY_SEC = int(os.getenv("MRW_BF_START_DELAY_SEC", "120"))           # 2 минуты после старта
SLEEP_AFTER_SEC  = int(os.getenv("MRW_BF_SLEEP_AFTER_SEC",  str(96*3600)))  # 96 часов между прогонами

BF_PG_BATCH = int(os.getenv("MRW_BF_PG_BATCH", "1000"))
BF_TS_BATCH = int(os.getenv("MRW_BF_TS_BATCH", "1000"))
BF_SLEEP_BETWEEN_BATCH_MS = int(os.getenv("MRW_BF_SLEEP_BETWEEN_BATCH_MS", "150"))

N_PCT = int(os.getenv("MRW_N_PCT", "200"))     # окно для p30/p70
N_ACC = int(os.getenv("MRW_N_ACC", "50"))      # окно для z-score ΔMACD
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5"))   # порог ускорения (для z-score ΔMACD)

RETENTION_TS_MS = 14 * 24 * 60 * 60 * 1000     # 14 суток
REGIME_VERSION = 2                              # пишем сразу v2
REGIME_PARAM = "regime9_code"

REQUIRED_TFS = ("h1", "m15", "m5")             # порядок обхода
TF_STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

HYST_TREND_BARS = int(os.getenv("MRW_R9_HYST_TREND_BARS", "2"))  # тренд↔флет
HYST_SUB_BARS   = int(os.getenv("MRW_R9_HYST_SUB_BARS", "1"))    # accel/stable/decel


# 🔸 Вспомогательные утилиты
async def _load_active_symbols(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    return [r["symbol"] for r in rows]

async def _ts_range_map(redis, key, start_ms, end_ms):
    if not key:
        return {}
    try:
        res = await redis.execute_command("TS.RANGE", key, start_ms, end_ms)
        return {int(ts): float(v) for ts, v in (res or [])}
    except Exception:
        return {}

def _tail_vals_up_to_ts(series_map, ts, n):
    ks = [t for t in series_map.keys() if t <= ts]
    if not ks:
        return []
    ks.sort()
    ks = ks[-n:]
    return [series_map[k] for k in ks]

def _build_features_for_core(tf, ts, step,
                             ema_map, macd_map, adx_map,
                             bbu_map, bbl_map, bbc_map,
                             atr_map_or_none):
    # базовые точки t-1 и t
    if (ts - step) not in ema_map or (ts - step) not in macd_map:
        return None
    ema_t1, ema_t = ema_map[ts - step], ema_map[ts]
    macd_t1, macd_t = macd_map[ts - step], macd_map[ts]

    # окно ΔMACD (N_ACC последних разностей)
    macd_keys = [t for t in macd_map.keys() if t <= ts]
    macd_keys.sort()
    tail = macd_keys[-(N_ACC + 1):]
    dhist = []
    for i in range(len(tail) - 1):
        a, b = tail[i], tail[i + 1]
        dhist.append(macd_map[b] - macd_map[a])

    # окна на N_PCT по ADX / BB / ATR
    adx_win = _tail_vals_up_to_ts(adx_map, ts, N_PCT)
    bbu_win = _tail_vals_up_to_ts(bbu_map, ts, N_PCT)
    bbl_win = _tail_vals_up_to_ts(bbl_map, ts, N_PCT)
    bbc_win = _tail_vals_up_to_ts(bbc_map, ts, N_PCT)

    if atr_map_or_none is not None:
        atr_win = _tail_vals_up_to_ts(atr_map_or_none, ts, N_PCT)
        atr_t = atr_map_or_none.get(ts, None)
    else:
        atr_win, atr_t = None, None

    # минимальная валидация окон
    if len(adx_win) == 0 or len(bbu_win) == 0 or len(bbl_win) == 0 or len(bbc_win) == 0:
        return None

    return {
        "ema_t1": ema_t1, "ema_t": ema_t,
        "macd_t1": macd_t1, "macd_t": macd_t,
        "dhist_win": dhist[-N_ACC:],
        "adx_win": adx_win,
        "bb_u_win": bbu_win, "bb_l_win": bbl_win, "bb_c_win": bbc_win,
        "atr_t": atr_t, "atr_win": atr_win,
    }


# 🔸 Прогон по одному (symbol, tf): чтение TS за 14 дней, скользящая классификация (ядро v2), батч-запись
async def _backfill_symbol_tf(pg, redis, symbol, tf, start_ms, end_ms):
    log = logging.getLogger("MRW_BF")
    step = TF_STEP_MS[tf]

    # ключи TS
    adx_key = f"ts_ind:{symbol}:{tf}:adx_dmi14_adx" if tf in {"m5","m15"} else f"ts_ind:{symbol}:{tf}:adx_dmi28_adx"
    ema_key = f"ts_ind:{symbol}:{tf}:ema21"
    macd_key= f"ts_ind:{symbol}:{tf}:macd12_macd_hist"
    bb_u    = f"ts_ind:{symbol}:{tf}:bb20_2_0_upper"
    bb_l    = f"ts_ind:{symbol}:{tf}:bb20_2_0_lower"
    bb_c    = f"ts_ind:{symbol}:{tf}:bb20_2_0_center"
    atr_key = f"ts_ind:{symbol}:{tf}:atr14" if tf in {"m5","m15"} else None

    # добавим слева запас для окон
    left_pad = max((N_PCT - 1), N_ACC) * step
    span_start = start_ms - left_pad

    # читаем ряды пачкой
    ema_map, macd_map, adx_map, bbu_map, bbl_map, bbc_map = await asyncio.gather(
        _ts_range_map(redis, ema_key,  span_start, end_ms),
        _ts_range_map(redis, macd_key, span_start, end_ms),
        _ts_range_map(redis, adx_key,  span_start, end_ms),
        _ts_range_map(redis, bb_u,     span_start, end_ms),
        _ts_range_map(redis, bb_l,     span_start, end_ms),
        _ts_range_map(redis, bb_c,     span_start, end_ms),
    )
    atr_map = await _ts_range_map(redis, atr_key, span_start, end_ms) if atr_key else {}

    # шкала времени — по EMA (репрезентативный ряд)
    ts_list = [ts for ts in sorted(ema_map.keys()) if start_ms <= ts <= end_ms]
    if not ts_list:
        log.debug(f"[EMPTY] {symbol}/{tf} — нет данных за окно")
        return

    # in-memory состояние гистерезиса для данного символа/ТФ
    state = RegimeState()
    params = RegimeParams(hyst_trend_bars=HYST_TREND_BARS, hyst_sub_bars=HYST_SUB_BARS, eps_z=EPS_Z)

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
        await asyncio.sleep(BF_SLEEP_BETWEEN_BATCH_MS / 1000)

    for ts in ts_list:
        # проверка наличия всех нужных рядов на t и t-1
        if (ts - step) not in ema_map or (ts - step) not in macd_map:
            continue
        if ts not in macd_map or ts not in adx_map or ts not in bbu_map or ts not in bbl_map or ts not in bbc_map:
            continue

        features = _build_features_for_core(
            tf, ts, step,
            ema_map, macd_map, adx_map,
            bbu_map, bbl_map, bbc_map,
            atr_map if atr_key else None
        )
        if features is None:
            continue

        code, state, diag = decide_regime_code(tf, features, state, params)
        open_time_dt = datetime.utcfromtimestamp(ts / 1000)

        pg_rows.append((
            symbol, tf, open_time_dt, code, REGIME_VERSION,
            diag["adx"], diag["adx_low"], diag["adx_high"],
            diag["bb_width"], diag["bb_low"], diag["bb_high"],
            diag["atr"], diag["atr_low"], diag["atr_high"],
            diag["ema_slope"], diag["macd_hist"], diag["d_hist"], diag["z_d_hist"]
        ))

        ts_key = f"ts_ind:{symbol}:{tf}:{REGIME_PARAM}"
        ts_cmds.append(
            redis.execute_command(
                "TS.ADD", ts_key, ts, str(code),
                "RETENTION", RETENTION_TS_MS,
                "DUPLICATE_POLICY", "last"
            )
        )

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
        log.debug(f"regular backfill: sleep {START_DELAY_SEC}s before run")
        await asyncio.sleep(START_DELAY_SEC)

        try:
            symbols = await _load_active_symbols(pg)
            if not symbols:
                log.debug("no active symbols, nothing to do")
            else:
                end_dt = datetime.utcnow()
                start_dt = end_dt - timedelta(days=BF_DAYS)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms   = int(end_dt.timestamp() * 1000)

                log.debug(f"regular backfill: start window [{start_dt.isoformat()} .. {end_dt.isoformat()}], symbols={len(symbols)}")

                # последовательный проход: символ → TF (h1 → m15 → m5)
                for sym in symbols:
                    for tf in REQUIRED_TFS:
                        await _backfill_symbol_tf(pg, redis, sym, tf, start_ms, end_ms)

                log.debug("regular backfill: finished")

        except Exception as e:
            log.error(f"regular backfill error: {e}", exc_info=True)

        log.debug(f"regular backfill: sleep {SLEEP_AFTER_SEC}s before next run")
        await asyncio.sleep(SLEEP_AFTER_SEC)