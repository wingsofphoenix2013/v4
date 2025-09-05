# positions_mw_backfill.py — бэкофилл MW (regime9 v2) по позициям из БД
# Источник данных: indicator_values_v4 (без Redis TS)
# Пишем в: positions_indicators_stat (param_name='mw', instance_id по TF захардкожен)
# Флаг в positions_v4: mrk_watcher_checked=true
# Фильтр: только стратегии с market_watcher=true
# Пакетная обработка: BATCH_SIZE (по умолчанию 500)
# Старт через START_DELAY_SEC (по умолчанию 120 сек), затем цикл с паузой SLEEP_SEC

import os
import asyncio
import logging
from datetime import datetime, timedelta

from regime9_core import RegimeState, RegimeParams, decide_regime_code

log = logging.getLogger("MW_POS_BF")

# Конфиг
BATCH_SIZE = int(os.getenv("MW_BF_BATCH_SIZE", "500"))
SLEEP_SEC  = int(os.getenv("MW_BF_SLEEP_SEC", "30"))
START_DELAY_SEC = int(os.getenv("MW_BF_START_DELAY_SEC", "120"))

# Окна/пороги regime9 v2 (как в live/backfill)
N_PCT = int(os.getenv("MRW_N_PCT", "200"))
N_ACC = int(os.getenv("MRW_N_ACC", "50"))
EPS_Z = float(os.getenv("MRW_EPS_Z", "0.5"))
HYST_TREND_BARS = int(os.getenv("MRW_R9_HYST_TREND_BARS", "2"))
HYST_SUB_BARS   = int(os.getenv("MRW_R9_HYST_SUB_BARS", "1"))

# TF и шаги
REQUIRED_TFS = ("m5", "m15", "h1")
STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step

# Захардкоженные instance_id для записи MW в positions_indicators_stat
MW_INSTANCE_ID = {"m5": 1001, "m15": 1002, "h1": 1003}

# Карта instance_id исходных индикаторов по TF (из твоих данных)
INST = {
    "m5":  {"ema21": 37, "macd": 24, "adx": 21, "bb": 33, "atr14": 7},
    "m15": {"ema21": 41, "macd": 25, "adx": 22, "bb": 62, "atr14": 8},
    "h1":  {"ema21": 47, "macd": 51, "adx": 58, "bb": 64}
}

# Имена параметров в indicator_values_v4
PARAMS = {
    "ema21": "ema21",
    "macd":  "macd12_macd_hist",
    "adx_m5m15": "adx_dmi14_adx",
    "adx_h1":    "adx_dmi28_adx",
    "bb_upper":  "bb20_2_0_upper",
    "bb_lower":  "bb20_2_0_lower",
    "bb_center": "bb20_2_0_center",
    "atr14":     "atr14",
}

# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции выборок из БД

async def fetch_positions_batch(pg, limit: int):
    sql = """
        SELECT p.position_uid, p.symbol, p.strategy_id, p.direction, p.created_at
        FROM positions_v4 p
        JOIN strategies_v4 s ON s.id = p.strategy_id
        WHERE s.market_watcher = true
          AND COALESCE(p.mrk_watcher_checked, false) = false
        ORDER BY p.created_at ASC
        LIMIT $1
    """
    async with pg.acquire() as conn:
        rows = await conn.fetch(sql, limit)
    return [
        {
            "position_uid": r["position_uid"],
            "symbol": r["symbol"],
            "strategy_id": r["strategy_id"],
            "direction": r["direction"],
            "created_at": r["created_at"],
        } for r in rows
    ]

async def fetch_series_map(conn, instance_id: int, symbol: str, start_dt: datetime, end_dt: datetime):
    rows = await conn.fetch(
        """
        SELECT open_time, value
        FROM indicator_values_v4
        WHERE instance_id = $1 AND symbol = $2
          AND open_time BETWEEN $3 AND $4
        ORDER BY open_time
        """,
        instance_id, symbol, start_dt, end_dt
    )
    return {r["open_time"]: float(r["value"]) for r in rows}

def tail_vals(series_map: dict, ts: datetime, n: int):
    keys = [k for k in series_map.keys() if k <= ts]
    if not keys:
        return []
    keys.sort()
    return [series_map[k] for k in keys[-n:]]

def last_before_or_eq(series_map: dict, ts: datetime):
    keys = [k for k in series_map.keys() if k <= ts]
    if not keys:
        return None
    return series_map[max(keys)]

# ──────────────────────────────────────────────────────────────────────────────
# Расчёт MW по данным БД на момент открытия позиции (bar_open_time)

async def compute_mw_for_position_tf(conn, symbol: str, tf: str, bar_open_dt: datetime):
    step_ms = STEP_MS[tf]
    step = timedelta(milliseconds=step_ms)
    # окно загружаем с запасом (N_ACC/N_PCT)
    left_win = max((N_PCT - 1), N_ACC)
    start_dt = bar_open_dt - left_win * step

    # instance_id по TF
    inst = INST[tf]
    iid_ema = inst["ema21"]
    iid_macd = inst["macd"]
    iid_adx = inst["adx"]
    iid_bb  = inst["bb"]
    iid_atr = inst.get("atr14")

    # Загружаем серии
    ema_map  = await fetch_series_map(conn, iid_ema,  symbol, start_dt, bar_open_dt)
    macd_map = await fetch_series_map(conn, iid_macd, symbol, start_dt, bar_open_dt)
    adx_map  = await fetch_series_map(conn, iid_adx,  symbol, start_dt, bar_open_dt)
    bb_u_map = await fetch_series_map(conn, iid_bb,   symbol, start_dt, bar_open_dt)  # upper
    bb_l_map = await fetch_series_map(conn, iid_bb,   symbol, start_dt, bar_open_dt)  # lower
    bb_c_map = await fetch_series_map(conn, iid_bb,   symbol, start_dt, bar_open_dt)  # center

    # bb_* мапы содержат все параметры по одному instance_id → отфильтруем по param_name
    # Упростим: пере-загрузим upper/lower/center отдельно через фильтр param_name.
    # (Чтобы не утяжелять, используем по три запроса — надежно и ясно.)

    async def fetch_param_map(iid: int, pname: str):
        rows = await conn.fetch(
            """
            SELECT open_time, value
            FROM indicator_values_v4
            WHERE instance_id = $1 AND symbol = $2 AND param_name = $3
              AND open_time BETWEEN $4 AND $5
            ORDER BY open_time
            """,
            iid, symbol, pname, start_dt, bar_open_dt
        )
        return {r["open_time"]: float(r["value"]) for r in rows}

    bb_u_map = await fetch_param_map(iid_bb, PARAMS["bb_upper"])
    bb_l_map = await fetch_param_map(iid_bb, PARAMS["bb_lower"])
    bb_c_map = await fetch_param_map(iid_bb, PARAMS["bb_center"])

    atr_map = {}
    if iid_atr is not None and tf in ("m5", "m15"):
        atr_map = await fetch_param_map(iid_atr, PARAMS["atr14"])

    # Значения на t и t-1
    t  = bar_open_dt
    t1 = bar_open_dt - step

    ema_t  = last_before_or_eq(ema_map,  t)
    ema_t1 = last_before_or_eq(ema_map,  t1)
    macd_t  = last_before_or_eq(macd_map, t)
    macd_t1 = last_before_or_eq(macd_map, t1)

    # Окна
    macd_series = tail_vals(macd_map, t, N_ACC + 1)
    dhist = [macd_series[i+1] - macd_series[i] for i in range(len(macd_series)-1)] if len(macd_series) >= 2 else []

    adx_win = tail_vals(adx_map, t, N_PCT)
    bb_u_win = tail_vals(bb_u_map, t, N_PCT)
    bb_l_win = tail_vals(bb_l_map, t, N_PCT)
    bb_c_win = tail_vals(bb_c_map, t, N_PCT)

    atr_t = None
    atr_win = None
    if tf in ("m5", "m15"):
        atr_t = last_before_or_eq(atr_map, t) if atr_map else None
        atr_win = tail_vals(atr_map, t, N_PCT) if atr_map else None

    # Валидация наличия
    if None in (ema_t, ema_t1, macd_t, macd_t1) or len(adx_win) == 0 or len(bb_u_win) == 0 or len(bb_l_win) == 0 or len(bb_c_win) == 0:
        return None

    features = {
        "ema_t1": float(ema_t), "ema_t": float(ema_t),
        "macd_t1": float(macd_t1), "macd_t": float(macd_t),
        "dhist_win": dhist[-N_ACC:],
        "adx_win": [float(x) for x in adx_win],
        "bb_u_win": [float(x) for x in bb_u_win],
        "bb_l_win": [float(x) for x in bb_l_win],
        "bb_c_win": [float(x) for x in bb_c_win],
        "atr_t": (float(atr_t) if atr_t is not None else None),
        "atr_win": ([float(x) for x in atr_win] if atr_win is not None else None),
    }

    state = RegimeState()  # локальный гистерезис в рамках одного расчёта
    params = RegimeParams(hyst_trend_bars=HYST_TREND_BARS, hyst_sub_bars=HYST_SUB_BARS, eps_z=EPS_Z)
    code, _, diag = decide_regime_code(tf, features, state, params)
    return code  # int 0..8

# ──────────────────────────────────────────────────────────────────────────────
# Основной воркер: START_DELAY_SEC → батчи по BATCH_SIZE → пауза SLEEP_SEC

async def run_positions_mw_backfill(pg):
    log.info("MW_POS_BF started: start_delay=%ds batch=%d sleep=%ds", START_DELAY_SEC, BATCH_SIZE, SLEEP_SEC)
    await asyncio.sleep(START_DELAY_SEC)

    while True:
        try:
            batch = await fetch_positions_batch(pg, BATCH_SIZE)
            if not batch:
                log.info("[WRITE] no pending positions (mrk_watcher_checked=false for MW)")
                await asyncio.sleep(SLEEP_SEC)
                continue

            total_positions = len(batch)
            rows = []
            processed_uids = []
            statuses = 0

            async with pg.acquire() as conn:
                for pos in batch:
                    uid = pos["position_uid"]
                    sym = pos["symbol"]
                    strat = pos["strategy_id"]
                    side = pos["direction"]
                    ca   = pos["created_at"]

                    try:
                        created_ms = int(ca.replace(tzinfo=None).timestamp() * 1000)
                    except Exception:
                        log.debug("[WRITE] uid=%s symbol=%s: bad created_at", uid, sym)
                        continue

                    wrote_any = False

                    for tf in REQUIRED_TFS:
                        bar_ms = floor_to_bar_ms(created_ms, tf)
                        bar_dt = datetime.utcfromtimestamp(bar_ms / 1000)

                        code = await compute_mw_for_position_tf(conn, sym, tf, bar_dt)
                        if code is None:
                            log.debug("[WRITE] uid=%s %s/%s: cannot compute MW", uid, sym, tf)
                            continue

                        rows.append((
                            uid, strat, side, tf,
                            MW_INSTANCE_ID[tf], "mw", str(code), code,
                            bar_dt,
                            None,  # enabled_at — фиктивный инстанс
                            None   # params_json
                        ))
                        statuses += 1
                        wrote_any = True

                    if wrote_any:
                        processed_uids.append(uid)

            # Запись в БД одним батчем + флаги
            if rows:
                async with pg.acquire() as conn:
                    async with conn.transaction():
                        await conn.executemany(
                            """
                            INSERT INTO positions_indicators_stat
                            (position_uid, strategy_id, direction, timeframe,
                             instance_id, param_name, value_str, value_num,
                             bar_open_time, enabled_at, params_json)
                            VALUES
                            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                            ON CONFLICT (position_uid, timeframe, instance_id, param_name, bar_open_time)
                            DO NOTHING
                            """,
                            rows
                        )
                        if processed_uids:
                            await conn.execute(
                                "UPDATE positions_v4 SET mrk_watcher_checked = true WHERE position_uid = ANY($1::text[])",
                                processed_uids
                            )

            # Пакетная сводка
            if processed_uids:
                log.info("[WRITE] batch positions=%d, processed=%d, mw_rows=%d",
                         total_positions, len(processed_uids), statuses)
            else:
                log.info("[WRITE] batch positions=%d, processed=0, mw_rows=0", total_positions)

            await asyncio.sleep(SLEEP_SEC)

        except Exception as e:
            log.error("MW_POS_BF loop error: %s", e, exc_info=True)
            await asyncio.sleep(SLEEP_SEC)