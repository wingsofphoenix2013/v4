# indicators_ema_status_backfill.py — бэкофилл EMA-status по закрытым позициям
# Этап 3 (WRITE): считаем EMA-status на момент открытия, пишем в positions_indicators_stat, ставим флаг emastatus_checked

import os
import asyncio
import logging
from datetime import datetime

log = logging.getLogger("EMA_STATUS_BF")

from indicators_ema_status import _classify_with_prev, EPS0, EPS1

# 🔸 Конфиг (ENV)
BATCH_SIZE = int(os.getenv("EMA_BF_BATCH_SIZE", "500"))           # позиций за проход
SLEEP_SEC  = int(os.getenv("EMA_BF_LOOP_SLEEP_SEC", "30"))        # пауза между проходами
EMA_LENS   = [int(x) for x in os.getenv("EMA_BF_EMA_LENS", "9,21,50,100,200").split(",")]
REQUIRED_TFS = ("m5", "m15", "h1")

STEP_MS = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}

def floor_to_bar_ms(ts_ms: int, tf: str) -> int:
    step = STEP_MS[tf]
    return (ts_ms // step) * step

def tf_table(tf: str) -> str:
    if tf == "m5":
        return "ohlcv4_m5"
    if tf == "m15":
        return "ohlcv4_m15"
    return "ohlcv4_h1"

# 🔸 Кандидаты к обработке (только стратегии с market_watcher=true)
async def fetch_positions_batch(pg, limit: int):
    sql = """
        SELECT p.position_uid, p.symbol, p.strategy_id, p.direction, p.created_at
        FROM positions_v4 p
        JOIN strategies_v4 s ON s.id = p.strategy_id
        WHERE p.status = 'closed'
          AND p.emastatus_checked = false
          AND s.market_watcher = true
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

# 🔸 Карта инстансов по TF: EMA по длинам, ATR(14), BB(20,2)
async def load_instances_by_tf(pg):
    out = {tf: {"ema": {}, "atr14": None, "bb20_2": None} for tf in REQUIRED_TFS}
    async with pg.acquire() as conn:
        inst_rows = await conn.fetch("""
            SELECT id, indicator, timeframe, enabled_at
            FROM indicator_instances_v4
            WHERE enabled = true AND timeframe = ANY($1::text[])
        """, list(REQUIRED_TFS))
        for row in inst_rows:
            iid = int(row["id"])
            ind = row["indicator"]
            tf  = row["timeframe"]
            params = await conn.fetch("""SELECT param, value FROM indicator_parameters_v4 WHERE instance_id = $1""", iid)
            p = {x["param"]: x["value"] for x in params}
            if ind == "ema":
                try:
                    L = int(p.get("length"))
                    out[tf]["ema"][L] = {"id": iid, "enabled_at": row["enabled_at"], "params": p}
                except Exception:
                    pass
            elif ind == "atr":
                try:
                    if int(p.get("length", 0)) == 14 and out[tf]["atr14"] is None:
                        out[tf]["atr14"] = {"id": iid, "enabled_at": row["enabled_at"], "params": p}
                except Exception:
                    pass
            elif ind == "bb":
                try:
                    length_ok = int(p.get("length", 0)) == 20
                    std_ok = abs(float(p.get("std", 0)) - 2.0) < 1e-9
                    if length_ok and std_ok and out[tf]["bb20_2"] is None:
                        out[tf]["bb20_2"] = {"id": iid, "enabled_at": row["enabled_at"], "params": p}
                except Exception:
                    pass
    return out

# 🔸 Чтение значений индикаторов на точном open_time
async def fetch_indicator_values(conn, instance_id: int, symbol: str, open_time: datetime):
    rows = await conn.fetch(
        """
        SELECT param_name, value
        FROM indicator_values_v4
        WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
        """,
        instance_id, symbol, open_time
    )
    out = {}
    for r in rows:
        try:
            out[r["param_name"]] = float(r["value"])
        except Exception:
            pass
    return out

# 🔸 Чтение close из OHLCV
async def fetch_close(conn, tf: str, symbol: str, open_time: datetime):
    table = tf_table(tf)
    row = await conn.fetchrow(
        f"SELECT close FROM {table} WHERE symbol = $1 AND open_time = $2",
        symbol, open_time
    )
    return float(row["close"]) if row else None

# 🔸 Основной воркер WRITE
async def run_indicators_ema_status_backfill(pg, redis):
    log.info("EMA_STATUS_BF write started: batch=%d sleep=%ds", BATCH_SIZE, SLEEP_SEC)

    inst_map = await load_instances_by_tf(pg)

    while True:
        try:
            batch = await fetch_positions_batch(pg, BATCH_SIZE)
            if not batch:
                log.info("[WRITE] no pending positions (closed & emastatus_checked=false)")
                await asyncio.sleep(SLEEP_SEC)
                continue

            total_positions = len(batch)
            rows = []            # для positions_indicators_stat
            processed_uids = []  # где удалось посчитать и что-то записать
            statuses_count = 0

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
                        step_ms = STEP_MS[tf]
                        bar_ms = floor_to_bar_ms(created_ms, tf)
                        open_dt = datetime.utcfromtimestamp(bar_ms / 1000)
                        prev_dt = datetime.utcfromtimestamp((bar_ms - step_ms) / 1000)

                        close_t = await fetch_close(conn, tf, sym, open_dt)
                        close_p = await fetch_close(conn, tf, sym, prev_dt)
                        if close_t is None or close_p is None:
                            log.debug("[WRITE] uid=%s %s/%s: missing close (t or prev)", uid, sym, tf)
                            continue

                        scale_t = None
                        scale_p = None
                        atr = inst_map[tf]["atr14"]
                        bb  = inst_map[tf]["bb20_2"]

                        if tf in ("m5", "m15") and atr is not None:
                            vals_t = await fetch_indicator_values(conn, atr["id"], sym, open_dt)
                            vals_p = await fetch_indicator_values(conn, atr["id"], sym, prev_dt)
                            at_t = vals_t.get("atr14")
                            at_p = vals_p.get("atr14")
                            if at_t is not None and at_t > 0.0:
                                scale_t = at_t
                            if at_p is not None and at_p > 0.0:
                                scale_p = at_p

                        if (scale_t is None or scale_p is None) and bb is not None:
                            vals_t = await fetch_indicator_values(conn, bb["id"], sym, open_dt)
                            vals_p = await fetch_indicator_values(conn, bb["id"], sym, prev_dt)
                            bbu_t = vals_t.get("bb20_2_0_upper")
                            bbl_t = vals_t.get("bb20_2_0_lower")
                            bbu_p = vals_p.get("bb20_2_0_upper")
                            bbl_p = vals_p.get("bb20_2_0_lower")
                            if scale_t is None and bbu_t is not None and bbl_t is not None and (bbu_t - bbl_t) > 0.0:
                                scale_t = bbu_t - bbl_t
                            if scale_p is None and bbu_p is not None and bbl_p is not None and (bbu_p - bbl_p) > 0.0:
                                scale_p = bbu_p - bbl_p

                        if scale_t is None or scale_p is None or scale_t <= 0.0 or scale_p <= 0.0:
                            log.debug("[WRITE] uid=%s %s/%s: missing scale (t or prev)", uid, sym, tf)
                            continue

                        for L in EMA_LENS:
                            ema_inst = inst_map[tf]["ema"].get(L)
                            if not ema_inst:
                                continue
                            vals_t = await fetch_indicator_values(conn, ema_inst["id"], sym, open_dt)
                            vals_p = await fetch_indicator_values(conn, ema_inst["id"], sym, prev_dt)
                            ema_t = vals_t.get(f"ema{L}")
                            ema_p = vals_p.get(f"ema{L}")
                            if ema_t is None or ema_p is None:
                                log.debug("[WRITE] uid=%s %s/%s ema%d: missing ema(t/prev)", uid, sym, tf, L)
                                continue

                            cls = _classify_with_prev(close_t, close_p, ema_t, ema_p, scale_t, scale_p, EPS0, EPS1, None)
                            if cls is None:
                                log.debug("[WRITE] uid=%s %s/%s ema%d: classify None", uid, sym, tf, L)
                                continue

                            code, label, nd, d, delta_d = cls
                            rows.append((
                                uid, strat, side, tf,
                                int(ema_inst["id"]), f"ema{L}_status", str(code), code,
                                open_dt,
                                ema_inst.get("enabled_at"),
                                None  # params_json — можно не писать на бэкофилле
                            ))
                            statuses_count += 1
                            wrote_any = True

                    if wrote_any:
                        processed_uids.append(uid)

            # 🔸 Запись в БД одним батчем + обновление флагов
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
                                "UPDATE positions_v4 SET emastatus_checked = true WHERE position_uid = ANY($1::text[])",
                                processed_uids
                            )

            # 🔸 Пакетная сводка
            if processed_uids:
                log.info("[WRITE] batch positions=%d, processed=%d, statuses=%d",
                         total_positions, len(processed_uids), statuses_count)
            else:
                log.info("[WRITE] batch positions=%d, processed=0, statuses=0", total_positions)

            await asyncio.sleep(SLEEP_SEC)

        except Exception as e:
            log.error("EMA_STATUS_BF write loop error: %s", e, exc_info=True)
            await asyncio.sleep(SLEEP_SEC)