# position_agregator_worker.py — воркер агрегации позиций (шаг 4: единая выборка снимков, обработка RSI и MFI)

import asyncio
import logging
import json
import math
from datetime import datetime

log = logging.getLogger("IND_AGG")

STREAM   = "signal_log_queue"          # читаем post-commit события
GROUP    = "indicators_agg_group"
CONSUMER = "ind_agg_1"

READ_COUNT = 50
READ_BLOCK_MS = 2000

RSI_BUCKET_STEP = 5
MFI_BUCKET_STEP = 5


# 🔸 Загрузка позиции по uid из positions_v4
async def _fetch_position(pg, position_uid: str):
    async with pg.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT
                position_uid,
                strategy_id,
                status,
                pnl,
                audited,
                closed_at
            FROM positions_v4
            WHERE position_uid = $1
            """,
            position_uid,
        )


# 🔸 Единая выборка всех снимков позиции
async def _fetch_snapshots_all(pg, position_uid: str):
    async with pg.acquire() as conn:
        return await conn.fetch(
            """
            SELECT timeframe, param_name, value_num
            FROM positions_indicators_stat
            WHERE position_uid = $1
              AND value_num IS NOT NULL
            """,
            position_uid,
        )


# 🔸 Разбиение снимков по индикаторам по префиксу param_name
def _partition_snapshots_by_indicator(rows):
    buckets = {
        "rsi": [],
        "mfi": [],
        "adx_dmi": [],
        "macd": [],
        "bb": [],
        "ema": [],
        "kama": [],
        "lr": [],
        "atr": [],
    }
    for r in rows or []:
        p = r["param_name"]
        if   p.startswith("rsi"):     buckets["rsi"].append(r)
        elif p.startswith("mfi"):     buckets["mfi"].append(r)
        elif p.startswith("adx_dmi"): buckets["adx_dmi"].append(r)
        elif p.startswith("macd"):    buckets["macd"].append(r)
        elif p.startswith("bb"):      buckets["bb"].append(r)
        elif p.startswith("ema"):     buckets["ema"].append(r)
        elif p.startswith("kama"):    buckets["kama"].append(r)
        elif p.startswith("lr"):      buckets["lr"].append(r)
        elif p.startswith("atr"):     buckets["atr"].append(r)
    return buckets


# 🔸 Расчёт корзины для значения [0..100] (RSI/MFI)
def _bucket_0_100(value: float, step: int) -> int | None:
    try:
        v = float(value)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    if v < 0.0:
        v = 0.0
    if v >= 100.0:
        v = 99.9999
    return int(math.floor(v / step) * step)


# 🔸 Сбор дельт по RSI для одной позиции
def _collect_rsi_deltas(snaps, strategy_id: int, pnl: float):
    deltas = []
    win = 1 if pnl is not None and float(pnl) > 0 else 0
    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        value = s["value_num"]
        bucket = _bucket_0_100(value, RSI_BUCKET_STEP)
        if bucket is None:
            continue
        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "rsi",
            "param_name": param,
            "bucket_type": "value_bin",
            "bucket_key": "value",
            "bucket_int": bucket,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Сбор дельт по MFI для одной позиции
def _collect_mfi_deltas(snaps, strategy_id: int, pnl: float):
    deltas = []
    win = 1 if pnl is not None and float(pnl) > 0 else 0
    for s in snaps:
        tf = s["timeframe"]
        param = s["param_name"]
        value = s["value_num"]
        bucket = _bucket_0_100(value, MFI_BUCKET_STEP)
        if bucket is None:
            continue
        deltas.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "indicator": "mfi",
            "param_name": param,
            "bucket_type": "value_bin",
            "bucket_key": "value",
            "bucket_int": bucket,
            "dc": 1,
            "dp": float(pnl) if pnl is not None else 0.0,
            "dw": win,
        })
    return deltas


# 🔸 Применение дельт к таблице агрегатов (value_bin/value) и отметка audited
async def _apply_aggregates_and_mark_audited(pg, position_uid: str, deltas: list):
    if not deltas:
        async with pg.acquire() as conn:
            await conn.execute(
                "UPDATE positions_v4 SET audited = TRUE WHERE position_uid = $1 AND audited = FALSE",
                position_uid,
            )
        return

    async with pg.acquire() as conn:
        async with conn.transaction():
            agg = {}
            for d in deltas:
                key = (d["strategy_id"], d["timeframe"], d["indicator"], d["param_name"],
                       d["bucket_type"], d["bucket_key"], d["bucket_int"])
                cur = agg.get(key, {"dc": 0, "dp": 0.0, "dw": 0})
                cur["dc"] += d["dc"]
                cur["dp"] += d["dp"]
                cur["dw"] += d["dw"]
                agg[key] = cur

            for key, m in agg.items():
                strategy_id, timeframe, indicator, param_name, bucket_type, bucket_key, bucket_int = key
                dc, dp, dw = m["dc"], m["dp"], m["dw"]

                row = await conn.fetchrow(
                    """
                    SELECT id, positions_closed, pnl_sum, wins
                    FROM indicator_aggregates_v4
                    WHERE strategy_id = $1
                      AND timeframe   = $2
                      AND indicator   = $3
                      AND param_name  = $4
                      AND bucket_type = 'value_bin'
                      AND bucket_key  = 'value'
                      AND bucket_int  = $5
                    FOR UPDATE
                    """,
                    strategy_id, timeframe, indicator, param_name, bucket_int
                )

                if row:
                    new_count = int(row["positions_closed"]) + dc
                    new_pnl   = float(row["pnl_sum"]) + dp
                    new_wins  = int(row["wins"]) + dw
                    new_avg   = new_pnl / new_count if new_count else 0.0
                    new_wr    = (new_wins / new_count) if new_count else 0.0

                    await conn.execute(
                        """
                        UPDATE indicator_aggregates_v4
                        SET positions_closed = $1,
                            pnl_sum          = $2,
                            wins             = $3,
                            avg_pnl          = $4,
                            winrate          = $5,
                            updated_at       = NOW()
                        WHERE id = $6
                        """,
                        new_count, new_pnl, new_wins, new_avg, new_wr, row["id"]
                    )
                else:
                    new_count = dc
                    new_pnl   = dp
                    new_wins  = dw
                    new_avg   = new_pnl / new_count if new_count else 0.0
                    new_wr    = (new_wins / new_count) if new_count else 0.0

                    await conn.execute(
                        """
                        INSERT INTO indicator_aggregates_v4 (
                            strategy_id, timeframe, indicator, param_name,
                            bucket_type, bucket_key, bucket_int,
                            positions_closed, pnl_sum, wins, avg_pnl, winrate, updated_at
                        ) VALUES ($1,$2,$3,$4,'value_bin','value',$5,$6,$7,$8,$9,$10,NOW())
                        """,
                        strategy_id, timeframe, indicator, param_name,
                        bucket_int,
                        new_count, new_pnl, new_wins, new_avg, new_wr
                    )

            await conn.execute(
                "UPDATE positions_v4 SET audited = TRUE WHERE position_uid = $1 AND audited = FALSE",
                position_uid,
            )


# 🔸 Основной воркер: читаем закрытия, считаем RSI/MFI и пишем агрегаты
async def run_position_aggregator_worker(pg, redis):
    try:
        await redis.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
        log.info(f"Группа {GROUP} создана для {STREAM}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"Группа {GROUP} уже существует")
        else:
            log.exception("Ошибка создания consumer group")
            return

    while True:
        try:
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            to_ack = []
            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        status = data.get("status")
                        if status != "closed":
                            continue

                        uid = data.get("position_uid")
                        if not uid:
                            log.warning("[SKIP] closed status without position_uid")
                            continue

                        row = await _fetch_position(pg, uid)
                        if not row:
                            log.warning(f"[SKIP] position not found in DB: uid={uid}")
                            continue

                        if row["audited"]:
                            log.debug(f"[SKIP] uid={uid} already audited")
                            continue
                        if row["status"] != "closed" or row["pnl"] is None:
                            log.warning(f"[SKIP] uid={uid} post-commit status mismatch (status={row['status']}, pnl={row['pnl']})")
                            continue

                        strategy_id = row["strategy_id"]
                        pnl = float(row["pnl"]) if row["pnl"] is not None else 0.0

                        snaps_all = await _fetch_snapshots_all(pg, uid)
                        parts = _partition_snapshots_by_indicator(snaps_all)

                        deltas = []
                        if parts["rsi"]:
                            deltas += _collect_rsi_deltas(parts["rsi"], strategy_id, pnl)
                        if parts["mfi"]:
                            deltas += _collect_mfi_deltas(parts["mfi"], strategy_id, pnl)

                        if not deltas:
                            log.info(f"[NO-RSI-MFI] uid={uid} → ставим audited=true без изменения агрегатов")
                            await _apply_aggregates_and_mark_audited(pg, uid, [])
                            continue

                        await _apply_aggregates_and_mark_audited(pg, uid, deltas)
                        log.info(f"[AGG] uid={uid} strategy={strategy_id} → записаны {len(deltas)} дельт (RSI/MFI)")

                    except Exception:
                        log.exception("Ошибка обработки сообщения signal_log_queue")

            if to_ack:
                await redis.xack(STREAM, GROUP, *to_ack)

        except Exception as e:
            log.error(f"Ошибка в цикле IND_AGG: {e}", exc_info=True)
            await asyncio.sleep(2)