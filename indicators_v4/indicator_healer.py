# indicator_healer.py — лечение пропусков индикаторов: пересчёт и дозапись в БД

import asyncio
import logging
from collections import defaultdict
from datetime import datetime

from indicators.compute_and_store import compute_snapshot_values_async

log = logging.getLogger("IND_HEALER")

STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Выборка «дыр» со статусом found и группировка по (instance_id, symbol, timeframe)
async def fetch_found_gaps_grouped(pg, limit_pairs: int = 1000):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT g.instance_id, g.symbol, g.open_time, g.param_name, i.timeframe
            FROM indicator_gap_v4 g
            JOIN indicator_instances_v4 i ON i.id = g.instance_id
            WHERE g.status = 'found' AND i.enabled = true
            ORDER BY g.instance_id, g.symbol, g.open_time
            LIMIT $1
            """,
            limit_pairs,
        )

    if not rows:
        return []

    grouped = {}  # (iid, symbol, tf) -> {open_time: set(param_name)}
    for r in rows:
        key = (r["instance_id"], r["symbol"], r["timeframe"])
        by_time = grouped.setdefault(key, defaultdict(set))
        by_time[r["open_time"]].add(r["param_name"])

    result = []
    for (iid, sym, tf), by_time in grouped.items():
        result.append((iid, sym, tf, dict(by_time)))
    return result

# 🔸 Данные инстанса: indicator, params, enabled_at, timeframe
async def fetch_instance(pg, instance_id: int):
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, indicator, timeframe, enabled_at FROM indicator_instances_v4 WHERE id = $1",
            instance_id,
        )
        if not row:
            return None
        params_rows = await conn.fetch(
            "SELECT param, value FROM indicator_parameters_v4 WHERE instance_id = $1",
            instance_id,
        )
    params = {p["param"]: p["value"] for p in params_rows}
    return {
        "id": row["id"],
        "indicator": row["indicator"],
        "timeframe": row["timeframe"],
        "enabled_at": row["enabled_at"],
        "params": params,
    }

# 🔸 Точность тикера
async def fetch_precision(pg, symbol: str) -> int:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT precision_price FROM tickers_v4 WHERE symbol = $1",
            symbol,
        )
    return int(row["precision_price"]) if row and row["precision_price"] is not None else 8

# 🔸 Оценка нужной глубины истории (в барах)
def estimate_depth_bars(indicator: str, params: dict) -> int:
    try:
        if indicator == "macd":
            slow = int(params.get("slow", 26))
            return max(60, slow * 3)
        length = int(params.get("length", 14))
        if indicator in ("adx_dmi",):
            return max(60, length * 4)
        return max(60, length * 3)
    except Exception:
        return 200

# 🔸 Загрузка OHLCV из Redis TS [start..end] по количеству баров
async def load_ts_window(redis, symbol: str, interval: str, end_ts_ms: int, count: int):
    step_ms = {"m5": 300_000, "m15": 900_000, "h1": 3_600_000}[interval]
    start_ts = end_ts_ms - (count - 1) * step_ms
    fields = ["o", "h", "l", "c", "v"]
    keys = {f: f"ts:{symbol}:{interval}:{f}" for f in fields}
    tasks = {f: redis.execute_command("TS.RANGE", keys[f], start_ts, end_ts_ms) for f in fields}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    series = {}
    for f, res in zip(tasks.keys(), results):
        if isinstance(res, Exception):
            log.warning(f"TS.RANGE {keys[f]} error: {res}")
            continue
        if res:
            series[f] = {int(ts): float(val) for ts, val in res if val is not None}

    if not series or "c" not in series:
        return None

    idx = sorted(series["c"].keys())
    df = {}
    for f in fields:
        col = []
        col_map = series.get(f, {})
        for ts in idx:
            col.append(col_map.get(ts))
        df[f] = col

    return {"index": idx, "data": df}

# 🔸 Вырезать срез по точному open_time
def slice_until(df, end_ts_ms: int):
    idx = df["index"]
    if not idx:
        return None
    try:
        pos = idx.index(end_ts_ms)
    except ValueError:
        return None
    new_idx = idx[: pos + 1]
    new_data = {k: v[: pos + 1] for k, v in df["data"].items()}
    return {"index": new_idx, "data": new_data}

# 🔸 Преобразовать псевдо-DF в pandas.DataFrame
def to_pandas(df_like):
    import pandas as pd
    if df_like is None:
        return None
    ts_index = pd.to_datetime(df_like["index"], unit="ms")
    out = pd.DataFrame(df_like["data"], index=ts_index)
    out.index.name = "open_time"
    return out

# 🔸 Записать вылеченные параметры в БД и обновить статус gap
async def write_healed(pg, instance_id: int, symbol: str, open_time: datetime, values: dict, missing_params: set):
    to_insert = []
    for pname in missing_params:
        if pname in values:
            try:
                to_insert.append((instance_id, symbol, open_time, pname, float(values[pname])))
            except Exception:
                pass

    if not to_insert:
        async with pg.acquire() as conn:
            await conn.execute(
                """
                UPDATE indicator_gap_v4
                SET attempts = attempts + 1, error = COALESCE(error, 'no_values')
                WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
                """,
                instance_id, symbol, open_time,
            )
        return 0

    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO indicator_values_v4
                (instance_id, symbol, open_time, param_name, value)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (instance_id, symbol, open_time, param_name)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                to_insert,
            )
            await conn.execute(
                """
                UPDATE indicator_gap_v4
                SET status = 'healed_db', healed_db_at = NOW()
                WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
                """,
                instance_id, symbol, open_time,
            )
    return len(to_insert)

# 🔸 Основной воркер healer: пересчёт и дозапись в БД
async def run_indicator_healer(pg, redis, pause_sec: int = 2):
    log.info("HEALER индикаторов запущен")
    sema = asyncio.Semaphore(4)

    while True:
        try:
            groups = await fetch_found_gaps_grouped(pg)
            if not groups:
                await asyncio.sleep(pause_sec)
                continue

            for iid, sym, tf, by_time in groups:
                try:
                    instance = await fetch_instance(pg, iid)
                    if not instance or instance["timeframe"] != tf:
                        continue

                    precision = await fetch_precision(pg, sym)
                    depth = estimate_depth_bars(instance["indicator"], instance["params"])

                    for ot, missing in by_time.items():
                        end_ts = int(ot.timestamp() * 1000)

                        df_like = await load_ts_window(redis, sym, tf, end_ts, depth)
                        df_slice = slice_until(df_like, end_ts)
                        pdf = to_pandas(df_slice)

                        if pdf is None or pdf.empty:
                            async with pg.acquire() as conn:
                                await conn.execute(
                                    """
                                    UPDATE indicator_gap_v4
                                    SET attempts = attempts + 1, error = COALESCE(error, 'no_ohlcv')
                                    WHERE instance_id = $1 AND symbol = $2 AND open_time = $3
                                    """,
                                    iid, sym, ot,
                                )
                            continue

                        async with sema:
                            values = await compute_snapshot_values_async(instance, sym, pdf, precision)

                        inserted = await write_healed(pg, iid, sym, ot, values, missing)
                        log.info(f"[{sym}] [{tf}] inst={iid} {ot} — вылечено {inserted}/{len(missing)}")

                except Exception as e:
                    log.error(f"[{sym}] [{tf}] inst={iid} ошибка лечения: {e}", exc_info=True)

            await asyncio.sleep(pause_sec)

        except Exception as e:
            log.error(f"Ошибка IND_HEALER: {e}", exc_info=True)
            await asyncio.sleep(pause_sec)