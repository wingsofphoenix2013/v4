# indicator_ts_filler.py — дозаполнение Redis TS по вылеченным в БД параметрам индикаторов

import asyncio
import logging

log = logging.getLogger("IND_TS_FILLER")

# 🔸 Выборка «дыр» со статусом healed_db и группировка по (instance_id, symbol, timeframe)
async def fetch_healed_db_gaps(pg, limit_rows: int = 5_000):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT g.instance_id, g.symbol, g.open_time, g.param_name, i.timeframe
            FROM indicator_gap_v4 g
            JOIN indicator_instances_v4 i ON i.id = g.instance_id
            WHERE g.status = 'healed_db' AND i.enabled = true
            ORDER BY g.detected_at
            LIMIT $1
            """,
            limit_rows,
        )
    if not rows:
        return []
    grouped = {}  # (iid, symbol, tf) -> {(open_time, param_name), ...}
    for r in rows:
        key = (r["instance_id"], r["symbol"], r["timeframe"])
        grouped.setdefault(key, set()).add((r["open_time"], r["param_name"]))
    result = []
    for (iid, sym, tf), items in grouped.items():
        result.append((iid, sym, tf, items))
    return result

# 🔸 Вытащить значения из БД для набора open_time
async def fetch_iv_values(pg, instance_id: int, symbol: str, open_times: list):
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT open_time, param_name, value
            FROM indicator_values_v4
            WHERE instance_id = $1
              AND symbol = $2
              AND open_time = ANY($3::timestamp[])
            """,
            instance_id, symbol, open_times,
        )
    return rows

# 🔸 Записать точки в TS и пометить в gap как healed_ts
async def write_ts_and_mark(pg, redis, instance_id: int, symbol: str, timeframe: str, rows):
    if not rows:
        return 0

    written = []
    tasks = []

    for r in rows:
        ot = r["open_time"]
        pname = r["param_name"]
        val = r["value"]

        key = f"ts_ind:{symbol}:{timeframe}:{pname}"
        ts_ms = int(ot.timestamp() * 1000)

        cmd = redis.execute_command(
            "TS.ADD", key, ts_ms, str(val),
            "RETENTION", 1_209_600_000,  # 14 дней
            "DUPLICATE_POLICY", "last"
        )
        if asyncio.iscoroutine(cmd):
            tasks.append(cmd)
        written.append((instance_id, symbol, ot, pname))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    if written:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                UPDATE indicator_gap_v4
                SET status = 'healed_ts', healed_ts_at = NOW()
                WHERE instance_id = $1 AND symbol = $2 AND open_time = $3 AND param_name = $4
                """,
                written,
            )

    return len(written)

# 🔸 Основной воркер TS-филлера
async def run_indicator_ts_filler(pg, redis, pause_sec: int = 0.5):
    log.debug("TS_FILLER индикаторов запущен")
    while True:
        try:
            groups = await fetch_healed_db_gaps(pg, limit_rows=5_000)
            if not groups:
                await asyncio.sleep(pause_sec)
                continue

            for iid, sym, tf, items in groups:
                try:
                    times = sorted({ot for (ot, _) in items})
                    rows = await fetch_iv_values(pg, iid, sym, times)

                    # фильтруем только те (open_time, param_name), что есть в gap-элементах
                    need = {(ot, pn) for (ot, pn) in items}
                    rows_need = [r for r in rows if (r["open_time"], r["param_name"]) in need]

                    n = await write_ts_and_mark(pg, redis, iid, sym, tf, rows_need)
                    log.debug(f"[{sym}] [{tf}] inst={iid} TS заполнен для {n} точек")

                except Exception as e:
                    log.error(f"[{sym}] [{tf}] inst={iid} ошибка TS_FILLER: {e}", exc_info=True)

            await asyncio.sleep(pause_sec)

        except Exception as e:
            log.error(f"Ошибка IND_TS_FILLER: {e}", exc_info=True)
            await asyncio.sleep(pause_sec)