# indicator_healer.py — подготовка к лечению пропусков индикаторов: выбор из БД и группировка

import asyncio
import logging
from collections import defaultdict

log = logging.getLogger("IND_HEALER")

STEP_MIN = {"m5": 5, "m15": 15, "h1": 60}

# 🔸 Выборка «дыр» со статусом found и группировка по (instance_id, symbol, timeframe)
async def fetch_found_gaps_grouped(pg, limit_pairs: int = 2000):
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

# 🔸 Основной воркер healer (этап подготовки: только логирование групп)
async def run_indicator_healer(pg, redis, pause_sec: int = 2):
    log.info("HEALER индикаторов запущен (этап подготовки)")
    while True:
        try:
            groups = await fetch_found_gaps_grouped(pg)
            if not groups:
                await asyncio.sleep(pause_sec)
                continue

            for iid, sym, tf, by_time in groups:
                total_points = len(by_time)
                total_params = sum(len(s) for s in by_time.values())
                log.info(f"[{sym}] [{tf}] inst={iid} пропуски: баров {total_points}, параметров {total_params}")
                for ot, params in by_time.items():
                    log.debug(f"[{sym}] [{tf}] inst={iid} {ot} → {sorted(params)}")

            await asyncio.sleep(pause_sec)

        except Exception as e:
            log.error(f"Ошибка IND_HEALER: {e}", exc_info=True)
            await asyncio.sleep(pause_sec)