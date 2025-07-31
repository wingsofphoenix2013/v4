# auditor.py — диагностика: поэтапный лог задач

import logging
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

from indicators.compute_and_store import get_expected_param_names

def generate_open_times(start, end, timeframe):
    step = {
        "m1": timedelta(minutes=1),
        "m5": timedelta(minutes=5),
        "m15": timedelta(minutes=15),
        "h1": timedelta(hours=1),
    }[timeframe]

    times = []
    t = start.replace(second=0, microsecond=0)
    while t <= end:
        aligned = (
            (timeframe == "m1") or
            (timeframe == "m5" and t.minute % 5 == 0) or
            (timeframe == "m15" and t.minute % 15 == 0) or
            (timeframe == "h1" and t.minute == 0)
        )
        if aligned:
            times.append(t)
        t += step

    return times

async def check_single_pair(pg, symbol, instance, params, now, semaphore, result_counter):
    log = logging.getLogger("GAP_CHECKER")
    try:
        instance_id = instance["id"]
        indicator = instance["indicator"]
        timeframe = instance["timeframe"]

        step = {
            "m1": timedelta(minutes=1),
            "m5": timedelta(minutes=5),
            "m15": timedelta(minutes=15),
            "h1": timedelta(hours=1),
        }[timeframe]

        end_time = now.replace(second=0, microsecond=0) - step
        min_time = end_time - timedelta(days=7)

        open_times = generate_open_times(min_time, end_time, timeframe)
        if not open_times:
            log.warning(f"⚠️ Нет open_time в диапазоне: {symbol} / {indicator} / {timeframe} / id={instance_id}")
            return

        expected_params = get_expected_param_names(indicator, params)

        async with pg.acquire() as conn:
            async with semaphore:
                rows = await conn.fetch("""
                    SELECT open_time, param_name FROM indicator_values_v4
                    WHERE instance_id = $1 AND symbol = $2 AND open_time BETWEEN $3 AND $4
                """, instance_id, symbol, min_time, end_time)

        existing = defaultdict(set)
        for row in rows:
            existing[row["open_time"]].add(row["param_name"])

        missing_count = 0
        for ts in open_times:
            recorded = existing.get(ts, set())
            if any(p not in recorded for p in expected_params):
                missing_count += 1

        if missing_count > 0:
            async with result_counter_lock:
                result_counter[instance_id]["indicator"] = indicator
                result_counter[instance_id]["timeframe"] = timeframe
                result_counter[instance_id]["params"] = params
                result_counter[instance_id]["missing"] += missing_count

        log.info(f"📊 {symbol} / {indicator} / {timeframe} / id={instance_id} → пропущено: {missing_count}")

    except Exception:
        log.exception(f"💥 Ошибка при проверке {symbol} / id={instance['id']}")

async def audit_gaps(pg):
    log = logging.getLogger("GAP_CHECKER")
    now = datetime.utcnow()
    min_time = now - timedelta(days=7)
    semaphore = asyncio.Semaphore(10)

    global result_counter_lock
    result_counter_lock = asyncio.Lock()
    result_counter = defaultdict(lambda: {
        "indicator": "",
        "timeframe": "",
        "params": {},
        "missing": 0
    })

    async with pg.acquire() as conn:
        symbols = [r['symbol'] for r in await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)]

        rows = await conn.fetch("""
            SELECT id, indicator, timeframe FROM indicator_instances_v4
            WHERE enabled = true
        """)
        instances = [{
            "id": r["id"],
            "indicator": r["indicator"],
            "timeframe": r["timeframe"]
        } for r in rows]

        param_rows = await conn.fetch("""
            SELECT instance_id, param, value FROM indicator_parameters_v4
        """)
        param_map = defaultdict(dict)
        for r in param_rows:
            param_map[r["instance_id"]][r["param"]] = r["value"]

    tasks = []
    for symbol in symbols:
        for inst in instances:
            params = param_map.get(inst["id"], {})
            tasks.append(check_single_pair(pg, symbol, inst, params, now, semaphore, result_counter))

    log.info(f"🧵 Сформировано задач: {len(tasks)}")
    await asyncio.gather(*tasks, return_exceptions=True)

    for instance_id, info in result_counter.items():
        indicator = info["indicator"]
        timeframe = info["timeframe"]
        params = ", ".join(f"{k}={v}" for k, v in sorted(info["params"].items()))
        count = info["missing"]
        log.info(f"📊 {indicator}({params}) / {timeframe} / id={instance_id} → пропущено: {count}")

    log.info("✅ Аудит пропусков завершён")