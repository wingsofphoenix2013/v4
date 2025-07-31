# auditor.py — аудит отсутствующих индикаторных значений в БД

import logging
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

from indicators.compute_and_store import get_expected_param_names  # реализуется отдельно

# 🔸 Генерация временных меток open_time по таймфрейму
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

# 🔸 Проверка одной связки symbol + instance_id
async def check_single_pair(pg, symbol, instance, params, min_time, now, semaphore):
    log = logging.getLogger("GAP_CHECKER")
    instance_id = instance["id"]
    indicator = instance["indicator"]
    timeframe = instance["timeframe"]

    step = {
        "m1": timedelta(minutes=1),
        "m5": timedelta(minutes=5),
        "m15": timedelta(minutes=15),
        "h1": timedelta(hours=1),
    }[timeframe]

    end_time = now - step
    end_time = end_time.replace(second=0, microsecond=0)
    open_times = generate_open_times(min_time, end_time, timeframe)
    if not open_times:
        return

    try:
        expected_params = get_expected_param_names(indicator, params)
    except Exception as e:
        log.warning(f"[{symbol}] id={instance_id} {indicator}: ошибка генерации param_name: {e}")
        return

    async with semaphore:
        async with pg.acquire() as conn:
            rows = await conn.fetch("""
                SELECT open_time, param_name FROM indicator_values_v4
                WHERE instance_id = $1 AND symbol = $2 AND open_time BETWEEN $3 AND $4
            """, instance_id, symbol, min_time, end_time)

        existing = defaultdict(set)
        for row in rows:
            existing[row["open_time"]].add(row["param_name"])

        missing = []
        for ts in open_times:
            recorded = existing.get(ts, set())
            if any(p not in recorded for p in expected_params):
                missing.append(ts)

        if not missing:
            return

        async with pg.acquire() as conn:
            await conn.executemany("""
                INSERT INTO indicator_gaps_v4 (instance_id, symbol, open_time)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """, [(instance_id, symbol, ts) for ts in missing])

        # Преобразуем параметры в строку вида length=14, fast=12 и т.д.
        param_str = ", ".join(f"{k}={v}" for k, v in sorted(params.items()))
        log.info(f"⚠️ Пропуски: {symbol} / {indicator}({param_str}) / {timeframe} / id={instance_id} → {len(missing)}")

# 🔸 Основной вход: аудит всех связок
async def audit_gaps(pg):
    log = logging.getLogger("GAP_CHECKER")
    now = datetime.utcnow()
    min_time = now - timedelta(days=7)
    semaphore = asyncio.Semaphore(10)

    async with pg.acquire() as conn:
        # 🔹 Активные тикеры
        symbols = [r['symbol'] for r in await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)]

        # 🔹 Активные индикаторы
        rows = await conn.fetch("""
            SELECT id, indicator, timeframe FROM indicator_instances_v4
            WHERE enabled = true
        """)
        instances = [{
            "id": r["id"],
            "indicator": r["indicator"],
            "timeframe": r["timeframe"]
        } for r in rows]

        # 🔹 Все параметры
        param_rows = await conn.fetch("""
            SELECT instance_id, param, value FROM indicator_parameters_v4
        """)
        param_map = defaultdict(dict)
        for r in param_rows:
            param_map[r["instance_id"]][r["param"]] = r["value"]

    # 🔹 Запуск задач в параллели
    tasks = []
    for symbol in symbols:
        for inst in instances:
            params = param_map.get(inst["id"], {})
            tasks.append(check_single_pair(pg, symbol, inst, params, min_time, now, semaphore))

    log.info(f"Запуск аудита по {len(tasks)} связкам...")
    await asyncio.gather(*tasks)
    log.info("✅ Аудит пропусков завершён")