# auditor.py — отладочная версия: проверка запуска задач

import logging
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio

# 🔸 Заглушка задачи проверки
async def check_single_pair(pg, symbol, instance, params, min_time, now, semaphore):
    log = logging.getLogger("GAP_CHECKER")
    try:
        instance_id = instance["id"]
        indicator = instance["indicator"]
        timeframe = instance["timeframe"]

        async with semaphore:
            log.info(f"👀 Проверка: {symbol} / {indicator} / {timeframe} / id={instance_id}")
    except Exception:
        log.exception(f"💥 Ошибка в задаче {symbol} / id={instance['id']}")

# 🔸 Основной вход
async def audit_gaps(pg):
    log = logging.getLogger("GAP_CHECKER")
    now = datetime.utcnow()
    min_time = now - timedelta(days=7)
    semaphore = asyncio.Semaphore(10)

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
            tasks.append(check_single_pair(pg, symbol, inst, params, min_time, now, semaphore))

    log.info(f"Запуск аудита по {len(tasks)} связкам...")
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("✅ Аудит завершён (отладочная версия)")