# auditor.py — аудит системы: структура, конфигурация и пропуски

import logging
from datetime import datetime, timedelta

# 🔸 Базовый аудит текущей конфигурации системы
async def analyze_config_state(pg):
    log = logging.getLogger("GAP_CHECKER")

    async with pg.acquire() as conn:
        # 🔹 Количество активных тикеров
        row = await conn.fetchrow("""
            SELECT COUNT(*) AS count
            FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        ticker_count = row["count"]

        # 🔹 Количество активных индикаторов
        row = await conn.fetchrow("""
            SELECT COUNT(*) AS count
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        indicator_count = row["count"]

        # 🔹 Уникальные таймфреймы среди активных индикаторов
        rows = await conn.fetch("""
            SELECT DISTINCT timeframe
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        timeframes = sorted(r["timeframe"] for r in rows)

    # 🔸 Вывод в лог
    log.debug(f"📦 Активных тикеров: {ticker_count}")
    log.debug(f"🧩 Активных индикаторов: {indicator_count}")
    log.debug(f"⏱ Таймфреймы среди индикаторов: {', '.join(timeframes) if timeframes else '—'}")

# 🔸 Генерация библиотеки open_time по каждому таймфрейму (24ч глубина)
async def analyze_open_times():
    log = logging.getLogger("GAP_CHECKER")

    timeframes = ["m1", "m5", "m15", "h1"]
    step_map = {
        "m1": timedelta(minutes=1),
        "m5": timedelta(minutes=5),
        "m15": timedelta(minutes=15),
        "h1": timedelta(hours=1),
    }

    now = datetime.utcnow().replace(second=0, microsecond=0)

    for tf in timeframes:
        step = step_map[tf]

        # последний включённый open_time — это now - 2 * step
        end_time = now - 2 * step
        start_time = end_time - timedelta(hours=24)

        open_times = []
        t = start_time

        while t <= end_time:
            open_times.append(t)
            t += step

        if open_times:
            log.info(f"🧪 {tf} → {len(open_times)} open_time ({open_times[0]} — {open_times[-1]})")
        else:
            log.warning(f"⚠️ {tf} → не найдено open_time")