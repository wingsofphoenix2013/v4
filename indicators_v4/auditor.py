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

# 🔸 Аудит контрольных open_time по активным таймфреймам (24 часа)
async def analyze_open_times(pg):
    log = logging.getLogger("GAP_CHECKER")

    step_map = {
        "m1": timedelta(minutes=1),
        "m5": timedelta(minutes=5),
        "m15": timedelta(minutes=15),
        "h1": timedelta(hours=1),
    }

    def align_down(dt: datetime, step: timedelta) -> datetime:
        seconds = int(step.total_seconds())
        timestamp = int(dt.timestamp())
        aligned = timestamp - (timestamp % seconds)
        return datetime.utcfromtimestamp(aligned)

    async with pg.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT timeframe
            FROM indicator_instances_v4
            WHERE enabled = true
        """)
        timeframes = sorted(r["timeframe"] for r in rows if r["timeframe"] in step_map)

    now = datetime.utcnow()

    for tf in timeframes:
        step = step_map[tf]
        end_time = align_down(now - 2 * step, step)         # исключаем текущую и предыдущую свечу
        start_time = end_time - timedelta(hours=24)

        open_times = []
        t = start_time
        while t <= end_time:
            open_times.append(t)
            t += step

        if open_times:
            log.info(f"🧪 {tf} → {len(open_times)} open_time ({open_times[0]} — {open_times[-1]})")
        else:
            log.warning(f"⚠️ {tf} → не найдено ни одного open_time")