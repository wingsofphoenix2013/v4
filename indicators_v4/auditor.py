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
# 🔸 Аудит записей в БД по каждому тикеру и индикатору
async def audit_storage_gaps(pg):
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

    now = datetime.utcnow()

    async with pg.acquire() as conn:
        # 🔹 Активные тикеры
        symbols = [r["symbol"] for r in await conn.fetch("""
            SELECT symbol FROM tickers_v4
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)]

        # 🔹 Активные индикаторы
        instances = await conn.fetch("""
            SELECT id, indicator, timeframe
            FROM indicator_instances_v4
            WHERE enabled = true
        """)

        # 🔹 Параметры индикаторов
        param_rows = await conn.fetch("""
            SELECT instance_id, param, value
            FROM indicator_parameters_v4
        """)
        param_map = defaultdict(dict)
        for r in param_rows:
            param_map[r["instance_id"]][r["param"]] = r["value"]

    for inst in instances:
        instance_id = inst["id"]
        indicator = inst["indicator"]
        timeframe = inst["timeframe"]
        params = param_map.get(instance_id, {})
        step = step_map[timeframe]

        # ⏱ Диапазон open_time (24 часа назад, выровненный)
        end_time = align_down(now - 2 * step, step)
        start_time = end_time - timedelta(hours=24)

        open_times = []
        t = start_time
        while t <= end_time:
            open_times.append(t)
            t += step
        expected_count = len(open_times)

        for symbol in symbols:
            async with pg.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT COUNT(DISTINCT open_time) AS actual
                    FROM indicator_values_v4
                    WHERE instance_id = $1 AND symbol = $2 AND open_time BETWEEN $3 AND $4
                """, instance_id, symbol, start_time, end_time)

            actual_count = row["actual"]
            param_str = ", ".join(f"{k}={v}" for k, v in sorted(params.items()))
            label = f"{symbol} / id={instance_id} / {indicator}({param_str}) / {timeframe}"

            if actual_count == expected_count:
                log.info(f"✅ {label} → {actual_count} / {expected_count}")
            else:
                missing = expected_count - actual_count
                log.warning(f"⚠️ {label} → {actual_count} / {expected_count} (не хватает {missing})")