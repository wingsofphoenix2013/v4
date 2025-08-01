# auditor.py — аудит системы: структура, конфигурация и пропуски

import logging

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
    log.info(f"📦 Активных тикеров: {ticker_count}")
    log.info(f"🧩 Активных индикаторов: {indicator_count}")
    log.info(f"⏱ Таймфреймы среди индикаторов: {', '.join(timeframes) if timeframes else '—'}")