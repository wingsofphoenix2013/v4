import asyncio
import logging
from infra import info_log

# Получаем логгер для модуля
logger = logging.getLogger("indicators_v4")

# 🔸 Точка входа indicators_v4
async def run_indicators_v4(pg, redis):
    """
    Основной воркер расчёта технических индикаторов.
    """
    info_log("indicators_v4", "🔸 indicators_v4 стартует")

    # Загрузка стартовых тикеров из базы
    enabled_tickers = await load_enabled_tickers(pg)
    info_log("indicators_v4", f"Загружено тикеров со статусом enabled: {len(enabled_tickers)}")

    # Загрузка активных расчётов индикаторов
    indicator_instances = await load_enabled_indicator_instances(pg)
    info_log("indicators_v4", f"Загружено активных расчётов индикаторов: {len(indicator_instances)}")

    # Загрузка параметров расчётов индикаторов
    indicator_params = await load_indicator_parameters(pg)
    info_log("indicators_v4", f"Загружено параметров индикаторов: {len(indicator_params)}")

    # TODO: Подписка на события tickers_v4_events и ohlcv_channel, цикл расчёта
    info_log("indicators_v4", "🔸 Основной цикл indicators_v4 запущен (лог-заглушка)")

    while True:
        await asyncio.sleep(60)  # Пульс воркера

# 🔸 Загрузка тикеров с status = 'enabled'
async def load_enabled_tickers(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM tickers_v4 WHERE status = 'enabled'")
        return [dict(row) for row in rows]

# 🔸 Загрузка активных расчётов индикаторов
async def load_enabled_indicator_instances(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM indicator_instances_v4 WHERE enabled = true")
        return [dict(row) for row in rows]

# 🔸 Загрузка параметров расчётов индикаторов
async def load_indicator_parameters(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM indicator_parameters_v4")
        return [dict(row) for row in rows]

# 🔸 Основная точка входа (для отдельного теста воркера)
async def main():
    # ВНИМАНИЕ: setup_logging() вызывается только один раз — в главном модуле!
    info_log("indicators_v4", "🔸 indicators_v4 main() стартует (отдельный запуск)")

    # Инициализация подключений
    from infra import init_pg_pool, init_redis_client
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # Запуск основного воркера
    await run_indicators_v4(pg, redis)

if __name__ == "__main__":
    asyncio.run(main())