import asyncio
import logging
import json
from infra import info_log

# Получаем логгер для модуля
logger = logging.getLogger("indicators_v4")

# 🔸 Подписка на события о смене статуса тикеров
async def subscribe_ticker_events(redis):
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")
    logger = logging.getLogger("indicators_v4")
    logger.info("Подписан на канал: tickers_v4_events")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                event = json.loads(message['data'])
                logger.info(f"Событие tickers_v4_events: {event}")
                # Здесь далее — обработка статуса тикера (включение/выключение)
            except Exception as e:
                logger.error(f"Ошибка при обработке tickers_v4_events: {e}")

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

    # Запуск задачи подписки на события о тикерах
    asyncio.create_task(subscribe_ticker_events(redis))
    
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
    info_log("indicators_v4", "🔸 indicators_v4 main() стартует (отдельный запуск)")

    # Инициализация подключений
    from infra import init_pg_pool, init_redis_client
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # Запуск основного воркера
    await run_indicators_v4(pg, redis)

if __name__ == "__main__":
    asyncio.run(main())