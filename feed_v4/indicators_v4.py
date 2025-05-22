import asyncio
import logging
import json
from infra import setup_logging

# Получаем логгер для модуля
log = logging.getLogger("indicators_v4")

# 🔸 Подписка на события о смене статуса тикеров
async def subscribe_ticker_events(redis, active_tickers):
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")
    log.info("Подписан на канал: tickers_v4_events")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                event = json.loads(message['data'])
                symbol = event.get("symbol", "").upper()
                action_type = event.get("type")
                action = event.get("action")

                if action_type == "status":
                    if action == "enabled":
                        active_tickers.add(symbol)
                        log.info(f"Тикер включён: {symbol}")
                    elif action == "disabled":
                        active_tickers.discard(symbol)
                        log.info(f"Тикер выключен: {symbol}")
            except Exception as e:
                log.error(f"Ошибка при обработке tickers_v4_events: {e}")
                
# 🔸 Подписка на ohlcv_channel (события по новым свечам)
async def subscribe_ohlcv_channel(redis, active_tickers, indicator_pool, param_pool):
    pubsub = redis.pubsub()
    await pubsub.subscribe("ohlcv_channel")
    log.info("Подписан на канал: ohlcv_channel")

    async for message in pubsub.listen():
        if message['type'] != 'message':
            continue
        try:
            event = json.loads(message['data'])
            symbol = event.get("symbol")
            interval = event.get("interval")

            if not symbol or not interval:
                continue
            if symbol.upper() not in active_tickers:
                log.debug(f"Пропущено событие для неактивного тикера: {symbol}")
                continue

            # Найти расчёты по symbol/interval (добавить фильтр по symbol если нужно)
            relevant_indicators = [
                ind for ind in indicator_pool.values()
                if ind.get("enabled", True)
                and ind["timeframe"] == interval
                # Если в индикаторе есть привязка к symbol — добавить:
                # and ind.get("symbol", "").upper() == symbol.upper()
            ]
            if not relevant_indicators:
                log.info(f"Нет активных расчётов для {symbol} / {interval}")
                continue

            log.info(f"Получено событие по {symbol} ({interval}) — найдено {len(relevant_indicators)} расчётов, запуск обработки индикатора")
            # Здесь далее — логика расчёта по каждому найденному индикатору

        except Exception as e:
            log.error(f"Ошибка при обработке ohlcv_channel: {e}")
# 🔸 Подписка на события об изменении статуса индикаторов
async def subscribe_indicator_events(pg, redis, indicator_pool, param_pool):
    pubsub = redis.pubsub()
    await pubsub.subscribe("indicators_v4_events")
    log.info("Подписан на канал: indicators_v4_events")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                event = json.loads(message['data'])
                log.debug(f"Событие indicators_v4_events: {event}")
                indicator_id = event.get("id")
                action = event.get("action")
                field = event.get("type")

                if field == "enabled":
                    if action == "true":
                        # Загрузка расчёта и параметров из БД
                        async with pg.acquire() as conn:
                            row = await conn.fetchrow("SELECT * FROM indicator_instances_v4 WHERE id = $1", indicator_id)
                            if row:
                                indicator = dict(row)
                                param_rows = await conn.fetch(
                                    "SELECT * FROM indicator_parameters_v4 WHERE instance_id = $1", indicator_id)
                                params = [dict(p) for p in param_rows]
                                indicator_pool[indicator_id] = indicator
                                param_pool[indicator_id] = params
                                log.info(f"Добавлен индикатор: id={indicator_id}")
                            else:
                                log.error(f"Индикатор id={indicator_id} не найден в БД")
                    elif action == "false":
                        if indicator_id in indicator_pool:
                            indicator_pool.pop(indicator_id)
                            param_pool.pop(indicator_id, None)
                            log.info(f"Индикатор id={indicator_id} отключён")
                elif field == "stream_publish":
                    # Переключение флага stream_publish без подгрузки параметров
                    if indicator_id in indicator_pool:
                        indicator_pool[indicator_id]["stream_publish"] = (action == "true")
                        log.info(f"Индикатор id={indicator_id} stream_publish = {action}")

            except Exception as e:
                log.error(f"Ошибка при обработке indicators_v4_events: {e}")
# 🔸 Точка входа indicators_v4
async def run_indicators_v4(pg, redis):
    """
    Основной воркер расчёта технических индикаторов.
    """
    log.info("🔸 indicators_v4 стартует")

    # Загрузка стартовых тикеров из базы
    enabled_tickers = await load_enabled_tickers(pg)
    log.info(f"Загружено тикеров со статусом enabled: {len(enabled_tickers)}")

    # Загрузка активных расчётов индикаторов
    indicator_instances = await load_enabled_indicator_instances(pg)
    log.info(f"Загружено активных расчётов индикаторов: {len(indicator_instances)}")

    # Загрузка параметров расчётов индикаторов
    indicator_params = await load_indicator_parameters(pg)
    log.info(f"Загружено параметров индикаторов: {len(indicator_params)}")

    # Формируем in-memory пулы для динамического управления
    active_tickers = set([t["symbol"].upper() for t in enabled_tickers])
    indicator_pool = {str(ind["id"]): ind for ind in indicator_instances}
    param_pool = {str(ind["id"]): [p for p in indicator_params if str(p["instance_id"]) == str(ind["id"])] for ind in indicator_instances}

    # Запуск задачи подписки на события о тикерах
    asyncio.create_task(subscribe_ticker_events(redis, active_tickers))

    # Запуск задачи подписки на ohlcv_channel
    asyncio.create_task(subscribe_ohlcv_channel(redis, active_tickers, indicator_pool, param_pool))

    # Запуск подписки на события о статусе индикаторов
    asyncio.create_task(subscribe_indicator_events(pg, redis, indicator_pool, param_pool))

    log.info("🔸 Основной цикл indicators_v4 запущен")

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
    log.info("🔸 indicators_v4 main() стартует (отдельный запуск)")

    # Инициализация подключений
    from infra import init_pg_pool, init_redis_client
    pg = await init_pg_pool()
    redis = await init_redis_client()

    # Запуск основного воркера
    await run_indicators_v4(pg, redis)

if __name__ == "__main__":
    asyncio.run(main())