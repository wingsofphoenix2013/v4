# indicators_v4 - модуль индикаторов
import asyncio
import logging
import json
from datetime import datetime
from infra import setup_logging

# 🔸 Блок импортов файлов конкретных индикаторов
from ema import ema_pandas

# Получаем логгер для модуля
log = logging.getLogger("indicators_v4")

# 🔸 Загрузка тикеров с status = 'enabled'
async def load_enabled_tickers(pg):
    async with pg.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM tickers_v4 WHERE status = 'enabled'")
        tickers = [dict(row) for row in rows]
        # Формируем словарь precision_price по тикеру (lower-case)
        ticker_precisions = {t["symbol"].lower(): t["precision_price"] for t in tickers}
        return tickers, ticker_precisions

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

# 🔸 Генерация ключа param_name для индикатора (например, 'ema21', 'rsi14', ...)
def get_param_name(indicator_instance, param_list):
    """
    Формирует строку param_name по типу индикатора и его параметрам.
    Например, 'ema21' для EMA с length=21.
    """
    indicator = indicator_instance["indicator"]
    params = {p["param"]: p["value"] for p in param_list}
    if "length" in params:
        return f"{indicator}{params['length']}"
    return indicator
# 🔸 Подписка на события о смене статуса тикеров
async def subscribe_ticker_events(redis, active_tickers):
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events")
    log.info("Подписан на канал: tickers_v4_events")

    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                event = json.loads(message['data'])
                symbol = event.get("symbol", "").lower()
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
# 🔸 Получения массива из 250 свечей
async def get_last_candles(redis, symbol, interval, n=250):
    """
    Возвращает массив последних n свечей (dict) по ключам ohlcv:{symbol}:{interval}:<timestamp>
    ВНИМАНИЕ: symbol и interval должны быть строго в нижнем регистре!
    """
    pattern = f"ohlcv:{symbol}:{interval}:*"
    log.info(f"DEBUG: вызов get_last_candles c symbol={symbol}, interval={interval}, pattern={pattern}")
    keys = await redis.keys(pattern)
    log.info(f"DEBUG: найдено ключей: {len(keys)} для pattern={pattern}")
    if not keys:
        log.info(f"Нет свечей для {symbol}/{interval} в Redis (ключи {pattern})")
        return []
    # Извлекаем timestamp из ключей, сортируем по времени (от новых к старым)
    keys_sorted = sorted(
        keys,
        key=lambda x: int((x.decode() if isinstance(x, bytes) else x).split(":")[-1]),
        reverse=True
    )
    # Берём только последние n ключей (по времени — от новых к старым)
    keys_needed = keys_sorted[:n]
    # mget — получить значения всех свечей сразу
    raw = await redis.mget(*keys_needed)
    log.debug(f"DEBUG: mget вернул {len(raw)} значений, первые 5: {raw[:5]}")
    candles = []
    for k in keys_needed:
        candle = await redis.json().get(k)
        if candle:
            candles.append(candle)
    log.debug(f"DEBUG: candles после redis.json().get: {len(candles)}, первые 2: {candles[:2]}")
    # Теперь сортируем свечи уже от старых к новым для расчёта индикаторов
    candles = sorted(candles, key=lambda c: c.get("ts", 0))
    if len(candles) < n:
        log.info(f"Недостаточно свечей для {symbol}/{interval}: есть {len(candles)}, требуется {n}. Расчёт не производится.")
        return []
    return candles
# 🔸 Подписка на ohlcv_channel (события по новым свечам)
async def subscribe_ohlcv_channel(redis, active_tickers, indicator_pool, param_pool, ticker_precisions):
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
            log.info(f"EVENT_RECEIVED {symbol}/{interval} at {datetime.now().isoformat()}")
            
            if not symbol or not interval:
                continue
            if symbol.lower() not in active_tickers:
                log.debug(f"Пропущено событие для неактивного тикера: {symbol}")
                continue

            # 🔸 Фильтруем расчёты по symbol/interval
            relevant_indicators = [
                ind for ind in indicator_pool.values()
                if ind.get("enabled", True)
                and ind["timeframe"] == interval
            ]
            if not relevant_indicators:
                log.debug(f"Нет активных расчётов для {symbol} / {interval}")
                continue

            for ind in relevant_indicators:
                param_name = ind["param_name"]

                # 🔸 Получаем параметры индикатора (например, period для EMA)
                params = param_pool.get(str(ind["id"]), [])
                params_dict = {p["param"]: p["value"] for p in params}
                if "length" not in params_dict:
                    log.error(
                        f"Нет параметра 'length' для индикатора id={ind['id']} "
                        f"(param_name={param_name}). params_dict={params_dict}, params={params}"
                    )
                    continue  # пропустить расчёт, чтобы не было ошибки
                period = int(params_dict["length"])

                # 🔸 Получаем массив свечей для symbol/interval (универсальный подход)
                candles = await get_last_candles(redis, symbol.lower(), interval, 250)
                if not candles:
                    log.info(f"Расчёт {param_name} для {symbol}/{interval}: отказ, недостаточно свечей")
                    continue

                # 🔸 Для EMA — берём только close-цены
                close_prices = [float(c["c"]) for c in candles if "c" in c]
                if len(close_prices) < period:
                    log.info(f"Расчёт {param_name} для {symbol}/{interval}: отказ, есть {len(close_prices)} цен, требуется минимум {period}")
                    continue

                log.info(f"BEFORE_EMA_CALC {symbol}/{interval} at {datetime.now().isoformat()}")
                ema_value = ema_pandas(close_prices, period)
                log.info(f"AFTER_EMA_CALC {symbol}/{interval} at {datetime.now().isoformat()}")
                precision = ticker_precisions.get(symbol.lower(), 6)
                if ema_value is not None:
                    ema_value_rounded = round(ema_value, precision)
                    log.info(f"{param_name.upper()} ({symbol.upper()}/{interval}): {ema_value_rounded}")
                else:
                    log.info(f"{param_name.upper()} ({symbol.upper()}/{interval}): недостаточно данных для расчёта EMA")

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

                # Получаем id как int для запросов в БД
                indicator_id_int = int(event.get("id"))
                indicator_id_str = str(indicator_id_int)
                action = event.get("action")
                field = event.get("type")

                if field == "enabled":
                    if action == "true":
                        # Загрузка расчёта и параметров из БД (используем int)
                        async with pg.acquire() as conn:
                            row = await conn.fetchrow("SELECT * FROM indicator_instances_v4 WHERE id = $1", indicator_id_int)
                            if row:
                                indicator = dict(row)
                                param_rows = await conn.fetch(
                                    "SELECT * FROM indicator_parameters_v4 WHERE instance_id = $1", indicator_id_int)
                                params = [dict(p) for p in param_rows]
                                indicator["param_name"] = get_param_name(indicator, params)
                                indicator_pool[indicator_id_str] = indicator
                                param_pool[indicator_id_str] = params
                                log.info(f"Добавлен индикатор: id={indicator_id_str}")
                            else:
                                log.error(f"Индикатор id={indicator_id_str} не найден в БД")
                    elif action == "false":
                        if indicator_id_str in indicator_pool:
                            indicator_pool.pop(indicator_id_str)
                            param_pool.pop(indicator_id_str, None)
                            log.info(f"Индикатор id={indicator_id_str} отключён")
                elif field == "stream_publish":
                    if indicator_id_str in indicator_pool:
                        indicator_pool[indicator_id_str]["stream_publish"] = (action == "true")
                        log.info(f"Индикатор id={indicator_id_str} stream_publish = {action}")

            except Exception as e:
                log.error(f"Ошибка при обработке indicators_v4_events: {e}")
# 🔸 Точка входа indicators_v4
async def run_indicators_v4(pg, redis):
    """
    Основной воркер расчёта технических индикаторов.
    """
    log.info("🔸 indicators_v4 стартует")

    # Загрузка стартовых тикеров из базы
    enabled_tickers, ticker_precisions = await load_enabled_tickers(pg)
    log.info(f"Загружено тикеров со статусом enabled: {len(enabled_tickers)}")

    # Загрузка активных расчётов индикаторов
    indicator_instances = await load_enabled_indicator_instances(pg)
    log.info(f"Загружено активных расчётов индикаторов: {len(indicator_instances)}")

    # Загрузка параметров расчётов индикаторов
    indicator_params = await load_indicator_parameters(pg)
    log.info(f"Загружено параметров индикаторов: {len(indicator_params)}")

    # Формируем param_name для каждого расчёта (обязательно до формирования пулов)
    for ind in indicator_instances:
        iid = str(ind["id"])
        param_list = [p for p in indicator_params if str(p["instance_id"]) == iid]
        ind["param_name"] = get_param_name(ind, param_list)

    # Формируем in-memory пулы для динамического управления
    active_tickers = set([t["symbol"].lower() for t in enabled_tickers])
    indicator_pool = {str(ind["id"]): ind for ind in indicator_instances}
    param_pool = {str(ind["id"]): [p for p in indicator_params if str(p["instance_id"]) == str(ind["id"])] for ind in indicator_instances}

    # Запуск задачи подписки на события о тикерах
    asyncio.create_task(subscribe_ticker_events(redis, active_tickers))

    # Запуск задачи подписки на ohlcv_channel
    asyncio.create_task(subscribe_ohlcv_channel(redis, active_tickers, indicator_pool, param_pool, ticker_precisions))

    # Запуск подписки на события о статусе индикаторов
    asyncio.create_task(subscribe_indicator_events(pg, redis, indicator_pool, param_pool))

    log.info("🔸 Основной цикл indicators_v4 запущен")

    while True:
        await asyncio.Event().wait()  # Пульс воркера
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