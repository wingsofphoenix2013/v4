# config_loader.py

import json
import asyncio
import logging

import infra
from infra import (
    set_enabled_tickers,
    set_enabled_strategies,
    set_enabled_indicators,
)

# 🔸 Логгер
log = logging.getLogger("CONFIG_LOADER")


# 🔸 Загрузка тикеров с разрешённым статусом и торговлей
async def load_enabled_tickers():
    query = """
        SELECT symbol, precision_price, precision_qty
        FROM tickers_v4
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        tickers = {r["symbol"]: dict(r) for r in rows}
        set_enabled_tickers(tickers)
        log.info(f"✅ Загружено тикеров: {len(tickers)}")


# 🔸 Загрузка всех стратегий
async def load_enabled_strategies():
    query = "SELECT * FROM strategies_v4"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        strategies = {r["id"]: dict(r) for r in rows}
        set_enabled_strategies(strategies)
        log.info(f"✅ Загружено стратегий: {len(strategies)}")


# 🔸 Загрузка активных индикаторов
async def load_enabled_indicators():
    query = """
        SELECT 
            id,
            indicator,
            timeframe,
            enabled,
            stream_publish,
            created_at
        FROM indicator_instances_v4
        WHERE enabled = true
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        indicators = {}

        for r in rows:
            key = str(r["id"])  # ключ = только ID
            indicators[key] = dict(r)

        infra.set_enabled_indicators(indicators)
        log.info(f"✅ Загружено индикаторов: {len(indicators)}")

        log.info("📄 Список загруженных индикаторов:")
        for key, item in indicators.items():
            indicator = item["indicator"]
            tf = item["timeframe"]
            log.info(f"• {key} → {indicator} ({tf})")
# 🔸 Слушатель PubSub событий
async def config_event_listener():
    log = logging.getLogger("CONFIG_LOADER")

    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe(
        "tickers_v4_events",
        "strategies_v4_events",
        "indicators_v4_events"
    )
    log.info("📡 Подписка на Redis каналы завершена")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            channel = message["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode()

            # 🔸 Формируем осмысленное описание события
            event_type = data.get("type")
            action = data.get("action")
            desc = f"{event_type} → {action}"

            log.info(f"🔔 Событие: {desc} в {channel}")

            # 🔸 Обновляем соответствующий кэш
            if channel == "tickers_v4_events":
                await load_enabled_tickers()
            elif channel == "strategies_v4_events":
                await load_enabled_strategies()
            elif channel == "indicators_v4_events":
                await load_enabled_indicators()

        except Exception as e:
            log.exception(f"❌ Ошибка при обработке сообщения PubSub: {e}")