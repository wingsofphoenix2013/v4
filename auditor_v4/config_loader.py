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
        SELECT *
        FROM indicator_instances_v4
        WHERE enabled = true
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        indicators = {r["id"]: dict(r) for r in rows}
        set_enabled_indicators(indicators)
        log.info(f"✅ Загружено индикаторов: {len(indicators)}")


# 🔸 Слушатель PubSub событий
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe("tickers_v4_events", "strategies_v4_events", "indicator_v4_events")
    log.info("📡 Подписка на конфигурационные каналы Redis начата")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
        except Exception:
            log.warning("⚠️ Невалидное сообщение в PubSub")
            continue

        event = data.get("event")
        if not event:
            continue

        channel = message["channel"]
        log.info(f"🔔 Событие: {event} в {channel}")

        try:
            if channel == "tickers_v4_events":
                await load_enabled_tickers()
            elif channel == "strategies_v4_events":
                await load_enabled_strategies()
            elif channel == "indicator_v4_events":
                await load_enabled_indicators()
        except Exception:
            log.exception(f"❌ Ошибка при обновлении конфигурации из {channel}")