# config_loader.py

import json
import asyncio
import logging

import infra
from infra import set_enabled_tickers

# 🔸 Логгер
log = logging.getLogger("CONFIG_LOADER")


# 🔸 Загрузка активных тикеров
async def load_enabled_tickers():
    query = """
        SELECT symbol, precision_price, precision_qty, created_at
        FROM tickers_v4
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        tickers = {r["symbol"]: dict(r) for r in rows}
        set_enabled_tickers(tickers)
        log.info(f"✅ Загружено тикеров: {len(tickers)}")


# 🔸 Слушатель событий Pub/Sub
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe("tickers_v4_events")
    log.info("📡 Подписка на канал tickers_v4_events завершена")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            channel = message["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode()

            event_type = data.get("type")
            action = data.get("action")
            desc = f"{event_type} → {action}"

            log.info(f"🔔 Событие: {desc} в {channel}")

            if channel == "tickers_v4_events":
                await load_enabled_tickers()

        except Exception as e:
            log.exception(f"❌ Ошибка при обработке события: {e}")