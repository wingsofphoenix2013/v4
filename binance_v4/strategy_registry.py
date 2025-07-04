# strategy_registry.py

import logging
import json

from infra import infra

log = logging.getLogger("STRATEGY_REGISTRY")

# 🔸 Кеш активных стратегий, разрешённых для Binance
binance_enabled_strategies: set[int] = set()

# 🔸 Канал Pub/Sub для обновлений стратегий
PUBSUB_CHANNEL = "binance_strategy_updates"


# 🔹 Первичная загрузка при старте
async def load_binance_enabled_strategies():
    query = "SELECT id FROM strategies_v4 WHERE binance_enabled = true"
    rows = await infra.pg_pool.fetch(query)

    binance_enabled_strategies.clear()
    binance_enabled_strategies.update(row["id"] for row in rows)

    log.info(f"📊 Загружено {len(binance_enabled_strategies)} стратегий с binance_enabled=true")


# 🔹 Проверка: разрешена ли стратегия для Binance
def is_strategy_binance_enabled(strategy_id: int) -> bool:
    return strategy_id in binance_enabled_strategies


# 🔹 Фоновая задача: слушает Redis Pub/Sub и обновляет кеш
async def run_binance_strategy_watcher():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)

    log.info(f"📡 Подписка на канал Redis: {PUBSUB_CHANNEL}")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            payload = json.loads(message["data"])
            strategy_id = int(payload["strategy_id"])
            enabled = bool(payload["binance_enabled"])

            if enabled:
                binance_enabled_strategies.add(strategy_id)
                log.info(f"✅ Стратегия {strategy_id} включена для Binance")
            else:
                binance_enabled_strategies.discard(strategy_id)
                log.info(f"🚫 Стратегия {strategy_id} отключена от Binance")

        except Exception:
            log.exception(f"❌ Ошибка обработки сообщения из {PUBSUB_CHANNEL}")