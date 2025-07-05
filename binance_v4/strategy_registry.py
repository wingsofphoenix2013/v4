# strategy_registry.py

import logging
import json

from infra import infra

log = logging.getLogger("STRATEGY_REGISTRY")

# 🔸 Кеш стратегий: strategy_id → leverage
binance_strategies: dict[int, int] = {}

# 🔸 Кеш точностей тикеров: symbol → precision_qty
symbol_precision_map: dict[str, int] = {}

# 🔸 Канал Pub/Sub для обновлений стратегий
PUBSUB_CHANNEL = "binance_strategy_updates"


# 🔹 Первичная загрузка при старте
async def load_binance_enabled_strategies():
    query = "SELECT id, leverage FROM strategies_v4 WHERE binance_enabled = true"
    rows = await infra.pg_pool.fetch(query)

    binance_strategies.clear()
    for row in rows:
        strategy_id = row["id"]
        leverage = row["leverage"] or 1
        binance_strategies[strategy_id] = int(leverage)

    log.info(f"📊 Загружено {len(binance_strategies)} стратегий с binance_enabled=true")


# 🔹 Проверка: разрешена ли стратегия для Binance
def is_strategy_binance_enabled(strategy_id: int) -> bool:
    return strategy_id in binance_strategies


# 🔹 Получение плеча по стратегии (по умолчанию 1)
def get_leverage(strategy_id: int) -> int:
    return binance_strategies.get(strategy_id, 1)


# 🔹 Слушатель Redis Pub/Sub для обновления стратегий в кеше
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
                row = await infra.pg_pool.fetchrow(
                    "SELECT leverage FROM strategies_v4 WHERE id = $1", strategy_id
                )
                leverage = row["leverage"] or 1
                binance_strategies[strategy_id] = int(leverage)
                log.info(f"✅ Стратегия {strategy_id} включена для Binance с плечом {leverage}")
            else:
                binance_strategies.pop(strategy_id, None)
                log.info(f"🚫 Стратегия {strategy_id} отключена от Binance")

        except Exception:
            log.exception(f"❌ Ошибка обработки сообщения из {PUBSUB_CHANNEL}")

# 🔸 Загрузка точностей тикеров из таблицы tickers_v4
async def load_symbol_precisions():
    query = "SELECT symbol, precision_qty FROM tickers_v4 WHERE is_active = true"
    rows = await infra.pg_pool.fetch(query)

    symbol_precision_map.clear()
    for row in rows:
        symbol = row["symbol"]
        precision = row["precision_qty"]
        if symbol and precision is not None:
            symbol_precision_map[symbol] = precision

    log.info(f"📊 Загружено precision для {len(symbol_precision_map)} тикеров")
    log.info(f"✅ Точности: {symbol_precision_map}")

# 🔸 Получение точности quantity по символу
def get_precision_for_symbol(symbol: str) -> int:
    return symbol_precision_map.get(symbol, 3)