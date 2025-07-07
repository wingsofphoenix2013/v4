# strategy_registry.py

import logging
import json

from infra import infra

log = logging.getLogger("STRATEGY_REGISTRY")

# 🔸 Кеш стратегий: strategy_id → {leverage, sl_policy, tp_levels}
binance_strategies: dict[int, dict] = {}

# 🔸 Кеш точностей тикеров: symbol → precision_qty
symbol_precision_map: dict[str, int] = {}
symbol_price_precision_map: dict[str, int] = {}

# 🔸 Канал Pub/Sub для обновлений стратегий
PUBSUB_CHANNEL = "binance_strategy_updates"


# 🔸 Загрузка всех разрешённых стратегий Binance при старте
async def load_binance_enabled_strategies():
    query = """
        SELECT s.id AS strategy_id, s.leverage,
               tp.level AS tp_level, tp.tp_type, tp.tp_value, tp.volume_percent,
               sl.sl_mode, sl.sl_value
        FROM strategies_v4 s
        LEFT JOIN strategy_tp_levels_v4 tp ON tp.strategy_id = s.id
        LEFT JOIN strategy_tp_sl_v4 sl ON sl.strategy_id = s.id AND sl.tp_level_id = tp.id
        WHERE s.binance_enabled = true
    """
    rows = await infra.pg_pool.fetch(query)

    binance_strategies.clear()

    for row in rows:
        sid = row["strategy_id"]
        level = row["tp_level"]

        if sid not in binance_strategies:
            binance_strategies[sid] = {
                "leverage": int(row["leverage"] or 1),
                "sl_policy": {},
                "tp_levels": {}
            }

        if level is not None:
            if row["sl_mode"] is not None:
                binance_strategies[sid]["sl_policy"][level] = {
                    "sl_mode": row["sl_mode"],
                    "sl_value": row["sl_value"]
                }

            binance_strategies[sid]["tp_levels"][level] = {
                "tp_type": row["tp_type"],
                "tp_value": row["tp_value"],
                "volume_percent": row["volume_percent"]
            }

    log.info(f"📊 Загружено {len(binance_strategies)} стратегий с binance_enabled=true")


# 🔸 Динамическая подгрузка полной конфигурации одной стратегии
async def load_single_strategy(strategy_id: int):
    query = """
        SELECT s.id AS strategy_id, s.leverage,
               tp.level AS tp_level, tp.tp_type, tp.tp_value, tp.volume_percent,
               sl.sl_mode, sl.sl_value
        FROM strategies_v4 s
        LEFT JOIN strategy_tp_levels_v4 tp ON tp.strategy_id = s.id
        LEFT JOIN strategy_tp_sl_v4 sl ON sl.strategy_id = s.id AND sl.tp_level_id = tp.id
        WHERE s.id = $1
    """
    rows = await infra.pg_pool.fetch(query, strategy_id)

    if not rows:
        return

    binance_strategies[strategy_id] = {
        "leverage": 1,
        "sl_policy": {},
        "tp_levels": {}
    }

    for row in rows:
        binance_strategies[strategy_id]["leverage"] = int(row["leverage"] or 1)
        level = row["tp_level"]

        if level is not None:
            if row["sl_mode"] is not None:
                binance_strategies[strategy_id]["sl_policy"][level] = {
                    "sl_mode": row["sl_mode"],
                    "sl_value": row["sl_value"]
                }

            binance_strategies[strategy_id]["tp_levels"][level] = {
                "tp_type": row["tp_type"],
                "tp_value": row["tp_value"],
                "volume_percent": row["volume_percent"]
            }

    log.info(f"🔁 Стратегия {strategy_id} подгружена динамически")


# 🔸 Проверка: разрешена ли стратегия для Binance
def is_strategy_binance_enabled(strategy_id: int) -> bool:
    return strategy_id in binance_strategies


# 🔸 Получение плеча по стратегии (по умолчанию 1)
def get_leverage(strategy_id: int) -> int:
    return binance_strategies.get(strategy_id, {}).get("leverage", 1)


# 🔸 Получение SL-политики для TP-уровня
def get_sl_policy(strategy_id: int, tp_level: int) -> dict | None:
    return binance_strategies.get(strategy_id, {}).get("sl_policy", {}).get(tp_level)


# 🔸 Получение полной конфигурации стратегии
def get_strategy_config(strategy_id: int) -> dict | None:
    return binance_strategies.get(strategy_id)


# 🔸 Слушатель Redis Pub/Sub для обновлений стратегий в кеше
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
                await load_single_strategy(strategy_id)
            else:
                binance_strategies.pop(strategy_id, None)
                log.info(f"🚫 Стратегия {strategy_id} отключена от Binance")

        except Exception:
            log.exception(f"❌ Ошибка обработки сообщения из {PUBSUB_CHANNEL}")


# 🔸 Загрузка точностей тикеров из таблицы tickers_v4
async def load_symbol_precisions():
    query = "SELECT symbol, precision_qty, precision_price FROM tickers_v4"
    rows = await infra.pg_pool.fetch(query)

    symbol_precision_map.clear()
    symbol_price_precision_map.clear()

    for row in rows:
        symbol = row["symbol"]
        qty_precision = row["precision_qty"]
        price_precision = row["precision_price"]

        if symbol:
            if qty_precision is not None:
                symbol_precision_map[symbol] = qty_precision
            if price_precision is not None:
                symbol_price_precision_map[symbol] = price_precision

    log.info(f"📊 Загружено quantity precision для {len(symbol_precision_map)} тикеров")
    log.info(f"📊 Загружено price precision для {len(symbol_price_precision_map)} тикеров")


# 🔸 Получение точности quantity по символу
def get_precision_for_symbol(symbol: str) -> int:
    return symbol_precision_map.get(symbol, 3)


# 🔸 Получение точности price по символу
def get_price_precision_for_symbol(symbol: str) -> int:
    return symbol_price_precision_map.get(symbol, 2)