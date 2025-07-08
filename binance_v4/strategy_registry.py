# strategy_registry.py

import logging
import json
import aiohttp

from infra import infra

log = logging.getLogger("STRATEGY_REGISTRY")

# 🔸 Кеш стратегий: strategy_id → {leverage, sl_policy, tp_levels}
binance_strategies: dict[int, dict] = {}

# 🔸 Кеш точностей тикеров: symbol → precision_qty
symbol_precision_map: dict[str, int] = {}
symbol_price_precision_map: dict[str, int] = {}
symbol_tick_size_map: dict[str, float] = {}
symbol_min_qty_map: dict[str, float] = {}

# 🔸 Канал Pub/Sub для обновлений стратегий
PUBSUB_CHANNEL = "binance_strategy_updates"

# 🔸 Загрузка всех разрешённых стратегий Binance при старте
async def load_binance_enabled_strategies():
    # Шаг 1: базовые параметры стратегий
    query_base = """
        SELECT s.id AS strategy_id, s.leverage, s.use_stoploss, s.sl_type, s.sl_value
        FROM strategies_v4 s
        WHERE s.binance_enabled = true
    """
    base_rows = await infra.pg_pool.fetch(query_base)

    binance_strategies.clear()

    for row in base_rows:
        sid = row["strategy_id"]
        binance_strategies[sid] = {
            "leverage": int(row["leverage"] or 1),
            "use_stoploss": row["use_stoploss"],
            "sl_type": row["sl_type"],
            "sl_value": row["sl_value"],
            "sl_policy": {},
            "tp_levels": {}
        }

    # Шаг 2: TP/SL уровни
    query_tp_sl = """
        SELECT s.id AS strategy_id,
               tp.level AS tp_level, tp.tp_type, tp.tp_value, tp.volume_percent,
               sl.sl_mode, sl.sl_value
        FROM strategies_v4 s
        LEFT JOIN strategy_tp_levels_v4 tp ON tp.strategy_id = s.id
        LEFT JOIN strategy_tp_sl_v4 sl ON sl.strategy_id = s.id AND sl.tp_level_id = tp.id
        WHERE s.binance_enabled = true
    """
    rows = await infra.pg_pool.fetch(query_tp_sl)

    for row in rows:
        sid = row["strategy_id"]
        level = row["tp_level"]

        if sid not in binance_strategies:
            continue

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

    for sid, cfg in binance_strategies.items():
        log.info(f"🔸 Стратегия {sid}: leverage={cfg['leverage']}, SL={cfg['sl_type']} {cfg['sl_value']}%")
        for level, tp in sorted(cfg["tp_levels"].items()):
            log.info(f"   • TP{level}: type={tp['tp_type']} value={tp['tp_value']} → {tp['volume_percent']}%")
        if not cfg["tp_levels"]:
            log.info("   • TP уровни: отсутствуют")


# 🔸 Динамическая подгрузка полной конфигурации одной стратегии
async def load_single_strategy(strategy_id: int):
    query_base = """
        SELECT s.id AS strategy_id, s.leverage, s.use_stoploss, s.sl_type, s.sl_value
        FROM strategies_v4 s
        WHERE s.id = $1
    """
    base = await infra.pg_pool.fetchrow(query_base, strategy_id)

    if not base:
        return

    binance_strategies[strategy_id] = {
        "leverage": int(base["leverage"] or 1),
        "use_stoploss": base["use_stoploss"],
        "sl_type": base["sl_type"],
        "sl_value": base["sl_value"],
        "sl_policy": {},
        "tp_levels": {}
    }

    query_tp_sl = """
        SELECT tp.level AS tp_level, tp.tp_type, tp.tp_value, tp.volume_percent,
               sl.sl_mode, sl.sl_value
        FROM strategy_tp_levels_v4 tp
        LEFT JOIN strategy_tp_sl_v4 sl ON sl.strategy_id = $1 AND sl.tp_level_id = tp.id
        WHERE tp.strategy_id = $1
    """
    rows = await infra.pg_pool.fetch(query_tp_sl, strategy_id)

    for row in rows:
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

# 🔸 Загрузка точностей тикеров из таблицы tickers_v4 + tickSize из Binance
async def load_symbol_precisions():
    query = "SELECT symbol, precision_qty, precision_price, min_qty FROM tickers_v4"
    rows = await infra.pg_pool.fetch(query)

    symbol_precision_map.clear()
    symbol_price_precision_map.clear()
    symbol_tick_size_map.clear()
    symbol_min_qty_map.clear()

    for row in rows:
        symbol = row["symbol"]
        qty_precision = row["precision_qty"]
        price_precision = row["precision_price"]
        min_qty = row["min_qty"]

        if symbol:
            if qty_precision is not None:
                symbol_precision_map[symbol] = qty_precision
            if price_precision is not None:
                symbol_price_precision_map[symbol] = price_precision
            if min_qty is not None:
                symbol_min_qty_map[symbol] = float(min_qty)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                data = await resp.json()
                all_symbols = {s["symbol"]: s for s in data.get("symbols", [])}

                for symbol in sorted(symbol_precision_map):
                    entry = all_symbols.get(symbol)
                    if not entry:
                        log.warning(f"❓ {symbol} отсутствует в Binance exchangeInfo")
                        continue

                    bin_qty = entry.get("quantityPrecision")
                    bin_price = entry.get("pricePrecision")

                    db_qty = symbol_precision_map.get(symbol, "-")
                    db_price = symbol_price_precision_map.get(symbol, "-")

                    # 🔸 tickSize
                    price_filter = next(
                        (f for f in entry.get("filters", []) if f["filterType"] == "PRICE_FILTER"),
                        None
                    )
                    tick_size = float(price_filter["tickSize"]) if price_filter else None
                    if tick_size:
                        symbol_tick_size_map[symbol] = tick_size

                    match_qty = "✅" if bin_qty == db_qty else "❗"
                    match_price = "✅" if bin_price == db_price else "❗"

                    # 🔸 tickSize vs min_qty
                    db_tick = symbol_min_qty_map.get(symbol)
                    if db_tick is not None and abs(tick_size - db_tick) > 1e-10:
                        match_tick = "❗"
                        log.info(f"  • {symbol:<10} | DB: tick={db_tick} | Binance: tick={tick_size} {match_tick}")
                    else:
                        match_tick = "✅"

                    log.info(
                        f"  • {symbol:<10} | DB: qty={db_qty}, price={db_price} | "
                        f"Binance: qty={bin_qty}, price={bin_price} | tick={tick_size} {match_qty}{match_price}{match_tick}"
                    )

    except Exception as e:
        log.warning(f"⚠️ Ошибка при получении данных от Binance: {e}")

    log.info(f"📊 Загружено quantity precision для {len(symbol_precision_map)} тикеров")
    log.info(f"📊 Загружено price precision для {len(symbol_price_precision_map)} тикеров")
    log.info(f"📊 Загружено tickSize для {len(symbol_tick_size_map)} тикеров")
    
# 🔸 Получение точности quantity по символу
def get_precision_for_symbol(symbol: str) -> int:
    return symbol_precision_map.get(symbol, 3)


# 🔸 Получение точности price по символу
def get_price_precision_for_symbol(symbol: str) -> int:
    return symbol_price_precision_map.get(symbol, 2)