import asyncio
import logging
from infra import infra

log = logging.getLogger("CONFIG_LOADER")

class ConfigState:
    def __init__(self):
        self.tickers: dict[str, dict] = {}
        self.strategies: dict[int, dict] = {}

    async def reload_ticker(self, symbol: str):
        async with infra.pg_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, symbol FROM tickers_v4
                WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
            """, symbol)
            if row:
                self.tickers[row["symbol"]] = dict(row)
                log.info(f"✅ [TICKERS] {symbol} загружен")
            else:
                await self.remove_ticker(symbol)

    async def remove_ticker(self, symbol: str):
        if symbol in self.tickers:
            del self.tickers[symbol]
            log.info(f"🧹 [TICKERS] {symbol} удалён")

    async def reload_strategy(self, strategy_id: int):
        async with infra.pg_pool.acquire() as conn:
            strategy = await conn.fetchrow("""
                SELECT * FROM strategies_v4 WHERE id = $1 AND enabled = true
            """, strategy_id)
            if not strategy:
                await self.remove_strategy(strategy_id)
                return

            tp_levels = await conn.fetch("""
                SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level
            """, strategy_id)

            sl_rules = await conn.fetch("""
                SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1 ORDER BY tp_level_id
            """, strategy_id)

            self.strategies[strategy_id] = {
                "meta": dict(strategy),
                "tp_levels": [dict(r) for r in tp_levels],
                "sl_rules": [dict(r) for r in sl_rules]
            }

            log.info(f"✅ [STRATEGIES] ID={strategy_id} успешно загружена: name={strategy['name']}")

    async def remove_strategy(self, strategy_id: int):
        if strategy_id in self.strategies:
            del self.strategies[strategy_id]
            log.info(f"🧹 [STRATEGIES] ID={strategy_id} отключена и удалена из памяти")

    async def reload_all(self):
        async with infra.pg_pool.acquire() as conn:
            tickers = await conn.fetch("""
                SELECT id, symbol FROM tickers_v4
                WHERE status = 'enabled' AND tradepermission = 'enabled'
            """)
            self.tickers = {r["symbol"]: dict(r) for r in tickers}

            strategies = await conn.fetch("""
                SELECT * FROM strategies_v4 WHERE enabled = true
            """)
            self.strategies = {}

            for row in strategies:
                strategy_id = row["id"]
                tp_levels = await conn.fetch("""
                    SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level
                """, strategy_id)
                sl_rules = await conn.fetch("""
                    SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1 ORDER BY tp_level_id
                """, strategy_id)
                self.strategies[strategy_id] = {
                    "meta": dict(row),
                    "tp_levels": [dict(r) for r in tp_levels],
                    "sl_rules": [dict(r) for r in sl_rules]
                }

        log.info(f"⚙️ [CONFIG] Загружено: {len(self.strategies)} стратегий, {len(self.tickers)} тикеров")

# 🔸 Глобальный объект состояния
config = ConfigState()

# 🔸 Инициализация при старте
async def init_config_state():
    await config.reload_all()

# 🔸 Слушатель Pub/Sub Redis
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe("tickers_v4_events", "strategies_v4_events")

    log.info("📡 [CONFIG_LOADER] Подписка на tickers_v4_events и strategies_v4_events")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        try:
            data = eval(msg["data"])  # или json.loads, если строка в JSON
            channel = msg["channel"]

            if channel == "tickers_v4_events":
                symbol = data.get("symbol")
                if data.get("action") == "true":
                    await config.reload_ticker(symbol)
                elif data.get("action") == "false":
                    await config.remove_ticker(symbol)

            elif channel == "strategies_v4_events":
                sid = int(data.get("id"))
                if data.get("action") == "true":
                    await config.reload_strategy(sid)
                elif data.get("action") == "false":
                    await config.remove_strategy(sid)

        except Exception:
            log.exception("Ошибка обработки сообщения из Pub/Sub")