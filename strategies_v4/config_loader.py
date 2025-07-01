import asyncio
import logging
import json

from typing import Dict, Set
from infra import infra

# 🔸 Логгер
log = logging.getLogger("CONFIG")

# 🔹 Приведение логических флагов стратегии к типу bool
def normalize_strategy_flags(strategy: dict) -> None:
    for key in (
        "enabled",
        "use_all_tickers",
        "use_stoploss",
        "allow_open",
        "reverse",
        "sl_protection"
    ):
        if key in strategy:
            strategy[key] = str(strategy[key]).lower() == "true"
            
# 🔸 Глобальное состояние конфигурации
class ConfigState:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}
        self.strategies: Dict[int, dict] = {}
        self.strategy_tickers: Dict[int, Set[str]] = {}
        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка всего состояния
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()
            await self._load_strategy_tickers()

    # 🔸 Обновление одного тикера
    async def reload_ticker(self, symbol: str):
        async with self._lock:
            row = await infra.pg_pool.fetchrow(
                "SELECT * FROM tickers_v4 WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'",
                symbol
            )
            if row:
                self.tickers[symbol] = dict(row)
                log.debug(f"🔄 Тикер обновлён: {symbol}")
            else:
                self.tickers.pop(symbol, None)
                log.debug(f"❌ Тикер удалён (не найден): {symbol}")

    # 🔸 Удаление тикера
    async def remove_ticker(self, symbol: str):
        async with self._lock:
            self.tickers.pop(symbol, None)
            log.debug(f"🗑️ Тикер удалён: {symbol}")

    # 🔸 Обновление стратегии
    async def reload_strategy(self, strategy_id: int):
        async with self._lock:
            row = await infra.pg_pool.fetchrow(
                "SELECT * FROM strategies_v4 WHERE id = $1 AND enabled = true AND archived = false",
                strategy_id
            )
            if not row:
                self.strategies.pop(strategy_id, None)
                self.strategy_tickers.pop(strategy_id, None)
                log.debug(f"🗑️ Стратегия удалена: id={strategy_id}")
                return

            strategy = dict(row)
            strategy["module_name"] = strategy["name"]

            normalize_strategy_flags(strategy)

            log.debug(
                f"[DEBUG-NORM] Strategy {strategy_id} → "
                f"enabled={strategy['enabled']} "
                f"reverse={strategy['reverse']} "
                f"sl_protection={strategy['sl_protection']} "
                f"allow_open={strategy['allow_open']}"
            )

            strategy["tp_levels"] = [
                dict(r) for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level",
                    strategy_id
                )
            ]
            strategy["sl_rules"] = [
                dict(r) for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1",
                    strategy_id
                )
            ]

            # 🔹 Обогащение sl_rules полем level
            level_map = {lvl["id"]: lvl["level"] for lvl in strategy["tp_levels"]}
            for rule in strategy["sl_rules"]:
                rule["level"] = level_map.get(rule["tp_level_id"])

            try:
                log.info(
                    f"♻️ Стратегия обновлена: id={strategy_id} | "
                    f"name={strategy['name']} | "
                    f"deposit={strategy['deposit']} | "
                    f"risk={strategy['max_risk']}% | "
                    f"leverage={strategy['leverage']} | "
                    f"timeframe={strategy['timeframe']} | "
                    f"SL={strategy['sl_type']}:{strategy['sl_value']} | "
                    f"SL_protect={strategy['sl_protection']}"
                )
            except Exception as e:
                log.exception(f"❌ Ошибка логирования стратегии {strategy_id}: {e}")
                log.debug(f"[DEBUG-FIELDS] strategy keys: {list(strategy.keys())}")

            self.strategies[strategy_id] = strategy

            tickers = await infra.pg_pool.fetch(
                '''
                SELECT t.symbol
                FROM strategy_tickers_v4 st
                JOIN tickers_v4 t ON st.ticker_id = t.id
                WHERE st.strategy_id = $1 AND st.enabled = true AND t.status = 'enabled' AND t.tradepermission = 'enabled'
                ''',
                strategy_id
            )
            self.strategy_tickers[strategy_id] = {r["symbol"] for r in tickers}

            log.debug(f"🔄 Стратегия обновлена: [id={strategy_id}] {strategy['human_name']}")

            # 🔸 Дополнительный лог с деталями загрузки
            log.debug(
                f"🧠 Загружена стратегия {strategy_id} | "
                f"TP={[{'level': r['level'], 'value': r['tp_value'], 'type': r['tp_type'], 'volume': r['volume_percent']} for r in strategy['tp_levels']]}, "
                f"SL={[{'tp_level_id': r['tp_level_id'], 'level': r['level'], 'mode': r['sl_mode']} for r in strategy['sl_rules']]}"
            )
            
    # 🔸 Удаление стратегии
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            log.debug(f"🗑️ Стратегия удалена: id={strategy_id}")

    # 🔸 Загрузка всех тикеров (только активных с разрешением)
    async def _load_tickers(self):
        rows = await infra.pg_pool.fetch(
            "SELECT * FROM tickers_v4 WHERE status = 'enabled' AND tradepermission = 'enabled'"
        )
        self.tickers = {r["symbol"]: dict(r) for r in rows}

    # 🔸 Загрузка всех стратегий (без архивных и отключённых)
    async def _load_strategies(self):
        rows = await infra.pg_pool.fetch(
            "SELECT * FROM strategies_v4 WHERE enabled = true AND archived = false"
        )

        self.strategies = {}
        for row in rows:
            strategy_id = row["id"]
            strategy = dict(row)
            strategy["module_name"] = strategy["name"]
            
            normalize_strategy_flags(strategy)
            
            log.debug(
                f"[DEBUG-NORM] Strategy {strategy_id} → "
                f"enabled={strategy['enabled']} "
                f"reverse={strategy['reverse']} "
                f"sl_protection={strategy['sl_protection']} "
                f"allow_open={strategy['allow_open']}"
            )

            strategy["tp_levels"] = [
                dict(r) for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level",
                    strategy_id
                )
            ]
            strategy["sl_rules"] = [
                dict(r) for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1",
                    strategy_id
                )
            ]

            # 🔹 Обогащение sl_rules полем level
            level_map = {lvl["id"]: lvl["level"] for lvl in strategy["tp_levels"]}
            for rule in strategy["sl_rules"]:
                rule["level"] = level_map.get(rule["tp_level_id"])

            self.strategies[strategy_id] = strategy

            # 🔸 Лог: детали загруженной стратегии
            log.debug(
                f"🧠 Загружена стратегия {strategy_id} | "
                f"TP={[{'level': r['level'], 'value': r['tp_value'], 'type': r['tp_type'], 'volume': r['volume_percent']} for r in strategy['tp_levels']]}, "
                f"SL={[{'tp_level_id': r['tp_level_id'], 'level': r['level'], 'mode': r['sl_mode']} for r in strategy['sl_rules']]}"
            )
            
    # 🔸 Загрузка связей стратегия ↔ тикеры
    async def _load_strategy_tickers(self):
        rows = await infra.pg_pool.fetch(
            '''
            SELECT s.strategy_id, t.symbol
            FROM strategy_tickers_v4 s
            JOIN tickers_v4 t ON s.ticker_id = t.id
            WHERE s.enabled = true AND t.status = 'enabled' AND t.tradepermission = 'enabled'
            '''
        )
        mapping = {}
        for row in rows:
            mapping.setdefault(row["strategy_id"], set()).add(row["symbol"])
        self.strategy_tickers = mapping


# 🔸 Глобальный объект конфигурации
config = ConfigState()

# 🔸 Первичная инициализация конфигурации
async def init_config_state():
    await config.reload_all()
    log.debug("✅ Конфигурация инициализирована")

# 🔸 Слушатель событий из Redis Pub/Sub
async def config_event_listener():
    redis = infra.redis_client
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events", "strategies_v4_events")

    log.debug("📡 Подписка на каналы Redis запущена")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            if msg["channel"] == "tickers_v4_events":
                symbol = data["symbol"]
                if data["action"] == "enabled":
                    await config.reload_ticker(symbol)
                elif data["action"] == "disabled":
                    await config.remove_ticker(symbol)
            elif msg["channel"] == "strategies_v4_events":
                strategy_id = int(data["id"])
                if data["action"] == "true":
                    await config.reload_strategy(strategy_id)
                elif data["action"] == "false":
                    await config.remove_strategy(strategy_id)
        except Exception:
            log.exception("❌ Ошибка обработки события из Redis")
# 🔸 Подписка на обновления стратегий из Redis Stream
async def listen_strategy_update_stream():
    stream = "strategy_update_stream"
    group = "strategy_runtime"
    consumer = "strategy_listener_1"
    redis = infra.redis_client
    log = logging.getLogger("CONFIG")

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.debug(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {group} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.debug(f"📥 Подписка на поток обновлений стратегий: {stream} → {group}")

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=1000
            )
            for _, records in entries:
                for record_id, data in records:
                    try:
                        if data.get("type") == "strategy" and data.get("action") == "update":
                            sid = int(data["id"])
                            await config.reload_strategy(sid)
                            log.info(f"♻️ Стратегия обновлена: id={sid}")
                        await redis.xack(stream, group, record_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи потока")
        except Exception:
            log.exception("❌ Ошибка чтения из потока")