# config_loader.py — конфигурация стратегий/тикеров с горячими обновлениями + подтверждения применённых изменений (Streams)

# 🔸 Импорты
import asyncio
import logging
import json
from typing import Dict, Set

from infra import infra

# 🔸 Константы каналов/стримов
PUBSUB_CHANNEL_TICKERS = "bb:tickers_events"
PUBSUB_CHANNEL_STRATEGIES = "strategies_v4_events"
STRATEGY_UPDATE_STREAM = "strategy_update_stream"   # входящие запросы на обновление (update)
STRATEGY_STATE_STREAM = "strategy_state_stream"     # подтверждения применённых изменений (applied)

# 🔸 Логгер
log = logging.getLogger("CONFIG")

# 🔸 Приведение логических флагов стратегии к типу bool
def normalize_strategy_flags(strategy: dict) -> None:
    for key in (
        "enabled",
        "use_all_tickers",
        "use_stoploss",
        "allow_open",
        "reverse",
        "sl_protection",
    ):
        if key in strategy:
            strategy[key] = str(strategy[key]).lower() == "true"

# 🔸 Публикация подтверждения "applied" в Stream (двухфазный протокол)
async def _publish_strategy_applied(strategy_id: int) -> None:
    # формируем минимальный payload подтверждения
    payload = {
        "type": "strategy",
        "action": "applied",
        "id": str(strategy_id),
    }
    try:
        await infra.redis_client.xadd(STRATEGY_STATE_STREAM, payload)
        log.debug("📤 APPLIED → %s: sid=%s", STRATEGY_STATE_STREAM, strategy_id)
    except Exception:
        log.exception("❌ Не удалось опубликовать 'applied' для стратегии id=%s", strategy_id)

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
                "SELECT * FROM tickers_bb WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'",
                symbol,
            )
            if row:
                self.tickers[symbol] = dict(row)
                log.info("🔄 Тикер обновлён: %s", symbol)
            else:
                self.tickers.pop(symbol, None)
                log.info("❌ Тикер удалён (не найден): %s", symbol)

    # 🔸 Удаление тикера
    async def remove_ticker(self, symbol: str):
        async with self._lock:
            self.tickers.pop(symbol, None)
            log.info("🗑️ Тикер удалён: %s", symbol)

    # 🔸 Обновление одной стратегии (и публикация подтверждения applied)
    async def reload_strategy(self, strategy_id: int):
        async with self._lock:
            row = await infra.pg_pool.fetchrow(
                "SELECT * FROM strategies_v4 WHERE id = $1 AND enabled = true AND archived = false",
                strategy_id,
            )
            if not row:
                self.strategies.pop(strategy_id, None)
                self.strategy_tickers.pop(strategy_id, None)
                log.info("🗑️ Стратегия удалена из рантайма: id=%s", strategy_id)
                # публикуем applied, чтобы потребители (например, informer) обновили своё состояние
                await _publish_strategy_applied(strategy_id)
                return

            strategy = dict(row)
            strategy["module_name"] = strategy["name"]

            normalize_strategy_flags(strategy)

            log.debug(
                "[DEBUG-NORM] Strategy %s → enabled=%s reverse=%s sl_protection=%s allow_open=%s",
                strategy_id,
                strategy.get("enabled"),
                strategy.get("reverse"),
                strategy.get("sl_protection"),
                strategy.get("allow_open"),
            )

            strategy["tp_levels"] = [
                dict(r)
                for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level",
                    strategy_id,
                )
            ]
            strategy["sl_rules"] = [
                dict(r)
                for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1",
                    strategy_id,
                )
            ]

            # обогащение sl_rules полем level
            level_map = {lvl["id"]: lvl["level"] for lvl in strategy["tp_levels"]}
            for rule in strategy["sl_rules"]:
                rule["level"] = level_map.get(rule["tp_level_id"])

            self.strategies[strategy_id] = strategy

            tickers = await infra.pg_pool.fetch(
                """
                SELECT t.symbol
                FROM strategy_tickers_v4 st
                JOIN tickers_bb t ON st.ticker_id = t.id
                WHERE st.strategy_id = $1 AND st.enabled = true AND t.status = 'enabled' AND t.tradepermission = 'enabled'
                """,
                strategy_id,
            )
            self.strategy_tickers[strategy_id] = {r["symbol"] for r in tickers}

            log.info("🔄 Стратегия обновлена: [id=%s] %s", strategy_id, strategy["human_name"])

            # дополнительный лог деталей
            log.debug(
                "🧠 Загружена стратегия %s | TP=%s, SL=%s",
                strategy_id,
                [
                    {
                        "level": r["level"],
                        "value": r["tp_value"],
                        "type": r["tp_type"],
                        "volume": r["volume_percent"],
                    }
                    for r in strategy["tp_levels"]
                ],
                [
                    {
                        "tp_level_id": r["tp_level_id"],
                        "level": r["level"],
                        "mode": r["sl_mode"],
                    }
                    for r in strategy["sl_rules"]
                ],
            )

        # публикация вне lock: подтверждение применённого состояния
        await _publish_strategy_applied(strategy_id)

    # 🔸 Удаление стратегии (и публикация подтверждения applied)
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            log.info("🗑️ Стратегия удалена: id=%s", strategy_id)
        # публикация вне lock
        await _publish_strategy_applied(strategy_id)

    # 🔸 Загрузка всех тикеров (только активных с разрешением)
    async def _load_tickers(self):
        rows = await infra.pg_pool.fetch(
            "SELECT * FROM tickers_bb WHERE status = 'enabled' AND tradepermission = 'enabled'"
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
                "[DEBUG-NORM] Strategy %s → enabled=%s reverse=%s sl_protection=%s allow_open=%s",
                strategy_id,
                strategy.get("enabled"),
                strategy.get("reverse"),
                strategy.get("sl_protection"),
                strategy.get("allow_open"),
            )

            strategy["tp_levels"] = [
                dict(r)
                for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_levels_v4 WHERE strategy_id = $1 ORDER BY level",
                    strategy_id,
                )
            ]
            strategy["sl_rules"] = [
                dict(r)
                for r in await infra.pg_pool.fetch(
                    "SELECT * FROM strategy_tp_sl_v4 WHERE strategy_id = $1",
                    strategy_id,
                )
            ]

            # обогащение sl_rules полем level
            level_map = {lvl["id"]: lvl["level"] for lvl in strategy["tp_levels"]}
            for rule in strategy["sl_rules"]:
                rule["level"] = level_map.get(rule["tp_level_id"])

            self.strategies[strategy_id] = strategy

            # лог: детали загруженной стратегии
            log.debug(
                "🧠 Загружена стратегия %s | TP=%s, SL=%s",
                strategy_id,
                [
                    {
                        "level": r["level"],
                        "value": r["tp_value"],
                        "type": r["tp_type"],
                        "volume": r["volume_percent"],
                    }
                    for r in strategy["tp_levels"]
                ],
                [
                    {
                        "tp_level_id": r["tp_level_id"],
                        "level": r["level"],
                        "mode": r["sl_mode"],
                    }
                    for r in strategy["sl_rules"]
                ],
            )

    # 🔸 Загрузка связей стратегия ↔ тикеры
    async def _load_strategy_tickers(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT s.strategy_id, t.symbol
            FROM strategy_tickers_v4 s
            JOIN tickers_bb t ON s.ticker_id = t.id
            WHERE s.enabled = true AND t.status = 'enabled' AND t.tradepermission = 'enabled'
            """
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
    log.info("✅ Конфигурация инициализирована")

# 🔸 Слушатель событий из Redis Pub/Sub (совместимость для тумблера включения/выключения)
async def config_event_listener():
    redis = infra.redis_client
    pubsub = redis.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL_TICKERS, PUBSUB_CHANNEL_STRATEGIES)

    log.info("📡 Подписка на каналы Redis запущена")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            if msg["channel"] == PUBSUB_CHANNEL_TICKERS:
                symbol = data["symbol"]
                if data["action"] == "enabled":
                    await config.reload_ticker(symbol)
                elif data["action"] == "disabled":
                    await config.remove_ticker(symbol)
            elif msg["channel"] == PUBSUB_CHANNEL_STRATEGIES:
                strategy_id = int(data["id"])
                if data["action"] == "true":
                    # перезагрузка стратегии → публикация 'applied' произойдёт внутри reload_strategy
                    await config.reload_strategy(strategy_id)
                elif data["action"] == "false":
                    # снятие стратегии → публикуем 'applied' из remove_strategy
                    await config.remove_strategy(strategy_id)
        except Exception:
            log.exception("❌ Ошибка обработки события из Redis")

# 🔸 Подписка на обновления стратегий из Redis Stream (запросы на update)
async def listen_strategy_update_stream():
    stream = STRATEGY_UPDATE_STREAM
    group = "strategy_runtime"
    consumer = "strategy_listener_1"
    redis = infra.redis_client

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.info("📡 Группа %s создана для %s", group, stream)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ Группа %s уже существует", group)
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info("📥 Подписка на поток обновлений стратегий: %s → %s", stream, group)

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=100,
                block=1000,
            )
            for _, records in entries:
                for record_id, data in records:
                    try:
                        # условия маршрута "update" из двухфазного протокола
                        if data.get("type") == "strategy" and data.get("action") == "update":
                            sid = int(data["id"])
                            await config.reload_strategy(sid)  # публикация 'applied' произойдёт внутри
                            log.info("♻️ Стратегия перезагружена по запросу: id=%s", sid)
                        await redis.xack(stream, group, record_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи потока")
        except Exception:
            log.exception("❌ Ошибка чтения из потока")