# trader_config.py — загрузка активных тикеров/стратегий и онлайн-апдейты (Pub/Sub)

# 🔸 Импорты
import asyncio
import logging
import json
from typing import Dict, Set

from trader_infra import infra

# 🔸 Логгер конфигурации
log = logging.getLogger("TRADER_CONFIG")

# 🔸 Константы каналов Pub/Sub (жёстко в коде)
TICKERS_EVENTS_CHANNEL = "bb:tickers_events"
STRATEGIES_EVENTS_CHANNEL = "strategies_v4_events"

# 🔸 Нормализация логических флагов стратегии
def _normalize_strategy_flags(strategy: dict) -> None:
    # список полей с булевой семантикой, которые встречаются в strategies_v4
    for key in (
        "enabled",
        "use_all_tickers",
        "use_stoploss",
        "allow_open",
        "reverse",
        "sl_protection",
        "market_watcher",
        "deathrow",
        "blacklist_watcher",
    ):
        if key in strategy:
            val = strategy[key]
            # приводим к bool, поддерживая как реальные bool, так и строковые значения
            strategy[key] = (str(val).lower() == "true") if not isinstance(val, bool) else val

# 🔸 Состояние конфигурации трейдера
class TraderConfigState:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}
        self.strategies: Dict[int, dict] = {}
        self.strategy_tickers: Dict[int, Set[str]] = {}
        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()
            await self._load_strategy_tickers()
            # итоговый лог по результату загрузки
            log.info(
                "✅ Конфигурация трейдера перезагружена: тикеров=%d, стратегий=%d",
                len(self.tickers),
                len(self.strategies),
            )

    # 🔸 Точечная перезагрузка одного тикера
    async def reload_ticker(self, symbol: str):
        async with self._lock:
            row = await infra.pg_pool.fetchrow(
                """
                SELECT *
                FROM tickers_bb
                WHERE symbol = $1 AND status = 'enabled' AND tradepermission = 'enabled'
                """,
                symbol,
            )
            if row:
                self.tickers[symbol] = dict(row)
                log.info("🔄 Тикер обновлён: %s", symbol)
            else:
                self.tickers.pop(symbol, None)
                log.info("🗑️ Тикер удалён (не активен): %s", symbol)

    # 🔸 Удаление тикера из состояния
    async def remove_ticker(self, symbol: str):
        async with self._lock:
            self.tickers.pop(symbol, None)
            log.info("🗑️ Тикер удалён: %s", symbol)

    # 🔸 Точечная перезагрузка стратегии
    async def reload_strategy(self, strategy_id: int):
        async with self._lock:
            row = await infra.pg_pool.fetchrow(
                """
                SELECT *
                FROM strategies_v4
                WHERE id = $1 AND enabled = true
                """,
                strategy_id,
            )

            if not row:
                self.strategies.pop(strategy_id, None)
                self.strategy_tickers.pop(strategy_id, None)
                log.info("🗑️ Стратегия удалена: id=%d", strategy_id)
                return

            strategy = dict(row)
            _normalize_strategy_flags(strategy)
            self.strategies[strategy_id] = strategy

            # загрузка разрешённых тикеров для этой стратегии
            tickers_rows = await infra.pg_pool.fetch(
                """
                SELECT t.symbol
                FROM strategy_tickers_v4 st
                JOIN tickers_bb t ON st.ticker_id = t.id
                WHERE st.strategy_id = $1
                  AND st.enabled = true
                  AND t.status = 'enabled'
                  AND t.tradepermission = 'enabled'
                """,
                strategy_id,
            )
            self.strategy_tickers[strategy_id] = {r["symbol"] for r in tickers_rows}

            log.info("🔄 Стратегия обновлена: id=%d (tickers=%d)", strategy_id, len(self.strategy_tickers[strategy_id]))

    # 🔸 Удаление стратегии из состояния
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            log.info("🗑️ Стратегия удалена: id=%d", strategy_id)

    # 🔸 Загрузка активных тикеров
    async def _load_tickers(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT *
            FROM tickers_bb
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            """
        )
        self.tickers = {r["symbol"]: dict(r) for r in rows}
        # результат загрузки тикеров
        log.info("📥 Загружены тикеры: %d", len(self.tickers))

    # 🔸 Загрузка активных стратегий (только enabled=true)
    async def _load_strategies(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT *
            FROM strategies_v4
            WHERE enabled = true
            """
        )
        strategies: Dict[int, dict] = {}
        for r in rows:
            s = dict(r)
            _normalize_strategy_flags(s)
            strategies[s["id"]] = s
        self.strategies = strategies
        # результат загрузки стратегий
        log.info("📥 Загружены стратегии: %d", len(self.strategies))

    # 🔸 Загрузка связей стратегия ↔ тикеры
    async def _load_strategy_tickers(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT st.strategy_id, t.symbol
            FROM strategy_tickers_v4 st
            JOIN tickers_bb t ON st.ticker_id = t.id
            WHERE st.enabled = true
              AND t.status = 'enabled'
              AND t.tradepermission = 'enabled'
            """
        )
        mapping: Dict[int, Set[str]] = {}
        for r in rows:
            mapping.setdefault(r["strategy_id"], set()).add(r["symbol"])
        self.strategy_tickers = mapping
        # результат загрузки связей
        log.info("📥 Загружены связи стратегия↔тикеры: записей=%d", len(rows))

# 🔸 Глобальный объект конфигурации
config = TraderConfigState()

# 🔸 Первичная инициализация конфигурации
async def init_trader_config_state():
    await config.reload_all()
    log.info("✅ Конфигурация трейдера загружена")

# 🔸 Слушатель Pub/Sub для онлайновых апдейтов
async def config_event_listener():
    redis = infra.redis_client
    pubsub = redis.pubsub()

    # подписка на существующие каналы
    await pubsub.subscribe(TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL)
    log.info("📡 Подписка на каналы Redis запущена: %s, %s", TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL)

    async for msg in pubsub.listen():
        if msg.get("type") != "message":
            continue

        try:
            channel = msg.get("channel")
            data_raw = msg.get("data")

            # поддержка байтового и строкового форматов
            if isinstance(data_raw, bytes):
                data_raw = data_raw.decode("utf-8", errors="ignore")

            data = json.loads(data_raw)

            # обработка событий по тикерам
            if channel == TICKERS_EVENTS_CHANNEL:
                symbol = data.get("symbol")
                action = data.get("action")
                if action == "enabled":
                    await config.reload_ticker(symbol)
                elif action == "disabled":
                    await config.remove_ticker(symbol)
                log.info("♻️ Обработано событие тикера: %s (%s)", symbol, action)

            # обработка событий по стратегиям
            elif channel == STRATEGIES_EVENTS_CHANNEL:
                sid = int(data.get("id"))
                action = data.get("action")
                if action == "true":
                    await config.reload_strategy(sid)
                elif action == "false":
                    await config.remove_strategy(sid)
                log.info("♻️ Обработано событие стратегии: id=%d (%s)", sid, action)

        except Exception:
            # логируем и продолжаем слушать дальше
            log.exception("❌ Ошибка обработки события Pub/Sub")