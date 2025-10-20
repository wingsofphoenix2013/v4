# trader_config.py — загрузка активных тикеров/стратегий, онлайн-апдейты (Pub/Sub) и кэш trader_winner

# 🔸 Импорты
import asyncio
import logging
import json
from decimal import Decimal
from typing import Dict, Set, Optional

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

# 🔸 Состояние конфигурации трейдера (+ in-memory кэш победителей)
class TraderConfigState:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}
        self.strategies: Dict[int, dict] = {}
        self.strategy_tickers: Dict[int, Set[str]] = {}

        # кэш победителей и их метаданных
        self.trader_winners: Set[int] = set()  # множество strategy_id
        self.trader_winners_min_deposit: Optional[Decimal] = None
        self.strategy_meta: Dict[int, dict] = {}  # только для текущих winners:
        # {sid: {"deposit": Decimal|None, "leverage": Decimal|None,
        #        "market_mirrow": int|None, "market_mirrow_long": int|None, "market_mirrow_short": int|None}}

        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()
            await self._load_strategy_tickers()
            await self._refresh_trader_winners_state_locked()
            # итоговый лог по результату загрузки
            log.debug(
                "✅ Конфигурация перезагружена: тикеров=%d, стратегий=%d, winners=%d (min_dep=%s)",
                len(self.tickers),
                len(self.strategies),
                len(self.trader_winners),
                self.trader_winners_min_deposit,
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
                log.debug("🔄 Тикер обновлён: %s", symbol)
            else:
                self.tickers.pop(symbol, None)
                log.debug("🗑️ Тикер удалён (не активен): %s", symbol)

    # 🔸 Удаление тикера из состояния
    async def remove_ticker(self, symbol: str):
        async with self._lock:
            self.tickers.pop(symbol, None)
            log.debug("🗑️ Тикер удалён: %s", symbol)

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
                # при удалении стратегии она точно не может быть winner
                self.trader_winners.discard(strategy_id)
                self.strategy_meta.pop(strategy_id, None)
                await self._recalc_min_deposit_locked()
                log.debug("🗑️ Стратегия удалена: id=%d", strategy_id)
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

            # обновим кэш победителей инкрементально (на основе свежей строки)
            await self._touch_winner_membership_locked(strategy)

            log.debug(
                "🔄 Стратегия обновлена: id=%d (tickers=%d, is_winner=%s)",
                strategy_id,
                len(self.strategy_tickers[strategy_id]),
                "true" if strategy.get("trader_winner") else "false",
            )

    # 🔸 Удаление стратегии из состояния
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            self.trader_winners.discard(strategy_id)
            self.strategy_meta.pop(strategy_id, None)
            await self._recalc_min_deposit_locked()
            log.debug("🗑️ Стратегия удалена: id=%d", strategy_id)

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
        log.debug("📥 Загружены тикеры: %d", len(self.tickers))

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
        log.debug("📥 Загружены стратегии: %d", len(self.strategies))

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
        log.debug("📥 Загружены связи стратегия↔тикеры: записей=%d", len(rows))

    # 🔸 Публичное обновление кэша победителей (батч из БД)
    async def refresh_trader_winners_state(self):
        async with self._lock:
            await self._refresh_trader_winners_state_locked()

    # Кэш победителей: батч-чтение из БД и обновление in-memory полей
    async def _refresh_trader_winners_state_locked(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT id, deposit, leverage, market_mirrow, market_mirrow_long, market_mirrow_short
            FROM strategies_v4
            WHERE enabled = true
              AND trader_winner = true
            """
        )
        winners: Set[int] = set()
        meta: Dict[int, dict] = {}
        min_dep: Optional[Decimal] = None

        for r in rows:
            sid = int(r["id"])
            dep = _as_decimal(r["deposit"])
            lev = _as_decimal(r["leverage"])
            mm = r["market_mirrow"]
            mml = r["market_mirrow_long"]
            mms = r["market_mirrow_short"]

            winners.add(sid)
            meta[sid] = {
                "deposit": dep,
                "leverage": lev,
                "market_mirrow": int(mm) if mm is not None else None,
                "market_mirrow_long": int(mml) if mml is not None else None,
                "market_mirrow_short": int(mms) if mms is not None else None,
            }
            if dep is not None and dep > 0 and (min_dep is None or dep < min_dep):
                min_dep = dep

        self.trader_winners = winners
        self.strategy_meta = meta
        self.trader_winners_min_deposit = min_dep
        log.debug(
            "🏷️ Кэш trader_winner обновлён: winners=%d, min_dep=%s",
            len(self.trader_winners),
            self.trader_winners_min_deposit,
        )

    # Инкрементальная корректировка кэша winners по одной стратегии
    async def _touch_winner_membership_locked(self, strategy_row: dict):
        sid = int(strategy_row["id"])
        is_winner = bool(strategy_row.get("trader_winner"))

        if is_winner:
            self.trader_winners.add(sid)
            self.strategy_meta[sid] = {
                "deposit": _as_decimal(strategy_row.get("deposit")),
                "leverage": _as_decimal(strategy_row.get("leverage")),
                "market_mirrow": strategy_row.get("market_mirrow"),
                "market_mirrow_long": strategy_row.get("market_mirrow_long"),
                "market_mirrow_short": strategy_row.get("market_mirrow_short"),
            }
        else:
            self.trader_winners.discard(sid)
            self.strategy_meta.pop(sid, None)

        await self._recalc_min_deposit_locked()

    # Пересчёт min(deposit) среди текущих winners (по in-memory meta)
    async def _recalc_min_deposit_locked(self):
        min_dep: Optional[Decimal] = None
        for sid in self.trader_winners:
            dep = _as_decimal(self.strategy_meta.get(sid, {}).get("deposit"))
            if dep is not None and dep > 0 and (min_dep is None or dep < min_dep):
                min_dep = dep
        self.trader_winners_min_deposit = min_dep


# 🔸 Глобальный объект конфигурации
config = TraderConfigState()

# 🔸 Первичная инициализация конфигурации
async def init_trader_config_state():
    await config.reload_all()
    log.debug("✅ Конфигурация трейдера загружена")

# 🔸 Слушатель Pub/Sub для онлайновых апдейтов
async def config_event_listener():
    redis = infra.redis_client
    pubsub = redis.pubsub()

    # подписка на существующие каналы
    await pubsub.subscribe(TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL)
    log.debug("📡 Подписка на каналы Redis запущена: %s, %s", TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL)

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
                log.debug("♻️ Обработано событие тикера: %s (%s)", symbol, action)

            # обработка событий по стратегиям
            elif channel == STRATEGIES_EVENTS_CHANNEL:
                sid = int(data.get("id"))
                action = data.get("action")
                if action == "true":
                    await config.reload_strategy(sid)
                elif action == "false":
                    await config.remove_strategy(sid)
                # после любого изменения стратегии — освежим кэш winners
                await config.refresh_trader_winners_state()
                log.debug("♻️ Обработано событие стратегии: id=%d (%s)", sid, action)

        except Exception:
            # логируем и продолжаем слушать дальше
            log.exception("❌ Ошибка обработки события Pub/Sub")


# 🔸 Утилиты
def _as_decimal(v) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None