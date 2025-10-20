# trader_config.py — загрузка активных тикеров/стратегий, онлайн-апдейты (Pub/Sub), кэш победителей и флаги Bybit + live_trading

# 🔸 Импорты
import asyncio
import logging
import json
from decimal import Decimal
from typing import Dict, Set, Optional, Any

from trader_infra import infra

# 🔸 Логгер конфигурации
log = logging.getLogger("TRADER_CONFIG")

# 🔸 Константы каналов Pub/Sub (жёстко в коде)
TICKERS_EVENTS_CHANNEL = "bb:tickers_events"
STRATEGIES_EVENTS_CHANNEL = "strategies_v4_events"
TRADER_SETTINGS_EVENTS_CHANNEL = "trader_settings_events"

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
        "trader_bybit",
        "trader_winner",
    ):
        if key in strategy:
            val = strategy[key]
            strategy[key] = (str(val).lower() == "true") if not isinstance(val, bool) else val

# 🔸 Состояние конфигурации трейдера (+ in-memory кэши победителей/Bybit и глобальных настроек)
class TraderConfigState:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}
        self.strategies: Dict[int, dict] = {}
        self.strategy_tickers: Dict[int, Set[str]] = {}

        # кэш победителей и их метаданных
        self.trader_winners: Set[int] = set()                  # множество strategy_id с trader_winner=true
        self.trader_winners_min_deposit: Optional[Decimal] = None
        self.strategy_meta: Dict[int, dict] = {}               # только для текущих winners (deposit/leverage/market_mirrow*)

        # кэш стратегий, допущенных к Bybit
        self.trader_bybit_enabled: Set[int] = set()            # множество strategy_id с trader_bybit=true
        self.eligible_for_bybit: Set[int] = set()              # winners ∩ bybit

        # глобальные настройки
        self.live_trading: bool = False

        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()           # заполняет strategies и trader_bybit_enabled
            await self._load_strategy_tickers()
            await self._refresh_trader_winners_state_locked()
            await self.refresh_trader_settings_locked()
            log.info(
                "✅ Конфигурация перезагружена: тикеров=%d, стратегий=%d, winners=%d (min_dep=%s), bybit=%d, eligible=%d, live_trading=%s",
                len(self.tickers),
                len(self.strategies),
                len(self.trader_winners),
                self.trader_winners_min_deposit,
                len(self.trader_bybit_enabled),
                len(self.eligible_for_bybit),
                str(self.live_trading).lower(),
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
                self.trader_winners.discard(strategy_id)
                self.strategy_meta.pop(strategy_id, None)
                self.trader_bybit_enabled.discard(strategy_id)
                await self._recalc_min_deposit_locked()
                self._recalc_eligible_locked()
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

            # обновим кэши по winner/bybit
            await self._touch_winner_membership_locked(strategy)
            self._touch_bybit_membership_locked(strategy)

            log.info(
                "🔄 Стратегия обновлена: id=%d (tickers=%d, winner=%s, bybit=%s, eligible=%s)",
                strategy_id,
                len(self.strategy_tickers[strategy_id]),
                str(strategy.get("trader_winner")).lower(),
                str(strategy.get("trader_bybit")).lower(),
                str(strategy_id in self.eligible_for_bybit).lower(),
            )

    # 🔸 Удаление стратегии из состояния
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            self.trader_winners.discard(strategy_id)
            self.strategy_meta.pop(strategy_id, None)
            self.trader_bybit_enabled.discard(strategy_id)
            await self._recalc_min_deposit_locked()
            self._recalc_eligible_locked()
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
        log.info("📥 Загружены тикеры: %d", len(self.tickers))

    # 🔸 Загрузка активных стратегий (только enabled=true) + кэш trader_bybit_enabled
    async def _load_strategies(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT *
            FROM strategies_v4
            WHERE enabled = true
            """
        )
        strategies: Dict[int, dict] = {}
        bybit_set: Set[int] = set()
        for r in rows:
            s = dict(r)
            _normalize_strategy_flags(s)
            sid = int(s["id"])
            strategies[sid] = s
            if bool(s.get("trader_bybit")):
                bybit_set.add(sid)
        self.strategies = strategies
        self.trader_bybit_enabled = bybit_set
        log.info("📥 Загружены стратегии: %d | bybit_enabled=%d", len(self.strategies), len(self.trader_bybit_enabled))

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
        log.info("📥 Загружены связи стратегия↔тикеры: записей=%d", len(rows))

    # 🔸 Публичное обновление кэша победителей (батч из БД)
    async def refresh_trader_winners_state(self):
        async with self._lock:
            await self._refresh_trader_winners_state_locked()

    # 🔸 Кэш победителей: батч-чтение из БД и обновление in-memory полей
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
        self._recalc_eligible_locked()
        log.info("🏷️ Кэш trader_winner обновлён: winners=%d, eligible_bybit=%d, min_dep=%s",
                 len(self.trader_winners), len(self.eligible_for_bybit), self.trader_winners_min_deposit)

    # 🔸 Инкрементальная корректировка кэша winners по одной стратегии
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
        self._recalc_eligible_locked()

    # 🔸 Инкрементальная корректировка кэша Bybit по одной стратегии
    def _touch_bybit_membership_locked(self, strategy_row: dict):
        sid = int(strategy_row["id"])
        if bool(strategy_row.get("trader_bybit")):
            self.trader_bybit_enabled.add(sid)
        else:
            self.trader_bybit_enabled.discard(sid)
        self._recalc_eligible_locked()

    # 🔸 Пересчёт min(deposit) среди текущих winners (по in-memory meta)
    async def _recalc_min_deposit_locked(self):
        min_dep: Optional[Decimal] = None
        for sid in self.trader_winners:
            dep = _as_decimal(self.strategy_meta.get(sid, {}).get("deposit"))
            if dep is not None and dep > 0 and (min_dep is None or dep < min_dep):
                min_dep = dep
        self.trader_winners_min_deposit = min_dep

    # 🔸 Пересчёт eligible_for_bybit = winners ∩ trader_bybit_enabled
    def _recalc_eligible_locked(self):
        self.eligible_for_bybit = set(sid for sid in self.trader_winners if sid in self.trader_bybit_enabled)

    # 🔸 Публичный метод: живой ли внешний трейдинг
    def is_live_trading(self) -> bool:
        return bool(self.live_trading)

    # 🔸 Обновление глобальных настроек из БД (батч)
    async def refresh_trader_settings(self):
        async with self._lock:
            await self.refresh_trader_settings_locked()

    # 🔸 Обновление глобальных настроек из БД (под локом)
    async def refresh_trader_settings_locked(self):
        rows = await infra.pg_pool.fetch(
            """
            SELECT key, value_bool, value_numeric, value_text
            FROM public.trader_settings
            WHERE key IN ('live_trading')
            """
        )
        kv = {r["key"]: (r["value_bool"], r["value_numeric"], r["value_text"]) for r in rows}
        lv = kv.get("live_trading", (None, None, None))[0]
        self.live_trading = bool(lv) if lv is not None else False
        log.info("🧩 trader_settings обновлены: live_trading=%s", str(self.live_trading).lower())

    # 🔸 Утилита: получить мету тикера (ticksize/min_qty/precision)
    def get_ticker_meta(self, symbol: str) -> Optional[Dict[str, Any]]:
        row = self.tickers.get(symbol)
        return dict(row) if row else None


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

    # подписка на каналы
    await pubsub.subscribe(TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL, TRADER_SETTINGS_EVENTS_CHANNEL)
    log.info("📡 Подписка на каналы Redis запущена: %s, %s, %s",
             TICKERS_EVENTS_CHANNEL, STRATEGIES_EVENTS_CHANNEL, TRADER_SETTINGS_EVENTS_CHANNEL)

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
                await config.refresh_trader_winners_state()
                log.info("♻️ Обработано событие стратегии: id=%d (%s)", sid, action)

            # обработка событий настроек (live_trading)
            elif channel == TRADER_SETTINGS_EVENTS_CHANNEL:
                key = str(data.get("key") or "")
                if key == "live_trading":
                    val = data.get("value_bool")
                    async with config._lock:
                        config.live_trading = bool(val) if val is not None else False
                    log.info("♻️ Обработано событие настроек: live_trading=%s", str(config.live_trading).lower())

        except Exception:
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