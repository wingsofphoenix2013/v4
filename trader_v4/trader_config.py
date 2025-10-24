# trader_config.py — загрузка активных тикеров/стратегий, онлайн-апдейты (Pub/Sub) и кэши: winners + полная политика SL/TP + RUNTIME позиций

# 🔸 Импорты
import asyncio
import logging
import json
from decimal import Decimal
from typing import Dict, Set, Optional, List, Any, Tuple
from dataclasses import dataclass
from datetime import datetime

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
        "trader_watcher",
        "trader_winner",
        "trader_bybit",
        "archived",
    ):
        if key in strategy:
            val = strategy[key]
            strategy[key] = (str(val).lower() == "true") if not isinstance(val, bool) else val

# 🔸 Runtime-снимок позиции (для централизованного состояния)
@dataclass
class PositionSnap:
    position_uid: str
    strategy_id: int
    symbol: str
    direction: str           # 'long' | 'short'
    opened_at: datetime
    had_tp: bool = False
    last_seen_at: Optional[datetime] = None

# 🔸 Состояние конфигурации трейдера (+ in-memory кэши winners и политики SL/TP + позиции)
class TraderConfigState:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}
        self.strategies: Dict[int, dict] = {}
        self.strategy_tickers: Dict[int, Set[str]] = {}

        # кэш победителей и их метаданных
        self.trader_winners: Set[int] = set()  # множество strategy_id
        self.trader_winners_min_deposit: Optional[Decimal] = None
        self.strategy_meta: Dict[int, dict] = {}  # только для текущих winners

        # полная политика SL/TP по стратегиям (все активные, не только winners)
        self.strategy_policy: Dict[int, dict] = {}

        # runtime-позиции (единый для всех воркеров)
        self.positions_runtime: Dict[str, PositionSnap] = {}          # {position_uid -> PositionSnap}
        self.positions_by_sid_symbol: Dict[Tuple[int, str], str] = {} # {(strategy_id, symbol) -> position_uid}

        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()
            await self._load_strategy_tickers()
            await self._load_all_policies()
            await self._refresh_trader_winners_state_locked()
            # итоговый лог по результату загрузки
            log.debug(
                "✅ Конфигурация перезагружена: тикеров=%d, стратегий=%d, winners=%d (min_dep=%s), policies=%d",
                len(self.tickers),
                len(self.strategies),
                len(self.trader_winners),
                self.trader_winners_min_deposit,
                len(self.strategy_policy),
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

    # 🔸 Точечная перезагрузка стратегии (включая политику SL/TP и связи тикеров)
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
                # стратегия не активна → полное удаление из in-memory
                self.strategies.pop(strategy_id, None)
                self.strategy_tickers.pop(strategy_id, None)
                self.trader_winners.discard(strategy_id)
                self.strategy_meta.pop(strategy_id, None)
                self.strategy_policy.pop(strategy_id, None)
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

            # обновим winners-мета инкрементально
            await self._touch_winner_membership_locked(strategy)

            # перезагрузим политику SL/TP для одной стратегии
            await self._load_strategy_policy_locked(strategy_id)

            log.debug(
                "🔄 Стратегия обновлена: id=%d (tickers=%d, is_winner=%s, policy_tp=%d)",
                strategy_id,
                len(self.strategy_tickers[strategy_id]),
                "true" if strategy.get("trader_winner") else "false",
                len(self.strategy_policy.get(strategy_id, {}).get("tp_levels", [])),
            )

    # 🔸 Удаление стратегии из состояния
    async def remove_strategy(self, strategy_id: int):
        async with self._lock:
            self.strategies.pop(strategy_id, None)
            self.strategy_tickers.pop(strategy_id, None)
            self.trader_winners.discard(strategy_id)
            self.strategy_meta.pop(strategy_id, None)
            self.strategy_policy.pop(strategy_id, None)
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
        log.debug(
            "🏷️ Кэш trader_winner обновлён: winners=%d, min_dep=%s",
            len(self.trader_winners),
            self.trader_winners_min_deposit,
        )

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

    # 🔸 Пересчёт min(deposit) среди текущих winners (по in-memory meta)
    async def _recalc_min_deposit_locked(self):
        min_dep: Optional[Decimal] = None
        for sid in self.trader_winners:
            dep = _as_decimal(self.strategy_meta.get(sid, {}).get("deposit"))
            if dep is not None and dep > 0 and (min_dep is None or dep < min_dep):
                min_dep = dep
        self.trader_winners_min_deposit = min_dep

    # 🔸 Батч-загрузка полной политики SL/TP для всех активных стратегий
    async def _load_all_policies(self):
        policies: Dict[int, dict] = {}

        # база SL берём из самой строки стратегии (sl_type/sl_value)
        for sid, s in self.strategies.items():
            policies[sid] = {
                "sl": {
                    "type": s.get("sl_type"),
                    "value": _as_decimal(s.get("sl_value")),
                },
                "tp_levels": [],            # заполним ниже
                "tp_sl_by_level": {},       # заполним ниже
            }

        if not self.strategies:
            self.strategy_policy = policies
            log.debug("📥 Политики SL/TP: стратегий нет")
            return

        sids = list(self.strategies.keys())

        # загрузим уровни TP пачкой
        tp_rows = await infra.pg_pool.fetch(
            """
            SELECT id, strategy_id, "level", tp_type, tp_value, volume_percent
            FROM strategy_tp_levels_v4
            WHERE strategy_id = ANY($1::int[])
            """,
            sids
        )

        # вспомогательное: map (strategy_id, tp_level_id) → level
        level_by_id: Dict[int, Dict[int, int]] = {}
        for r in tp_rows:
            sid = int(r["strategy_id"])
            lvl = int(r["level"])
            if sid not in policies:
                continue
            item = {
                "id": int(r["id"]),
                "level": lvl,
                "tp_type": r["tp_type"],
                "tp_value": _as_decimal(r["tp_value"]),
                "volume_percent": _as_decimal(r["volume_percent"]),
            }
            policies[sid]["tp_levels"].append(item)
            level_by_id.setdefault(sid, {})[int(r["id"])] = lvl

        # загрузим правила SL после TP пачкой
        sl_after_rows = await infra.pg_pool.fetch(
            """
            SELECT strategy_id, tp_level_id, sl_mode, sl_value
            FROM strategy_tp_sl_v4
            WHERE strategy_id = ANY($1::int[])
            """,
            sids
        )
        for r in sl_after_rows:
            sid = int(r["strategy_id"])
            if sid not in policies:
                continue
            lvl_map = level_by_id.get(sid, {})
            lvl = lvl_map.get(int(r["tp_level_id"]))
            if lvl is None:
                # нет соответствующего уровня (данные вне согласованности) — пропустим
                continue
            policies[sid]["tp_sl_by_level"][lvl] = {
                "sl_mode": r["sl_mode"],
                "sl_value": _as_decimal(r["sl_value"]),
            }

        # отсортируем tp_levels внутри каждой стратегии по level
        for sid, pol in policies.items():
            pol["tp_levels"].sort(key=lambda x: x["level"])

        self.strategy_policy = policies
        log.debug("📥 Политики SL/TP загружены: стратегий=%d, всего уровней=%d",
                  len(self.strategy_policy),
                  sum(len(p["tp_levels"]) for p in self.strategy_policy.values()))

    # 🔸 Точечная загрузка политики SL/TP для одной стратегии (под замком)
    async def _load_strategy_policy_locked(self, strategy_id: int):
        s = self.strategies.get(strategy_id)
        if not s:
            self.strategy_policy.pop(strategy_id, None)
            return

        policy = {
            "sl": {"type": s.get("sl_type"), "value": _as_decimal(s.get("sl_value"))},
            "tp_levels": [],
            "tp_sl_by_level": {},
        }

        # уровни TP
        tp_rows = await infra.pg_pool.fetch(
            """
            SELECT id, "level", tp_type, tp_value, volume_percent
            FROM strategy_tp_levels_v4
            WHERE strategy_id = $1
            ORDER BY "level"
            """,
            strategy_id
        )
        level_by_id: Dict[int, int] = {}
        for r in tp_rows:
            rid = int(r["id"])
            lvl = int(r["level"])
            policy["tp_levels"].append({
                "id": rid,
                "level": lvl,
                "tp_type": r["tp_type"],
                "tp_value": _as_decimal(r["tp_value"]),
                "volume_percent": _as_decimal(r["volume_percent"]),
            })
            level_by_id[rid] = lvl

        # SL после TP
        sl_after = await infra.pg_pool.fetch(
            """
            SELECT tp_level_id, sl_mode, sl_value
            FROM strategy_tp_sl_v4
            WHERE strategy_id = $1
            """,
            strategy_id
        )
        for r in sl_after:
            lvl = level_by_id.get(int(r["tp_level_id"]))
            if lvl is None:
                continue
            policy["tp_sl_by_level"][lvl] = {
                "sl_mode": r["sl_mode"],
                "sl_value": _as_decimal(r["sl_value"]),
            }

        self.strategy_policy[strategy_id] = policy

    # 🔸 Bootstrap runtime-позиций (вызывать после init_trader_config_state)
    async def init_positions_runtime_state(self):
        # читаем активные позиции в системе
        rows = await infra.pg_pool.fetch(
            """
            SELECT position_uid, strategy_id, symbol, direction, created_at
            FROM public.trader_positions_v4
            WHERE status IN ('open','closing')
            """
        )

        if not rows:
            async with self._lock:
                self.positions_runtime = {}
                self.positions_by_sid_symbol = {}
            log.info("🔎 POS_RUNTIME: loaded active positions — 0 items")
            return

        # список uid для вычисления had_tp (после последнего opened)
        uids: List[str] = [str(r["position_uid"]) for r in rows if r.get("position_uid")]

        # вычислим had_tp с привязкой к последнему opened (одним запросом через join к агрегации opened)
        tp_after_open_rows = await infra.pg_pool.fetch(
            """
            WITH last_open AS (
                SELECT position_uid, max(emitted_ts) AS opened_at
                FROM public.trader_signals
                WHERE event = 'opened' AND position_uid = ANY($1::text[])
                GROUP BY position_uid
            )
            SELECT DISTINCT s.position_uid
            FROM public.trader_signals s
            JOIN last_open o ON o.position_uid = s.position_uid
            WHERE s.event = 'tp_hit'
              AND s.emitted_ts > o.opened_at
            """,
            uids
        )
        had_tp_set = {str(r["position_uid"]) for r in tp_after_open_rows}

        # собираем новые словари
        new_runtime: Dict[str, PositionSnap] = {}
        new_index: Dict[Tuple[int, str], str] = {}
        now = datetime.utcnow()

        for r in rows:
            uid = str(r["position_uid"])
            sid = int(r["strategy_id"])
            sym = str(r["symbol"])
            direc = (str(r["direction"]) or "").lower()
            opened_at = r["created_at"] or now
            had_tp = uid in had_tp_set

            snap = PositionSnap(
                position_uid=uid,
                strategy_id=sid,
                symbol=sym,
                direction=direc,
                opened_at=opened_at,
                had_tp=had_tp,
                last_seen_at=now,
            )
            new_runtime[uid] = snap
            new_index[(sid, sym)] = uid

        async with self._lock:
            self.positions_runtime = new_runtime
            self.positions_by_sid_symbol = new_index

        log.info("🔎 POS_RUNTIME: loaded active positions — %d items", len(new_runtime))

    # 🔸 Публичное API runtime-позиций для воркеров

    async def note_opened(self, position_uid: str, strategy_id: int, symbol: str, direction: str, opened_at: Optional[datetime] = None):
        # создать/обновить запись о позиции (после opened v2)
        snap = PositionSnap(
            position_uid=position_uid,
            strategy_id=strategy_id,
            symbol=symbol,
            direction=(direction or "").lower(),
            opened_at=opened_at or datetime.utcnow(),
            had_tp=False,
            last_seen_at=datetime.utcnow(),
        )
        async with self._lock:
            self.positions_runtime[position_uid] = snap
            self.positions_by_sid_symbol[(strategy_id, symbol)] = position_uid

    async def note_tp_hit(self, position_uid: str, _ts: Optional[datetime] = None):
        async with self._lock:
            snap = self.positions_runtime.get(position_uid)
            if snap:
                snap.had_tp = True
                snap.last_seen_at = _ts or datetime.utcnow()

    async def note_closed(self, position_uid: str, _ts: Optional[datetime] = None):
        async with self._lock:
            snap = self.positions_runtime.pop(position_uid, None)
            if snap:
                self.positions_by_sid_symbol.pop((snap.strategy_id, snap.symbol), None)

    async def had_tp_since_open(self, position_uid: str) -> bool:
        async with self._lock:
            snap = self.positions_runtime.get(position_uid)
            return bool(snap and snap.had_tp)

    async def get_position(self, position_uid: str) -> Optional[PositionSnap]:
        async with self._lock:
            snap = self.positions_runtime.get(position_uid)
            return PositionSnap(**snap.__dict__) if snap else None

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

            # обработка событий по стратегиям (включая политику)
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
def _as_decimal(v: Any) -> Optional[Decimal]:
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    except Exception:
        return None