# trader_config.py — загрузка активных тикеров/стратегий, онлайн-апдейты (Pub/Sub/Stream) и кэш политик (TP/SL) для trader_winner

# 🔸 Импорты
import asyncio
import logging
import json
from decimal import Decimal
from typing import Dict, Set, Optional, List, Tuple

from trader_infra import infra

# 🔸 Логгер конфигурации
log = logging.getLogger("TRADER_CONFIG")

# 🔸 Константы каналов Pub/Sub (жёстко в коде)
TICKERS_EVENTS_CHANNEL = "bb:tickers_events"
STRATEGIES_EVENTS_CHANNEL = "strategies_v4_events"

# 🔸 Константы стрима состояния стратегий (двухфазный протокол applied)
STRATEGY_STATE_STREAM = "strategy_state_stream"
STATE_CG = "trader_v4_state_cg"
STATE_CONSUMER = "state-worker-1"


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
        "trader_winner",
    ):
        if key in strategy:
            val = strategy[key]
            # приводим к bool, поддерживая как реальные bool, так и строковые значения
            strategy[key] = (str(val).lower() == "true") if not isinstance(val, bool) else val


# 🔸 Состояние конфигурации трейдера (+ in-memory кэш победителей и их политик)
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

        # кэш полной политики по стратегиям-победителям
        self.strategy_policies: Dict[int, dict] = {}
        # структура policy:
        # {
        #   "strategy": {…копия строки из strategies_v4…},
        #   "tickers": set[str],
        #   "initial_sl": {"mode": "percent"|"atr", "value": Decimal} | None,
        #   "tp_levels": [{"level": int, "tp_type": str, "tp_value": Decimal|None,
        #                  "volume_percent": Decimal|None, "sl_mode": str|None, "sl_value": Decimal|None}, ...]
        # }

        self._lock = asyncio.Lock()

    # 🔸 Полная перезагрузка базовой конфигурации
    async def reload_all(self):
        async with self._lock:
            await self._load_tickers()
            await self._load_strategies()
            await self._load_strategy_tickers()
            await self._refresh_trader_winners_state_locked()
            # итоговый лог по результату загрузки
            log.info(
                "✅ Конфигурация перезагружена: тикеров=%d, стратегий=%d, winners=%d (min_dep=%s)",
                len(self.tickers),
                len(self.strategies),
                len(self.trader_winners),
                self.trader_winners_min_deposit,
            )

    # 🔸 Пакетная загрузка политик для всех текущих winners
    async def reload_all_policies_for_winners(self):
        async with self._lock:
            # подготовка набора id победителей
            winner_ids: List[int] = list(self.trader_winners)
            if not winner_ids:
                self.strategy_policies.clear()
                log.info("🏷️ Кэш политик: winners=0 → policies=0 (очищено)")
                return

            # загрузка политик для набора winners
            policies, total_levels = await self._load_policies_for_sids_locked(winner_ids)
            self.strategy_policies = policies

            # сводный лог
            log.info(
                "🏷️ Кэш политик перезагружен: winners=%d, policies=%d, tp_levels_total=%d",
                len(self.trader_winners),
                len(self.strategy_policies),
                total_levels,
            )

    # 🔸 Точечная загрузка/пересборка политики по одной стратегии (если она winner)
    async def reload_strategy_policy(self, strategy_id: int):
        async with self._lock:
            # стратегия должна быть winner и присутствовать в базовом кэше
            if strategy_id not in self.trader_winners or strategy_id not in self.strategies:
                self.strategy_policies.pop(strategy_id, None)
                log.info("🏷️ Политика НЕ загружена: id=%d (не winner/нет стратегии)", strategy_id)
                return

            # загрузка политики по одному sid
            policies, total_levels = await self._load_policies_for_sids_locked([strategy_id])
            self.strategy_policies.update(policies)

            # логи результата
            tp_count = len(policies.get(strategy_id, {}).get("tp_levels", []))
            log.info(
                "🏷️ Политика обновлена: id=%d, tp_levels=%d (total_levels=%d)",
                strategy_id,
                tp_count,
                total_levels,
            )

    # 🔸 Удаление политики из кэша (напр., при потере статуса winner)
    async def remove_strategy_policy(self, strategy_id: int):
        async with self._lock:
            existed = strategy_id in self.strategy_policies
            self.strategy_policies.pop(strategy_id, None)
            log.info("🗑️ Политика удалена: id=%d (was=%s)", strategy_id, "true" if existed else "false")

    # 🔸 Единая реакция на изменение стратегии (Pub/Sub applied/true/false, Stream applied)
    async def on_strategy_changed(self, strategy_id: int):
        async with self._lock:
            # перечитать строку стратегии
            row = await infra.pg_pool.fetchrow(
                """
                SELECT *
                FROM strategies_v4
                WHERE id = $1 AND enabled = true
                """,
                strategy_id,
            )

            if not row:
                # удалить стратегию из кэша, связки и winners
                self.strategies.pop(strategy_id, None)
                self.strategy_tickers.pop(strategy_id, None)
                self.trader_winners.discard(strategy_id)
                self.strategy_meta.pop(strategy_id, None)
                await self._recalc_min_deposit_locked()
                # удалить политику, если была
                self.strategy_policies.pop(strategy_id, None)
                log.info("🗑️ Стратегия удалена из состояния: id=%d (winner=%s, policy_dropped=true)",
                         strategy_id, "true" if strategy_id in self.trader_winners else "false")
                return

            # обновить стратегию и флаги
            strategy = dict(row)
            _normalize_strategy_flags(strategy)
            self.strategies[strategy_id] = strategy

            # загрузить разрешённые тикеры для этой стратегии
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

            # обновить кэш winners/мета инкрементально
            await self._touch_winner_membership_locked(strategy)

            # пересобрать min(deposit) среди winners
            await self._recalc_min_deposit_locked()

            # если теперь winner — пересобрать политику, иначе удалить
            if strategy_id in self.trader_winners:
                policies, _ = await self._load_policies_for_sids_locked([strategy_id])
                self.strategy_policies.update(policies)
                tp_count = len(self.strategy_policies.get(strategy_id, {}).get("tp_levels", []))
                log.info(
                    "🔄 Стратегия обновлена: id=%d (tickers=%d, is_winner=true, tp_levels=%d)",
                    strategy_id,
                    len(self.strategy_tickers[strategy_id]),
                    tp_count,
                )
            else:
                had_policy = self.strategy_policies.pop(strategy_id, None) is not None
                log.info(
                    "🔄 Стратегия обновлена: id=%d (tickers=%d, is_winner=false, policy_dropped=%s)",
                    strategy_id,
                    len(self.strategy_tickers[strategy_id]),
                    "true" if had_policy else "false",
                )

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
        log.info(
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

    # 🔸 Пересчёт min(deposit) среди текущих winners (по in-memory meta)
    async def _recalc_min_deposit_locked(self):
        min_dep: Optional[Decimal] = None
        for sid in self.trader_winners:
            dep = _as_decimal(self.strategy_meta.get(sid, {}).get("deposit"))
            if dep is not None and dep > 0 and (min_dep is None or dep < min_dep):
                min_dep = dep
        self.trader_winners_min_deposit = min_dep

    # 🔸 Внутренний загрузчик политик для набора стратегий (держать self._lock)
    async def _load_policies_for_sids_locked(self, sids: List[int]) -> Tuple[Dict[int, dict], int]:
        # начальная инициализация
        policies: Dict[int, dict] = {}
        total_levels = 0

        # подготовка «скелетов» политик из уже загруженных стратегий/тикеров
        for sid in sids:
            srow = self.strategies.get(sid, {})
            initial_sl = None
            # условия достаточности начального SL
            if srow and srow.get("use_stoploss") and srow.get("sl_type") in ("percent", "atr"):
                sl_val = _as_decimal(srow.get("sl_value"))
                if sl_val is not None and sl_val > 0:
                    initial_sl = {"mode": srow.get("sl_type"), "value": sl_val}
            policies[sid] = {
                "strategy": dict(srow) if srow else {},
                "tickers": set(self.strategy_tickers.get(sid, set())),
                "initial_sl": initial_sl,
                "tp_levels": [],
            }

        # если нечего грузить — вернуть пустой результат
        if not sids:
            return policies, total_levels

        # загрузить TP-линейки
        tp_rows = await infra.pg_pool.fetch(
            """
            SELECT id AS tp_level_id, strategy_id, level, tp_type, tp_value, volume_percent
            FROM strategy_tp_levels_v4
            WHERE strategy_id = ANY($1::int[])
            ORDER BY strategy_id, level
            """,
            sids,
        )

        # загрузить SL-политику по TP
        sl_rows = await infra.pg_pool.fetch(
            """
            SELECT strategy_id, tp_level_id, sl_mode, sl_value
            FROM strategy_tp_sl_v4
            WHERE strategy_id = ANY($1::int[])
            """,
            sids,
        )

        # построить быстрый маппер (sid, tp_level_id) -> (sl_mode, sl_value)
        sl_map: Dict[Tuple[int, int], Tuple[Optional[str], Optional[Decimal]]] = {}
        for r in sl_rows:
            key = (int(r["strategy_id"]), int(r["tp_level_id"]))
            sl_map[key] = (r["sl_mode"], _as_decimal(r["sl_value"]))

        # собрать уровни по стратегиям
        for r in tp_rows:
            sid = int(r["strategy_id"])
            lvl_id = int(r["tp_level_id"])
            # подтянуть sl_mode/sl_value, если есть
            sl_mode, sl_value = sl_map.get((sid, lvl_id), (None, None))
            # сформировать элемент уровня
            level_item = {
                "level": int(r["level"]),
                "tp_type": r["tp_type"],
                "tp_value": _as_decimal(r["tp_value"]),
                "volume_percent": _as_decimal(r["volume_percent"]),
                "sl_mode": sl_mode,
                "sl_value": sl_value,
            }
            # добавить к политике
            policies.setdefault(sid, {
                "strategy": dict(self.strategies.get(sid, {})),
                "tickers": set(self.strategy_tickers.get(sid, set())),
                "initial_sl": policies.get(sid, {}).get("initial_sl"),
                "tp_levels": [],
            })
            policies[sid]["tp_levels"].append(level_item)
            total_levels += 1

        # финальный лог по одному sid (при точечной загрузке)
        if len(sids) == 1:
            sid = sids[0]
            log.info(
                "📥 Загрузка политики: id=%d, levels=%d, tickers=%d, initial_sl=%s",
                sid,
                len(policies.get(sid, {}).get("tp_levels", [])),
                len(policies.get(sid, {}).get("tickers", set())),
                "yes" if policies.get(sid, {}).get("initial_sl") else "no",
            )

        return policies, total_levels


# 🔸 Глобальный объект конфигурации
config = TraderConfigState()


# 🔸 Первичная инициализация конфигурации (без загрузки политик)
async def init_trader_config_state():
    await config.reload_all()
    log.info("✅ Конфигурация трейдера загружена (без политик TP/SL)")


# 🔸 Слушатель Pub/Sub для онлайновых апдейтов (тикеры/стратегии)
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
                sid_raw = data.get("id")
                sid = int(sid_raw) if sid_raw is not None else None
                action = data.get("action")
                # единая точка реакции — перечитать стратегию, winners и политику
                if sid is not None:
                    await config.on_strategy_changed(sid)
                log.info("♻️ Обработано событие стратегии: id=%s (%s)", sid_raw, action)

        except Exception:
            # логируем и продолжаем слушать дальше
            log.exception("❌ Ошибка обработки события Pub/Sub")

# 🔸 Слушатель стрима состояния стратегий (двухфазный applied)
async def strategy_state_listener():
    redis = infra.redis_client

    # создание CG (id="$" — только новые записи)
    try:
        await redis.xgroup_create(STRATEGY_STATE_STREAM, STATE_CG, id="$", mkstream=True)
        log.info("📡 Создана группа %s для стрима %s", STATE_CG, STRATEGY_STATE_STREAM)
    except Exception:
        # группа уже существует
        pass

    # сброс offset CG на '$' — читать строго только новые записи после старта
    try:
        await redis.execute_command("XGROUP", "SETID", STRATEGY_STATE_STREAM, STATE_CG, "$")
        log.info("⏩ Группа %s для %s сброшена на $ (только новые)", STATE_CG, STRATEGY_STATE_STREAM)
    except Exception:
        log.exception("❌ Не удалось сбросить CG %s для %s на $", STATE_CG, STRATEGY_STATE_STREAM)

    # чтение из стрима в вечном цикле
    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=STATE_CG,
                consumername=STATE_CONSUMER,
                streams={STRATEGY_STATE_STREAM: ">"},
                count=200,
                block=1000,  # мс
            )
            # обработка пакета записей
            if not entries:
                continue

            for _, records in entries:
                for entry_id, fields in records:
                    try:
                        # ожидаемый формат: {'type': 'strategy', 'action': 'applied', 'id': '<sid>'}
                        if fields.get("type") == "strategy" and fields.get("action") == "applied":
                            sid_raw = fields.get("id")
                            sid = int(sid_raw) if sid_raw is not None else None
                            if sid is not None:
                                await config.on_strategy_changed(sid)
                                log.info("✅ Обработан applied из state-stream: id=%s", sid_raw)
                        # ack после успешной обработки
                        await redis.xack(STRATEGY_STATE_STREAM, STATE_CG, entry_id)
                    except Exception:
                        # оставим запись в pending — будет повтор
                        log.exception("❌ Ошибка обработки записи state-stream (id=%s)", entry_id)
        except Exception:
            log.exception("❌ Ошибка чтения из state-stream")
            # короткая пауза перед повтором
            await asyncio.sleep(1)

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