# ind_live_config.py — конфигурация ind_live_v4: активные тикеры, инстансы индикаторов (m5/m15/h1), стратегии (market_watcher), L1-кэш live-значений; подписка на Pub/Sub

# 🔸 Импорты
import asyncio
import json
import logging
import time
from typing import Dict, Tuple, Optional, Set, Any, List

# 🔸 Логгер
log = logging.getLogger("IND_LIVE_CONFIG")


# 🔸 Константы БД и каналов
BB_TICKERS_TABLE      = "tickers_bb"
IND_INSTANCES_TABLE   = "indicator_instances_v4"
IND_PARAMS_TABLE      = "indicator_parameters_v4"
STRATEGIES_TABLE      = "strategies_v4"

PUBSUB_TICKERS        = "tickers_v4_events"        # включение/выключение тикеров
PUBSUB_INDICATORS     = "indicators_v4_events"     # включение/выключение индикаторов
PUBSUB_STRATEGIES     = "strategies_v4_events"     # изменения стратегий (enabled/archived/market_watcher)


# 🔸 Вспомогательное: монотонное время (для L1 TTL)
def _mono() -> float:
    return time.monotonic()


# 🔸 L1-кэш live-значений индикаторов на текущем баре
class LiveCache:
    # структура записи: {(symbol, tf): {"bar_open_ms": int, "expires_at": float, "values": dict[param->str]}}
    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}

    async def set(self, symbol: str, tf: str, bar_open_ms: int, values: Dict[str, str], ttl_sec: int = 90) -> None:
        key = (symbol, tf)
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            self._store[key] = {
                "bar_open_ms": int(bar_open_ms),
                "expires_at": _mono() + float(ttl_sec),
                "values": dict(values) if values else {},
            }

    async def get(
        self,
        symbol: str,
        tf: str,
        needed: Optional[Set[str]],
        expect_bar_open_ms: int
    ) -> Optional[Dict[str, str]]:
        key = (symbol, tf)
        rec = self._store.get(key)
        if not rec:
            return None
        if _mono() > float(rec.get("expires_at", 0)):
            self._store.pop(key, None)
            return None
        if int(rec.get("bar_open_ms", -1)) != int(expect_bar_open_ms):
            return None
        vals: Dict[str, str] = rec.get("values", {})  # type: ignore[assignment]
        if not vals:
            return None
        if needed is None:
            return dict(vals)
        if not needed.issubset(vals.keys()):
            return None
        return {k: vals[k] for k in needed}

    def purge_expired(self) -> int:
        now = _mono()
        removed = 0
        for key in list(self._store.keys()):
            if now > float(self._store[key].get("expires_at", 0)):
                self._store.pop(key, None)
                removed += 1
        return removed


# 🔸 Главный конфиг ind_live_v4
class IndLiveConfig:
    def __init__(self, pg, redis) -> None:
        self.pg = pg
        self.redis = redis

        # в памяти: активные тикеры и инстансы
        self.active_tickers: Dict[str, int] = {}                 # symbol -> precision_price
        self.indicator_instances: Dict[int, Dict[str, Any]] = {} # id -> {indicator,timeframe,params,enabled_at}

        # стратегии: id -> market_watcher (только enabled & not archived)
        self.active_strategies: Dict[int, bool] = {}

        # L1-кэш
        self.live_cache = LiveCache()

    # 🔸 Геттеры (для воркеров)
    def get_active_symbols(self) -> List[str]:
        return list(self.active_tickers.keys())

    def get_precision(self, symbol: str) -> int:
        return int(self.active_tickers.get(symbol, 8))

    def get_instances_by_tf(self, tf: str) -> List[Dict[str, Any]]:
        return [
            {
                "id": iid,
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "enabled_at": inst.get("enabled_at"),
                "params": inst["params"],
            }
            for iid, inst in self.indicator_instances.items()
            if inst["timeframe"] == tf
        ]

    def get_strategy_mw(self, strategy_id: int) -> bool:
        return bool(self.active_strategies.get(int(strategy_id), False))

    # 🔸 Инициализация: одноразовая загрузка тикеров, инстансов и стратегий из БД
    async def initialize(self) -> None:
        await self._load_initial_tickers()
        await self._load_initial_indicators()
        await self._load_initial_strategies()
        log.info(
            f"CONFIG INIT: symbols={len(self.active_tickers)} instances={len(self.indicator_instances)} "
            f"strategies={len(self.active_strategies)}"
        )

    # 🔸 Загрузка активных тикеров (enabled & tradepermission)
    async def _load_initial_tickers(self) -> None:
        log_init = logging.getLogger("CONFIG_INIT")
        async with self.pg.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT symbol, precision_price
                FROM {BB_TICKERS_TABLE}
                WHERE status = 'enabled' AND tradepermission = 'enabled'
            """)
        self.active_tickers.clear()
        for r in rows:
            sym = r["symbol"]
            prec = int(r["precision_price"]) if r["precision_price"] is not None else 8
            self.active_tickers[sym] = prec
            log_init.debug(f"Ticker ON: {sym} → precision={prec}")
        log_init.info(f"Loaded active tickers: {len(self.active_tickers)}")

    # 🔸 Загрузка активных инстансов (все TF, фильтрация TF снаружи при использовании)
    async def _load_initial_indicators(self) -> None:
        log_init = logging.getLogger("CONFIG_INIT")
        async with self.pg.acquire() as conn:
            instances = await conn.fetch(f"""
                SELECT id, indicator, timeframe, enabled_at
                FROM {IND_INSTANCES_TABLE}
                WHERE enabled = true
                  AND timeframe IN ('m5','m15','h1')
            """)
            id_list = [int(x["id"]) for x in instances]
            params_by_id: Dict[int, Dict[str, Any]] = {}
            for inst_id in id_list:
                params = await conn.fetch(f"""
                    SELECT param, value
                    FROM {IND_PARAMS_TABLE}
                    WHERE instance_id = $1
                """, inst_id)
                params_by_id[inst_id] = {p["param"]: p["value"] for p in params}

        self.indicator_instances.clear()
        for inst in instances:
            iid = int(inst["id"])
            self.indicator_instances[iid] = {
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "params": params_by_id.get(iid, {}),
                "enabled_at": inst["enabled_at"],
            }
            log_init.debug(f"Indicator ON: id={inst['id']} {inst['indicator']} {params_by_id.get(iid, {})}")
        log_init.info(f"Loaded active indicator instances: {len(self.indicator_instances)}")

    # 🔸 Загрузка стратегий (enabled & not archived) с полем market_watcher
    async def _load_initial_strategies(self) -> None:
        log_init = logging.getLogger("CONFIG_INIT")
        async with self.pg.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT id, COALESCE(market_watcher, false) AS market_watcher
                FROM {STRATEGIES_TABLE}
                WHERE enabled = true AND archived = false
            """)
        self.active_strategies.clear()
        for r in rows:
            sid = int(r["id"])
            mw = bool(r["market_watcher"])
            self.active_strategies[sid] = mw
            log_init.debug(f"Strategy ON: id={sid} market_watcher={mw}")
        log_init.info(f"Loaded active strategies: {len(self.active_strategies)}")

    # 🔸 Подписка на события тикеров (Pub/Sub: tickers_v4_events)
    async def run_ticker_events(self) -> None:
        log_t = logging.getLogger("CFG_TICKERS")
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(PUBSUB_TICKERS)
        log_t.debug(f"Subscribed to {PUBSUB_TICKERS}")

        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            try:
                data = json.loads(msg["data"])
            except Exception:
                continue

            symbol = data.get("symbol")
            if not symbol:
                continue

            status = str(data.get("status") or "").lower()
            tradepermission = str(data.get("tradepermission") or "").lower()
            action = str(data.get("action") or "").lower()

            should_enable = False
            if action in ("enable", "enabled", "true", "on"):
                should_enable = True
            elif status == "enabled" and tradepermission == "enabled":
                should_enable = True

            try:
                if should_enable:
                    prec = data.get("precision_price")
                    if prec is None:
                        async with self.pg.acquire() as conn:
                            row = await conn.fetchrow(f"""
                                SELECT precision_price
                                FROM {BB_TICKERS_TABLE}
                                WHERE symbol = $1
                            """, symbol)
                            prec = row["precision_price"] if row and row["precision_price"] is not None else 8
                    self.active_tickers[symbol] = int(prec)
                    log_t.debug(f"Ticker ON: {symbol} → precision={prec}")
                else:
                    if self.active_tickers.pop(symbol, None) is not None:
                        log_t.debug(f"Ticker OFF: {symbol}")
            except Exception as e:
                log_t.warning(f"Ticker event error for {symbol}: {e}", exc_info=True)

    # 🔸 Подписка на события индикаторов (Pub/Sub: indicators_v4_events)
    async def run_indicator_events(self) -> None:
        log_i = logging.getLogger("CFG_INDI")
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(PUBSUB_INDICATORS)
        log_i.debug(f"Subscribed to {PUBSUB_INDICATORS}")

        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            try:
                data = json.loads(msg["data"])
            except Exception:
                continue

            try:
                iid = int(data.get("id"))
            except Exception:
                continue

            ev_type = data.get("type")
            action = str(data.get("action") or "").lower()

            if ev_type != "enabled":
                continue

            try:
                if action in ("true", "enable", "enabled", "on"):
                    async with self.pg.acquire() as conn:
                        row = await conn.fetchrow(f"""
                            SELECT id, indicator, timeframe, enabled_at
                            FROM {IND_INSTANCES_TABLE}
                            WHERE id = $1
                        """, iid)
                        if not row:
                            self.indicator_instances.pop(iid, None)
                            continue

                        if row["timeframe"] not in ("m5", "m15", "h1"):
                            self.indicator_instances.pop(iid, None)
                            continue

                        params = await conn.fetch(f"""
                            SELECT param, value
                            FROM {IND_PARAMS_TABLE}
                            WHERE instance_id = $1
                        """, iid)
                        param_map = {p["param"]: p["value"] for p in params}

                    self.indicator_instances[iid] = {
                        "indicator": row["indicator"],
                        "timeframe": row["timeframe"],
                        "params": param_map,
                        "enabled_at": row["enabled_at"],
                    }
                    log_i.debug(f"Indicator ON: id={iid} {row['indicator']} {param_map}")
                else:
                    if self.indicator_instances.pop(iid, None) is not None:
                        log_i.debug(f"Indicator OFF: id={iid}")
            except Exception as e:
                log_i.warning(f"Indicator event error id={iid}: {e}", exc_info=True)

    # 🔸 Подписка на события стратегий (Pub/Sub: strategies_v4_events), гибкий парсер (поддержка action:true/false)
    async def run_strategy_events(self) -> None:
        log_s = logging.getLogger("CFG_STRAT")
        pubsub = self.redis.pubsub()
        try:
            await pubsub.subscribe(PUBSUB_STRATEGIES)
            log_s.debug(f"Subscribed to {PUBSUB_STRATEGIES}")
        except Exception as e:
            log_s.warning(f"Subscribe error {PUBSUB_STRATEGIES}: {e}")

        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue

            # парсим JSON; при ошибке — просто игнорируем это событие (не ломаем цикл)
            try:
                data = json.loads(msg["data"])
            except Exception:
                log_s.debug("CFG_STRAT: bad json, ignore")
                continue

            sid_raw = data.get("id")
            try:
                sid = int(sid_raw) if sid_raw is not None else None
            except Exception:
                sid = None

            # допускаем разные форматы событий
            action  = str(data.get("action") or "").lower()
            mw_val  = data.get("market_watcher")
            enabled = data.get("enabled")
            archived = data.get("archived")

            try:
                if sid is None:
                    # событие без id — мягко перечитаем все стратегии
                    await self._load_initial_strategies()
                    log_s.debug("CFG_STRAT: fallback full reload (no id)")
                    continue

                # прямое обновление market_watcher
                if mw_val is not None:
                    self.active_strategies[int(sid)] = bool(mw_val)
                    log_s.info(f"Strategy MW update: id={sid} → {bool(mw_val)}")
                    continue

                # включение/выключение по action: true/false или on/off/enable/disable
                if action in ("true", "on", "enable", "enabled"):
                    async with self.pg.acquire() as conn:
                        row = await conn.fetchrow(f"""
                            SELECT id, COALESCE(market_watcher,false) AS mw
                            FROM {STRATEGIES_TABLE}
                            WHERE id=$1 AND enabled=true AND archived=false
                        """, int(sid))
                    if row:
                        self.active_strategies[int(row["id"])] = bool(row["mw"])
                        log_s.info(f"Strategy ON: id={sid} market_watcher={bool(row['mw'])}")
                    else:
                        if self.active_strategies.pop(int(sid), None) is not None:
                            log_s.info(f"Strategy OFF (by db): id={sid}")
                    continue

                if action in ("false", "off", "disable", "disabled") or (enabled is not None) or (archived is not None):
                    async with self.pg.acquire() as conn:
                        row = await conn.fetchrow(f"""
                            SELECT id, COALESCE(market_watcher,false) AS mw
                            FROM {STRATEGIES_TABLE}
                            WHERE id=$1 AND enabled=true AND archived=false
                        """, int(sid))
                    if row:
                        self.active_strategies[int(row["id"])] = bool(row["mw"])
                        log_s.info(f"Strategy ON: id={sid} market_watcher={bool(row['mw'])}")
                    else:
                        if self.active_strategies.pop(int(sid), None) is not None:
                            log_s.info(f"Strategy OFF: id={sid}")
                    continue

                # непонятный формат — мягкая полная перечитка
                await self._load_initial_strategies()
                log_s.debug(f"CFG_STRAT: fallback full reload (unhandled event) data={data}")

            except Exception as e:
                log_s.warning(f"Strategy event error id={sid}: {e}", exc_info=True)