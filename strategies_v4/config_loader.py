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
                log.info(f"🔄 Тикер обновлён: {symbol}")
            else:
                self.tickers.pop(symbol, None)
                log.info(f"❌ Тикер удалён (не найден): {symbol}")

    # 🔸 Удаление тикера
    async def remove_ticker(self, symbol: str):
        async with self._lock:
            self.tickers.pop(symbol, None)
            log.info(f"🗑️ Тикер удалён: {symbol}")

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
                log.info(f"🗑️ Стратегия удалена: id={strategy_id}")
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

            log.info(f"🔄 Стратегия обновлена: [id={strategy_id}] {strategy['human_name']}")

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
            log.info(f"🗑️ Стратегия удалена: id={strategy_id}")

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
    log.info("✅ Конфигурация инициализирована")

# 🔸 Слушатель событий из Redis Pub/Sub
async def config_event_listener():
    redis = infra.redis_client
    pubsub = redis.pubsub()
    await pubsub.subscribe("tickers_v4_events", "strategies_v4_events")

    log.info("📡 Подписка на каналы Redis запущена")

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
        log.info(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Группа {group} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    log.info(f"📥 Подписка на поток обновлений стратегий: {stream} → {group}")

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
            
# 🔸 Глобальный кэш
_entry_whitelist = {
    "long": {"snapshots": [], "patterns": []},
    "short": {"snapshots": [], "patterns": []}
}

# 🔸 Геттер
def get_entry_whitelist() -> dict:
    return _entry_whitelist

# 🔸 Обновление кэша entry whitelist (раз в 2 минуты)
log = logging.getLogger("ENTRY_WHITELIST")

async def entry_whitelist_refresher_loop():
    while True:
        try:
            log.debug("🔄 Обновление entry whitelist...")
            result = await _load_entry_whitelist()
            _entry_whitelist["long"] = result["long"]
            _entry_whitelist["short"] = result["short"]
            log.info("✅ Entry whitelist обновлён")

            log.debug(f"📌 long.snapshots: {sorted(result['long']['snapshots'])}")
            log.debug(f"📌 long.patterns:  {sorted(result['long']['patterns'])}")
            log.debug(f"📌 short.snapshots: {sorted(result['short']['snapshots'])}")
            log.debug(f"📌 short.patterns:  {sorted(result['short']['patterns'])}")

        except Exception:
            log.exception("❌ Ошибка обновления entry whitelist")

        await asyncio.sleep(120)

# 🔸 Запрос в БД и построение кэша
async def _load_entry_whitelist() -> dict:
    pool = infra.pg_pool
    if not pool:
        raise RuntimeError("❌ PostgreSQL пул не инициализирован")

    result = {
        "long": {"snapshots": [], "patterns": []},
        "short": {"snapshots": [], "patterns": []}
    }

    try:
        async with pool.acquire() as conn:
            rows_snapshots = await conn.fetch("""
                SELECT direction, emasnapshot_dict_id AS id, num_trades, winrate
                FROM positions_emasnapshot_m5_stat
                WHERE direction IN ('long', 'short')
            """)

            rows_patterns = await conn.fetch("""
                SELECT direction, pattern_id AS id, num_trades, winrate
                FROM positions_emapattern_m5_stat
                WHERE direction IN ('long', 'short')
            """)

            for direction in ["long", "short"]:
                for rows, key in [
                    (rows_snapshots, "snapshots"),
                    (rows_patterns, "patterns")
                ]:
                    grouped: dict[int, dict] = {}

                    for r in rows:
                        if r["direction"] != direction:
                            continue

                        _id = r["id"]
                        grouped.setdefault(_id, {"total_trades": 0, "weighted_sum": 0.0})

                        grouped[_id]["total_trades"] += r["num_trades"]
                        grouped[_id]["weighted_sum"] += r["winrate"] * r["num_trades"]

                    enriched = []
                    for _id, stats in grouped.items():
                        total = stats["total_trades"]
                        if total == 0:
                            continue
                        weighted_winrate = stats["weighted_sum"] / total
                        enriched.append((_id, total, weighted_winrate))

                    enriched.sort(key=lambda x: -x[1])  # по убыванию total_trades
                    cutoff = int(len(enriched) * 0.75)
                    top_ids = [eid for (eid, _, win) in enriched[:cutoff] if win > 0.6]
                    result[direction][key] = top_ids

    except Exception:
        log.exception("❌ Ошибка загрузки whitelist из БД")
        raise

    return result