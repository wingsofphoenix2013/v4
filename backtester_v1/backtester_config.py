# backtester_config.py — конфигурация и кеш метаданных для backtester_v1

import logging
from typing import Dict, Any, List, Optional

# 🔸 Логгер модуля
log = logging.getLogger("BT_CONFIG")

# 🔸 Глобальные кеши тикеров, индикаторов, псевдо-сигналов, сценариев и анализаторов
bt_tickers: Dict[str, Dict[str, Any]] = {}                 # symbol -> {fields}
bt_indicator_instances: Dict[int, Dict[str, Any]] = {}     # instance_id -> {indicator, timeframe, enabled_at, params}
bt_signal_instances: Dict[int, Dict[str, Any]] = {}        # signal_id -> {key, name, timeframe, mode, backfill_days, type, enabled, params}
bt_scenarios: Dict[int, Dict[str, Any]] = {}               # scenario_id -> {key, name, type, enabled, created_at, params}
bt_scenario_signal_links: List[Dict[str, Any]] = []        # элементы: {id, scenario_id, signal_id, enabled, created_at}

bt_analysis_instances: Dict[int, Dict[str, Any]] = {}      # analysis_id -> {family_key, key, name, enabled, params}
bt_analysis_connections: List[Dict[str, Any]] = []         # элементы: {id, scenario_id, signal_id, analysis_id, enabled, created_at, updated_at}

# 🔸 Загрузка активных тикеров (status = enabled, tradepermission = enabled)
async def load_initial_tickers(pg) -> int:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                symbol,
                status,
                tradepermission,
                is_active,
                precision_price,
                precision_qty,
                min_qty,
                ticksize,
                activated_at
            FROM tickers_bb
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            """
        )

    bt_tickers.clear()

    for r in rows:
        symbol = r["symbol"]
        bt_tickers[symbol] = {
            "status": r["status"],
            "tradepermission": r["tradepermission"],
            "is_active": r["is_active"],
            "precision_price": r["precision_price"],
            "precision_qty": r["precision_qty"],
            "min_qty": r["min_qty"],
            "ticksize": r["ticksize"],
            "activated_at": r["activated_at"],
        }

    count = len(bt_tickers)
    log.info(f"BT_CONFIG: загружено активных тикеров: {count}")
    return count


# 🔸 Загрузка включённых инстансов индикаторов и их параметров
async def load_initial_indicators(pg, timeframes: Optional[List[str]] = None) -> int:
    async with pg.acquire() as conn:
        if timeframes:
            rows = await conn.fetch(
                """
                SELECT id, indicator, timeframe, enabled_at
                FROM indicator_instances_v4
                WHERE enabled = true
                  AND timeframe = ANY($1::text[])
                """,
                timeframes,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, indicator, timeframe, enabled_at
                FROM indicator_instances_v4
                WHERE enabled = true
                """
            )

        instances: Dict[int, Dict[str, Any]] = {}

        for r in rows:
            iid = r["id"]

            # грузим параметры конкретного инстанса индикатора
            params_rows = await conn.fetch(
                """
                SELECT param, value
                FROM indicator_parameters_v4
                WHERE instance_id = $1
                """,
                iid,
            )
            params = {p["param"]: p["value"] for p in params_rows}

            instances[iid] = {
                "id": iid,
                "indicator": r["indicator"],
                "timeframe": r["timeframe"],
                "enabled_at": r["enabled_at"],
                "params": params,
            }

    bt_indicator_instances.clear()
    bt_indicator_instances.update(instances)

    count = len(bt_indicator_instances)
    log.info(f"BT_CONFIG: загружено инстансов индикаторов: {count}")
    return count

# 🔸 Загрузка инстансов псевдо-сигналов и их параметров
async def load_initial_signals(
    pg,
    timeframes: Optional[List[str]] = None,
    only_enabled: bool = True,
) -> int:
    async with pg.acquire() as conn:
        conditions: List[str] = []
        params: List[Any] = []

        # формируем динамический WHERE
        if only_enabled:
            conditions.append("enabled = true")
        if timeframes:
            conditions.append("timeframe = ANY($1::text[])")
            params.append(timeframes)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        rows = await conn.fetch(
            f"""
            SELECT
                id,
                key,
                name,
                timeframe,
                mode,
                backfill_days,
                type,
                enabled,
                created_at,
                updated_at
            FROM bt_signals_instances
            {where_clause}
            """,
            *params,
        )

        # готовим структуру сигналов
        signals: Dict[int, Dict[str, Any]] = {}
        signal_ids: List[int] = []

        for r in rows:
            sid = r["id"]
            signal_ids.append(sid)
            signals[sid] = {
                "id": sid,
                "key": r["key"],
                "name": r["name"],
                "timeframe": r["timeframe"],
                "mode": r["mode"],
                "backfill_days": r["backfill_days"],
                "type": r["type"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "params": {},  # заполним ниже
            }

        if not signal_ids:
            bt_signal_instances.clear()
            log.info("BT_CONFIG: инстансов псевдо-сигналов не найдено (фильтр применён)")
            return 0

        # загрузка параметров для всех выбранных сигналов
        params_rows = await conn.fetch(
            """
            SELECT signal_id, param_type, param_name, param_value
            FROM bt_signals_parameters
            WHERE signal_id = ANY($1::int[])
            """,
            signal_ids,
        )

    # наполняем params внутри каждого сигнала
    for p in params_rows:
        sid = p["signal_id"]
        if sid not in signals:
            continue
        signal = signals[sid]
        signal_params = signal.setdefault("params", {})
        param_name = p["param_name"]
        signal_params[param_name] = {
            "type": p["param_type"],
            "value": p["param_value"],
        }

    bt_signal_instances.clear()
    bt_signal_instances.update(signals)

    count = len(bt_signal_instances)
    log.info(
        "BT_CONFIG: загружено инстансов псевдо-сигналов: %s",
        count,
    )
    return count

# 🔸 Загрузка инстансов сценариев и их параметров
async def load_initial_scenarios(pg) -> int:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, key, name, type, enabled, created_at
            FROM bt_scenario_instances
            WHERE enabled = true
            """
        )

        scenarios: Dict[int, Dict[str, Any]] = {}
        scenario_ids: List[int] = []

        for r in rows:
            sid = r["id"]
            scenario_ids.append(sid)
            scenarios[sid] = {
                "id": sid,
                "key": r["key"],
                "name": r["name"],
                "type": r["type"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
                "params": {},  # заполним ниже
            }

        if scenario_ids:
            params_rows = await conn.fetch(
                """
                SELECT scenario_id, param_name, param_type, param_value
                FROM bt_scenario_parameters
                WHERE scenario_id = ANY($1::int[])
                """,
                scenario_ids,
            )
        else:
            params_rows = []

    # наполняем params внутри каждого сценария
    for p in params_rows:
        sid = p["scenario_id"]
        if sid not in scenarios:
            continue
        scenario = scenarios[sid]
        scenario_params = scenario.setdefault("params", {})
        param_name = p["param_name"]
        scenario_params[param_name] = {
            "type": p["param_type"],
            "value": p["param_value"],
        }

    bt_scenarios.clear()
    bt_scenarios.update(scenarios)

    count = len(bt_scenarios)
    log.info(f"BT_CONFIG: загружено инстансов сценариев: {count}")
    return count


# 🔸 Загрузка связок сценарий ↔ псевдо-сигнал
async def load_initial_scenario_signals(pg, only_enabled: bool = True) -> int:
    async with pg.acquire() as conn:
        if only_enabled:
            # берём только те связки, где:
            # - связка enabled = true
            # - сам сценарий enabled = true
            rows = await conn.fetch(
                """
                SELECT s.id, s.scenario_id, s.signal_id, s.enabled, s.created_at
                FROM bt_scenario_signals s
                JOIN bt_scenario_instances si
                  ON si.id = s.scenario_id
                WHERE s.enabled = true
                  AND si.enabled = true
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, scenario_id, signal_id, enabled, created_at
                FROM bt_scenario_signals
                """
            )

    links: List[Dict[str, Any]] = []
    for r in rows:
        links.append(
            {
                "id": r["id"],
                "scenario_id": r["scenario_id"],
                "signal_id": r["signal_id"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
            }
        )

    bt_scenario_signal_links.clear()
    bt_scenario_signal_links.extend(links)

    count = len(bt_scenario_signal_links)
    log.info(f"BT_CONFIG: загружено связок сценарий-сигнал: {count}")
    return count


# 🔸 Загрузка инстансов анализаторов и их параметров
async def load_initial_analysis_instances(pg, only_enabled: bool = True) -> int:
    async with pg.acquire() as conn:
        conditions: List[str] = []
        params: List[Any] = []

        if only_enabled:
            conditions.append("enabled = true")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        rows = await conn.fetch(
            f"""
            SELECT
                id,
                family_key,
                key,
                name,
                enabled,
                created_at,
                updated_at
            FROM bt_analysis_instances
            {where_clause}
            """,
            *params,
        )

        analyses: Dict[int, Dict[str, Any]] = {}
        analysis_ids: List[int] = []

        for r in rows:
            aid = r["id"]
            analysis_ids.append(aid)
            analyses[aid] = {
                "id": aid,
                "family_key": r["family_key"],
                "key": r["key"],
                "name": r["name"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "params": {},  # заполним ниже
            }

        if analysis_ids:
            params_rows = await conn.fetch(
                """
                SELECT analysis_id, param_name, param_type, param_value
                FROM bt_analysis_parameters
                WHERE analysis_id = ANY($1::int[])
                """,
                analysis_ids,
            )
        else:
            params_rows = []

    # наполняем params внутри каждого анализатора
    for p in params_rows:
        aid = p["analysis_id"]
        if aid not in analyses:
            continue
        analysis = analyses[aid]
        analysis_params = analysis.setdefault("params", {})
        param_name = p["param_name"]
        analysis_params[param_name] = {
            "type": p["param_type"],
            "value": p["param_value"],
        }

    bt_analysis_instances.clear()
    bt_analysis_instances.update(analyses)

    count = len(bt_analysis_instances)
    log.info(f"BT_CONFIG: загружено инстансов анализаторов: {count}")
    return count


# 🔸 Загрузка связок сценарий ↔ сигнал ↔ анализатор
async def load_initial_analysis_connections(pg, only_enabled: bool = True) -> int:
    async with pg.acquire() as conn:
        if only_enabled:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    scenario_id,
                    signal_id,
                    analysis_id,
                    enabled,
                    created_at,
                    updated_at
                FROM bt_analysis_connections
                WHERE enabled = true
                """
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    scenario_id,
                    signal_id,
                    analysis_id,
                    enabled,
                    created_at,
                    updated_at
                FROM bt_analysis_connections
                """
            )

    links: List[Dict[str, Any]] = []
    for r in rows:
        links.append(
            {
                "id": r["id"],
                "scenario_id": r["scenario_id"],
                "signal_id": r["signal_id"],
                "analysis_id": r["analysis_id"],
                "enabled": r["enabled"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
            }
        )

    bt_analysis_connections.clear()
    bt_analysis_connections.extend(links)

    count = len(bt_analysis_connections)
    log.info(f"BT_CONFIG: загружено связок сценарий-сигнал-анализатор: {count}")
    return count


# 🔸 Геттеры для тикеров
def get_all_ticker_symbols() -> List[str]:
    return list(bt_tickers.keys())


def get_ticker_info(symbol: str) -> Optional[Dict[str, Any]]:
    return bt_tickers.get(symbol)


def get_ticker_precision(symbol: str) -> int:
    ticker = bt_tickers.get(symbol)
    if not ticker:
        return 8
    return int(ticker.get("precision_price") or 8)


# 🔸 Геттеры для инстансов индикаторов
def get_all_indicator_instances() -> Dict[int, Dict[str, Any]]:
    return bt_indicator_instances


def get_indicator_instance(instance_id: int) -> Optional[Dict[str, Any]]:
    return bt_indicator_instances.get(instance_id)


def get_indicator_instances_by_timeframe(timeframe: str) -> List[Dict[str, Any]]:
    return [
        inst for inst in bt_indicator_instances.values()
        if inst.get("timeframe") == timeframe
    ]


# 🔸 Геттеры для инстансов псевдо-сигналов
def get_all_signal_instances() -> Dict[int, Dict[str, Any]]:
    return bt_signal_instances


def get_signal_instance(signal_id: int) -> Optional[Dict[str, Any]]:
    return bt_signal_instances.get(signal_id)


def get_signal_instances_by_timeframe(timeframe: str) -> List[Dict[str, Any]]:
    return [
        s for s in bt_signal_instances.values()
        if s.get("timeframe") == timeframe
    ]


def get_enabled_signals() -> List[Dict[str, Any]]:
    return [
        s for s in bt_signal_instances.values()
        if s.get("enabled")
    ]

# 🔸 Геттеры для сценариев и связок сценарий ↔ сигнал
def get_all_scenarios() -> Dict[int, Dict[str, Any]]:
    return bt_scenarios


def get_scenario_instance(scenario_id: int) -> Optional[Dict[str, Any]]:
    return bt_scenarios.get(scenario_id)


def get_all_scenario_signal_links() -> List[Dict[str, Any]]:
    return bt_scenario_signal_links


def get_scenario_signal_links_for_signal(signal_id: int) -> List[Dict[str, Any]]:
    return [
        link for link in bt_scenario_signal_links
        if link.get("signal_id") == signal_id
    ]


def get_scenario_signal_links_for_scenario(scenario_id: int) -> List[Dict[str, Any]]:
    return [
        link for link in bt_scenario_signal_links
        if link.get("scenario_id") == scenario_id
    ]


def get_scenarios_for_signal(signal_id: int) -> List[Dict[str, Any]]:
    # сценарии, привязанные к данному сигналу
    scenario_ids = {
        link["scenario_id"]
        for link in bt_scenario_signal_links
        if link.get("signal_id") == signal_id
    }
    return [
        bt_scenarios[sid]
        for sid in scenario_ids
        if sid in bt_scenarios
    ]


def get_signals_for_scenario(scenario_id: int) -> List[int]:
    # signal_id, привязанные к данному сценарию
    return [
        link["signal_id"]
        for link in bt_scenario_signal_links
        if link.get("scenario_id") == scenario_id
    ]


# 🔸 Гetterы для анализаторов и связок сценарий ↔ сигнал ↔ анализатор
def get_all_analysis_instances() -> Dict[int, Dict[str, Any]]:
    return bt_analysis_instances


def get_analysis_instance(analysis_id: int) -> Optional[Dict[str, Any]]:
    return bt_analysis_instances.get(analysis_id)


def get_enabled_analysis_instances() -> List[Dict[str, Any]]:
    return [
        a for a in bt_analysis_instances.values()
        if a.get("enabled")
    ]


def get_all_analysis_connections() -> List[Dict[str, Any]]:
    return bt_analysis_connections


def get_analysis_connections_for_scenario_signal(
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    return [
        link for link in bt_analysis_connections
        if link.get("scenario_id") == scenario_id and link.get("signal_id") == signal_id
    ]


def get_analysis_ids_for_scenario_signal(
    scenario_id: int,
    signal_id: int,
) -> List[int]:
    # analysis_id, привязанные к данной паре (сценарий, сигнал)
    return [
        link["analysis_id"]
        for link in bt_analysis_connections
        if link.get("scenario_id") == scenario_id and link.get("signal_id") == signal_id
    ]