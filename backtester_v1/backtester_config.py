# backtester_config.py — конфигурация и кеш метаданных для backtester_v1

import logging
from typing import Dict, Any, List, Optional

# 🔸 Логгер модуля
log = logging.getLogger("BT_CONFIG")

# 🔸 Глобальные кеши тикеров, индикаторов и псевдо-сигналов
bt_tickers: Dict[str, Dict[str, Any]] = {}                 # symbol -> {fields}
bt_indicator_instances: Dict[int, Dict[str, Any]] = {}     # instance_id -> {indicator, timeframe, enabled_at, params}
bt_signal_instances: Dict[int, Dict[str, Any]] = {}        # signal_id -> {key, name, timeframe, mode, backfill_days, type, enabled, params}


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


# 🔸 Загрузка включенных инстансов индикаторов и их параметров
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
async def load_initial_signals(pg, timeframes: Optional[List[str]] = None, only_enabled: bool = True) -> int:
    async with pg.acquire() as conn:
        conditions = []
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
            """
            ,
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
    log.info(f"BT_CONFIG: загружено инстансов псевдо-сигналов: {count}")
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