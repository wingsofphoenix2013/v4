# backtester_config.py — конфигурация и кеш метаданных для backtester_v1

import logging
from typing import Dict, Any, List, Optional

# 🔸 Логгер модуля
log = logging.getLogger("BT_CONFIG")

# 🔸 Глобальные кеши
bt_tickers: Dict[str, Dict[str, Any]] = {}          # symbol -> {fields}
bt_indicator_instances: Dict[int, Dict[str, Any]] = {}  # instance_id -> {indicator, timeframe, enabled_at, params}


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
            # фильтрация по списку ТФ, если задан
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