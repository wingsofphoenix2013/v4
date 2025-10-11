# indicators_v4_main.py — управляющий модуль live-публикации индикаторов m5 (ind_live:*) без стримов и без PACK/MW

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timedelta

from infra import init_pg_pool, init_redis_client, setup_logging, run_safe_loop
from live_indicators_m5 import run_live_indicators_m5


# 🔸 Глобальное состояние (in-memory)
active_tickers: dict[str, int] = {}        # symbol -> precision_price
indicator_instances: dict[int, dict] = {}  # instance_id -> {indicator, timeframe, params, enabled_at}


# 🔸 Константы источника данных (PostgreSQL)
BB_TICKERS_TABLE = "tickers_bb"
IND_INSTANCES_TABLE = "indicator_instances_v4"
IND_PARAMS_TABLE = "indicator_parameters_v4"

# 🔸 Периоды обновления конфигурации (без стримов)
REFRESH_TICKERS_SEC = 15 * 60  # 15 минут
REFRESH_INDICATORS_SEC = 60    # 1 минута


# 🔸 Вспомогательные геттеры для воркера live m5
def get_instances_by_tf(tf: str):
    return [
        {
            "id": iid,
            "indicator": inst["indicator"],
            "timeframe": inst["timeframe"],
            "enabled_at": inst.get("enabled_at"),
            "params": inst["params"],
        }
        for iid, inst in indicator_instances.items()
        if inst["timeframe"] == tf
    ]


def get_precision(symbol: str) -> int:
    return active_tickers.get(symbol, 8)


def get_active_symbols():
    return list(active_tickers.keys())


# 🔸 Одноразовая загрузка активных тикеров из PostgreSQL (без стримов)
async def load_initial_tickers(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol, precision_price
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
        for row in rows:
            active_tickers[row["symbol"]] = int(row["precision_price"]) if row["precision_price"] is not None else 8
            log.debug(f"Loaded ticker: {row['symbol']} → precision={row['precision_price']}")
    log.info(f"INIT: активных тикеров загружено: {len(active_tickers)}")


# 🔸 Периодический рефреш активных тикеров (опрашиваем БД; избегаем стримов)
async def run_refresh_tickers(pg):
    log = logging.getLogger("REFRESH_TICKERS")
    while True:
        try:
            async with pg.acquire() as conn:
                rows = await conn.fetch(f"""
                    SELECT symbol, precision_price
                    FROM {BB_TICKERS_TABLE}
                    WHERE status = 'enabled' AND tradepermission = 'enabled'
                """)
            current = set(active_tickers.keys())
            fresh = set()
            updated = 0
            new_syms = 0
            removed = 0

            for r in rows:
                sym = r["symbol"]
                prec = int(r["precision_price"]) if r["precision_price"] is not None else 8
                fresh.add(sym)
                if sym not in active_tickers:
                    new_syms += 1
                elif active_tickers[sym] != prec:
                    updated += 1
                active_tickers[sym] = prec

            for sym in list(current - fresh):
                active_tickers.pop(sym, None)
                removed += 1

            log.debug(f"TICKERS_REFRESH: updated={updated}, new={new_syms}, removed={removed}, active={len(active_tickers)}")
        except Exception as e:
            log.warning(f"TICKERS_REFRESH error: {e}", exc_info=True)
        finally:
            await asyncio.sleep(REFRESH_TICKERS_SEC)


# 🔸 Одноразовая загрузка включённых инстансов индикаторов m5 и их параметров
async def load_initial_indicators(pg):
    log = logging.getLogger("INIT")
    async with pg.acquire() as conn:
        instances = await conn.fetch(f"""
            SELECT id, indicator, timeframe, enabled_at
            FROM {IND_INSTANCES_TABLE}
            WHERE enabled = true
              AND timeframe = 'm5'
        """)
        for inst in instances:
            params = await conn.fetch(f"""
                SELECT param, value
                FROM {IND_PARAMS_TABLE}
                WHERE instance_id = $1
            """, inst["id"])
            param_map = {p["param"]: p["value"] for p in params}

            indicator_instances[int(inst["id"])] = {
                "indicator": inst["indicator"],
                "timeframe": inst["timeframe"],
                "params": param_map,
                "enabled_at": inst["enabled_at"],
            }
            log.debug(f"Loaded instance id={inst['id']} → {inst['indicator']} {param_map}, enabled_at={inst['enabled_at']}")
    log.info(f"INIT: активных инстансов m5 загружено: {len(indicator_instances)}")


# 🔸 Периодический рефреш инстансов индикаторов m5 (опрашиваем БД; избегаем стримов)
async def run_refresh_indicators(pg):
    log = logging.getLogger("REFRESH_IND")
    while True:
        try:
            new_map: dict[int, dict] = {}
            async with pg.acquire() as conn:
                instances = await conn.fetch(f"""
                    SELECT id, indicator, timeframe, enabled_at
                    FROM {IND_INSTANCES_TABLE}
                    WHERE enabled = true
                      AND timeframe = 'm5'
                """)
                for inst in instances:
                    params = await conn.fetch(f"""
                        SELECT param, value
                        FROM {IND_PARAMS_TABLE}
                        WHERE instance_id = $1
                    """, inst["id"])
                    param_map = {p["param"]: p["value"] for p in params}
                    new_map[int(inst["id"])] = {
                        "indicator": inst["indicator"],
                        "timeframe": inst["timeframe"],
                        "params": param_map,
                        "enabled_at": inst["enabled_at"],
                    }
            # замещаем карту атомарно
            indicator_instances.clear()
            indicator_instances.update(new_map)
            log.debug(f"INDICATORS_REFRESH: active_m5={len(indicator_instances)}")
        except Exception as e:
            log.warning(f"INDICATORS_REFRESH error: {e}", exc_info=True)
        finally:
            await asyncio.sleep(REFRESH_INDICATORS_SEC)


# 🔸 Точка входа
async def main():
    setup_logging()
    pg = await init_pg_pool()
    redis = await init_redis_client()

    await load_initial_tickers(pg)
    await load_initial_indicators(pg)

    # запускаем только live-публикацию m5 и периодические обновления конфигурации (без стримов/гейтвея)
    await asyncio.gather(
        run_safe_loop(lambda: run_refresh_tickers(pg), "REFRESH_TICKERS"),
        run_safe_loop(lambda: run_refresh_indicators(pg), "REFRESH_INDICATORS"),
        run_safe_loop(lambda: run_live_indicators_m5(pg, redis, get_instances_by_tf, get_precision, get_active_symbols), "LIVE_M5"),
    )


# 🔸 Запуск
if __name__ == "__main__":
    asyncio.run(main())