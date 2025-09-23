# 🔸 config_loader.py — тикеры + стратегии (market_watcher-only, Bybit)

import json
import asyncio
import logging

import infra
from infra import (
    set_enabled_tickers,
    set_market_watcher_strategies,
    add_market_watcher_strategy,
    remove_market_watcher_strategy,
)

log = logging.getLogger("CONFIG_LOADER")


# 🔸 Загрузка активных тикеров (Bybit)
async def load_enabled_tickers():
    query = """
        SELECT symbol, precision_price, precision_qty, created_at
        FROM tickers_bb
        WHERE status = 'enabled' AND tradepermission = 'enabled'
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        tickers = {r["symbol"]: dict(r) for r in rows}
        set_enabled_tickers(tickers)
        log.info("✅ Загружено тикеров: %d", len(tickers))


# 🔸 Предварительная загрузка стратегий с market_watcher=true
async def load_market_watcher_strategies():
    query = """
        SELECT id
        FROM strategies_v4
        WHERE enabled = true
          AND (archived IS NOT TRUE)
          AND market_watcher = true
    """
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        ids = {int(r["id"]) for r in rows}
        set_market_watcher_strategies(ids)
        log.info("✅ Загружено стратегий market_watcher: %d", len(ids))


# 🔸 Точечная обработка события по стратегии (enable/disable/изменение флагов)
async def handle_strategy_event(payload: dict):
    sid = payload.get("id")
    if not sid:
        return

    # Истина в БД важнее, чем поля из события — перечитываем состояние
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                enabled,
                COALESCE(archived, false)        AS archived,
                COALESCE(market_watcher, false)  AS mw
            FROM strategies_v4
            WHERE id = $1
            """,
            int(sid),
        )

    if not row:
        # Стратегия исчезла — почистим кэш
        remove_market_watcher_strategy(int(sid))
        log.info("🧹 strategy id=%s не найдена в БД — удалена из кэша market_watcher (если была)", sid)
        return

    enabled = bool(row["enabled"])
    archived = bool(row["archived"])
    mw = bool(row["mw"])

    # --- market_watcher кэш
    should_mw = enabled and (not archived) and mw
    in_mw = (int(sid) in infra.market_watcher_strategies)

    if should_mw and not in_mw:
        add_market_watcher_strategy(int(sid))
        log.info("➕ strategy id=%s добавлена в кэш market_watcher", sid)
    elif (not should_mw) and in_mw:
        remove_market_watcher_strategy(int(sid))
        log.info("➖ strategy id=%s удалена из кэша market_watcher", sid)
    else:
        log.debug(
            "ℹ️ strategy id=%s — MW кэш без изменений (enabled=%s, archived=%s, mw=%s)",
            sid, enabled, archived, mw
        )


# 🔸 Слушатель событий Pub/Sub (тикеры + стратегии)
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    # Bybit-канал для тикеров; канал стратегий оставляем прежним
    await pubsub.subscribe("bb:tickers_events", "strategies_v4_events")
    log.info("📡 Подписка на каналы: bb:tickers_events, strategies_v4_events")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            channel = message["channel"]  # decode_responses=True → уже строка

            if channel == "bb:tickers_events":
                log.info("🔔 Событие тикеров: %s", data)
                await load_enabled_tickers()

            elif channel == "strategies_v4_events":
                log.info("🔔 Событие стратегий: %s", data)
                # событие — триггер, состояние читаем из БД
                await handle_strategy_event(data)

        except Exception as e:
            log.exception("❌ Ошибка при обработке события: %s", e)