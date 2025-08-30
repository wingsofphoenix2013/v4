# 🔸 config_loader.py — тикеры + стратегии (market_watcher)

import json
import asyncio
import logging

import infra
from infra import set_enabled_tickers, (
    set_market_watcher_strategies,
    add_market_watcher_strategy,
    remove_market_watcher_strategy,
)

log = logging.getLogger("CONFIG_LOADER")


# 🔸 Загрузка активных тикеров
async def load_enabled_tickers():
    query = """
        SELECT symbol, precision_price, precision_qty, created_at
        FROM tickers_v4
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


# 🔸 Точечная обработка события по стратегии (enable/disable)
async def handle_strategy_event(payload: dict):
    sid = payload.get("id")
    if not sid:
        return

    # Читаем текущее состояние стратегии из БД (истина в БД важнее, чем слово в событии)
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, enabled, COALESCE(archived, false) AS archived, COALESCE(market_watcher, false) AS mw
            FROM strategies_v4
            WHERE id = $1
            """,
            int(sid),
        )

    if not row:
        # Стратегия исчезла — на всякий случай удалим из кэша
        remove_market_watcher_strategy(int(sid))
        log.info("🧹 strategy id=%s не найдена в БД — удалена из кэша (если была)", sid)
        return

    enabled = bool(row["enabled"])
    archived = bool(row["archived"])
    mw = bool(row["mw"])

    should_be_in_cache = enabled and (not archived) and mw
    in_cache = (int(sid) in infra.market_watcher_strategies)

    if should_be_in_cache and not in_cache:
        add_market_watcher_strategy(int(sid))
        log.info("➕ strategy id=%s добавлена в кэш market_watcher", sid)
    elif (not should_be_in_cache) and in_cache:
        remove_market_watcher_strategy(int(sid))
        log.info("➖ strategy id=%s удалена из кэша market_watcher", sid)
    else:
        log.debug("ℹ️ strategy id=%s — кэш без изменений (enabled=%s, archived=%s, mw=%s)", sid, enabled, archived, mw)


# 🔸 Слушатель событий Pub/Sub (тикеры + стратегии)
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe("tickers_v4_events", "strategies_v4_events")
    log.info("📡 Подписка на каналы: tickers_v4_events, strategies_v4_events")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        try:
            data = json.loads(message["data"])
            channel = message["channel"]  # decode_responses=True → уже строка

            if channel == "tickers_v4_events":
                log.info("🔔 Событие тикеров: %s", data)
                await load_enabled_tickers()

            elif channel == "strategies_v4_events":
                log.info("🔔 Событие стратегий: %s", data)
                # у тебя событие формата {"id":..., "type":"enabled", "action":"true|false", ...}
                # просто проверим текущее состояние стратегии и обновим кэш точечно
                await handle_strategy_event(data)

        except Exception as e:
            log.exception("❌ Ошибка при обработке события: %s", e)