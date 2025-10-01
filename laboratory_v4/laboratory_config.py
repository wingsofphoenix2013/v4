# laboratory_config.py — загрузка конфигов laboratory_v4: тикеры, стратегии, whitelist; слушатели Pub/Sub и Streams

import json
import logging
import infra
from infra import (
    set_enabled_tickers,
    set_enabled_strategies,
    start_pack_update,
    finish_pack_update,
    set_pack_whitelist_for_strategy,
    clear_pack_whitelist_for_strategy,
    start_mw_update,
    finish_mw_update,
    set_mw_whitelist_for_strategy,
    clear_mw_whitelist_for_strategy,
)
import asyncio

log = logging.getLogger("LAB_CONFIG")


# 🔸 Первичная загрузка тикеров (enabled=true, tradepermission=enabled)
async def load_enabled_tickers():
    query = "SELECT * FROM tickers_bb WHERE status='enabled' AND tradepermission='enabled'"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        tickers = {r["symbol"]: dict(r) for r in rows}
        set_enabled_tickers(tickers)
        log.info("✅ Загружено тикеров: %d", len(tickers))


# 🔸 Первичная загрузка стратегий (enabled=true)
async def load_enabled_strategies():
    query = "SELECT * FROM strategies_v4 WHERE enabled=true"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        strategies = {int(r["id"]): dict(r) for r in rows}
        set_enabled_strategies(strategies)
        log.info("✅ Загружено стратегий: %d", len(strategies))


# 🔸 Первичная загрузка whitelist (PACK)
async def load_pack_whitelist():
    query = "SELECT * FROM oracle_pack_whitelist"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        grouped: dict[int, list] = {}
        for r in rows:
            sid = int(r["strategy_id"])
            grouped.setdefault(sid, []).append(dict(r))
        for sid, data in grouped.items():
            set_pack_whitelist_for_strategy(sid, data, {"loaded": True})
        log.info("✅ Загружено PACK whitelist (стратегий: %d)", len(grouped))


# 🔸 Первичная загрузка whitelist (MW)
async def load_mw_whitelist():
    query = "SELECT * FROM oracle_mw_whitelist"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)
        grouped: dict[int, list] = {}
        for r in rows:
            sid = int(r["strategy_id"])
            grouped.setdefault(sid, []).append(dict(r))
        for sid, data in grouped.items():
            set_mw_whitelist_for_strategy(sid, data, {"loaded": True})
        log.info("✅ Загружено MW whitelist (стратегий: %d)", len(grouped))


# 🔸 Точечное обновление стратегии (по событию)
async def handle_strategy_event(payload: dict):
    sid = payload.get("id")
    if not sid:
        return
    sid = int(sid)
    async with infra.pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM strategies_v4 WHERE id=$1", sid)
    if not row:
        if sid in infra.enabled_strategies:
            infra.enabled_strategies.pop(sid, None)
            log.info("🧹 strategy id=%s удалена из кэша", sid)
        return
    enabled = bool(row["enabled"])
    if enabled:
        infra.enabled_strategies[sid] = dict(row)
        log.info("➕ strategy id=%s добавлена/обновлена в кэше", sid)
    else:
        infra.enabled_strategies.pop(sid, None)
        log.info("➖ strategy id=%s удалена из кэша (enabled=false)", sid)


# 🔸 Слушатель событий Pub/Sub (тикеры + стратегии)
async def config_event_listener():
    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe("bb:tickers_events", "strategies_v4_events")
    log.info("📡 Подписка на каналы: bb:tickers_events, strategies_v4_events")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            data = json.loads(message["data"])
            channel = message["channel"]
            if channel == "bb:tickers_events":
                log.info("🔔 Событие тикеров: %s", data)
                await load_enabled_tickers()
            elif channel == "strategies_v4_events":
                log.info("🔔 Событие стратегий: %s", data)
                await handle_strategy_event(data)
        except Exception as e:
            log.exception("❌ Ошибка при обработке события: %s", e)


# 🔸 Слушатель Streams обновлений whitelist (PACK + MW)
async def whitelist_stream_listener():
    streams = {
        "oracle:pack_lists:reports_ready": "pack",
        "oracle:mw_whitelist:reports_ready": "mw",
    }
    last_ids = {k: "0-0" for k in streams.keys()}
    log.info("📡 Подписка на Streams: %s", ", ".join(streams.keys()))

    while True:
        try:
            response = await infra.redis_client.xread(streams=last_ids, block=5000, count=10)
            if not response:
                continue
            for stream_name, messages in response:
                for msg_id, fields in messages:
                    last_ids[stream_name] = msg_id
                    try:
                        payload = {k: json.loads(v) if v.startswith("{") else v for k, v in fields.items()}
                        sid = int(payload.get("strategy_id", 0))
                        if not sid:
                            continue
                        if streams[stream_name] == "pack":
                            await start_pack_update(sid)
                            try:
                                async with infra.pg_pool.acquire() as conn:
                                    rows = await conn.fetch("SELECT * FROM oracle_pack_whitelist WHERE strategy_id=$1", sid)
                                if rows:
                                    set_pack_whitelist_for_strategy(sid, [dict(r) for r in rows], {"report": payload})
                                else:
                                    clear_pack_whitelist_for_strategy(sid)
                            finally:
                                finish_pack_update(sid)
                        elif streams[stream_name] == "mw":
                            await start_mw_update(sid)
                            try:
                                async with infra.pg_pool.acquire() as conn:
                                    rows = await conn.fetch("SELECT * FROM oracle_mw_whitelist WHERE strategy_id=$1", sid)
                                if rows:
                                    set_mw_whitelist_for_strategy(sid, [dict(r) for r in rows], {"report": payload})
                                else:
                                    clear_mw_whitelist_for_strategy(sid)
                            finally:
                                finish_mw_update(sid)
                    except Exception:
                        log.exception("❌ Ошибка при обновлении WL по событию (stream=%s, msg=%s)", stream_name, msg_id)
        except asyncio.CancelledError:
            log.info("⏹️ Остановка слушателя Streams")
            raise
        except Exception:
            log.exception("❌ Ошибка в цикле whitelist_stream_listener")
            await asyncio.sleep(5)