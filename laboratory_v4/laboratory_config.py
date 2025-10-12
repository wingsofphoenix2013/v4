# 🔸 laboratory_config.py — стартовая загрузка laboratory_v4: кэши тикеров/стратегий/WL/BL и слушатели обновлений

# 🔸 Импорты
import asyncio
import json
import logging
from typing import Dict, Set, Tuple

import laboratory_infra as infra
from laboratory_infra import (
    set_lab_tickers,
    set_lab_strategies,
    replace_mw_whitelist,
    replace_pack_list,
    update_mw_whitelist_for_strategy,
    update_pack_list_for_strategy,
)

# 🔸 Логгер
log = logging.getLogger("LAB_CONFIG")

# 🔸 Константы потоков/групп
MW_WL_READY_STREAM = "oracle:mw_whitelist:reports_ready"
PACK_LISTS_READY_STREAM = "oracle:pack_lists:reports_ready"

LAB_LISTS_GROUP = "LAB_LISTS_GROUP"
LAB_LISTS_WORKER = "LAB_LISTS_WORKER"

# 🔸 Константы каналов Pub/Sub
PUBSUB_TICKERS = "bb:tickers_events"
PUBSUB_STRATEGIES = "strategies_v4_events"


# 🔸 Первичная стартовая загрузка (кэш тикеров, стратегий, WL/BL)
async def load_initial_config():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск initial_config: PG/Redis не инициализированы")
        return

    # тикеры
    await _load_active_tickers()
    # стратегии
    await _load_active_strategies()
    # MW WL (v1/v2)
    await _load_mw_whitelists_all()
    # PACK WL/BL (v1/v2)
    await _load_pack_lists_all()

    # итог
    log.info("✅ LAB стартовая конфигурация загружена: тикеры=%d, стратегии=%d, mw_wl[v1]=%d, mw_wl[v2]=%d, pack_wl[v1]=%d, pack_wl[v2]=%d, pack_bl[v1]=%d, pack_bl[v2]=%d",
             len(infra.lab_tickers),
             len(infra.lab_strategies),
             len(infra.lab_mw_wl.get('v1', {})),
             len(infra.lab_mw_wl.get('v2', {})),
             len(infra.lab_pack_wl.get('v1', {})),
             len(infra.lab_pack_wl.get('v2', {})),
             len(infra.lab_pack_bl.get('v1', {})),
             len(infra.lab_pack_bl.get('v2', {})),
             )


# 🔸 Слушатель списков (Streams): обновление кэшей WL/BL oracle по сообщениям
async def lists_stream_listener():
    # условия достаточности
    if infra.redis_client is None:
        log.debug("❌ Пропуск lists_stream_listener: Redis не инициализирован")
        return

    # создать consumer group (идемпотентно)
    for s in (MW_WL_READY_STREAM, PACK_LISTS_READY_STREAM):
        try:
            await infra.redis_client.xgroup_create(name=s, groupname=LAB_LISTS_GROUP, id="$", mkstream=True)
            log.debug("📡 LAB: создана consumer group для стрима %s", s)
        except Exception as e:
            if "BUSYGROUP" in str(e):
                pass
            else:
                log.exception("❌ LAB: ошибка создания consumer group для %s", s)
                return

    log.debug("🚀 LAB: старт lists_stream_listener")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=LAB_LISTS_GROUP,
                consumername=LAB_LISTS_WORKER,
                streams={MW_WL_READY_STREAM: ">", PACK_LISTS_READY_STREAM: ">"},
                count=128,
                block=30_000,
            )
            if not resp:
                continue

            # аккумулируем ack
            acks: Dict[str, list] = {}

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        if stream_name == MW_WL_READY_STREAM:
                            # ожидаем: {strategy_id, report_id, time_frame='7d', version='v1'|'v2', ...}
                            sid = int(payload.get("strategy_id", 0))
                            version = str(payload.get("version", "")).lower()
                            if sid and version in ("v1", "v2"):
                                await _reload_mw_wl_for_strategy(sid, version)
                                log.info("🔁 LAB: MW WL обновлён из стрима (sid=%s, version=%s)", sid, version)
                            else:
                                log.debug("ℹ️ MW_WL_READY: пропуск payload=%s", payload)

                        elif stream_name == PACK_LISTS_READY_STREAM:
                            # ожидаем: {strategy_id, report_id, time_frame='7d', version='v1'|'v2', rows_whitelist, rows_blacklist, ...}
                            sid = int(payload.get("strategy_id", 0))
                            version = str(payload.get("version", "")).lower()
                            if sid and version in ("v1", "v2"):
                                await _reload_pack_lists_for_strategy(sid, version)
                                log.info("🔁 LAB: PACK WL/BL обновлены из стрима (sid=%s, version=%s)", sid, version)
                            else:
                                log.debug("ℹ️ PACK_LISTS_READY: пропуск payload=%s", payload)

                        acks.setdefault(stream_name, []).append(msg_id)
                    except Exception:
                        log.exception("❌ LAB: ошибка обработки сообщения в %s", stream_name)

            # ACK после успешной обработки
            for s, ids in acks.items():
                if ids:
                    try:
                        await infra.redis_client.xack(s, LAB_LISTS_GROUP, *ids)
                    except Exception:
                        log.exception("⚠️ LAB: ошибка ACK в %s (ids=%s)", s, ids)

        except asyncio.CancelledError:
            log.debug("⏹️ LAB: lists_stream_listener остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB: ошибка цикла lists_stream_listener — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Слушатель Pub/Sub конфигов: тикеры и стратегии
async def config_event_listener():
    # условия достаточности
    if infra.redis_client is None:
        log.debug("❌ Пропуск config_event_listener: Redis не инициализирован")
        return

    pubsub = infra.redis_client.pubsub()
    await pubsub.subscribe(PUBSUB_TICKERS, PUBSUB_STRATEGIES)
    log.info("📡 LAB: подписка на каналы: %s, %s", PUBSUB_TICKERS, PUBSUB_STRATEGIES)

    async for message in pubsub.listen():
        if message.get("type") != "message":
            continue
        try:
            channel = message["channel"]  # decode_responses=True → уже str
            # события тикеров → полная перезагрузка кэша тикеров
            if channel == PUBSUB_TICKERS:
                await _load_active_tickers()
                log.info("🔔 LAB: обновлён кэш тикеров по событию %s", channel)
            # события стратегий → полная перезагрузка кэша стратегий
            elif channel == PUBSUB_STRATEGIES:
                await _load_active_strategies()
                log.info("🔔 LAB: обновлён кэш стратегий по событию %s", channel)
        except Exception:
            log.exception("❌ LAB: ошибка обработки сообщения Pub/Sub")


# 🔸 Загрузчики (SQL минимальный, вычисления в Python)

async def _load_active_tickers():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, precision_price, precision_qty, status, tradepermission, created_at
            FROM tickers_bb
            WHERE status = 'enabled' AND tradepermission = 'enabled'
            """
        )
        tickers = {str(r["symbol"]): dict(r) for r in rows}
        set_lab_tickers(tickers)
    log.info("✅ LAB: загружены активные тикеры (%d)", len(infra.lab_tickers))


async def _load_active_strategies():
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, name, human_name, enabled, COALESCE(archived,false) AS archived, created_at
            FROM strategies_v4
            WHERE enabled = true AND (archived IS NOT TRUE)
            """
        )
        strategies = {int(r["id"]): dict(r) for r in rows}
        set_lab_strategies(strategies)
    log.info("✅ LAB: загружены активные стратегии (%d)", len(infra.lab_strategies))


async def _load_mw_whitelists_all():
    # строим карты по версиям: (sid, timeframe, direction) -> {(agg_base, agg_state)}
    v_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str]]]] = {"v1": {}, "v2": {}}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.version,
                   w.strategy_id,
                   a.timeframe,
                   a.direction,
                   a.agg_base,
                   a.agg_state
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d'
            """
        )

    for r in rows:
        ver = str(r["version"]).lower()
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"])
        direction = str(r["direction"])
        base = str(r["agg_base"])
        state = str(r["agg_state"])
        key = (sid, tf, direction)
        bucket = v_maps.setdefault(ver, {}).setdefault(key, set())
        bucket.add((base, state))

    # применяем замены кэшей
    for ver in ("v1", "v2"):
        replace_mw_whitelist(ver, v_maps.get(ver, {}))

    log.info("✅ LAB: MW WL загружены: v1=%d срезов, v2=%d срезов",
             len(infra.lab_mw_wl.get("v1", {})),
             len(infra.lab_mw_wl.get("v2", {})))


async def _load_pack_lists_all():
    # строим карты по версиям и типу списка
    wl_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}
    bl_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {"v1": {}, "v2": {}}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.version,
                   w.list,
                   w.strategy_id,
                   a.timeframe,
                   a.direction,
                   a.pack_base,
                   a.agg_key,
                   a.agg_value
            FROM oracle_pack_whitelist w
            JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d'
            """
        )

    for r in rows:
        ver = str(r["version"]).lower()
        list_tag = str(r["list"]).lower()  # 'whitelist'|'blacklist'
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"])
        direction = str(r["direction"])
        pack_base = str(r["pack_base"])
        agg_key = str(r["agg_key"])
        agg_value = str(r["agg_value"])
        key = (sid, tf, direction)
        target = wl_maps if list_tag == "whitelist" else bl_maps
        bucket = target.setdefault(ver, {}).setdefault(key, set())
        bucket.add((pack_base, agg_key, agg_value))

    # применяем замены кэшей
    for ver in ("v1", "v2"):
        replace_pack_list("whitelist", ver, wl_maps.get(ver, {}))
        replace_pack_list("blacklist", ver, bl_maps.get(ver, {}))

    log.info("✅ LAB: PACK WL/BL загружены: wl[v1]=%d, wl[v2]=%d, bl[v1]=%d, bl[v2]=%d",
             len(infra.lab_pack_wl.get("v1", {})),
             len(infra.lab_pack_wl.get("v2", {})),
             len(infra.lab_pack_bl.get("v1", {})),
             len(infra.lab_pack_bl.get("v2", {})))


# 🔸 Точечные перезагрузки по сообщениям стримов

async def _reload_mw_wl_for_strategy(strategy_id: int, version: str):
    slice_map: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT a.timeframe, a.direction, a.agg_base, a.agg_state
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.strategy_id = $1 AND w.version = $2
            """,
            int(strategy_id), str(version)
        )
    for r in rows:
        key = (str(r["timeframe"]), str(r["direction"]))
        slice_map.setdefault(key, set()).add((str(r["agg_base"]), str(r["agg_state"])))

    update_mw_whitelist_for_strategy(version, strategy_id, slice_map)


async def _reload_pack_lists_for_strategy(strategy_id: int, version: str):
    wl_slice: Dict[Tuple[str, str], Set[Tuple[str, str, str]]] = {}
    bl_slice: Dict[Tuple[str, str], Set[Tuple[str, str, str]]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.list, a.timeframe, a.direction, a.pack_base, a.agg_key, a.agg_value
            FROM oracle_pack_whitelist w
            JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.strategy_id = $1 AND w.version = $2
            """,
            int(strategy_id), str(version)
        )

    for r in rows:
        key = (str(r["timeframe"]), str(r["direction"]))
        tpl = (str(r["pack_base"]), str(r["agg_key"]), str(r["agg_value"]))
        if str(r["list"]).lower() == "whitelist":
            wl_slice.setdefault(key, set()).add(tpl)
        else:
            bl_slice.setdefault(key, set()).add(tpl)

    update_pack_list_for_strategy("whitelist", version, strategy_id, wl_slice)
    update_pack_list_for_strategy("blacklist", version, strategy_id, bl_slice)