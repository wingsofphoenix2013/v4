# 🔸 laboratory_config.py — стартовая загрузка laboratory_v4: кэши тикеров/стратегий/MW-WL/MW-BL/PACK-WL/PACK-BL (+winrate), Active-пороги (MW-BL, PACK-BL) и слушатель единого стрима all_ready

# 🔸 Импорты
import asyncio
import json
import logging
from typing import Dict, Set, Tuple, Dict as _Dict, Set as _Set, Tuple as _Tuple

import laboratory_infra as infra
from laboratory_infra import (
    set_lab_tickers,
    set_lab_strategies,
    replace_mw_whitelist,
    replace_mw_blacklist,
    replace_pack_list,
    update_mw_whitelist_for_strategy,
    update_mw_blacklist_for_strategy,
    update_pack_list_for_strategy,
    # Active-пороги:
    set_bl_active_bulk,
    upsert_bl_active,
    set_mw_bl_active_bulk,
    upsert_mw_bl_active,
)

# 🔸 Логгер
log = logging.getLogger("LAB_CONFIG")

# 🔸 Константы потоков/групп
ALL_READY_STREAM = "oracle:pack_lists:all_ready"
LAB_LISTS_GROUP = "LAB_LISTS_GROUP"
LAB_LISTS_WORKER = "LAB_LISTS_WORKER"

# 🔸 Константы каналов Pub/Sub
PUBSUB_TICKERS = "bb:tickers_events"
PUBSUB_STRATEGIES = "strategies_v4_events"

# 🔸 Версии/режимы
ACTIVE_LISTS_VERSION = "v5"          # активная версия при initial-load для Active-таблиц (в payload тоже приходит "v5")
ALLOWED_VERSIONS = ("v1", "v2", "v3", "v4", "v5")
DECISION_MODE_SMOOTHED = "smoothed"  # режим для best_threshold_smoothed


# 🔸 Первичная стартовая загрузка (кэш тикеров, стратегий, WL/BL v1–v5 + Active-пороги)
async def load_initial_config():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.info("❌ Пропуск initial_config: PG/Redis не инициализированы")
        return

    # тикеры
    await _load_active_tickers()
    # стратегии
    await _load_active_strategies()
    # MW WL (v1–v5) + winrate карты
    await _load_mw_whitelists_all()
    # MW BL (v1–v5) + winrate карты
    await _load_mw_blacklists_all()
    # PACK WL/BL (v1–v5) + winrate карты
    await _load_pack_lists_all()
    # Active-пороги MW-BL (используем smoothed как рабочий)
    await _load_mw_bl_active_all()
    # Active-пороги PACK-BL (используем smoothed как рабочий)
    await _load_pack_bl_active_all()

    # итог
    log.info(
        "✅ LAB стартовая конфигурация загружена: "
        "тикеры=%d, стратегии=%d, "
        "mw_wl[v1]=%d, mw_wl[v2]=%d, mw_wl[v3]=%d, mw_wl[v4]=%d, mw_wl[v5]=%d, "
        "mw_bl[v1]=%d, mw_bl[v2]=%d, mw_bl[v3]=%d, mw_bl[v4]=%d, mw_bl[v5]=%d, "
        "pack_wl[v1]=%d, pack_wl[v2]=%d, pack_wl[v3]=%d, pack_wl[v4]=%d, pack_wl[v5]=%d, "
        "pack_bl[v1]=%d, pack_bl[v2]=%d, pack_bl[v3]=%d, pack_bl[v4]=%d, pack_bl[v5]=%d, "
        "mw_bl_active=%d, pack_bl_active=%d (version=%s, mode=%s)",
        len(infra.lab_tickers),
        len(infra.lab_strategies),
        len(infra.lab_mw_wl.get("v1", {})),
        len(infra.lab_mw_wl.get("v2", {})),
        len(infra.lab_mw_wl.get("v3", {})),
        len(infra.lab_mw_wl.get("v4", {})),
        len(infra.lab_mw_wl.get("v5", {})),
        len(infra.lab_mw_bl.get("v1", {})),
        len(infra.lab_mw_bl.get("v2", {})),
        len(infra.lab_mw_bl.get("v3", {})),
        len(infra.lab_mw_bl.get("v4", {})),
        len(infra.lab_mw_bl.get("v5", {})),
        len(infra.lab_pack_wl.get("v1", {})),
        len(infra.lab_pack_wl.get("v2", {})),
        len(infra.lab_pack_wl.get("v3", {})),
        len(infra.lab_pack_wl.get("v4", {})),
        len(infra.lab_pack_wl.get("v5", {})),
        len(infra.lab_pack_bl.get("v1", {})),
        len(infra.lab_pack_bl.get("v2", {})),
        len(infra.lab_pack_bl.get("v3", {})),
        len(infra.lab_pack_bl.get("v4", {})),
        len(infra.lab_pack_bl.get("v5", {})),
        len(infra.lab_mw_bl_active),
        len(infra.lab_bl_active),
        ACTIVE_LISTS_VERSION,
        DECISION_MODE_SMOOTHED,
    )


# 🔸 Слушатель единого стрима (all_ready): обновление кэшей WL/BL (MW+PACK) и Active-порогов по событиям oracle_v4
async def lists_stream_listener():
    # условия достаточности
    if infra.redis_client is None:
        log.info("❌ Пропуск lists_stream_listener: Redis не инициализирован")
        return

    # создать consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(name=ALL_READY_STREAM, groupname=LAB_LISTS_GROUP, id="$", mkstream=True)
        log.info("📡 LAB: создана consumer group для стрима %s", ALL_READY_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ LAB: ошибка создания consumer group для %s", ALL_READY_STREAM)
            return

    log.info("🚀 LAB: старт lists_stream_listener (stream=%s)", ALL_READY_STREAM)

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=LAB_LISTS_GROUP,
                consumername=LAB_LISTS_WORKER,
                streams={ALL_READY_STREAM: ">"},
                count=128,
                block=30_000,
            )
            if not resp:
                continue

            # аккумулируем ack
            acks = []

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))

                        # общие поля
                        sid = int(payload.get("strategy_id", 0))
                        version = str(payload.get("version", "")).lower()
                        window_start = str(payload.get("window_start", "") or "")
                        window_end = str(payload.get("window_end", "") or "")
                        rules_exact = int(payload.get("rules_exact", 0))
                        rules_bykey = int(payload.get("rules_bykey", 0))
                        analysis_rows = int(payload.get("analysis_rows", 0))
                        active_rows = int(payload.get("active_rows", 0))
                        generated_at = str(payload.get("generated_at", "") or "")

                        # валидация
                        if not sid or version not in ALLOWED_VERSIONS:
                            log.info("ℹ️ ALL_READY: пропуск payload sid=%s version=%s", sid, version)
                            acks.append(msg_id)
                            continue

                        # последовательная перезагрузка: MW WL → MW BL → PACK WL/BL → Active (MW-BL, PACK-BL)
                        await _reload_mw_wl_for_strategy(sid, version)
                        await _reload_mw_bl_for_strategy(sid, version)
                        await _reload_pack_lists_for_strategy(sid, version)
                        mw_upd = await _reload_mw_bl_active_for_strategy(sid, version)
                        pack_upd = await _reload_pack_bl_active_for_strategy(sid, version)

                        # суммирующий лог по сообщению
                        log.info(
                            "🔁 LAB: all_ready применён — sid=%d, version=%s, window=[%s..%s], "
                            "oracle: rules_exact=%d, rules_bykey=%d, analysis_rows=%d, active_rows=%d, "
                            "active_upd[mw=%d, pack=%d], generated_at=%s",
                            sid, version, window_start, window_end,
                            rules_exact, rules_bykey, analysis_rows, active_rows,
                            mw_upd, pack_upd, generated_at
                        )

                        acks.append(msg_id)
                    except Exception:
                        log.exception("❌ LAB: ошибка обработки сообщения в %s", stream_name)

            # ACK после успешной обработки
            if acks:
                try:
                    await infra.redis_client.xack(ALL_READY_STREAM, LAB_LISTS_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ LAB: ошибка ACK (ids=%s)", acks)

        except asyncio.CancelledError:
            log.info("⏹️ LAB: lists_stream_listener остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ LAB: ошибка цикла lists_stream_listener — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Слушатель Pub/Sub конфигов: тикеры и стратегии
async def config_event_listener():
    # условия достаточности
    if infra.redis_client is None:
        log.info("❌ Пропуск config_event_listener: Redis не инициализирован")
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


# 🔸 Загрузка MW Whitelist (все версии v1–v5, 7d)
async def _load_mw_whitelists_all():
    # карты по версиям:
    #   v_maps: (sid, tf, dir) -> {(agg_base, agg_state)}
    #   wr_maps: (sid, tf, dir) -> {(agg_base, agg_state) -> winrate}
    v_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str]]]] = {}
    wr_maps: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.version,
                   w.strategy_id,
                   a.timeframe,
                   a.direction,
                   a.agg_base,
                   a.agg_state,
                   a.winrate
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.list = 'whitelist'
            """
        )

    for r in rows:
        ver = str(r["version"]).lower()
        if ver not in ALLOWED_VERSIONS:
            continue
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["agg_base"]); state = str(r["agg_state"])
        wr = float(r["winrate"] or 0.0)
        key = (sid, tf, direction)
        v_maps.setdefault(ver, {}).setdefault(key, set()).add((base, state))
        wr_maps.setdefault(ver, {}).setdefault(key, {})[(base, state)] = wr

    for ver in ALLOWED_VERSIONS:
        replace_mw_whitelist(ver, v_maps.get(ver, {}), wr_map=wr_maps.get(ver, {}))

    log.info(
        "✅ LAB: MW WL загружены: v1=%d, v2=%d, v3=%d, v4=%d, v5=%d",
        len(infra.lab_mw_wl.get("v1", {})),
        len(infra.lab_mw_wl.get("v2", {})),
        len(infra.lab_mw_wl.get("v3", {})),
        len(infra.lab_mw_wl.get("v4", {})),
        len(infra.lab_mw_wl.get("v5", {})),
    )


# 🔸 Загрузка MW Blacklist (все версии v1–v5, 7d)
async def _load_mw_blacklists_all():
    # карты по версиям:
    #   v_maps: (sid, tf, dir) -> {(agg_base, agg_state)}
    #   wr_maps: (sid, tf, dir) -> {(agg_base, agg_state) -> winrate}
    v_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str]]]] = {}
    wr_maps: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.version,
                   w.strategy_id,
                   a.timeframe,
                   a.direction,
                   a.agg_base,
                   a.agg_state,
                   a.winrate
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.list = 'blacklist'
            """
        )

    for r in rows:
        ver = str(r["version"]).lower()
        if ver not in ALLOWED_VERSIONS:
            continue
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["agg_base"]); state = str(r["agg_state"])
        wr = float(r["winrate"] or 0.0)
        key = (sid, tf, direction)
        v_maps.setdefault(ver, {}).setdefault(key, set()).add((base, state))
        wr_maps.setdefault(ver, {}).setdefault(key, {})[(base, state)] = wr

    for ver in ALLOWED_VERSIONS:
        infra.replace_mw_blacklist(ver, v_maps.get(ver, {}), wr_map=wr_maps.get(ver, {}))

    log.info(
        "✅ LAB: MW BL загружены: v1=%d, v2=%d, v3=%d, v4=%d, v5=%d",
        len(infra.lab_mw_bl.get("v1", {})),
        len(infra.lab_mw_bl.get("v2", {})),
        len(infra.lab_mw_bl.get("v3", {})),
        len(infra.lab_mw_bl.get("v4", {})),
        len(infra.lab_mw_bl.get("v5", {})),
    )


# 🔸 Загрузка PACK WL/BL (все версии v1–v5, 7d)
async def _load_pack_lists_all():
    # карты по версиям и типу списка:
    #   wl_maps/bl_maps: (sid, tf, dir) -> {(pack_base, agg_key, agg_value)}
    #   wl_wr_maps/bl_wr_maps: (sid, tf, dir) -> {(pack_base, agg_key, agg_value) -> winrate}
    wl_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {}
    bl_maps: Dict[str, Dict[Tuple[int, str, str], Set[Tuple[str, str, str]]]] = {}
    wl_wr_maps: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = {}
    bl_wr_maps: Dict[str, Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]]] = {}

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
                   a.agg_value,
                   a.winrate
            FROM oracle_pack_whitelist w
            JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d'
            """
        )

    for r in rows:
        ver = str(r["version"]).lower()
        if ver not in ALLOWED_VERSIONS:
            continue
        lst = str(r["list"]).lower()  # whitelist|blacklist
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["pack_base"]); akey = str(r["agg_key"]); aval = str(r["agg_value"])
        wr = float(r["winrate"] or 0.0)

        key = (sid, tf, direction)
        tpl = (base, akey, aval)

        if lst == "whitelist":
            wl_maps.setdefault(ver, {}).setdefault(key, set()).add(tpl)
            wl_wr_maps.setdefault(ver, {}).setdefault(key, {})[tpl] = wr
        else:
            bl_maps.setdefault(ver, {}).setdefault(key, set()).add(tpl)
            bl_wr_maps.setdefault(ver, {}).setdefault(key, {})[tpl] = wr

    for ver in ALLOWED_VERSIONS:
        replace_pack_list("whitelist", ver, wl_maps.get(ver, {}), wr_map=wl_wr_maps.get(ver, {}))
        replace_pack_list("blacklist", ver, bl_maps.get(ver, {}), wr_map=bl_wr_maps.get(ver, {}))

    log.info(
        "✅ LAB: PACK WL/BL загружены: wl[v1]=%d, wl[v2]=%d, wl[v3]=%d, wl[v4]=%d, wl[v5]=%d, "
        "bl[v1]=%d, bl[v2]=%d, bl[v3]=%d, bl[v4]=%d, bl[v5]=%d",
        len(infra.lab_pack_wl.get("v1", {})),
        len(infra.lab_pack_wl.get("v2", {})),
        len(infra.lab_pack_wl.get("v3", {})),
        len(infra.lab_pack_wl.get("v4", {})),
        len(infra.lab_pack_wl.get("v5", {})),
        len(infra.lab_pack_bl.get("v1", {})),
        len(infra.lab_pack_bl.get("v2", {})),
        len(infra.lab_pack_bl.get("v3", {})),
        len(infra.lab_pack_bl.get("v4", {})),
        len(infra.lab_pack_bl.get("v5", {})),
    )


# 🔸 Загрузка Active-порогов MW-BL (smoothed) — initial-load
async def _load_mw_bl_active_all():
    cache: _Dict[_Tuple[int, str, str, str, str], dict] = {}
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT strategy_id, timeframe, direction,
                   best_threshold_smoothed, best_roi, roi_base, positions_total, deposit_used, computed_at
            FROM oracle_mw_bl_active
            """
        )
    for r in rows:
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"]); direction = str(r["direction"])
        threshold = int(r["best_threshold_smoothed"] or 0)
        cache[(sid, ACTIVE_LISTS_VERSION, DECISION_MODE_SMOOTHED, direction, tf)] = {
            "threshold": threshold,
            "best_roi": float(r["best_roi"] or 0.0),
            "roi_base": float(r["roi_base"] or 0.0),
            "positions_total": int(r["positions_total"] or 0),
            "deposit_used": float(r["deposit_used"] or 0.0),
            "computed_at": str(r["computed_at"] or "") or "",
        }
    set_mw_bl_active_bulk(cache)
    log.info("✅ LAB: MW-BL Active загружены (initial): records=%d, version=%s, mode=%s",
             len(infra.lab_mw_bl_active), ACTIVE_LISTS_VERSION, DECISION_MODE_SMOOTHED)


# 🔸 Загрузка Active-порогов PACK-BL (smoothed) — initial-load
async def _load_pack_bl_active_all():
    cache: _Dict[_Tuple[int, str, str, str, str], dict] = {}
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT strategy_id, timeframe, direction,
                   best_threshold_smoothed, best_roi, roi_base, positions_total, deposit_used, computed_at
            FROM oracle_pack_bl_active
            """
        )
    for r in rows:
        sid = int(r["strategy_id"])
        tf = str(r["timeframe"]); direction = str(r["direction"])
        threshold = int(r["best_threshold_smoothed"] or 0)
        cache[(sid, ACTIVE_LISTS_VERSION, DECISION_MODE_SMOOTHED, direction, tf)] = {
            "threshold": threshold,
            "best_roi": float(r["best_roi"] or 0.0),
            "roi_base": float(r["roi_base"] or 0.0),
            "positions_total": int(r["positions_total"] or 0),
            "deposit_used": float(r["deposit_used"] or 0.0),
            "computed_at": str(r["computed_at"] or "") or "",
        }
    set_bl_active_bulk(cache)
    log.info("✅ LAB: PACK-BL Active загружены (initial): records=%d, version=%s, mode=%s",
             len(infra.lab_bl_active), ACTIVE_LISTS_VERSION, DECISION_MODE_SMOOTHED)


# 🔸 Точечные перезагрузки по сообщению стрима (sid+version)

async def _reload_mw_wl_for_strategy(strategy_id: int, version: str):
    slice_map: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
    wr_map: Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT a.timeframe, a.direction, a.agg_base, a.agg_state, a.winrate
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.strategy_id = $1 AND w.version = $2 AND w.list = 'whitelist'
            """,
            int(strategy_id), str(version)
        )

    for r in rows:
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["agg_base"]); state = str(r["agg_state"])
        wr = float(r["winrate"] or 0.0)

        key = (tf, direction)
        slice_map.setdefault(key, set()).add((base, state))
        wr_map.setdefault((int(strategy_id), tf, direction), {})[(base, state)] = wr

    update_mw_whitelist_for_strategy(version, strategy_id, slice_map, wr_map=wr_map)
    log.info("🔁 LAB: MW WL обновлён из all_ready — sid=%d, version=%s, slices=%d",
             strategy_id, version, sum(len(v) for v in slice_map.values()))


async def _reload_mw_bl_for_strategy(strategy_id: int, version: str):
    slice_map: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
    wr_map: Dict[Tuple[int, str, str], Dict[Tuple[str, str], float]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT a.timeframe, a.direction, a.agg_base, a.agg_state, a.winrate
            FROM oracle_mw_whitelist w
            JOIN oracle_mw_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.strategy_id = $1 AND w.version = $2 AND w.list = 'blacklist'
            """,
            int(strategy_id), str(version)
        )

    for r in rows:
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["agg_base"]); state = str(r["agg_state"])
        wr = float(r["winrate"] or 0.0)

        key = (tf, direction)
        slice_map.setdefault(key, set()).add((base, state))
        wr_map.setdefault((int(strategy_id), tf, direction), {})[(base, state)] = wr

    update_mw_blacklist_for_strategy(version, strategy_id, slice_map, wr_map=wr_map)
    log.info("🔁 LAB: MW BL обновлён из all_ready — sid=%d, version=%s, slices=%d",
             strategy_id, version, sum(len(v) for v in slice_map.values()))


async def _reload_pack_lists_for_strategy(strategy_id: int, version: str):
    wl_slice: Dict[Tuple[str, str], Set[Tuple[str, str, str]]] = {}
    bl_slice: Dict[Tuple[str, str], Set[Tuple[str, str, str]]] = {}
    wl_wr: Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]] = {}
    bl_wr: Dict[Tuple[int, str, str], Dict[Tuple[str, str, str], float]] = {}

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT w.list, a.timeframe, a.direction, a.pack_base, a.agg_key, a.agg_value, a.winrate
            FROM oracle_pack_whitelist w
            JOIN oracle_pack_aggregated_stat a ON a.id = w.aggregated_id
            WHERE a.time_frame = '7d' AND w.strategy_id = $1 AND w.version = $2
            """,
            int(strategy_id), str(version)
        )

    for r in rows:
        lst = str(r["list"]).lower()
        tf = str(r["timeframe"]); direction = str(r["direction"])
        base = str(r["pack_base"]); akey = str(r["agg_key"]); aval = str(r["agg_value"])
        wr = float(r["winrate"] or 0.0)

        key = (tf, direction)
        fullkey = (int(strategy_id), tf, direction)
        tpl = (base, akey, aval)

        if lst == "whitelist":
            wl_slice.setdefault(key, set()).add(tpl)
            wl_wr.setdefault(fullkey, {})[tpl] = wr
        else:
            bl_slice.setdefault(key, set()).add(tpl)
            bl_wr.setdefault(fullkey, {})[tpl] = wr

    update_pack_list_for_strategy("whitelist", version, strategy_id, wl_slice, wr_map=wl_wr)
    update_pack_list_for_strategy("blacklist", version, strategy_id, bl_slice, wr_map=bl_wr)
    log.info(
        "🔁 LAB: PACK WL/BL обновлены из all_ready — sid=%d, version=%s, wl_slices=%d, bl_slices=%d",
        strategy_id, version, sum(len(v) for v in wl_slice.values()), sum(len(v) for v in bl_slice.values())
    )


# 🔸 Active: точечная перезагрузка MW-BL (smoothed) по sid
async def _reload_mw_bl_active_for_strategy(strategy_id: int, version: str) -> int:
    updated = 0
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timeframe, direction, best_threshold_smoothed, best_roi, roi_base, positions_total, deposit_used, computed_at
            FROM oracle_mw_bl_active
            WHERE strategy_id = $1
            """,
            int(strategy_id),
        )
    for r in rows:
        tf = str(r["timeframe"]); direction = str(r["direction"])
        upsert_mw_bl_active(
            master_sid=int(strategy_id),
            version=str(version),
            decision_mode=DECISION_MODE_SMOOTHED,
            direction=direction,
            tf=tf,
            threshold=int(r["best_threshold_smoothed"] or 0),
            best_roi=float(r["best_roi"] or 0.0),
            roi_base=float(r["roi_base"] or 0.0),
            positions_total=int(r["positions_total"] or 0),
            deposit_used=float(r["deposit_used"] or 0.0),
            computed_at=str(r["computed_at"] or "") or "",
        )
        updated += 1
    log.info("🔁 LAB: MW-BL Active обновлён из all_ready — sid=%d, version=%s, updated=%d, mode=%s",
             strategy_id, version, updated, DECISION_MODE_SMOOTHED)
    return updated


# 🔸 Active: точечная перезагрузка PACK-BL (smoothed) по sid
async def _reload_pack_bl_active_for_strategy(strategy_id: int, version: str) -> int:
    updated = 0
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT timeframe, direction, best_threshold_smoothed, best_roi, roi_base, positions_total, deposit_used, computed_at
            FROM oracle_pack_bl_active
            WHERE strategy_id = $1
            """,
            int(strategy_id),
        )
    for r in rows:
        tf = str(r["timeframe"]); direction = str(r["direction"])
        upsert_bl_active(
            master_sid=int(strategy_id),
            version=str(version),
            decision_mode=DECISION_MODE_SMOOTHED,
            direction=direction,
            tf=tf,
            threshold=int(r["best_threshold_smoothed"] or 0),
            best_roi=float(r["best_roi"] or 0.0),
            roi_base=float(r["roi_base"] or 0.0),
            positions_total=int(r["positions_total"] or 0),
            deposit_used=float(r["deposit_used"] or 0.0),
            computed_at=str(r["computed_at"] or "") or "",
        )
        updated += 1
    log.info("🔁 LAB: PACK-BL Active обновлён из all_ready — sid=%d, version=%s, updated=%d, mode=%s",
             strategy_id, version, updated, DECISION_MODE_SMOOTHED)
    return updated