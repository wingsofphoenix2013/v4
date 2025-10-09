# laboratory_config.py — загрузка конфигов laboratory_v4: тикеры, стратегии, whitelist (версионные); слушатели Pub/Sub и Streams

# 🔸 Импорты
import json
import logging
import asyncio

import laboratory_infra as infra
from laboratory_infra import (
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

# 🔸 Логгер
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


# 🔸 Первичная загрузка whitelist (PACK) — версионная
async def load_pack_whitelist():
    query = "SELECT * FROM oracle_pack_whitelist"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)

    # группировка по (strategy_id, version)
    grouped: dict[tuple[int, str], list] = {}
    for r in rows:
        sid = int(r["strategy_id"])
        ver = str(r.get("version") or "v1").strip().lower()
        grouped.setdefault((sid, ver), []).append(dict(r))

    for (sid, ver), data in grouped.items():
        set_pack_whitelist_for_strategy(sid, data, {"loaded": True}, version=ver)

    log.info("✅ Загружено PACK whitelist (стратегий×версий: %d)", len(grouped))


# 🔸 Первичная загрузка whitelist (MW) — версионная
async def load_mw_whitelist():
    query = "SELECT * FROM oracle_mw_whitelist"
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query)

    # группировка по (strategy_id, version)
    grouped: dict[tuple[int, str], list] = {}
    for r in rows:
        sid = int(r["strategy_id"])
        ver = str(r.get("version") or "v1").strip().lower()
        grouped.setdefault((sid, ver), []).append(dict(r))

    for (sid, ver), data in grouped.items():
        set_mw_whitelist_for_strategy(sid, data, {"loaded": True}, version=ver)

    log.info("✅ Загружено MW whitelist (стратегий×версий: %d)", len(grouped))


# 🔸 Точечное обновление стратегии (по событию Pub/Sub)
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


# 🔸 Вспомогательное: распаковка payload из записи XREAD/XREADGROUP
def _extract_stream_payload(fields: dict) -> dict:
    """
    Поддерживает два формата:
      1) Плоские поля: strategy_id=..., report_id=..., ...
      2) Один ключ 'data' с JSON-строкой: {"strategy_id":..., ...}
    Также нормализует синонимы метрик: rows_whitelist/rows_blacklist ↔ wl/bl, rows_inserted ↔ rows_total.
    Добавляет нормализованную версию 'version' (v1 по умолчанию).
    """
    # базовая распаковка
    payload: dict = {}
    for k, v in fields.items():
        if isinstance(v, str) and v.startswith("{"):
            try:
                payload[k] = json.loads(v)
            except Exception:
                payload[k] = v
        else:
            payload[k] = v

    # если всё лежит под 'data' — разворачиваем
    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    # нормализация типов (числа могут прийти как строки)
    def _as_int(x, default=None):
        try:
            return int(x)
        except Exception:
            return default

    # синонимы полей
    if "wl" not in payload and "rows_whitelist" in payload:
        payload["wl"] = payload["rows_whitelist"]
    if "bl" not in payload and "rows_blacklist" in payload:
        payload["bl"] = payload["rows_blacklist"]
    if "rows_total" not in payload and "rows_inserted" in payload:
        payload["rows_total"] = payload["rows_inserted"]

    # приведение ключевых полей к ожидаемым типам
    if "strategy_id" in payload:
        payload["strategy_id"] = _as_int(payload["strategy_id"], None)
    if "report_id" in payload:
        payload["report_id"] = _as_int(payload["report_id"], None)

    # нормализация версии
    ver = str(payload.get("version") or "v1").strip().lower()
    payload["version"] = ver

    return payload


# 🔸 Слушатель Streams обновлений whitelist (PACK + MW) — версионный
async def whitelist_stream_listener():
    streams = {
        "oracle:pack_lists:reports_ready": "pack",
        "oracle:mw_whitelist:reports_ready": "mw",
    }
    last_ids = {k: "$" for k in streams.keys()}
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
                        # нормализуем payload
                        payload = _extract_stream_payload(fields)

                        sid = payload.get("strategy_id")
                        ver = payload.get("version") or "v1"
                        if not sid:
                            log.info("⚠️ Пропуск события stream=%s msg=%s: нет strategy_id в payload=%s",
                                     stream_name, msg_id, payload)
                            continue

                        if streams[stream_name] == "pack":
                            await start_pack_update(sid, ver)
                            try:
                                async with infra.pg_pool.acquire() as conn:
                                    rows = await conn.fetch(
                                        "SELECT * FROM oracle_pack_whitelist WHERE strategy_id=$1 AND version=$2",
                                        sid, ver
                                    )
                                if rows:
                                    meta = {
                                        "version": ver,
                                        "report_id": payload.get("report_id"),
                                        "time_frame": payload.get("time_frame"),
                                        "window_end": payload.get("window_end"),
                                        "generated_at": payload.get("generated_at"),
                                        "rows_total": payload.get("rows_total"),
                                        "wl": payload.get("wl"),
                                        "bl": payload.get("bl"),
                                    }
                                    set_pack_whitelist_for_strategy(sid, [dict(r) for r in rows], meta, version=ver)
                                else:
                                    clear_pack_whitelist_for_strategy(sid, version=ver)
                            finally:
                                finish_pack_update(sid, ver)

                        elif streams[stream_name] == "mw":
                            await start_mw_update(sid, ver)
                            try:
                                async with infra.pg_pool.acquire() as conn:
                                    rows = await conn.fetch(
                                        "SELECT * FROM oracle_mw_whitelist WHERE strategy_id=$1 AND version=$2",
                                        sid, ver
                                    )
                                if rows:
                                    meta = {
                                        "version": ver,
                                        "report_id": payload.get("report_id"),
                                        "time_frame": payload.get("time_frame"),
                                        "window_end": payload.get("window_end"),
                                        "generated_at": payload.get("generated_at"),
                                        "rows_total": payload.get("rows_total"),
                                        "wl": payload.get("wl") or payload.get("rows_whitelist"),
                                    }
                                    set_mw_whitelist_for_strategy(sid, [dict(r) for r in rows], meta, version=ver)
                                else:
                                    clear_mw_whitelist_for_strategy(sid, version=ver)
                            finally:
                                finish_mw_update(sid, ver)

                    except Exception:
                        log.exception("❌ Ошибка при обновлении WL по событию (stream=%s, msg=%s)", stream_name, msg_id)

        except asyncio.CancelledError:
            log.info("⏹️ Остановка слушателя Streams")
            raise
        except Exception:
            log.exception("❌ Ошибка в цикле whitelist_stream_listener")
            await asyncio.sleep(5)