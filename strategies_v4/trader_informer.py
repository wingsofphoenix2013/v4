# trader_informer.py — воркер мгновенных уведомлений о жизненных событиях позиций (только для trader_winner=true)

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Set

from infra import infra
from config_loader import config
from position_state_loader import position_registry

# 🔸 Логгер
log = logging.getLogger("TRADER_INFORMER")

# 🔸 Константы стримов и Consumer Groups
STREAM_OPEN = "positions_open_stream"
STREAM_UPD = "positions_update_stream"
STREAM_OUT = "positions_bybit_status"

CG_OPEN = "pos_status_open_cg"
CG_OPEN_CONSUMER = "pos_status_open_1"

CG_UPD = "pos_status_update_cg"
CG_UPD_CONSUMER = "pos_status_update_1"

XREAD_BLOCK_MS = 1000
XREAD_COUNT = 100

PUBSUB_CHANNEL_STRATEGIES = "strategies_v4_events"

# 🔸 Кэш направлений: position_uid → direction
_dir_cache: Dict[str, str] = {}

# 🔸 Watchlist: набор strategy_id с trader_winner=true
_watch_ids: Set[int] = set()


# 🔸 Хелперы
def _b2s(x):
    return x.decode("utf-8", errors="replace") if isinstance(x, (bytes, bytearray)) else x

def _get(d: dict, key: str):
    return _b2s(d.get(key) if key in d else d.get(key.encode(), None))

def _now_iso() -> str:
    # время публикации воркером (UTC, naive, ISO8601)
    return datetime.utcnow().isoformat(timespec="milliseconds")

def _strategy_type(strategy_id: int) -> str:
    s = config.strategies.get(strategy_id) or {}
    return "reverse" if bool(s.get("reverse")) and bool(s.get("sl_protection")) else "plain"

def _map_closed_event(reason: str) -> str:
    # reason из position_handler: tp-full-hit / full-sl-hit / sl-tp-hit / reverse-signal-stop / sl-protect-stop ...
    r = (reason or "").strip().lower().replace("-", "_")
    mapping = {
        "tp_full_hit": "closed.tp_full_hit",
        "full_sl_hit": "closed.full_sl_hit",
        "sl_tp_hit": "closed.sl_tp_hit",
        "reverse_signal_stop": "closed.reverse_signal_stop",
        "sl_protect_stop": "closed.sl_protect_stop",
    }
    return mapping.get(r, f"closed.{r}" if r else "closed.unknown")

def _message_for_event(event: str, tp_level: Optional[int] = None) -> str:
    # краткая человекочитаемая строка
    if event == "opened":
        return "position opened"
    if event == "tp_hit":
        return f"tp level {tp_level} hit" if tp_level is not None else "tp hit"
    if event == "sl_replaced":
        return "sl replaced"
    if event == "closed.tp_full_hit":
        return "closed by take-profit (full)"
    if event == "closed.full_sl_hit":
        return "closed by stop-loss (full)"
    if event == "closed.sl_tp_hit":
        return "closed by stop-loss (after tp)"
    if event == "closed.reverse_signal_stop":
        return "closed by reverse signal"
    if event == "closed.sl_protect_stop":
        return "closed by sl-protect"
    return "position event"

def _is_watched(strategy_id: int) -> bool:
    return strategy_id in _watch_ids

def _find_direction_from_registry(position_uid: str) -> Optional[str]:
    # быстрый поиск направления по рантайм-реестру (если кэша нет)
    for st in position_registry.values():
        if st.uid == position_uid:
            return str(st.direction)
    return None

def _remember_direction(uid: str, direction: str):
    _dir_cache[uid] = direction

def _forget_direction(uid: str):
    if uid in _dir_cache:
        del _dir_cache[uid]

def _rebuild_watch_ids() -> Set[int]:
    # пересобираем watchlist из живой конфигурации
    return {sid for sid, s in (config.strategies or {}).items() if s.get("trader_winner", False)}


# 🔸 Публикация события в выходной стрим
async def _publish(strategy_id: int, position_uid: str, direction: str, event: str, *, tp_level: Optional[int] = None):
    payload = {
        "strategy_id": str(strategy_id),
        "position_uid": str(position_uid),
        "direction": direction,
        "event": event,
        "message": _message_for_event(event, tp_level),
        "strategy_type": _strategy_type(strategy_id),
        "side": "system",
        "ts": _now_iso(),
    }
    if event == "tp_hit" and tp_level is not None:
        payload["tp_level"] = str(int(tp_level))

    await infra.redis_client.xadd(STREAM_OUT, payload)
    log.info(
        "[PUB] sid=%s uid=%s dir=%s type=%s event=%s ts=%s",
        strategy_id,
        position_uid,
        direction,
        payload["strategy_type"],
        event,
        payload["ts"],
    )


# 🔸 Обработка OPENED (из positions_open_stream)
async def _handle_open_records(records):
    for record_id, data in records:
        try:
            strategy_id_raw = _get(data, "strategy_id")
            position_uid = _get(data, "position_uid")
            direction = _get(data, "direction")
            event_type = _get(data, "event_type")

            if not (strategy_id_raw and position_uid and direction and event_type == "opened"):
                await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)
                continue

            strategy_id = int(strategy_id_raw)
            if not _is_watched(strategy_id):
                await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)
                continue

            _remember_direction(position_uid, direction)
            await _publish(strategy_id, position_uid, direction, "opened")

        except Exception:
            log.exception("❌ Ошибка обработки записи OPENED (id=%s)", record_id)
        finally:
            try:
                await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)
            except Exception:
                log.exception("❌ XACK OPENED (id=%s) не удался", record_id)


# 🔸 Обработка UPDATE (из positions_update_stream)
async def _handle_update_records(records):
    for record_id, raw in records:
        try:
            raw_data = _get(raw, "data")
            if not raw_data:
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            try:
                evt = json.loads(raw_data)
            except Exception:
                log.exception("❌ Парсинг JSON из positions_update_stream (id=%s)", record_id)
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            event_type = str(evt.get("event_type") or "")
            position_uid = str(evt.get("position_uid") or "")
            strategy_id = int(evt.get("strategy_id"))

            if not (event_type and position_uid):
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            if not _is_watched(strategy_id):
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            direction = _dir_cache.get(position_uid)
            if direction is None:
                if event_type == "closed" and evt.get("original_direction"):
                    direction = str(evt.get("original_direction"))
                else:
                    direction = _find_direction_from_registry(position_uid)

            if direction is None:
                log.warning("[SKIP] direction unknown for uid=%s sid=%s event_type=%s", position_uid, strategy_id, event_type)
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            if event_type == "tp_hit":
                tp_level = int(evt.get("tp_level"))
                await _publish(strategy_id, position_uid, direction, "tp_hit", tp_level=tp_level)

            elif event_type == "sl_replaced":
                await _publish(strategy_id, position_uid, direction, "sl_replaced")

            elif event_type == "closed":
                reason = str(evt.get("close_reason") or "")
                event = _map_closed_event(reason)
                await _publish(strategy_id, position_uid, direction, event)
                _forget_direction(position_uid)

            else:
                # неизвестные/нерелевантные типы — просто ACK
                pass

        except Exception:
            log.exception("❌ Ошибка обработки записи UPDATE (id=%s)", record_id)
        finally:
            try:
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
            except Exception:
                log.exception("❌ XACK UPDATE (id=%s) не удался", record_id)


# 🔸 Циклы чтения стримов
async def _read_open_loop():
    redis = infra.redis_client
    # создаём CG
    try:
        await redis.xgroup_create(STREAM_OPEN, CG_OPEN, id="$", mkstream=True)
        log.info("📡 CG создана: %s → %s", STREAM_OPEN, CG_OPEN)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ CG уже существует: %s", CG_OPEN)
        else:
            log.exception("❌ Ошибка создания CG для %s", STREAM_OPEN)
            return

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_OPEN,
                consumername=CG_OPEN_CONSUMER,
                streams={STREAM_OPEN: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not entries:
                continue

            for _, records in entries:
                await _handle_open_records(records)

        except Exception:
            log.exception("❌ Ошибка в цикле чтения OPEN")
            await asyncio.sleep(1.0)

async def _read_update_loop():
    redis = infra.redis_client
    # создаём CG
    try:
        await redis.xgroup_create(STREAM_UPD, CG_UPD, id="$", mkstream=True)
        log.info("📡 CG создана: %s → %s", STREAM_UPD, CG_UPD)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ CG уже существует: %s", CG_UPD)
        else:
            log.exception("❌ Ошибка создания CG для %s", STREAM_UPD)
            return

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_UPD,
                consumername=CG_UPD_CONSUMER,
                streams={STREAM_UPD: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS
            )
            if not entries:
                continue

            for _, records in entries:
                await _handle_update_records(records)

        except Exception:
            log.exception("❌ Ошибка в цикле чтения UPDATE")
            await asyncio.sleep(1.0)


# 🔸 Слежение за изменениями watchlist (через Pub/Sub стратегий)
async def _watchlist_pubsub_loop():
    # один раз при старте — собрать и залогировать размер watchlist
    global _watch_ids
    _watch_ids = _rebuild_watch_ids()
    log.info("🔎 TRADER_INFORMER: watchlist initialized — %d strategies", len(_watch_ids))

    # подписка на изменения стратегий
    redis = infra.redis_client
    pubsub = redis.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL_STRATEGIES)
    log.info("📡 Подписка на %s (для обновления watchlist)", PUBSUB_CHANNEL_STRATEGIES)

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(_b2s(msg["data"]))
        except Exception:
            continue

        # при любом событии стратегий — пересобираем watchlist и логируем добавления
        new_ids = _rebuild_watch_ids()
        added = new_ids - _watch_ids
        removed = _watch_ids - new_ids

        if added:
            log.info("✅ watchlist: added %s (total=%d)", sorted(added), len(new_ids))
        if removed:
            log.info("🗑️ watchlist: removed %s (total=%d)", sorted(removed), len(new_ids))

        _watch_ids = new_ids


# 🔸 Публичная точка запуска воркера
async def run_trader_informer():
    log.info("🚀 Запуск воркера TRADER_INFORMER → %s", STREAM_OUT)
    await asyncio.gather(
        _read_open_loop(),
        _read_update_loop(),
        _watchlist_pubsub_loop(),  # только сообщения об изменениях; без heartbeat
    )