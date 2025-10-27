# trader_informer.py — воркер мгновенных уведомлений о жизненных событиях позиций (opened v2 + выборочная публикация v1.4)

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from dataclasses import dataclass
from typing import Optional, Dict, Set, Any, List

from infra import infra
from config_loader import config
from position_state_loader import position_registry

# 🔸 Логгер
log = logging.getLogger("TRADER_INFORMER")

# 🔸 Константы стримов и Consumer Groups
STREAM_OPEN = "positions_open_stream"
STREAM_UPD = "positions_update_stream"
STREAM_OUT = "positions_bybit_status"

STRATEGY_STATE_STREAM = "strategy_state_stream"

CG_OPEN = "pos_status_open_cg"
CG_OPEN_CONSUMER = "pos_status_open_1"

CG_UPD = "pos_status_update_cg"
CG_UPD_CONSUMER = "pos_status_update_1"

CG_STATE = "pos_status_state_cg"
CG_STATE_CONSUMER = "pos_status_state_1"

XREAD_BLOCK_MS = 1000
XREAD_COUNT = 100

# 🔸 Кэш позиции по uid (для быстрых публикаций без БД)
@dataclass
class _PosSnap:
    entry_price: Optional[Decimal] = None
    quantity: Optional[Decimal] = None
    quantity_left: Optional[Decimal] = None
    direction: Optional[str] = None
    symbol: Optional[str] = None

# 🔸 In-memory кэши
_pos_cache: Dict[str, _PosSnap] = {}
_watch_ids: Set[int] = set()

# 🔸 Хелперы
def _b2s(x):
    return x.decode("utf-8", errors="replace") if isinstance(x, (bytes, bytearray)) else x

def _get(d: dict, key: str):
    return _b2s(d.get(key) if key in d else d.get(key.encode(), None))

def _now_utc_with_ms():
    # возвращает (ts_iso_Z, ts_ms_str, emitted_dt_naive_utc)
    now = datetime.now(timezone.utc)
    ts_iso_z = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    ts_ms = str(int(now.timestamp() * 1000))
    emitted_dt_naive = now.replace(tzinfo=None)
    return ts_iso_z, ts_ms, emitted_dt_naive

def _to_dec(val: Optional[Any]) -> Optional[Decimal]:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None

def _get_leverage_from_config(strategy_id: int) -> Optional[Decimal]:
    s = config.strategies.get(strategy_id) or {}
    return _to_dec(s.get("leverage"))

def _is_watched(strategy_id: int) -> bool:
    return strategy_id in _watch_ids

def _is_reverse_strategy(strategy_id: int) -> bool:
    s = config.strategies.get(strategy_id) or {}
    return bool(s.get("reverse")) and bool(s.get("sl_protection"))

def _fill_cache_from_registry(position_uid: str) -> Optional[_PosSnap]:
    # попытка восстановить из рантайм-реестра (при холодном старте/гонках)
    for st in position_registry.values():
        if st.uid == position_uid:
            snap = _PosSnap(
                entry_price=_to_dec(st.entry_price),
                quantity=_to_dec(st.quantity),
                quantity_left=_to_dec(st.quantity_left),
                direction=str(st.direction) if st.direction else None,
                symbol=str(st.symbol) if getattr(st, "symbol", None) else None,
            )
            _pos_cache[position_uid] = snap
            return snap
    return None

def _get_snap(uid: str) -> Optional[_PosSnap]:
    snap = _pos_cache.get(uid)
    if snap:
        return snap
    return _fill_cache_from_registry(uid)

def _compute_margin_used(entry_price: Optional[Decimal], qty_left: Optional[Decimal], leverage: Optional[Decimal]) -> Optional[Decimal]:
    # margin_used = (quantity_left * entry_price) / leverage ; квантование до 4 знаков
    if entry_price is None or qty_left is None or leverage in (None, Decimal("0")):
        return None
    val = (qty_left * entry_price) / leverage
    try:
        return val.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
    except Exception:
        return val

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

# 🔸 Запись события в БД (таблица trader_signals) — с сохранением stream_id
async def _persist_signal(
    *,
    strategy_id: int,
    position_uid: str,
    direction: str,
    event: str,
    message: str,
    strategy_type: str,
    emitted_ts_dt: datetime,
    stream_id: Optional[str] = None,
    tp_level: Optional[int] = None,
    extras: Optional[Dict[str, str]] = None,
):
    # подготовить числовые поля из extras
    lev = _to_dec((extras or {}).get("leverage")) if extras else None
    qty = _to_dec((extras or {}).get("quantity")) if extras else None
    qty_left = _to_dec((extras or {}).get("quantity_left")) if extras else None
    margin = _to_dec((extras or {}).get("margin_used")) if extras else None
    extras_json = json.dumps(extras or {})

    try:
        async with infra.pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO public.trader_signals (
                    strategy_id, position_uid, direction, event, tp_level,
                    message, strategy_type,
                    leverage, quantity, quantity_left, margin_used,
                    side, emitted_ts, received_at, extras, stream_id
                )
                VALUES ($1,$2,$3,$4,$5,
                        $6,$7,
                        $8,$9,$10,$11,
                        'system',$12, NOW(), $13, $14)
                """,
                strategy_id, position_uid, direction, event, tp_level,
                message, strategy_type,
                lev, qty, qty_left, margin,
                emitted_ts_dt, extras_json, stream_id,
            )
    except Exception:
        # не ломаем основное выполнение: публикация уже произошла
        log.exception("⚠️ Не удалось записать сигнал в trader_signals (sid=%s uid=%s event=%s stream_id=%s)",
                      strategy_id, position_uid, event, stream_id)

# 🔸 Публикация события в выходной стрим (общая) — вернуть stream_id
async def _publish(strategy_id: int, payload: Dict[str, str]) -> str:
    stream_id = await infra.redis_client.xadd(STREAM_OUT, payload)
    return str(stream_id)

# 🔸 Проверка: публиковать ли sl_replaced (reverse-ветка, стартовый SL на entry на весь объём)
def _should_publish_sl_replaced_reverse(strategy_id: int, position_uid: str, evt: Dict[str, Any]) -> bool:
    # стратегия должна быть реверсной (и под защитой)
    if not _is_reverse_strategy(strategy_id):
        return False

    # нужен снапшот с entry_price и quantity_left
    snap = _get_snap(position_uid)
    if not snap or snap.entry_price is None or snap.quantity_left is None:
        return False

    # должны присутствовать sl_targets в событии
    sl_json = evt.get("sl_targets")
    if not sl_json:
        return False

    try:
        sl_targets: List[Dict[str, Any]] = json.loads(sl_json)
    except Exception:
        return False

    # выбираем активные SL (не hit/не canceled)
    active = [
        sl for sl in sl_targets
        if (sl.get("type") == "sl"
            and not bool(sl.get("hit"))
            and not bool(sl.get("canceled")))
    ]
    if not active:
        return False

    # проверяем: есть SL с ценой = entry и объёмом = quantity_left
    entry = snap.entry_price
    qty_left = snap.quantity_left

    def _dec(x): return _to_dec(x)

    for sl in active:
        sl_price = _dec(sl.get("price"))
        sl_qty = _dec(sl.get("quantity"))
        if sl_price is not None and sl_qty is not None:
            if sl_price == entry and sl_qty == qty_left:
                return True
    return False

# 🔸 Обработка OPENED (из positions_open_stream) — публикация opened v2 и запись в БД
async def _handle_open_records(records):
    for record_id, data in records:
        try:
            # обязательные поля
            strategy_id_raw = _get(data, "strategy_id")
            position_uid = _get(data, "position_uid")
            direction = _get(data, "direction")
            event_type = _get(data, "event_type")

            if not (strategy_id_raw and position_uid and direction and event_type == "opened"):
                # нерелевантная запись — ACK сразу
                await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)
                continue

            strategy_id = int(strategy_id_raw)
            if not _is_watched(strategy_id):
                # не в watchlist — ACK
                await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)
                continue

            # данные из события открытия
            symbol = _get(data, "symbol")
            entry_price = _to_dec(_get(data, "entry_price"))
            quantity = _to_dec(_get(data, "quantity"))
            quantity_left = _to_dec(_get(data, "quantity_left")) or quantity
            # leverage: сначала из события, иначе из конфига
            lev_event = _to_dec(_get(data, "leverage"))
            lev_cfg = _get_leverage_from_config(strategy_id)
            leverage = lev_event if (lev_event is not None and lev_event > 0) else lev_cfg

            # проверки полноты для opened v2
            missing = []
            if not symbol:
                missing.append("symbol")
            if entry_price is None:
                missing.append("entry_price")
            if quantity is None:
                missing.append("quantity")
            if quantity_left is None:
                missing.append("quantity_left")
            if leverage is None or leverage <= 0:
                missing.append("leverage")

            if missing:
                # не ACK — пусть запись останется в pending для повторной попытки
                log.error("❌ opened v2: недостаточно данных (%s). record_id=%s sid=%s uid=%s",
                          ",".join(missing), record_id, strategy_id, position_uid)
                continue

            # кэшируем снапшот (поможет для апдейтов)
            _pos_cache[position_uid] = _PosSnap(
                entry_price=entry_price, quantity=quantity, quantity_left=quantity_left,
                direction=direction, symbol=symbol
            )

            # расчёт margin_used
            margin_used = _compute_margin_used(entry_price, quantity_left, leverage)
            if margin_used is None:
                log.error("❌ opened v2: не удалось посчитать margin_used. record_id=%s sid=%s uid=%s", record_id, strategy_id, position_uid)
                continue  # без ACK

            # формируем v2 payload
            ts_iso_z, ts_ms, emitted_dt = _now_utc_with_ms()
            payload = {
                "strategy_id": str(strategy_id),
                "position_uid": str(position_uid),
                "symbol": symbol,
                "direction": direction,
                "event": "opened",
                "message": _message_for_event("opened"),
                "strategy_type": _strategy_type(strategy_id),
                "side": "system",
                "ts": ts_iso_z,
                "ts_ms": ts_ms,
                "schema": "v2",
                "leverage": str(leverage),
                "quantity": str(quantity),
                "quantity_left": str(quantity_left),
                "margin_used": str(margin_used),
            }

            # публикация → запись в БД → ACK
            stream_id = await _publish(strategy_id, payload)
            extras = {
                "leverage": payload["leverage"],
                "quantity": payload["quantity"],
                "quantity_left": payload["quantity_left"],
                "margin_used": payload["margin_used"],
                "symbol": symbol,
                "schema": "v2",
                "ts_ms": ts_ms,
            }
            await _persist_signal(
                strategy_id=strategy_id,
                position_uid=position_uid,
                direction=direction,
                event="opened",
                message=payload["message"],
                strategy_type=payload["strategy_type"],
                emitted_ts_dt=emitted_dt,
                stream_id=stream_id,
                extras=extras,
            )
            await infra.redis_client.xack(STREAM_OPEN, CG_OPEN, record_id)

            log.info(
                "[PUB] opened v2 sid=%s uid=%s sym=%s dir=%s type=%s qty=%s lev=%s margin=%s ts=%s stream_id=%s",
                strategy_id, position_uid, symbol, direction, payload["strategy_type"],
                payload["quantity"], payload["leverage"], payload["margin_used"], payload["ts"], stream_id,
            )

        except Exception:
            # любая иная ошибка — без ACK (повтор)
            log.exception("❌ Ошибка обработки записи OPENED (id=%s)", record_id)

# 🔸 Обработка UPDATE (из positions_update_stream) — выборочная публикация и журнал
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

            # определяем направление/символ позиции
            snap = _get_snap(position_uid)
            direction: Optional[str] = snap.direction if snap else None
            symbol: Optional[str] = snap.symbol if snap else (str(evt.get("symbol")) if evt.get("symbol") else None)

            if direction is None:
                if event_type == "closed" and evt.get("original_direction"):
                    direction = str(evt.get("original_direction"))
                else:
                    direction = None
            if direction is None:
                for st in position_registry.values():
                    if st.uid == position_uid and st.direction:
                        direction = str(st.direction)
                        break

            if direction is None:
                log.warning("[SKIP] direction unknown for uid=%s sid=%s event_type=%s", position_uid, strategy_id, event_type)
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            # общее время для события
            ts_iso_z, ts_ms, emitted_dt = _now_utc_with_ms()

            # публикация по типам
            if event_type == "tp_hit":
                # обновим кэш остатка, но НЕ публикуем и НЕ пишем в БД
                new_left = _to_dec(evt.get("quantity_left"))
                if snap and new_left is not None:
                    snap.quantity_left = new_left
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            elif event_type == "sl_replaced":
                # публикуем ТОЛЬКО sl_replaced из реверс-ветки (SL=entry на весь текущий объем)
                if _should_publish_sl_replaced_reverse(strategy_id, position_uid, evt):
                    payload = {
                        "strategy_id": str(strategy_id),
                        "position_uid": position_uid,
                        "direction": direction,
                        "event": "sl_replaced",
                        "message": _message_for_event("sl_replaced"),
                        "strategy_type": _strategy_type(strategy_id),
                        "side": "system",
                        "ts": ts_iso_z,
                    }
                    if symbol:
                        payload["symbol"] = symbol

                    stream_id = await _publish(strategy_id, payload)
                    # журналируем только опубликованное
                    extras_db: Dict[str, str] = {"ts_ms": ts_ms}
                    if symbol:
                        extras_db["symbol"] = symbol
                    await _persist_signal(
                        strategy_id=strategy_id,
                        position_uid=position_uid,
                        direction=direction,
                        event="sl_replaced",
                        message=payload["message"],
                        strategy_type=payload["strategy_type"],
                        emitted_ts_dt=emitted_dt,
                        stream_id=stream_id,
                        extras=extras_db,
                    )
                    await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                else:
                    # не соответствует критериям — ACK без публикации и без записи в БД
                    await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            elif event_type == "closed":
                reason = str(evt.get("close_reason") or "")
                event = _map_closed_event(reason)

                lev = _get_leverage_from_config(strategy_id)
                extras_pub = {"quantity_left": "0", "margin_used": "0"}
                if symbol:
                    extras_pub["symbol"] = symbol
                if lev is not None:
                    extras_pub["leverage"] = str(lev)
                if snap and snap.quantity is not None:
                    extras_pub["quantity"] = str(snap.quantity)

                payload = {
                    "strategy_id": str(strategy_id),
                    "position_uid": position_uid,
                    "direction": direction,
                    "event": event,
                    "message": _message_for_event(event),
                    "strategy_type": _strategy_type(strategy_id),
                    "side": "system",
                    "ts": ts_iso_z,
                }
                payload.update(extras_pub)

                stream_id = await _publish(strategy_id, payload)

                # позиция закрыта — очищаем кэш
                if position_uid in _pos_cache:
                    del _pos_cache[position_uid]

                # журналируем только опубликованное
                extras_db = dict(extras_pub)
                extras_db["ts_ms"] = ts_ms
                await _persist_signal(
                    strategy_id=strategy_id,
                    position_uid=position_uid,
                    direction=direction,
                    event=event,
                    message=payload["message"],
                    strategy_type=payload["strategy_type"],
                    emitted_ts_dt=emitted_dt,
                    stream_id=stream_id,
                    extras=extras_db,
                )

                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

            else:
                # неизвестные/нерелевантные типы — просто ACK
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
                continue

        except Exception:
            log.exception("❌ Ошибка обработки записи UPDATE (id=%s)", record_id)
            try:
                await infra.redis_client.xack(STREAM_UPD, CG_UPD, record_id)
            except Exception:
                log.exception("❌ XACK UPDATE (id=%s) не удался")

# 🔸 Цикл чтения OPEN
async def _read_open_loop():
    redis = infra.redis_client
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
                block=XREAD_BLOCK_MS,
            )
            if not entries:
                continue

            for _, records in entries:
                await _handle_open_records(records)

        except Exception:
            log.exception("❌ Ошибка в цикле чтения OPEN")
            await asyncio.sleep(1.0)

# 🔸 Цикл чтения UPDATE
async def _read_update_loop():
    redis = infra.redis_client
    try:
        await redis.xgroup_create(STREAM_UPD, CG_UPD, id="$", mkstream=True)
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
                block=XREAD_BLOCK_MS,
            )
            if not entries:
                continue

            for _, records in entries:
                await _handle_update_records(records)

        except Exception:
            log.exception("❌ Ошибка в цикле чтения UPDATE")
            await asyncio.sleep(1.0)

# 🔸 Слежение за применёнными изменениями стратегий (Streams)
async def _read_state_loop():
    redis = infra.redis_client
    try:
        await redis.xgroup_create(STRATEGY_STATE_STREAM, CG_STATE, id="$", mkstream=True)
        log.info("📡 CG создана: %s → %s", STRATEGY_STATE_STREAM, CG_STATE)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info("ℹ️ CG уже существует: %s", CG_STATE)
        else:
            log.exception("❌ Ошибка создания CG для %s", STRATEGY_STATE_STREAM)
            return

    # один раз при старте — собрать и залогировать размер watchlist
    global _watch_ids
    _watch_ids = {sid for sid, s in (config.strategies or {}).items() if s.get("trader_winner", False)}
    log.info("🔎 TRADER_INFORMER: watchlist initialized — %d strategies", len(_watch_ids))

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=CG_STATE,
                consumername=CG_STATE_CONSUMER,
                streams={STRATEGY_STATE_STREAM: ">"},
                count=XREAD_COUNT,
                block=XREAD_BLOCK_MS,
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, data in records:
                    try:
                        if data.get("type") == "strategy" and data.get("action") == "applied":
                            # при каждом 'applied' пересобираем watchlist из config
                            new_ids = {sid for sid, s in (config.strategies or {}).items() if s.get("trader_winner", False)}
                            added = new_ids - _watch_ids
                            removed = _watch_ids - new_ids

                            if added:
                                log.info("✅ watchlist: added %s (total=%d)", sorted(added), len(new_ids))
                            if removed:
                                log.info("🗑️ watchlist: removed %s (total=%d)", sorted(removed), len(new_ids))

                            _watch_ids = new_ids

                        await redis.xack(STRATEGY_STATE_STREAM, CG_STATE, record_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки записи state/applied")
        except Exception:
            log.exception("❌ Ошибка чтения из state-stream")
            await asyncio.sleep(1.0)

# 🔸 Публичная точка запуска воркера
async def run_trader_informer():
    log.info("🚀 Запуск воркера TRADER_INFORMER → %s", STREAM_OUT)
    await asyncio.gather(
        _read_open_loop(),
        _read_update_loop(),
        _read_state_loop(),  # обновление watchlist по двухфазному протоколу (Streams)
    )