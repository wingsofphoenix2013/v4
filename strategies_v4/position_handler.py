# position_handler.py

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
import json

from infra import infra
from infra import get_field, set_field
from position_state_loader import position_registry
from config_loader import config
from core_io import reverse_entry

# 🔸 Логгер для обработчика позиций
log = logging.getLogger("POSITION_HANDLER")

# 🔸 Отправка обновлённой позиции в Redis-поток
async def push_position_update(position, redis):
    def serialize_targets(targets):
        return [
            {
                "level": t["level"],
                "price": str(t["price"]) if t["price"] is not None else None,
                "quantity": str(t["quantity"]),
                "type": t["type"],
                "hit": bool(t["hit"]),
                "hit_at": t["hit_at"].isoformat() if t["hit_at"] else None,
                "canceled": bool(t["canceled"]),
                "source": t.get("source", "price")
            }
            for t in targets
        ]

    payload = {
        "position_uid": position.uid,
        "strategy_id": position.strategy_id,
        "quantity_left": str(position.quantity_left),
        "status": position.status,
        "exit_price": str(position.exit_price) if position.exit_price else None,
        "close_reason": position.close_reason,
        "pnl": str(position.pnl),
        "planned_risk": str(position.planned_risk),
        "closed_at": position.closed_at.isoformat() if position.closed_at else None,
        "tp_targets": serialize_targets(position.tp_targets),
        "sl_targets": serialize_targets(position.sl_targets)
    }

    try:
        await redis.xadd("positions_update_stream", {"data": json.dumps(payload)})
        log.info(f"📤 Обновление позиции отправлено в Redis: uid={position.uid}")
    except Exception as e:
        log.warning(f"⚠️ Ошибка отправки обновления позиции: {e}")

# 🔸 Главный цикл мониторинга всех позиций
async def run_position_monitor_loop():
    log.info("✅ [POSITION_HANDLER] Цикл мониторинга позиций запущен")
    while True:
        try:
            for position in list(position_registry.values()):
                asyncio.create_task(process_position(position))
            await asyncio.sleep(1)
        except Exception:
            log.exception("❌ [POSITION_HANDLER] Ошибка в основном цикле")


# 🔸 Обработка одной позиции под lock
async def process_position(position):
    async with position.lock:
        log.debug(f"🔒 [POSITION_HANDLER] LOCK: позиция {position.uid}")
        await check_tp(position)
        await check_sl(position)

# 🔸 Проверка TP-уровней позиции (по цене)
async def check_tp(position):
    active_tp = sorted(
        [
            t for t in position.tp_targets
            if get_field(t, "type") == "tp"
            and get_field(t, "source") == "price"
            and not get_field(t, "hit")
            and not get_field(t, "canceled")
        ],
        key=lambda t: get_field(t, "level")
    )

    if not active_tp:
        return

    tp = active_tp[0]

    redis = infra.redis_client
    mark_str = await redis.get(f"price:{position.symbol}")
    if not mark_str:
        log.warning(f"[TP] Позиция {position.uid}: не удалось получить цену markprice")
        return

    mark = Decimal(mark_str)
    tp_price = get_field(tp, "price")
    tp_level = int(get_field(tp, "level"))

    log.debug(
        f"[TP-CHECK] Позиция {position.symbol} | mark={mark} vs target={tp_price} (level {tp_level})"
    )

    if position.direction == "long" and mark < tp_price:
        return
    if position.direction == "short" and mark > tp_price:
        return

    # TP сработал
    qty = get_field(tp, "quantity")
    entry_price = position.entry_price
    pnl_gain = (tp_price - entry_price) * qty if position.direction == "long" else (entry_price - tp_price) * qty

    tp["hit"] = True
    tp["hit_at"] = datetime.utcnow()

    position.quantity_left -= qty
    position.planned_risk = Decimal("0")
    position.close_reason = f"tp-{tp_level}-hit"
    position.pnl += pnl_gain

    log.info(
        f"🎯 TP сработал: позиция {position.symbol} | уровень {tp_level} | объём {qty} | pnl += {pnl_gain:.6f}"
    )
    log.debug(f"📉 Остаток позиции: quantity_left = {position.quantity_left}")

    # 🔄 Применение SL-политики после TP
    strategy = config.strategies.get(position.strategy_id)
    level_to_id = {int(lvl["level"]): lvl["id"] for lvl in strategy.get("tp_levels", [])}
    tp_level_id = level_to_id.get(tp_level)

    if not tp_level_id:
        log.debug(f"[SL-POLICY] Не найден tp_level_id для strategy={position.strategy_id}, level={tp_level}")

    sl_policy = next(
        (rule for rule in strategy.get("sl_rules", []) if rule["tp_level_id"] == tp_level_id),
        None
    )

    if sl_policy and sl_policy["sl_mode"] != "none" and position.quantity_left > 0:
        # Отмена текущих SL целей
        for sl in position.sl_targets:
            if not get_field(sl, "hit") and not get_field(sl, "canceled"):
                set_field(sl, "canceled", True)

        # Расчёт новой SL цены
        sl_mode = sl_policy["sl_mode"]
        sl_value = sl_policy.get("sl_value")
        new_sl_price = None

        if sl_mode == "entry":
            new_sl_price = position.entry_price
        elif sl_mode == "percent":
            offset = tp_price * Decimal(sl_value) / Decimal("100")
            new_sl_price = tp_price - offset if position.direction == "long" else tp_price + offset
        elif sl_mode == "atr":
            atr_key = f"ind:{position.symbol}:{strategy['meta']['timeframe']}:atr14"
            atr_raw = await redis.get(atr_key)
            if not atr_raw:
                log.warning(f"[SL-POLICY] Не удалось получить ATR для {position.symbol}")
                return
            offset = Decimal(atr_raw) * Decimal(sl_value)
            new_sl_price = tp_price - offset if position.direction == "long" else tp_price + offset
        else:
            log.warning(f"[SL-POLICY] Неизвестный sl_mode: {sl_mode}")
            return

        # Добавление новой SL цели
        max_level = max((get_field(sl, "level", 0) for sl in position.sl_targets), default=0)
        position.sl_targets.append({
            "level": max_level + 1,
            "price": new_sl_price,
            "quantity": position.quantity_left,
            "type": "sl",
            "source": "price",
            "hit": False,
            "hit_at": None,
            "canceled": False
        })

        log.debug(
            f"🛡️ Новый SL создан: позиция {position.uid} | цена {new_sl_price:.8f} | режим {sl_mode} | уровень {max_level + 1}"
        )

    if position.quantity_left <= 0:
        position.status = "closed"
        position.exit_price = mark
        position.close_reason = "full-tp-hit"
        position.closed_at = datetime.utcnow()

        # Отмена всех активных SL целей
        for sl in position.sl_targets:
            if not get_field(sl, "hit") and not get_field(sl, "canceled"):
                set_field(sl, "canceled", True)
                sl_level = get_field(sl, "level")
                log.debug(f"⚠️ SL отменён: позиция {position.symbol} | уровень {sl_level}")

        log.info(f"✅ Позиция {position.symbol} полностью закрыта по TP")

        # Удаление позиции из памяти
        del position_registry[(position.strategy_id, position.symbol)]

    # Отправка обновления в Redis
    await push_position_update(position, redis)
# 🔸 Обработка SL-уровня позиции (по цене)
async def check_sl(position):
    active_sl = sorted(
        [
            sl for sl in position.sl_targets
            if get_field(sl, "type") == "sl"
            and get_field(sl, "source") == "price"
            and not get_field(sl, "hit")
            and not get_field(sl, "canceled")
        ],
        key=lambda sl: get_field(sl, "level")
    )

    if not active_sl:
        return

    sl = active_sl[0]

    redis = infra.redis_client
    mark_str = await redis.get(f"price:{position.symbol}")
    if not mark_str:
        log.warning(f"[SL] Позиция {position.uid}: не удалось получить цену markprice")
        return

    mark = Decimal(mark_str)
    sl_price = get_field(sl, "price")
    sl_level = get_field(sl, "level")

    log.debug(
        f"[SL-CHECK] Позиция {position.symbol} | mark={mark} vs sl_price={sl_price} (level {sl_level})"
    )

    triggered = False
    if position.direction == "long" and mark <= sl_price:
        triggered = True
    elif position.direction == "short" and mark >= sl_price:
        triggered = True

    if not triggered:
        return

    # SL сработал
    set_field(sl, "hit", True)
    set_field(sl, "hit_at", datetime.utcnow())

    # Отмена всех активных TP целей
    for tp in position.tp_targets:
        if not get_field(tp, "hit") and not get_field(tp, "canceled"):
            tp["canceled"] = True
            tp_level = get_field(tp, "level")
            log.debug(f"⚠️ TP отменён: позиция {position.symbol} | уровень {tp_level}")

    # Закрытие позиции
    qty = get_field(sl, "quantity")
    entry_price = position.entry_price
    pnl_loss = (mark - entry_price) * qty if position.direction == "long" else (entry_price - mark) * qty

    position.quantity_left = Decimal("0")
    position.planned_risk = Decimal("0")
    position.status = "closed"
    position.exit_price = mark
    position.closed_at = datetime.utcnow()
    position.pnl += pnl_loss

    # Причина закрытия
    if sl_level == 1:
        position.close_reason = "sl-full-hit"
    else:
        position.close_reason = "sl-tp-hit"

    log.info(
        f"🛑 SL сработал: позиция {position.symbol} | уровень {sl_level} | объём {qty} | pnl += {pnl_loss:.6f}"
    )
    log.debug(f"✅ Позиция {position.symbol} закрыта по SL: статус={position.status}, причина={position.close_reason}")

    # Удаление позиции из памяти
    del position_registry[(position.strategy_id, position.symbol)]

    # Отправка обновления в Redis
    await push_position_update(position, redis)

# 🔸 Принудительное закрытие позиции по SL-защите
async def full_protect_stop(position, from_reverse=False):
    async with position.lock:
        log.debug(f"🔒 [POSITION_HANDLER] LOCK: позиция {position.symbol} → full protect stop")

        # 1. Отмена всех активных целей
        for target in position.tp_targets + position.sl_targets:
            if not get_field(target, "hit") and not get_field(target, "canceled"):
                set_field(target, "canceled", True)

        # 2. Получение текущей цены
        redis = infra.redis_client
        mark_str = await redis.get(f"price:{position.symbol}")
        if not mark_str:
            log.warning(f"[PROTECT] Не удалось получить цену для {position.symbol}")
            return
        mark = Decimal(mark_str)

        # 3–7. Закрытие позиции
        position.status = "closed"
        position.exit_price = mark
        position.close_reason = "sl-protect-stop"
        position.closed_at = datetime.utcnow()

        qty = position.quantity_left
        entry_price = position.entry_price
        pnl = (mark - entry_price) * qty if position.direction == "long" else (entry_price - mark) * qty
        position.pnl += pnl

        position.planned_risk = Decimal("0")
        position.quantity_left = Decimal("0")

        # 8. Обновление в БД
        await push_position_update(position, redis)

        # 9. Удаление из памяти
        del position_registry[(position.strategy_id, position.symbol)]

        log.info(
            f"✅ Позиция {position.symbol} закрыта через защиту SL: статус=closed, причина=sl-protect-stop"
        )

        # 🔟 Если вызов пришёл из реверса — запускаем повторный вход
        if from_reverse:
            await reverse_entry(position.uid)
        
# 🔸 Перемещение SL на уровень entry (для SL-защиты)
async def raise_sl_to_entry(position, sl):
    async with position.lock:
        if get_field(sl, "hit") or get_field(sl, "canceled"):
            log.debug(f"[PROTECT] SL уже неактивен: позиция {position.uid} | уровень {get_field(sl, 'level')}")
            return

        # Отмена текущего SL
        set_field(sl, "canceled", True)
        sl_level = get_field(sl, "level")
        log.info(f"⚠️ SL отменён для переноса: позиция {position.symbol} | уровень {sl_level}")

        # Создание нового SL на уровне entry
        entry_price = position.entry_price
        qty = get_field(sl, "quantity")

        max_level = max((get_field(t, "level", 0) for t in position.sl_targets), default=0)

        position.sl_targets.append({
            "level": max_level + 1,
            "price": entry_price,
            "quantity": qty,
            "type": "sl",
            "source": "price",
            "hit": False,
            "hit_at": None,
            "canceled": False
        })

        # Обнуление запланированного риска
        position.planned_risk = Decimal("0")

        log.info(
            f"🛡️ SL перенесён на entry: {position.symbol} | новая цена {entry_price:.8f} | уровень {max_level + 1}"
        )

        # Отправка обновления в Redis
        redis = infra.redis_client
        await push_position_update(position, redis)
# 🔸 Закрытие позиции по TP-сигналу в рамках реверса
async def full_reverse_stop(position):
    async with position.lock:

        log.debug(f"🔒 [POSITION_HANDLER] LOCK: позиция {position.symbol} → reverse stop")

        # 1. Отмена всех активных целей
        for target in position.tp_targets + position.sl_targets:
            if not get_field(target, "hit") and not get_field(target, "canceled"):
                set_field(target, "canceled", True)

        # 2. Получение текущей цены
        redis = infra.redis_client
        mark_str = await redis.get(f"price:{position.symbol}")
        if not mark_str:
            log.warning(f"[REVERSE] Не удалось получить цену для {position.symbol}")
            return
        mark = Decimal(mark_str)

        # 3–7. Закрытие позиции и финализация
        position.status = "closed"
        position.exit_price = mark
        position.close_reason = "tp-signal-stop"
        position.closed_at = datetime.utcnow()

        qty = position.quantity_left
        entry_price = position.entry_price
        pnl = (mark - entry_price) * qty if position.direction == "long" else (entry_price - mark) * qty
        position.pnl += pnl

        position.planned_risk = Decimal("0")
        position.quantity_left = Decimal("0")

        # 8. Обновление в БД
        await push_position_update(position, redis)

        # 9. Удаление из памяти
        del position_registry[(position.strategy_id, position.symbol)]

        log.info(
            f"📉 Позиция {position.symbol} закрыта по reverse: статус=closed, причина=tp-signal-stop, pnl={pnl:.6f}"
        )

        # 10. Вызов reverse_entry
        await reverse_entry(position.uid)