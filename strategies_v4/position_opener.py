# position_opener.py

import logging
import json
import asyncio
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from datetime import datetime
import uuid

from infra import infra
from config_loader import config
from position_state_loader import PositionState, position_registry

log = logging.getLogger("POSITION_OPENER")

# 🔸 Расчет позиции на основе параметров стратегии, цены и текущих рисков
async def calculate_position_size(signal: dict, context: dict) -> dict:
    try:
        redis = context["redis"]
        strategy_id = int(signal["strategy_id"])
        symbol = signal["symbol"]
        direction = signal["direction"]
        route = signal["route"]

        strategy = config.strategies[strategy_id]
        meta = strategy["meta"]
        tp_levels = strategy.get("tp_levels", [])
        ticker = config.tickers.get(symbol)

        if not ticker:
            return {"route": route, "status": "skip", "reason": "тикер не найден в config"}

        leverage = Decimal(meta["leverage"])
        deposit = Decimal(meta["deposit"])
        position_limit = Decimal(meta["position_limit"])
        max_risk_pct = Decimal(meta["max_risk"])
        sl_type = meta["sl_type"]
        sl_value = Decimal(meta["sl_value"])
        timeframe = meta["timeframe"]

        precision_price = int(ticker["precision_price"])
        precision_qty = int(ticker["precision_qty"])
        min_qty = Decimal(ticker["min_qty"])

        mark_price_raw = await redis.get(f"price:{symbol}")
        if not mark_price_raw:
            return {"route": route, "status": "skip", "reason": "нет цены актива"}

        try:
            entry_price = Decimal(mark_price_raw)
        except InvalidOperation:
            return {"route": route, "status": "skip", "reason": "некорректная цена актива"}

        if sl_type == "percent":
            offset = entry_price * sl_value / Decimal("100")
        elif sl_type == "atr":
            atr_key = f"ind:{symbol}:{timeframe}:atr14"
            atr_raw = await redis.get(atr_key)
            if not atr_raw:
                return {"route": route, "status": "skip", "reason": "ATR не найден"}
            offset = Decimal(atr_raw) * sl_value
        else:
            return {"route": route, "status": "skip", "reason": f"неизвестный sl_type: {sl_type}"}

        stop_loss_price = entry_price - offset if direction == "long" else entry_price + offset
        stop_loss_price = round(stop_loss_price, precision_price)
        risk_per_unit = abs(entry_price - stop_loss_price)

        if risk_per_unit == 0:
            return {"route": route, "status": "skip", "reason": "нулевой риск на единицу"}

        tp_prices = []
        for level in tp_levels:
            tp_type = level["tp_type"]
            tp_value = Decimal(level["tp_value"]) if level["tp_value"] is not None else None

            if tp_type == "percent":
                offset = entry_price * tp_value / Decimal("100")
            elif tp_type == "atr":
                atr_key = f"ind:{symbol}:{timeframe}:atr14"
                atr_raw = await redis.get(atr_key)
                if not atr_raw:
                    return {"route": route, "status": "skip", "reason": "ATR не найден для TP"}
                offset = Decimal(atr_raw) * tp_value
            elif tp_type == "signal":
                tp_prices.append(None)
                continue
            else:
                return {"route": route, "status": "skip", "reason": f"неизвестный tp_type: {tp_type}"}

            tp_price = entry_price + offset if direction == "long" else entry_price - offset
            tp_prices.append(round(tp_price, precision_price))

        used_risk = Decimal("0")
        used_margin_sum = Decimal("0")
        for p in position_registry.values():
            if p.strategy_id == strategy_id and p.status in ("open", "partial"):
                used_risk += p.planned_risk
                notional = p.entry_price * p.quantity
                used_margin_sum += notional / leverage

        available_risk = deposit * max_risk_pct / Decimal("100") - used_risk
        if available_risk <= 0:
            return {"route": route, "status": "skip", "reason": "доступный риск <= 0"}

        qty_by_risk = available_risk / risk_per_unit
        qty_by_margin = (position_limit * leverage) / entry_price
        quantity = min(qty_by_risk, qty_by_margin)

        notional_value = entry_price * quantity
        used_margin = notional_value / leverage
        total_margin = used_margin_sum + used_margin

        if total_margin > deposit:
            adjusted_margin = deposit - used_margin_sum
            if adjusted_margin <= 0:
                return {"route": route, "status": "skip", "reason": "депозит полностью занят другими позициями"}

            adjusted_notional = adjusted_margin * leverage
            adjusted_qty_by_margin = adjusted_notional / entry_price
            quantity = min(qty_by_risk, adjusted_qty_by_margin)
            quantity = quantity.quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)

            notional_value = entry_price * quantity
            used_margin = notional_value / leverage
        else:
            quantity = quantity.quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)
            notional_value = entry_price * quantity
            used_margin = notional_value / leverage

        # 🔒 Глобальная проверка на минимальную маржу позиции
        if used_margin < position_limit * Decimal("0.75"):
            return {"route": route, "status": "skip", "reason": "маржа позиции меньше 75% от лимита"}

        if quantity < min_qty:
            return {"route": route, "status": "skip", "reason": "объем меньше минимального"}

        planned_risk = risk_per_unit * quantity

        # 📌 Формирование целей TP с полным набором полей
        tp_targets = []
        total_allocated = Decimal("0")
        for i, level in enumerate(tp_levels):
            volume_percent = Decimal(level["volume_percent"])
            if i < len(tp_levels) - 1:
                qty = (quantity * volume_percent / 100).quantize(Decimal(f"1e-{precision_qty}"), rounding=ROUND_DOWN)
                total_allocated += qty
            else:
                qty = quantity - total_allocated
            tp_targets.append({
                "level": level["level"],
                "price": tp_prices[i],
                "quantity": qty,
                "type": "tp",
                "hit": False,
                "hit_at": None,
                "canceled": False,
                "source": "signal" if level["tp_type"] == "signal" else "price"
            })
            log.debug(f"🎯 [POSITION_OPENER] TP{level['level']}: price={tp_prices[i]} quantity={qty}")

        return {
            "route": route,
            "quantity": quantity,
            "notional_value": notional_value,
            "used_margin": used_margin,
            "planned_risk": planned_risk,
            "entry_price": entry_price,
            "stop_loss_price": stop_loss_price,
            "tp_prices": tp_prices,
            "tp_targets": tp_targets,
            "source": "signal" if tp_type == "signal" else "price"
        }

    except Exception as e:
        log.exception("❌ Ошибка в calculate_position_size")
        return {"route": signal.get("route"), "status": "skip", "reason": "внутренняя ошибка"}

# 🔸 Основная функция открытия позиции
async def open_position(signal: dict, strategy_obj, context: dict) -> dict:
    result = await calculate_position_size(signal, context)

    if result.get("status") == "skip":
        reason = result.get("reason", "неизвестная причина отказа")
        log.debug(f"🚫 [POSITION_OPENER] Открытие позиции отменено: {reason}")

        redis = context.get("redis")
        log_id = signal.get("log_id")
        strategy_id = signal.get("strategy_id")

        if redis and log_id is not None:
            log_record = {
                "log_id": log_id,
                "strategy_id": strategy_id,
                "status": "skip",
                "position_uid": None,
                "note": reason,
                "logged_at": datetime.utcnow().isoformat()
            }
            try:
                await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
            except Exception as e:
                log.warning(f"⚠️ [POSITION_OPENER] Ошибка записи в Redis log_queue: {e}")

        return {"status": "skipped", "reason": reason}

    # 🔹 Генерация уникального идентификатора позиции
    position_uid = str(uuid.uuid4())

    # 🔹 Проверка: позиция уже существует
    key = (int(signal["strategy_id"]), signal["symbol"])
    if key in position_registry:
        log.warning(f"⚠️ Позиция уже существует в памяти: strategy={key[0]} symbol={key[1]}")
        return {"status": "duplicate", "reason": "позиция уже есть в памяти"}

    # 🔹 Логирование итогов расчета
    log.debug(
        f"✅ [POSITION_OPENER] Открытие позиции: "
        f"strategy={signal['strategy_id']} symbol={signal['symbol']} "
        f"qty={result['quantity']} price={result['entry_price']} uid={position_uid}"
    )

    # 🔹 Логирование SL и TP
    stop_price = result["stop_loss_price"]
    tp_prices = result["tp_prices"]

    log.debug(f"🔔 [POSITION_OPENER] SL: {stop_price}")
    for i, tp in enumerate(tp_prices, start=1):
        log.debug(f"🎯 [POSITION_OPENER] TP{i}: {tp}")

    # 🔹 Расчёт комиссии и PnL
    notional = result["entry_price"] * result["quantity"]
    fee = notional * Decimal("0.001")
    pnl = -fee

    # 🔹 Создание объекта PositionState и сохранение в память
    position = PositionState(
        uid=position_uid,
        strategy_id=int(signal["strategy_id"]),
        symbol=signal["symbol"],
        direction=signal["direction"],
        entry_price=result["entry_price"],
        quantity=result["quantity"],
        quantity_left=result["quantity"],
        status="open",
        created_at=datetime.utcnow(),
        exit_price=None,
        closed_at=None,
        close_reason="в работе",
        pnl=pnl,
        planned_risk=result["planned_risk"],
        route=signal["route"],
        tp_targets=result["tp_targets"],
        sl_targets=[{
            "level": 1,
            "price": result["stop_loss_price"],
            "quantity": result["quantity"],
            "type": "sl",
            "hit": False,
            "hit_at": None,
            "canceled": False,
            "source": "price"
        }],
        log_id=signal["log_id"]
    )

    position_registry[(position.strategy_id, position.symbol)] = position
    log.info(f"📌 [POSITION_OPENER] Позиция сохранена в память: uid={position_uid}")
    
    # 🔹 Подготовка Redis-логов
    redis = context.get("redis")
    log_id = signal.get("log_id")
    strategy_id = signal.get("strategy_id")

    def normalize_targets(targets):
        return [
            {
                "level": int(t["level"]),
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

    if redis and log_id is not None:
        # 🔹 Лог открытия
        log_record = {
            "log_id": log_id,
            "strategy_id": strategy_id,
            "status": "opened",
            "position_uid": position_uid,
            "note": "позиция открыта",
            "logged_at": datetime.utcnow().isoformat()
        }
        try:
            await redis.xadd("signal_log_queue", {"data": json.dumps(log_record)})
        except Exception as e:
            log.warning(f"⚠️ [POSITION_OPENER] Ошибка записи успешного открытия в Redis log_queue: {e}")

        # 🔹 Данные позиции для записи в БД
        position_data = {
            "position_uid": position_uid,
            "strategy_id": position.strategy_id,
            "symbol": position.symbol,
            "direction": position.direction,
            "entry_price": str(position.entry_price),
            "quantity": str(position.quantity),
            "notional_value": str(notional),
            "quantity_left": str(position.quantity_left),
            "status": position.status,
            "created_at": position.created_at.isoformat(),
            "planned_risk": str(position.planned_risk),
            "route": signal["route"],
            "log_id": position.log_id,
            "pnl": str(pnl),
            "close_reason": "в работе",
            "tp_targets": normalize_targets(position.tp_targets),
            "sl_targets": normalize_targets(position.sl_targets)
        }
        try:
            await redis.xadd("positions_stream", {"data": json.dumps(position_data)})
            log.debug(f"[DEBUG] position_data for Redis: {position_data}")
            log.debug(f"📤 [POSITION_OPENER] Позиция отправлена в Redis для записи в БД")
        except Exception as e:
            log.warning(f"⚠️ [POSITION_OPENER] Ошибка отправки позиции в Redis: {e}")

    return {"status": "opened", "position_uid": position_uid, **result}
# 🔸 Слушатель потока strategy_opener_stream
async def run_position_opener_loop():
    log.debug("🧭 [POSITION_OPENER] Запуск слушателя strategy_opener_stream")

    redis = infra.redis_client
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={"strategy_opener_stream": last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id
                    try:
                        # 🔸 Извлечение и декодирование сигнала
                        payload = json.loads(msg_data["data"])

                        # 🔸 Вызов функции открытия позиции с расчетом
                        result = await open_position(payload, None, {"redis": redis})

                        # 🔸 Логирование результата
                        if result.get("status") == "skipped":
                            reason = result.get("reason", "неизвестная причина")
                            log.debug(f"🚫 [POSITION_OPENER] Команда отклонена: {reason}")

                        elif result.get("status") == "opened":
                            log.debug(f"📥 [POSITION_OPENER] Позиция открыта: "
                                     f"qty={result['quantity']} price={result['entry_price']}")

                    except Exception as e:
                        log.warning(f"⚠️ [POSITION_OPENER] Ошибка обработки команды: {e}")

        except Exception:
            log.exception("❌ [POSITION_OPENER] Ошибка при чтении из strategy_opener_stream")
            await asyncio.sleep(5)