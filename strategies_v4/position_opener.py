# position_opener.py

import logging
import json
import asyncio
from decimal import Decimal, InvalidOperation
from datetime import datetime

from infra import infra
from config_loader import config
from position_state_loader import position_registry

log = logging.getLogger("POSITION_OPENER")

# 🔸 Расчет позиции на основе параметров стратегии, цены и текущих рисков
async def calculate_position_size(signal: dict, context: dict) -> dict:
    try:
        redis = context["redis"]
        strategy_id = int(signal["strategy_id"])
        symbol = signal["symbol"]
        direction = signal["direction"]
        route = signal["route"]

        # Получение конфигурации стратегии и тикера
        strategy = config.strategies[strategy_id]
        meta = strategy["meta"]
        tp_levels = strategy.get("tp_levels", [])
        ticker = config.tickers.get(symbol)

        if not ticker:
            return {"route": route, "status": "skip", "reason": "тикер не найден в config"}

        # Извлечение параметров стратегии и тикера
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

        # Получение текущей цены актива
        mark_price_raw = await redis.get(f"price:{symbol}")
        if not mark_price_raw:
            return {"route": route, "status": "skip", "reason": "нет цены актива"}

        try:
            entry_price = Decimal(mark_price_raw)
        except InvalidOperation:
            return {"route": route, "status": "skip", "reason": "некорректная цена актива"}

        # Расчет уровня стоп-лосса
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

        if direction == "long":
            stop_loss_price = entry_price - offset
        else:
            stop_loss_price = entry_price + offset

        stop_loss_price = round(stop_loss_price, precision_price)
        risk_per_unit = abs(entry_price - stop_loss_price)

        if risk_per_unit == 0:
            return {"route": route, "status": "skip", "reason": "нулевой риск на единицу"}

        # Расчет уровней тейк-профита
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

            if direction == "long":
                tp_price = entry_price + offset
            else:
                tp_price = entry_price - offset

            tp_prices.append(round(tp_price, precision_price))

        # Подсчет используемого риска по уже открытым позициям стратегии
        used_risk = sum(
            p.planned_risk for p in position_registry.values()
            if p.strategy_id == strategy_id and p.status in ("open", "partial")
        )

        available_risk = deposit * max_risk_pct / Decimal("100") - used_risk
        if available_risk <= 0:
            return {"route": route, "status": "skip", "reason": "доступный риск <= 0"}

        # Расчет максимального объема позиции
        qty_by_risk = available_risk / risk_per_unit
        qty_by_margin = (deposit * leverage) / entry_price
        quantity = min(qty_by_risk, qty_by_margin)

        quantity = quantity.quantize(Decimal(f"1e-{precision_qty}"))
        if quantity < min_qty:
            return {"route": route, "status": "skip", "reason": "объем меньше минимального"}

        # Расчет итоговых показателей позиции
        notional_value = entry_price * quantity
        used_margin = notional_value / leverage
        planned_risk = risk_per_unit * quantity

        if used_margin < position_limit * Decimal("0.75"):
            return {"route": route, "status": "skip", "reason": "используемая маржа слишком мала"}

        # Возврат итогового расчета
        return {
            "route": route,
            "quantity": quantity,
            "notional_value": notional_value,
            "used_margin": used_margin,
            "planned_risk": planned_risk,
            "entry_price": entry_price,
            "stop_loss_price": stop_loss_price,
            "tp_prices": tp_prices
        }

    except Exception as e:
        log.exception("❌ Ошибка в calculate_position_size")
        return {"route": signal.get("route"), "status": "skip", "reason": "внутренняя ошибка"}

# 🔸 Основная функция открытия позиции
async def open_position(signal: dict, strategy_obj, context: dict) -> dict:
    result = await calculate_position_size(signal, context)

    if result.get("status") == "skip":
        reason = result.get("reason", "неизвестная причина отказа")
        log.info(f"🚫 [POSITION_OPENER] Открытие позиции отменено: {reason}")
        return {"status": "skipped", "reason": reason}

    # Здесь позже будет: создание записи в БД, обновление состояния
    log.info(
        f"✅ [POSITION_OPENER] Открытие позиции: "
        f"strategy={signal['strategy_id']} symbol={signal['symbol']} "
        f"qty={result['quantity']} price={result['entry_price']}"
    )

    return {"status": "opened", **result}

# 🔸 Слушатель потока strategy_opener_stream
async def run_position_opener_loop():
    log.info("🧭 [POSITION_OPENER] Запуск слушателя strategy_opener_stream")

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
                            log.info(f"🚫 [POSITION_OPENER] Команда отклонена: {reason}")

                        elif result.get("status") == "opened":
                            log.info(f"📥 [POSITION_OPENER] Позиция открыта: "
                                     f"qty={result['quantity']} price={result['entry_price']}")

                    except Exception as e:
                        log.warning(f"⚠️ [POSITION_OPENER] Ошибка обработки команды: {e}")

        except Exception:
            log.exception("❌ [POSITION_OPENER] Ошибка при чтении из strategy_opener_stream")
            await asyncio.sleep(5)