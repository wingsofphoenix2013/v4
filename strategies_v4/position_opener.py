# position_opener.py 

import asyncio
import logging
import uuid
import json
from datetime import datetime
from dataclasses import dataclass, asdict

from infra import infra, get_price, get_indicator
from config_loader import config
from position_state_loader import position_registry, PositionState, Target

log = logging.getLogger("POSITION_OPENER")

@dataclass
class PositionCalculation:
    entry_price: float
    quantity: float
    planned_risk: float
    tp_targets: list
    sl_target: dict
    route: str
    log_uid: str

# 🔹 Расчёт параметров позиции, TP и SL
async def calculate_position_size(data: dict):
    strategy_id = int(data["strategy_id"])
    symbol = data["symbol"]
    direction = data["direction"]

    strategy = config.strategies.get(strategy_id)
    if not strategy:
        return "skip", "strategy not found"

    if not strategy.get("tp_levels"):
        return "skip", "strategy has no TP levels"

    ticker = config.tickers.get(symbol)
    if not ticker:
        return "skip", "ticker not found"

    try:
        precision_price = int(ticker["precision_price"])
        precision_qty = int(ticker["precision_qty"])
        min_qty = float(ticker.get("min_qty") or 10 ** (-precision_qty))
    except Exception:
        return "skip", "invalid precision in ticker"

    entry_price = await get_price(symbol)
    if entry_price is None:
        return "skip", "entry price not available"

    log.info(f"[STAGE 1] entry_price={entry_price} precision_price={precision_price} precision_qty={precision_qty}")

    # === Этап 2: Расчёт SL ===
    sl_type = strategy.get("sl_type")
    sl_value = strategy.get("sl_value")

    if not sl_type or sl_value is None:
        return "skip", "SL settings not defined"

    if sl_type == "percent":
        delta = float(entry_price) * float(sl_value) / 100
    elif sl_type == "atr":
        tf = strategy.get("timeframe").lower()
        log.info(f"[TP] strategy_id={strategy_id} timeframe={tf} — querying atr14")
        atr = await get_indicator(symbol, tf, "atr14")
        if atr is None:
            return "skip", "ATR not available"
        delta = float(atr) * float(sl_value)
    else:
        return "skip", f"unknown sl_type: {sl_type}"

    if direction == "long":
        stop_loss_price = entry_price - delta
    else:
        stop_loss_price = entry_price + delta

    risk_per_unit = abs(entry_price - stop_loss_price)

    # Округление
    factor = 10 ** precision_price
    stop_loss_price = round(stop_loss_price * factor) / factor
    risk_per_unit = round(risk_per_unit * factor) / factor

    if risk_per_unit == 0:
        return "skip", "risk_per_unit is zero"

    log.info(f"[STAGE 2] sl_type={sl_type} stop_price={stop_loss_price} risk_per_unit={risk_per_unit}")

    # === Этап 3: Расчёт TP ===
    tp_targets = []
    atr = None

    for level_conf in strategy["tp_levels"]:
        level = level_conf["level"]
        tp_type = level_conf["tp_type"]
        tp_value = level_conf["tp_value"]

        if tp_type == "signal":
            price = None
        elif tp_type == "percent":
            delta = float(entry_price) * float(tp_value) / 100
            price = entry_price + delta if direction == "long" else entry_price - delta
        elif tp_type == "atr":
            if atr is None:
                tf = strategy.get("timeframe").lower()
                log.info(f"[TP] strategy_id={strategy_id} timeframe={tf} — querying atr14")
                atr = await get_indicator(symbol, tf, "atr14")
                if atr is None:
                    return "skip", "ATR not available for TP"
            delta = float(atr) * float(tp_value)
            price = entry_price + delta if direction == "long" else entry_price - delta
        else:
            return "skip", f"unknown tp_type: {tp_type}"

        if price is not None:
            price = round(price * factor) / factor

        tp_targets.append(Target(
            type="tp",
            level=level,
            price=price,
            quantity=None,
            hit=False,
            hit_at=None,
            canceled=False
        ))

        log.info(f"[TP] level={level} type={tp_type} price={price}")

    log.info(f"[STAGE 3] TP targets prepared: {len(tp_targets)}")

    # === Этап 4: Учёт открытых позиций и доступного риска ===
    used_risk = sum(
        p.planned_risk for p in position_registry.values()
        if p.strategy_id == strategy_id
    )

    deposit = float(strategy["deposit"])
    max_risk_pct = float(strategy["max_risk"])
    max_allowed_risk = deposit * max_risk_pct / 100
    available_risk = max(0, max_allowed_risk - used_risk)

    if available_risk <= 0:
        return "skip", "available risk exhausted"

    log.info(f"[STAGE 4] used_risk={used_risk} max_allowed_risk={max_allowed_risk} available_risk={available_risk}")

    # === Этап 5: Расчёт объёма позиции ===
    leverage = float(strategy["leverage"])
    position_limit = float(strategy["position_limit"])

    qty_by_risk = available_risk / risk_per_unit
    qty_by_margin = (position_limit * leverage) / entry_price

    quantity_raw = min(qty_by_risk, qty_by_margin)
    factor_qty = 10 ** precision_qty
    quantity = int(quantity_raw * factor_qty) / factor_qty

    if quantity < min_qty:
        return "skip", "quantity below min_qty"

    log.info(f"[STAGE 5] qty_by_risk={qty_by_risk} qty_by_margin={qty_by_margin} quantity={quantity}")
    
    # === Этап 6: Финальные валидации ===
    used_margin = (entry_price * quantity) / leverage
    margin_threshold = 0.75 * position_limit

    if used_margin < margin_threshold:
        return "skip", f"used margin {used_margin:.4f} below 75% of position limit {margin_threshold:.4f}"

    if quantity < min_qty:
        return "skip", "final quantity below min_qty"

    log.info(f"[STAGE 6] used_margin={used_margin} (threshold={margin_threshold}) — OK")

    # === Этап 7: Формирование TP с quantity ===
    volume_percents = [lvl["volume_percent"] for lvl in strategy["tp_levels"]]
    quantities = []
    total_assigned = 0.0

    for i, percent in enumerate(volume_percents):
        percent = float(percent)
        if i < len(volume_percents) - 1:
            q = quantity * (percent / 100)
            q = int(q * factor_qty) / factor_qty
            quantities.append(q)
            total_assigned += q
        else:
            q = quantity - total_assigned
            q = round(q * factor_qty) / factor_qty
            quantities.append(q)

    for tp, q in zip(tp_targets, quantities):
        tp.quantity = q

    log.info(f"[STAGE 7] TP quantities: {[tp.quantity for tp in tp_targets]} (total={sum(quantities)})")
    
    # === Этап 8: Расчёт planned_risk и SL Target ===
    planned_risk = round(risk_per_unit * quantity * factor) / factor

    sl_target = Target(
        type="sl",
        level=1,
        price=stop_loss_price,
        quantity=quantity,
        hit=False,
        hit_at=None,
        canceled=False
    )

    log.info(f"[STAGE 8] planned_risk={planned_risk} SL quantity={quantity} SL price={stop_loss_price}")
    
    return PositionCalculation(
        entry_price=entry_price,
        quantity=quantity,
        planned_risk=planned_risk,
        tp_targets=tp_targets,
        sl_target=sl_target,
        route=data["route"],
        log_uid=data["log_uid"]
    )
    
# 🔹 Открытие позиции и публикация события
async def open_position(calc_result: PositionCalculation, signal_data: dict):
    position_uid = str(uuid.uuid4())

    # Создание позиции в оперативной памяти
    state = PositionState(
        uid=position_uid,
        strategy_id=int(signal_data["strategy_id"]),
        symbol=signal_data["symbol"],
        direction=signal_data["direction"],
        entry_price=calc_result.entry_price,
        quantity=calc_result.quantity,
        quantity_left=calc_result.quantity,
        status="open",
        created_at=datetime.utcnow(),
        exit_price=None,
        closed_at=None,
        close_reason=None,
        pnl=None,
        planned_risk=calc_result.planned_risk,
        route=calc_result.route,
        tp_targets=calc_result.tp_targets,
        sl_targets=[calc_result.sl_target],
        log_uid=calc_result.log_uid
    )

    position_registry[(state.strategy_id, state.symbol)] = state

    # Подготовка события для Redis
    payload = {
        "position_uid": position_uid,
        "strategy_id": str(state.strategy_id),
        "symbol": state.symbol,
        "direction": state.direction,
        "entry_price": str(state.entry_price),
        "quantity": str(state.quantity),
        "quantity_left": str(state.quantity_left),
        "created_at": state.created_at.isoformat(),
        "planned_risk": str(state.planned_risk),
        "route": state.route,
        "log_uid": state.log_uid,
        "tp_targets": json.dumps([asdict(t) for t in state.tp_targets]),
        "sl_targets": json.dumps([asdict(t) for t in state.sl_targets]),
        "event_type": "opened",
        "received_at": signal_data.get("received_at", datetime.utcnow().isoformat()),
        "latency_ms": "0"  # будет рассчитываться в core_io
    }

    try:
        await infra.redis_client.xadd("positions_open_stream", payload)
        log.info(f"📬 Позиция создана и отправлена в Redis: {position_uid}")
    except Exception:
        log.exception("❌ Ошибка отправки позиции в Redis")

# 🔹 Логгирование skip-события в Redis Stream
async def publish_skip_reason(log_uid: str, strategy_id: int, reason: str):
    try:
        record = {
            "log_uid": log_uid,
            "strategy_id": str(strategy_id),
            "status": "skip",
            "note": reason,
            "position_uid": "",
            "logged_at": datetime.utcnow().isoformat()
        }
        await infra.redis_client.xadd("signal_log_queue", record)
        log.info(f"⚠️ [SKIP] strategy_id={strategy_id} log_uid={log_uid} reason=\"{reason}\"")
    except Exception:
        log.exception("❌ Ошибка при записи skip-события в Redis")

# 🔹 Основной воркер
async def run_position_opener_loop():
    stream = "strategy_opener_stream"
    group = "position_opener_group"
    consumer = "position_opener_1"
    redis = infra.redis_client

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.info(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.info(f"ℹ️ Группа {group} уже существует")
        else:
            log.exception("❌ Ошибка создания Consumer Group")
            return

    while True:
        try:
            entries = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=10,
                block=1000
            )
            if not entries:
                continue

            for _, records in entries:
                for record_id, raw in records:
                    raw_data = raw.get(b"data") or raw.get("data")
                    if isinstance(raw_data, bytes):
                        raw_data = raw_data.decode()

                    try:
                        data = json.loads(raw_data)
                    except Exception:
                        log.exception("❌ Невозможно распарсить JSON из поля 'data'")
                        await redis.xack(stream, group, record_id)
                        continue

                    log.info(f"[RAW DATA] {data}")

                    try:
                        strategy_id = int(data["strategy_id"])
                        log_uid = data["log_uid"]
                    except KeyError as e:
                        log.exception(f"❌ Отсутствует ключ в данных: {e}")
                        await redis.xack(stream, group, record_id)
                        continue

                    result = await calculate_position_size(data)
                    if isinstance(result, tuple) and result[0] == "skip":
                        reason = result[1]
                        await publish_skip_reason(log_uid, strategy_id, reason)
                        await redis.xack(stream, group, record_id)
                        continue
                    else:
                        await open_position(result, data)
                        await redis.xack(stream, group, record_id)

        except Exception:
            log.exception("❌ Ошибка в основном цикле position_opener_loop")
            await asyncio.sleep(5)