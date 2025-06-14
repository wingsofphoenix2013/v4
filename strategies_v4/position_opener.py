# position_opener.py 

import asyncio
import logging
import uuid
import json
from datetime import datetime
from dataclasses import dataclass, asdict
from decimal import Decimal, ROUND_DOWN
import time

from infra import infra, get_price, get_indicator
from config_loader import config
from position_state_loader import position_registry, PositionState, Target

log = logging.getLogger("POSITION_OPENER")

@dataclass
class PositionCalculation:
    entry_price: Decimal
    quantity: Decimal
    planned_risk: Decimal
    tp_targets: list[Target]
    sl_target: Target
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
        min_qty = Decimal(str(ticker.get("min_qty") or 10 ** (-precision_qty)))
    except Exception:
        return "skip", "invalid precision in ticker"

    factor_price = Decimal(f"1e-{precision_price}")
    factor_qty = Decimal(f"1e-{precision_qty}")

    entry_price_raw = await get_price(symbol)
    if entry_price_raw is None:
        return "skip", "entry price not available"

    entry_price = Decimal(str(entry_price_raw)).quantize(factor_price, rounding=ROUND_DOWN)
    log.debug(f"[STAGE 1] entry_price={entry_price} precision_price={precision_price} precision_qty={precision_qty}")

    # === Этап 2: Расчёт SL ===
    sl_type = strategy.get("sl_type")
    sl_value_raw = strategy.get("sl_value")

    if not sl_type or sl_value_raw is None:
        return "skip", "SL settings not defined"

    sl_value = Decimal(str(sl_value_raw))

    if sl_type == "percent":
        delta = (entry_price * sl_value / Decimal("100")).quantize(factor_price, rounding=ROUND_DOWN)
    elif sl_type == "atr":
        tf = strategy.get("timeframe").lower()
        log.debug(f"[TP] strategy_id={strategy_id} timeframe={tf} — querying atr14")
        atr_raw = await get_indicator(symbol, tf, "atr14")
        if atr_raw is None:
            return "skip", "ATR not available"
        atr = Decimal(str(atr_raw))
        delta = (atr * sl_value).quantize(factor_price, rounding=ROUND_DOWN)
    else:
        return "skip", f"unknown sl_type: {sl_type}"

    stop_loss_price = (entry_price - delta) if direction == "long" else (entry_price + delta)
    risk_per_unit = abs(entry_price - stop_loss_price).quantize(factor_price, rounding=ROUND_DOWN)

    if risk_per_unit == Decimal("0"):
        return "skip", "risk_per_unit is zero"

    log.debug(f"[STAGE 2] sl_type={sl_type} stop_price={stop_loss_price} risk_per_unit={risk_per_unit}")
    # === Этап 3: Расчёт TP ===
    tp_targets = []
    atr = None

    for level_conf in strategy["tp_levels"]:
        level = level_conf["level"]
        tp_type = level_conf["tp_type"]
        tp_value = Decimal(str(level_conf["tp_value"]))

        if tp_type == "signal":
            price = None
        elif tp_type == "percent":
            delta = (entry_price * tp_value / Decimal("100")).quantize(factor_price, rounding=ROUND_DOWN)
            price = entry_price + delta if direction == "long" else entry_price - delta
        elif tp_type == "atr":
            if atr is None:
                tf = strategy.get("timeframe").lower()
                log.debug(f"[TP] strategy_id={strategy_id} timeframe={tf} — querying atr14")
                atr_raw = await get_indicator(symbol, tf, "atr14")
                if atr_raw is None:
                    return "skip", "ATR not available for TP"
                atr = Decimal(str(atr_raw))
            delta = (atr * tp_value).quantize(factor_price, rounding=ROUND_DOWN)
            price = entry_price + delta if direction == "long" else entry_price - delta
        else:
            return "skip", f"unknown tp_type: {tp_type}"

        if price is not None:
            price = price.quantize(factor_price, rounding=ROUND_DOWN)

        tp_targets.append(Target(
            type="tp",
            level=level,
            price=price,
            quantity=None,
            hit=False,
            hit_at=None,
            canceled=False
        ))

        log.debug(f"[TP] level={level} type={tp_type} price={price}")

    log.debug(f"[STAGE 3] TP targets prepared: {len(tp_targets)}")

    # === Этап 4: Учёт открытых позиций и доступного риска ===
    used_risk = sum(
        p.planned_risk for p in position_registry.values()
        if p.strategy_id == strategy_id
    )

    deposit = Decimal(str(strategy["deposit"]))
    max_risk_pct = Decimal(str(strategy["max_risk"]))
    max_allowed_risk = deposit * max_risk_pct / Decimal("100")
    available_risk = max(Decimal("0"), max_allowed_risk - used_risk)

    if available_risk <= 0:
        return "skip", "available risk exhausted"

    log.debug(f"[STAGE 4] used_risk={used_risk} max_allowed_risk={max_allowed_risk} available_risk={available_risk}")

    # === Этап 5: Расчёт объёма позиции ===
    leverage = Decimal(str(strategy["leverage"]))
    position_limit = Decimal(str(strategy["position_limit"]))

    qty_by_risk = available_risk / risk_per_unit
    qty_by_margin = (position_limit * leverage) / entry_price

    quantity_raw = min(qty_by_risk, qty_by_margin)
    quantity = (quantity_raw // factor_qty) * factor_qty

    if quantity < min_qty:
        return "skip", "quantity below min_qty"

    log.debug(f"[STAGE 5] qty_by_risk={qty_by_risk} qty_by_margin={qty_by_margin} quantity={quantity}")
    
    # === Этап 6: Финальные валидации ===
    used_margin = (entry_price * quantity) / leverage
    margin_threshold = position_limit * Decimal("0.75")

    if used_margin < margin_threshold:
        return "skip", f"used margin {used_margin:.4f} below 75% of position limit {margin_threshold:.4f}"

    if quantity < min_qty:
        return "skip", "final quantity below min_qty"

    log.debug(f"[STAGE 6] used_margin={used_margin} (threshold={margin_threshold}) — OK")
    # === Этап 7: Формирование TP с quantity ===
    volume_percents = [Decimal(str(lvl["volume_percent"])) for lvl in strategy["tp_levels"]]
    quantities = []
    total_assigned = Decimal("0")

    for i, percent in enumerate(volume_percents):
        if i < len(volume_percents) - 1:
            q = (quantity * percent / Decimal("100"))
            q = (q // factor_qty) * factor_qty
            quantities.append(q)
            total_assigned += q
        else:
            q = quantity - total_assigned
            q = q.quantize(factor_qty, rounding=ROUND_DOWN)
            quantities.append(q)

    for tp, q in zip(tp_targets, quantities):
        tp.quantity = q

    log.debug(f"[STAGE 7] TP quantities: {[tp.quantity for tp in tp_targets]} (total={sum(quantities)})")

    # === Этап 8: Расчёт planned_risk и SL Target ===
    planned_risk = (risk_per_unit * quantity).quantize(factor_price, rounding=ROUND_DOWN)

    sl_target = Target(
        type="sl",
        level=1,
        price=stop_loss_price,
        quantity=quantity,
        hit=False,
        hit_at=None,
        canceled=False
    )

    log.debug(f"[STAGE 8] planned_risk={planned_risk} SL quantity={quantity} SL price={stop_loss_price}")
    
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

    # 🔸 Расчёт notional_value и комиссии (pnl)
    precision_price = int(config.tickers[signal_data["symbol"]]["precision_price"])
    factor_price = Decimal(f"1e-{precision_price}")

    notional_value = (calc_result.entry_price * calc_result.quantity).quantize(factor_price, rounding=ROUND_DOWN)
    pnl = (-notional_value * Decimal("0.001")).quantize(factor_price, rounding=ROUND_DOWN)

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
        pnl=pnl,
        planned_risk=calc_result.planned_risk,
        route=calc_result.route,
        tp_targets=calc_result.tp_targets,
        sl_targets=[calc_result.sl_target],
        log_uid=calc_result.log_uid,
        notional_value=notional_value
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
        "notional_value": str(state.notional_value),
        "pnl": str(state.pnl),
        "created_at": state.created_at.isoformat(),
        "planned_risk": str(state.planned_risk),
        "route": state.route,
        "log_uid": state.log_uid,
        "tp_targets": json.dumps([asdict(t) for t in state.tp_targets], default=str),
        "sl_targets": json.dumps([asdict(t) for t in state.sl_targets], default=str),
        "event_type": "opened",
        "received_at": signal_data.get("received_at", datetime.utcnow().isoformat()),
        "latency_ms": "0"
    }

    try:
        await infra.redis_client.xadd("positions_open_stream", payload)
        log.debug(f"📬 Позиция создана и отправлена в Redis: {position_uid}")
    except Exception:
        log.exception("❌ Ошибка отправки позиции в Redis")

    # 🔸 Логгирование факта открытия позиции
    try:
        log_entry = {
            "log_uid": calc_result.log_uid,
            "strategy_id": str(signal_data["strategy_id"]),
            "status": "opened",
            "note": "открытие позиции",
            "position_uid": position_uid,
            "logged_at": datetime.utcnow().isoformat()
        }
        await infra.redis_client.xadd("signal_log_queue", log_entry)
    except Exception:
        log.exception("❌ Ошибка при логировании opened в signal_log_queue")
                
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
        log.debug(f"⚠️ [SKIP] strategy_id={strategy_id} log_uid={log_uid} reason=\"{reason}\"")
    except Exception:
        log.exception("❌ Ошибка при записи skip-события в Redis")
# 🔸 Обработка одной записи из потока
async def handle_open_request(record_id: str, raw: dict, redis):
    async with sem:
        import time
        start = time.monotonic()

        try:
            raw_data = raw.get(b"data") or raw.get("data")
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode()
            data = json.loads(raw_data)

            strategy_id = int(data["strategy_id"])
            log_uid = data["log_uid"]

            log.debug(f"▶ START open_request log_uid={log_uid} t={start:.3f}")

            result = await calculate_position_size(data)
            if isinstance(result, tuple) and result[0] == "skip":
                reason = result[1]
                await publish_skip_reason(log_uid, strategy_id, reason)
            else:
                key = (strategy_id, data["symbol"])
                if key in position_registry:
                    log.warning(f"⚠️ Позиция уже существует, повторное открытие заблокировано: {key}")
                    return
                await open_position(result, data)

        except Exception:
            log.exception("❌ Ошибка при обработке записи позиции")
        finally:
            end = time.monotonic()
            duration_ms = (end - start) * 1000
            log.debug(f"⏹ END   open_request log_uid={log_uid} t={end:.3f} Δ={duration_ms:.1f}ms")
            await redis.xack("strategy_opener_stream", "position_opener_group", record_id)
# 🔹 Основной воркер
MAX_PARALLEL_OPENS = 10
sem = asyncio.Semaphore(MAX_PARALLEL_OPENS)

async def run_position_opener_loop():
    stream = "strategy_opener_stream"
    group = "position_opener_group"
    consumer = "position_opener_1"
    redis = infra.redis_client

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        log.debug(f"📡 Группа {group} создана для {stream}")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            log.debug(f"ℹ️ Группа {group} уже существует")
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
                    asyncio.create_task(handle_open_request(record_id, raw, redis))

        except Exception:
            log.exception("❌ Ошибка в основном цикле position_opener_loop")
            await asyncio.sleep(5)