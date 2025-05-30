import logging
import json
import asyncio
import infra
from infra import ENABLED_SIGNALS, ENABLED_TICKERS, ENABLED_STRATEGIES

# 🔸 Публикация лога сигнала в Redis Stream для core_io
async def publish_signal_log(data: dict, signal_id: int, direction: str, status: str):
    await infra.REDIS.xadd(
        "signals_log_stream",
        {
            "signal_id": str(signal_id),
            "symbol": data.get("symbol"),
            "direction": direction,
            "source": "stream",
            "message": data.get("message"),
            "raw_message": json.dumps(data),
            "bar_time": data.get("bar_time"),
            "sent_at": data.get("sent_at"),
            "received_at": data.get("received_at"),
            "status": status,
            "uid": f"{data.get('symbol')}_{data.get('bar_time')}_{data.get('message')}",,
        }
    )

# 🔸 Обработка одного сигнала из Redis Stream
async def process_signal(data: dict):
    log = logging.getLogger("PROCESSOR")

    symbol = data.get("symbol")
    message = data.get("message")

    if not symbol or not message:
        log.warning(f"Пропущен сигнал без symbol/message: {data}")
        return

    direction = None
    signal_id = None
    for sid, phrases in ENABLED_SIGNALS.items():
        if message == phrases["long"]:
            direction = "long"
            signal_id = sid
            break
        elif message == phrases["short"]:
            direction = "short"
            signal_id = sid
            break

    if not direction:
        log.warning(f"Не удалось определить направление сигнала: {message} — сигнал проигнорирован")
        return

    if symbol not in ENABLED_TICKERS:
        log.warning(f"Тикер {symbol} не входит в ENABLED_TICKERS — сигнал отброшен")
        return

    matched_strategies = []
    for strategy_id, strategy in ENABLED_STRATEGIES.items():
        if strategy["signal_id"] != signal_id:
            continue
        if strategy["allow_open"] or strategy["reverse"]:
            matched_strategies.append(strategy_id)

    status = "ignored" if not matched_strategies else "dispatched"

    # Добавляем matched_strategies только если они есть
    log_data = {**data}
    if status == "dispatched":
        log_data["strategies"] = matched_strategies

    await publish_signal_log(
        log_data,
        signal_id=signal_id,
        direction=direction,
        status=status
    )

    if status == "dispatched":
        log.debug(f"Сигнал принят для стратегий: {symbol} | {direction} | signal_id={signal_id} | стратегии: {matched_strategies}")
    else:
        log.debug(f"Сигнал проигнорирован: {symbol} | {direction} | signal_id={signal_id}")