import logging
from infra import ENABLED_SIGNALS, ENABLED_TICKERS, ENABLED_STRATEGIES

# 🔸 Обработка одного сигнала из Redis Stream
async def process_signal(data: dict):
    log = logging.getLogger("PROCESSOR")

    symbol = data.get("symbol")
    message = data.get("message")

    if not symbol or not message:
        log.warning(f"Пропущен сигнал без symbol/message: {data}")
        return

    # 🔍 Определение направления
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
        log.warning(f"Не удалось определить направление сигнала: {message}")
        return

    # 🔍 Проверка разрешённого тикера
    if symbol not in ENABLED_TICKERS:
        log.warning(f"Тикер {symbol} не входит в ENABLED_TICKERS — сигнал отклонён")
        return

    # 🔍 Фильтрация подходящих стратегий
    matched_strategies = []
    for strategy_id, strategy in ENABLED_STRATEGIES.items():
        if strategy["signal_id"] != signal_id:
            continue
        if strategy["allow_open"] or strategy["reverse"]:
            matched_strategies.append(strategy_id)

    if not matched_strategies:
        log.info(f"Нет подходящих стратегий для сигнала: {symbol} | {direction}")
        return

    log.info(f"Сигнал принят к обработке: {symbol} | {direction} | signal_id={signal_id} | стратегии: {matched_strategies}")