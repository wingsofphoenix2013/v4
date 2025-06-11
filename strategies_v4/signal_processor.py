# signal_processor.py

import asyncio
import logging
from datetime import datetime
from infra import infra
from config_loader import config
from position_state_loader import position_registry

# 🔸 Логгер маршрутизатора сигналов
log = logging.getLogger("SIGNAL_PROCESSOR")

# 🔸 Главный воркер: слушает Redis Stream и обрабатывает сигналы
async def run_signal_loop():
    stream = "strategy_input_stream"
    last_id = "0"

    log.info(f"📡 Подписка на Redis Stream: {stream}")

    while True:
        try:
            entries = await infra.redis_client.xread({stream: last_id}, count=50, block=1000)
            if not entries:
                continue

            for stream_name, records in entries:
                for record_id, data in records:
                    last_id = record_id
                    await process_signal(data)

        except Exception:
            log.exception("❌ Ошибка чтения из Redis Stream")
            await asyncio.sleep(5)

# 🔸 Обработка одного сигнала
async def process_signal(data: dict):
    try:
        strategy_id = int(data["strategy_id"])
        symbol = data["symbol"]
        direction = data["direction"]
        log_uid = data["log_uid"]
        received_at = data["received_at"]

        strategy = config.strategies.get(strategy_id)
        if not strategy:
            return await route_ignore(
                strategy_id, symbol, direction, log_uid,
                "стратегия не найдена в конфигурации"
            )

        if not strategy["allow_open"]:
            if not strategy["reverse"]:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "открытие запрещено, реверсы отключены"
                )
            else:
                # Пока не реализовано, логировать отдельно позже
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "открытие запрещено, запуск реверса (не реализовано)"
                )

        if not strategy["use_all_tickers"]:
            allowed = config.strategy_tickers.get(strategy_id, set())
            if symbol not in allowed:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "тикер не разрешён для этой стратегии"
                )
                
        # 🔸 Проверка позиции
        position = position_registry.get((strategy_id, symbol))
        if position:
            if position.direction == direction:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "повтор сигнала в ту же сторону"
                )
            if not strategy["reverse"] and not strategy["sl_protect"]:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "реверс и SL защита отключены"
                )
            if not strategy["reverse"] and strategy["sl_protect"]:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "маршрут protect не реализован"
                )
            if strategy["reverse"] and strategy["sl_protect"]:
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "маршрут reverse не реализован"
                )

        # 🔸 Обработка new_entry — стратегия готова к вызову
        strategy_instance = strategy_registry.get(f"strategy_{strategy_id}")
        if not strategy_instance:
            return await route_ignore(
                strategy_id, symbol, direction, log_uid,
                "класс стратегии не найден"
            )

        context = {"redis": infra.redis_client}
        result = await strategy_instance.validate_signal(data, context)

        if result is True:
            # пока открытие позиции не реализовано
            return await route_ignore(
                strategy_id, symbol, direction, log_uid,
                "позиция не открыта: маршрут new_entry пока не реализован"
            )
        elif isinstance(result, tuple) and result[0] == "ignore":
            note = result[1]
            return await route_ignore(strategy_id, symbol, direction, log_uid, note)
        else:
            return await route_ignore(
                strategy_id, symbol, direction, log_uid,
                "некорректный ответ от validate_signal()"
            )

    except Exception:
        log.exception("❌ Ошибка обработки сигнала")

# 🔸 Маршрут ignore: логируем отказ
async def route_ignore(strategy_id, symbol, direction, log_uid, reason: str):
    log.info(f"⚠️ [IGNORE] {symbol} (strategy {strategy_id}, {direction}): {reason}")

    record = {
        "log_uid": log_uid,
        "strategy_id": str(strategy_id),
        "status": "ignore",
        "note": reason,
        "position_uid": "",
        "logged_at": datetime.utcnow().isoformat()
    }

    try:
        await infra.redis_client.xadd("signal_log_queue", record)
    except Exception:
        log.exception("❌ Ошибка при записи ignore-лога в Redis")