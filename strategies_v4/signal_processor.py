# signal_processor.py

import asyncio
import logging
from datetime import datetime
import json

from infra import infra
from position_state_loader import position_registry
from config_loader import config

log = logging.getLogger("SIGNAL_PROCESSOR")

# 🔸 Названия стримов
STRATEGY_INPUT_STREAM = "strategy_input_stream"
SIGNAL_LOG_STREAM = "signal_log_queue"

# 🔸 Проверка базовой маршрутизации сигнала
def route_signal_base(meta, signal_direction, symbol):
    key = (meta["id"], symbol)
    position = position_registry.get(key)

    if position and position.direction == signal_direction:
        return "ignore", "уже есть позиция в этом направлении"

    if position is None:
        if meta["allow_open"]:
            return "new_entry", "вход разрешён"
        return "ignore", "вход запрещён (allow_open = false)"

    if not meta["reverse"] and not meta["sl_protection"]:
        return "ignore", "вход запрещён, защита выключена"
    if not meta["reverse"] and meta["sl_protection"]:
        return "protect", "включена SL-защита"
    if meta["reverse"] and meta["sl_protection"]:
        return "reverse", "разрешён реверс"

    return "ignore", "неизвестное состояние"

# 🔸 Обработчик сигнала защиты (заглушка)
async def handle_protect_signal(msg_data):
    log.info("🧪 Вход в handle_protect_signal")
    log.info(f"🛡️ [PROTECT] Обработка сигнала защиты: strategy={msg_data.get('strategy_id')}, symbol={msg_data.get('symbol')}")

# 🔸 Обработчик сигнала реверса (заглушка)
async def handle_reverse_signal(msg_data):
    log.info("🧪 Вход в handle_reverse_signal")
    log.info(f"🔁 [REVERSE] Обработка сигнала реверса: strategy={msg_data.get('strategy_id')}, symbol={msg_data.get('symbol')}")

# 🔸 Диспетчер маршрутов: вызывает нужную обработку по route
async def route_and_dispatch_signal(msg_data, strategy_registry, redis):
    route = msg_data.get("route")
    strategy_id = int(msg_data.get("strategy_id"))
    symbol = msg_data.get("symbol")

    if route == "new_entry":
        strategy_name = config.strategies[strategy_id]["meta"]["name"]
        strategy_obj = strategy_registry.get(strategy_name)
        if not strategy_obj:
            log.warning(f"⚠️ Strategy not found in registry: {strategy_name}")
            return

        context = {"redis": redis}
        result = strategy_obj.run(msg_data, context)
        if asyncio.iscoroutine(result):
            await result

    elif route == "protect":
        await handle_protect_signal(msg_data)

    elif route == "reverse":
        await handle_reverse_signal(msg_data)

    elif route == "ignore":
        pass  # уже обработано ранее

    else:
        log.warning(f"⚠️ Неизвестный маршрут в dispatch: {route}")

# 🔸 Основной цикл обработки сигналов
async def run_signal_loop(strategy_registry):
    log.info("🚦 [SIGNAL_PROCESSOR] Запуск цикла обработки сигналов")

    redis = infra.redis_client
    last_id = "$"

    while True:
        try:
            response = await redis.xread(
                streams={STRATEGY_INPUT_STREAM: last_id},
                count=10,
                block=1000
            )

            if not response:
                continue

            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    last_id = msg_id

                    strategy_id = int(msg_data.get("strategy_id"))
                    signal_id = int(msg_data.get("signal_id"))
                    symbol = msg_data.get("symbol")
                    direction = msg_data.get("direction")
                    time = msg_data.get("time")
                    log_id = msg_data.get("log_id")

                    if not all([strategy_id, signal_id, symbol, direction, time, log_id]):
                        log.warning(f"⚠️ Неполный сигнал: {msg_data}")
                        continue

                    strategy = config.strategies.get(strategy_id)
                    if not strategy:
                        log.warning(f"⚠️ Стратегия {strategy_id} не найдена в config.strategies")
                        continue

                    meta = strategy["meta"]
                    route, note = route_signal_base(meta, direction, symbol)

                    if route == "new_entry" and not meta["use_all_tickers"]:
                        allowed = config.strategy_tickers.get(strategy_id, set())
                        if symbol not in allowed:
                            route = "ignore"
                            note = "тикер не разрешён для стратегии"

                    # 🔸 Валидация стратегии (если допущен new_entry)
                    if route == "new_entry":
                        strategy_name = meta["name"]
                        strategy_obj = strategy_registry.get(strategy_name)

                        if not strategy_obj:
                            route = "ignore"
                            note = f"strategy_registry: '{strategy_name}' не найдена"
                        else:
                            context = {"redis": redis}
                            result = strategy_obj.validate_signal(msg_data, context)
                            if asyncio.iscoroutine(result):
                                result = await result

                            if result != True:
                                if result == "logged":
                                    route = "ignore"
                                    note = None
                                else:
                                    route = "ignore"
                                    note = "отклонено стратегией: validate_signal() = False"

                    if route == "ignore":
                        log.info(f"🚫 ОТКЛОНЕНО: strategy={strategy_id}, symbol={symbol}, reason={note}")

                        if note is not None:
                            log_record = {
                                "log_id": log_id,
                                "strategy_id": strategy_id,
                                "status": route,
                                "position_id": None,
                                "note": note,
                                "logged_at": datetime.utcnow().isoformat()
                            }

                            await redis.xadd(SIGNAL_LOG_STREAM, {"data": json.dumps(log_record)})
                    else:
                        log.info(f"✅ ДОПУЩЕНО: strategy={strategy_id}, symbol={symbol}, route={route}, note={note}")

                        # 🔸 Диспетчеризация маршрута обработки
                        msg_data["route"] = route
                        await route_and_dispatch_signal(msg_data, strategy_registry, redis)
                            
        except Exception as e:
            log.exception("❌ Ошибка при чтении из Redis — повтор через 5 секунд")
            await asyncio.sleep(5)