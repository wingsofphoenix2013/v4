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

                    if not all([strategy_id, signal_id, symbol, direction, time]):
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
                            context = {}  # пока пусто
                            if not strategy_obj.validate_signal(msg_data, context):
                                route = "ignore"
                                note = "отклонено стратегией: validate_signal() = False"

                    if route == "ignore":
                        log.info(f"🚫 ОТКЛОНЕНО: strategy={strategy_id}, symbol={symbol}, reason={note}")
                    else:
                        log.info(f"✅ ДОПУЩЕНО: strategy={strategy_id}, symbol={symbol}, route={route}, note={note}")

                    log_record = {
                        "log_id": signal_id,
                        "strategy_id": strategy_id,
                        "status": route,
                        "position_id": None,
                        "note": note,
                        "logged_at": datetime.utcnow().isoformat()
                    }

                    await redis.xadd(SIGNAL_LOG_STREAM, {"data": json.dumps(log_record)})

        except Exception as e:
            log.exception("❌ Ошибка при чтении из Redis — повтор через 5 секунд")
            await asyncio.sleep(5)