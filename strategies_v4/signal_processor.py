# signal_processor.py

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
import json

from infra import infra
from position_state_loader import position_registry
from config_loader import config
from position_handler import full_protect_stop, raise_sl_to_entry, get_field, set_field

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

# 🔸 Обработчик сигнала защиты позиции (protect)
async def handle_protect_signal(msg_data):
    strategy_id = int(msg_data.get("strategy_id"))
    symbol = msg_data.get("symbol")
    is_reverse = msg_data.get("is_reverse", False)  # 🔹 передаётся только при реверсе

    position = position_registry.get((strategy_id, symbol))
    if not position:
        log.debug(f"[PROTECT] Позиция не найдена: strategy={strategy_id}, symbol={symbol}")
        return

    redis = infra.redis_client
    mark_str = await redis.get(f"price:{symbol}")
    if not mark_str:
        log.warning(f"[PROTECT] Не удалось получить markprice для {symbol}")
        return

    mark = Decimal(mark_str)
    entry = position.entry_price

    # 🔹 Вариант 1: позиция в зоне убытка → полное защитное закрытие
    if (
        (position.direction == "long" and mark <= entry) or
        (position.direction == "short" and mark >= entry)
    ):
        log.debug(
            f"[PROTECT] Позиция в зоне убытка (mark={mark}, entry={entry}, direction={position.direction}) → вызов full_protect_stop"
        )
        await full_protect_stop(position, is_reverse=is_reverse)
        return

    # 🔹 Вариант 2: позиция в плюсе → проверка SL
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
        log.debug(f"[PROTECT] Нет активных SL для позиции {position.uid}")
        return

    sl = active_sl[0]
    sl_price = get_field(sl, "price")

    if (
        (position.direction == "long" and sl_price < entry) or
        (position.direction == "short" and sl_price > entry)
    ):
        log.debug(
            f"[PROTECT] SL ниже безопасного уровня: sl={sl_price}, entry={entry}, direction={position.direction} → перемещаем"
        )
        await raise_sl_to_entry(position, sl)
    else:
        log.debug(
            f"[PROTECT] SL уже на уровне entry или лучше: sl={sl_price}, entry={entry}, direction={position.direction} → ничего не делаем"
        )
# 🔸 Обработчик сигнала реверса (reverse)
async def handle_reverse_signal(msg_data):
    strategy_id = int(msg_data.get("strategy_id"))
    symbol = msg_data.get("symbol")

    position = position_registry.get((strategy_id, symbol))
    if not position:
        log.debug(f"[REVERSE] Позиция не найдена: strategy={strategy_id}, symbol={symbol}")
        return

    # 🔍 Находим активный TP (source может быть price или signal)
    active_tp = sorted(
        [
            t for t in position.tp_targets
            if get_field(t, "type") == "tp"
            and not get_field(t, "hit")
            and not get_field(t, "canceled")
        ],
        key=lambda t: get_field(t, "level")
    )

    if not active_tp:
        log.debug(f"[REVERSE] Нет активных TP для позиции {position.uid}")
        return

    tp = active_tp[0]
    tp_source = get_field(tp, "source")

    # 🧭 Выбор пути: SL-защита или закрытие с реверсом
    if tp_source == "price":
        log.debug(f"[REVERSE] Активный TP через цену → route: protect")
        await handle_protect_signal({**msg_data, "is_reverse": True})  # передаём флаг логически
    elif tp_source == "signal":
        log.debug(f"[REVERSE] Активный TP со стороны сигнала → route: full_reverse_stop")
        from position_handler import full_reverse_stop
        await full_reverse_stop(position, msg_data)
    else:
        log.warning(f"[REVERSE] Неизвестный тип source в TP: {tp_source}")

# 🔸 Диспетчер маршрутов: вызывает нужную обработку по route
async def route_and_dispatch_signal(msg_data, strategy_registry, redis):
    route = msg_data.get("route")
    strategy_id = int(msg_data.get("strategy_id"))
    symbol = msg_data.get("symbol")

    log.debug(f"[ROUTER] Начало обработки: symbol={symbol}, strategy={strategy_id}, route={route}")

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
        log.debug(f"[ROUTER] Обработка завершена: new_entry {symbol} ({strategy_id})")

    elif route == "protect":
        await handle_protect_signal(msg_data)
        log.debug(f"[ROUTER] Обработка завершена: protect {symbol} ({strategy_id})")

    elif route == "reverse":
        await handle_reverse_signal(msg_data)
        log.debug(f"[ROUTER] Обработка завершена: reverse {symbol} ({strategy_id})")

    elif route == "ignore":
        pass  # уже обработано ранее

    else:
        log.warning(f"⚠️ Неизвестный маршрут в dispatch: {route}")
        
# 🔸 Основной цикл обработки сигналов
async def run_signal_loop(strategy_registry):
    log.debug("🚦 [SIGNAL_PROCESSOR] Запуск цикла обработки сигналов")

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

                    log.debug(f"[SIGNAL_LOOP] 📨 Сигнал из потока: {msg_data}")

                    symbol = msg_data.get("symbol")
                    direction = msg_data.get("direction")
                    time = msg_data.get("time")
                    log_id = msg_data.get("log_id")
                    signal_id = int(msg_data.get("signal_id", 0) or 0)

                    if not all([symbol, direction, time, log_id, signal_id]):
                        log.warning(f"⚠️ Неполный сигнал: {msg_data}")
                        continue

                    seen_keys = set()
                    tasks = []

                    for strategy_id, strategy in config.strategies.items():
                        meta = strategy["meta"]
                        key = (strategy_id, symbol)

                        if key in seen_keys:
                            continue  # исключаем дубликаты
                        seen_keys.add(key)

                        route, note = route_signal_base(meta, direction, symbol)

                        if route == "new_entry" and not meta.get("use_all_tickers", False):
                            allowed = config.strategy_tickers.get(strategy_id, set())
                            if symbol not in allowed:
                                route = "ignore"
                                note = "тикер не разрешён для стратегии"

                        strategy_name = meta["name"]
                        strategy_obj = strategy_registry.get(strategy_name)

                        async def process_strategy(strategy_id=strategy_id, meta=meta, route=route, note=note):
                            context = {"redis": redis}
                            local_msg = msg_data.copy()
                            local_msg["strategy_id"] = strategy_id
                            local_msg["route"] = route

                            if route == "new_entry":
                                if not strategy_obj:
                                    return await log_ignore(strategy_id, log_id, None, "strategy not found")

                                result = strategy_obj.validate_signal(local_msg, context)
                                if asyncio.iscoroutine(result):
                                    result = await result

                                if result != True:
                                    if result == "logged":
                                        return  # уже залогировано
                                    return await log_ignore(strategy_id, log_id, None, "отклонено стратегией")

                            # ДОПУЩЕНО
                            key = (strategy_id, symbol)
                            position = position_registry.get(key)
                            if position:
                                local_msg["position_uid"] = position.uid

                            await route_and_dispatch_signal(local_msg, strategy_registry, redis)

                        async def log_ignore(strategy_id, log_id, position_uid, note):
                            if note:
                                log.debug(f"🚫 ОТКЛОНЕНО: strategy={strategy_id}, symbol={symbol}, reason={note}")
                                log_record = {
                                    "log_id": log_id,
                                    "strategy_id": strategy_id,
                                    "status": "ignore",
                                    "position_uid": position_uid,
                                    "note": note,
                                    "logged_at": datetime.utcnow().isoformat()
                                }
                                await redis.xadd(SIGNAL_LOG_STREAM, {"data": json.dumps(log_record)})

                        tasks.append(process_strategy())

                    if tasks:
                        await asyncio.gather(*tasks)

        except Exception:
            log.exception("❌ Ошибка при чтении из Redis — повтор через 5 секунд")
            await asyncio.sleep(5)