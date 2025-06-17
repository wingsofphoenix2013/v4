# signal_processor.py

import asyncio
import logging
from datetime import datetime
from infra import infra, get_price
from config_loader import config
from position_state_loader import position_registry
from position_handler import Target, full_protect_stop

# 🔸 Логгер маршрутизатора сигналов
log = logging.getLogger("SIGNAL_PROCESSOR")

# 🔸 Глобальная инициализация стратегий
strategy_registry = {}

def set_strategy_registry(registry: dict):
    global strategy_registry
    strategy_registry = registry

# 🔸 Главный воркер: слушает Redis Stream и обрабатывает сигналы
async def run_signal_loop():
    stream = "strategy_input_stream"
    last_id = "$"

    log.debug(f"📡 Подписка на Redis Stream: {stream}")

    while True:
        try:
            entries = await infra.redis_client.xread({stream: last_id}, count=50, block=1000)
            if not entries:
                continue

            for stream_name, records in entries:
                for record_id, data in records:
                    last_id = record_id
                    asyncio.create_task(process_signal(data))

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

            if not strategy.get("reverse", False) and not strategy.get("sl_protection", False):
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "реверс и SL защита отключены"
                )

            if not strategy.get("reverse", False) and strategy.get("sl_protection", True):
                price = await get_price(symbol)
                if price is None:
                    log.warning(f"⚠️ PROTECT: нет цены для {symbol}, сигнал пропущен")
                    return

                entry = position.entry_price
                price_is_worse = (
                    price < entry if position.direction == "long"
                    else price > entry
                )

                if price_is_worse:
                    await full_protect_stop(position)
                    await route_protect(
                        strategy_id, symbol, log_uid,
                        "позиция закрыта через SL-protect",
                        position.uid
                    )
                else:
                    sl = next((
                        s for s in position.sl_targets
                        if not s.hit and not s.canceled and s.price is not None
                    ), None)

                    if sl:
                        sl_below_entry = (
                            sl.price < entry if position.direction == "long"
                            else sl.price > entry
                        )

                        if sl_below_entry:
                            sl.canceled = True

                            new_sl = Target(
                                type="sl",
                                level=1,
                                price=entry,
                                quantity=sl.quantity,
                                hit=False,
                                hit_at=None,
                                canceled=False
                            )
                            position.sl_targets.append(new_sl)
                            log.info(f"🛡️ PROTECT: SL обновлён до цены входа {entry} для {position.uid}")

                            await route_protect(
                                strategy_id, symbol, log_uid,
                                "обновлён SL до уровня entry",
                                position.uid
                            )
                        else:
                            log.info(f"🛡️ PROTECT: SL уже на входе или выше ({sl.price} ≥ {entry}), пропущено")

                            await route_protect(
                                strategy_id, symbol, log_uid,
                                "действий по SL-protect нет, уровень SL выше цены входа",
                                position.uid
                            )
                return

            if strategy.get("reverse", False) and strategy.get("sl_protection", False):
                return await route_ignore(
                    strategy_id, symbol, direction, log_uid,
                    "маршрут reverse не реализован"
                )
        # 🔸 Обработка new_entry — стратегия готова к вызову
        modname = strategy.get("module_name", f"strategy_{strategy_id}")
        strategy_instance = strategy_registry.get(modname)

        if not strategy_instance:
            return await route_ignore(
                strategy_id, symbol, direction, log_uid,
                f"класс стратегии '{modname}' не найден"
            )

        context = {"redis": infra.redis_client}
        result = await strategy_instance.validate_signal(data, context)

        if result is True:
            await strategy_instance.run(data, context)
            return

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
    log.debug(f"⚠️ [IGNORE] {symbol} (strategy {strategy_id}, {direction}): {reason}")

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
# 🔸 Логирование действия маршрута SL-protect
async def route_protect(strategy_id, symbol, log_uid, note, position_uid):
    record = {
        "log_uid": log_uid,
        "strategy_id": str(strategy_id),
        "status": "protect",
        "note": note,
        "position_uid": str(position_uid),
        "logged_at": datetime.utcnow().isoformat()
    }
    try:
        await infra.redis_client.xadd("signal_log_queue", record)
    except Exception:
        log.exception("❌ Ошибка при логировании protect в signal_log_queue")