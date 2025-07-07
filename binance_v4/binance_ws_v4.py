# binance_ws_v4.py

import asyncio
import logging
from datetime import datetime

from infra import infra
from strategy_registry import get_leverage

log = logging.getLogger("BINANCE_WS")

# 🔸 Главный воркер для обработки WebSocket сообщений
async def run_binance_ws_listener():
    ws = infra.binance_ws_client
    if ws is None:
        log.error("❌ WebSocket клиент не инициализирован")
        return

    log.info("🔄 Запуск Binance WebSocket listener")
    log.info(f"🧾 Активный listenKey: {infra.binance_ws_listen_key}")

    # ⏳ Отложенный запуск теста смены плеча через 2 минуты
    asyncio.create_task(delayed_test_change_leverage(120))

    while True:
        try:
            log.info("🧪 Ожидаем сообщение из WebSocket")
            msg = await ws.receive_json()
            log.info(f"🛰 RAW сообщение от Binance WS: {msg}")
            await handle_execution_report(msg)
        except Exception as e:
            log.exception(f"⚠️ Ошибка при обработке WebSocket-сообщения: {e}")
            await asyncio.sleep(1)


# 🔸 Обработка executionReport (в том числе не FILLED)
async def handle_execution_report(msg: dict):
    if msg.get("e") != "executionReport":
        return

    order_id = msg.get("i")
    status = msg.get("X")
    exec_type = msg.get("x")

    log.info(f"📬 executionReport: orderId={order_id}, status={status}, exec_type={exec_type}")

    if status != "FILLED" or exec_type != "TRADE":
        log.debug("ℹ️ Пропуск: не FINISHED исполнение TRADE")
        return

    # Поиск позиции по order_id
    position_uid = None
    for puid, data in infra.inflight_positions.items():
        if data.get("order_id") == order_id:
            position_uid = puid
            break

    if not position_uid:
        log.warning(f"⚠️ FILLED ордер {order_id}, но position_uid не найден в кэше")
        return

    info = infra.inflight_positions[position_uid]
    strategy_id = info["strategy_id"]
    symbol = info["symbol"]
    side = info["side"]
    qty = float(msg["z"])
    price = float(msg["Z"]) / qty if qty else 0.0
    entry_time = datetime.utcfromtimestamp(msg["T"] / 1000)

    log.info(f"✅ FILLED: orderId={order_id}, position_uid={position_uid}, qty={qty}, price={price:.6f}")

    # 📝 Запись позиции
    await infra.pg_pool.execute(
        """
        INSERT INTO binance_positions_v4 (
            position_uid, strategy_id, symbol, direction,
            entry_price, entry_time, leverage, position_side,
            executed_qty, notional_value, status, binance_update_ts
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10, 'open', NOW()
        )
        """,
        position_uid,
        strategy_id,
        symbol,
        "long" if side == "BUY" else "short",
        price,
        entry_time,
        get_leverage(strategy_id),
        side,
        qty,
        price * qty
    )

    log.info(f"📄 Позиция записана в binance_positions_v4: {position_uid}")

    # 🔄 Обновление статуса ордера
    await infra.pg_pool.execute(
        """
        UPDATE binance_orders_v4
        SET status = 'FILLED', updated_at = NOW()
        WHERE binance_order_id = $1
        """,
        order_id
    )

    log.info(f"📊 Ордер {order_id} обновлён в binance_orders_v4 -> FILLED")

    # 🧹 Удалить из inflight-кэша
    infra.inflight_positions.pop(position_uid, None)


# 🔸 Тестовая функция смены плеча (генерация события)
async def delayed_test_change_leverage(delay_sec: int):
    await asyncio.sleep(delay_sec)
    log.info("🚀 Отправка запроса на смену плеча для BTCUSDT (для проверки WS)")
    try:
        result = infra.binance_client.change_leverage(symbol="BTCUSDT", leverage=9)
        log.info(f"✅ Ответ от Binance: {result}")
    except Exception as e:
        log.exception(f"❌ Ошибка при смене плеча: {e}")