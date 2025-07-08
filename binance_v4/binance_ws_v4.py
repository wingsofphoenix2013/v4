# binance_ws_v4.py

import asyncio
import aiohttp
import logging
import json

from infra import (
    infra,
    get_binance_listen_key,
    keep_alive_binance_listen_key,
)

from strategy_registry import (
    get_strategy_config,
    get_price_precision_for_symbol,
    get_precision_for_symbol,
    get_tick_size_for_symbol,
    round_to_tick,
)

log = logging.getLogger("BINANCE_WS")

# 🔸 Временное сопоставление orderId → стратегия и параметры
filled_order_map: dict[int, dict] = {}  # order_id → {"strategy_id", "direction", "quantity"}


# 🔸 Обработчик WebSocket Binance
async def run_binance_ws_listener():
    while True:
        try:
            log.info("🔌 Запуск подключения к Binance User Data Stream")

            listen_key = await get_binance_listen_key()
            asyncio.create_task(keep_alive_binance_listen_key())

            ws_url = f"wss://stream.binancefuture.com/ws/{listen_key}"
            log.info(f"🌐 Подключение к WebSocket: {ws_url}")

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    log.info("✅ WebSocket соединение установлено")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)

                            if data.get("e") == "ORDER_TRADE_UPDATE":
                                order = data.get("o", {})
                                if order.get("X") == "FILLED":
                                    await on_order_filled(order)

                            log.info(f"📨 Сообщение: {msg.data}")

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            log.warning("⚠️ Ошибка WebSocket-соединения, выход из цикла")
                            break

        except Exception as e:
            log.exception(f"❌ Ошибка в Binance WebSocket слушателе: {e}")

        log.info("⏳ Перезапуск подключения через 5 секунд...")
        await asyncio.sleep(5)

# 🔸 Обработка FILLED-события: расчёт TP и SL
async def on_order_filled(order: dict):
    order_id = order["i"]
    symbol = order["s"]
    entry_price = float(order["ap"])
    qty = float(order["q"])

    if order_id not in filled_order_map:
        log.warning(f"⚠️ FILLED для неизвестного orderId={order_id} — игнорируем")
        return

    context = filled_order_map[order_id]
    strategy_id = context["strategy_id"]
    direction = context["direction"]

    config = get_strategy_config(strategy_id)
    if not config:
        log.warning(f"⚠️ Стратегия {strategy_id} не найдена в кеше — игнорируем")
        return

    tp_levels = config.get("tp_levels", {})
    price_precision = get_price_precision_for_symbol(symbol)

    log.info(f"📐 FILLED стратегия {strategy_id}, symbol={symbol}, entry={entry_price:.{price_precision}f}, qty={qty}")

    # 🔸 TP-уровни
    for level, tp in sorted(tp_levels.items()):
        if tp["tp_type"] != "percent":
            continue

        try:
            tp_value = float(tp["tp_value"])
            volume = qty * (float(tp["volume_percent"]) / 100)
            percent = tp_value / 100

            if direction == "long":
                tp_price = entry_price * (1 + percent)
            else:
                tp_price = entry_price * (1 - percent)

            tp_price = round(tp_price, price_precision)
            log.info(f"🔸 TP{level}: {tp_price:.{price_precision}f} | {tp['volume_percent']}% → {volume:.4f}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка расчёта TP{level}: {e}")

    # 🔸 SL: всегда percent
    sl_value = float(config["sl_value"])
    percent = sl_value / 100

    if direction == "long":
        sl_price = entry_price * (1 - percent)
    else:
        sl_price = entry_price * (1 + percent)

    sl_price = round(sl_price, price_precision)
    log.info(f"🔸 SL (initial): {sl_price:.{price_precision}f} ({sl_value}%)")

    # 🔸 Размещение ордеров
    await place_tp_sl_orders(
        symbol=symbol,
        direction=direction,
        entry_price=entry_price,
        qty=qty,
        strategy_id=strategy_id
    )

# 🔸 Размещение TP и SL ордеров после открытия позиции
async def place_tp_sl_orders(symbol: str, direction: str, qty: float, entry_price: float, strategy_id: int):
    config = get_strategy_config(strategy_id)
    if not config:
        log.warning(f"⚠️ Стратегия {strategy_id} не найдена в кеше для размещения TP/SL")
        return

    tp_levels = config.get("tp_levels", {})
    price_precision = get_price_precision_for_symbol(symbol)
    tick = get_tick_size_for_symbol(symbol)

    total_tp_volume = 0.0
    sorted_tp = sorted(tp_levels.items())
    num_tp = len(sorted_tp)

    # 🔸 TP ордера
    for i, (level, tp) in enumerate(sorted_tp):
        if tp["tp_type"] != "percent":
            continue

        percent = float(tp["tp_value"]) / 100
        volume_percent = float(tp["volume_percent"])

        if i < num_tp - 1:
            volume = qty * (volume_percent / 100)
            total_tp_volume += volume
        else:
            volume = qty - total_tp_volume  # 🔸 остаток для последнего TP

        if direction == "long":
            tp_price = entry_price * (1 + percent)
            side = "SELL"
        else:
            tp_price = entry_price * (1 - percent)
            side = "BUY"

        tp_price = round_to_tick(tp_price, tick)
        volume = round(volume, get_precision_for_symbol(symbol))
        volume_str = f"{volume:.{get_precision_for_symbol(symbol)}f}"

        try:
            resp = infra.binance_client.new_order(
                symbol=symbol,
                side=side,
                type="LIMIT",
                timeInForce="GTC",
                quantity=volume_str,
                price=f"{tp_price:.{price_precision}f}",
                reduceOnly=True
            )
            log.info(f"📌 TP{level} ордер размещён: qty={volume_str}, price={tp_price:.{price_precision}f}")
        except Exception as e:
            log.warning(f"⚠️ Ошибка размещения TP{level}: {e}")

    # 🔸 SL ордер (STOP_MARKET)
    sl_value = float(config.get("sl_value", 0))
    sl_percent = sl_value / 100

    if direction == "long":
        sl_price = entry_price * (1 - sl_percent)
        side = "SELL"
    else:
        sl_price = entry_price * (1 + sl_percent)
        side = "BUY"

    sl_price = round_to_tick(sl_price, tick)
    qty_str = f"{qty:.{get_precision_for_symbol(symbol)}f}"

    try:
        resp = infra.binance_client.new_order(
            symbol=symbol,
            side=side,
            type="STOP_MARKET",
            stopPrice=f"{sl_price:.{price_precision}f}",
            quantity=qty_str,
            reduceOnly=True
        )
        log.info(f"📌 SL ордер размещён: qty={qty_str}, stopPrice={sl_price:.{price_precision}f}")
    except Exception as e:
        log.warning(f"⚠️ Ошибка размещения SL: {e}")