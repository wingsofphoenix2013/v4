# binance_ws_v4.py

import asyncio
import aiohttp
import logging
import json

from infra import get_binance_listen_key, keep_alive_binance_listen_key
from strategy_registry import get_strategy_config, get_price_precision_for_symbol

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
    sl_policy = config.get("sl_policy", {})
    price_precision = get_price_precision_for_symbol(symbol)

    log.info(f"📐 FILLED стратегия {strategy_id}, symbol={symbol}, entry={entry_price:.{price_precision}f}, qty={qty}")

    # 🔸 TP-уровни
    for level, tp in sorted(tp_levels.items()):
        if tp["tp_type"] != "percent":
            continue

        tp_value = float(tp["tp_value"])
        volume = qty * (float(tp["volume_percent"]) / 100)
        percent = tp_value / 100

        if direction == "long":
            tp_price = entry_price * (1 + percent)
        else:
            tp_price = entry_price * (1 - percent)

        tp_price = round(tp_price, price_precision)
        log.info(f"🔸 TP{level}: {tp_price:.{price_precision}f} | {tp['volume_percent']}% → {volume:.4f}")

    # 🔸 SL-политика на уровне 0
    sl = sl_policy.get(0)
    if sl:
        sl_mode = sl["sl_mode"]
        sl_value = float(sl["sl_value"]) if sl["sl_value"] is not None else None

        if sl_mode == "percent" and sl_value is not None:
            percent = sl_value / 100
            if direction == "long":
                sl_price = entry_price * (1 - percent)
            else:
                sl_price = entry_price * (1 + percent)
            sl_price = round(sl_price, price_precision)
            log.info(f"🔸 SL (percent): {sl_price:.{price_precision}f} ({sl_value}%)")

        elif sl_mode == "entry":
            sl_price = round(entry_price, price_precision)
            log.info(f"🔸 SL (entry): {sl_price:.{price_precision}f}")

        elif sl_mode == "none":
            log.info("🔸 SL: none")

        else:
            log.warning(f"⚠️ SL режим '{sl_mode}' не поддерживается")
    else:
        log.info("🔸 Нет SL-политики на уровне 0")