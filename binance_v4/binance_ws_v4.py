# binance_ws_v4.py

import asyncio
import aiohttp
import logging
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

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

from core_io import insert_binance_position, insert_binance_order, update_binance_order_status

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
                                order_id = order.get("i")
                                status = order.get("X")

                                if order_id and status:
                                    try:
                                        await update_binance_order_status(order_id, status)
                                    except Exception:
                                        log.exception(f"❌ Ошибка обновления статуса ордера {order_id}")

                                if status == "FILLED":
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

    if order_id not in filled_order_map:
        log.warning(f"⚠️ FILLED для неизвестного orderId={order_id} — игнорируем")
        return

    context = filled_order_map[order_id]
    strategy_id = context["strategy_id"]
    direction = context["direction"]
    position_uid = context["position_uid"]

    config = get_strategy_config(strategy_id)
    if not config:
        log.warning(f"⚠️ Стратегия {strategy_id} не найдена в кеше — игнорируем")
        return

    price_precision = get_price_precision_for_symbol(symbol)
    qty_precision = get_precision_for_symbol(symbol)
    entry_price = Decimal(order["ap"]).quantize(Decimal("1." + "0" * price_precision), rounding=ROUND_DOWN)
    qty = Decimal(str(order["q"])).quantize(Decimal("1." + "0" * qty_precision), rounding=ROUND_DOWN)

    log.info(f"📐 FILLED стратегия {strategy_id}, symbol={symbol}, entry={entry_price:.{price_precision}f}, qty={qty}")

    # 🔸 Сохраняем позицию в базу
    try:
        entry_time_ms = int(order["T"])
        entry_time = datetime.fromtimestamp(entry_time_ms / 1000, tz=timezone.utc)
        leverage = config.get("leverage", 1)
        position_side = "LONG" if direction == "long" else "SHORT"
        notional_value = (entry_price * qty).quantize(Decimal("1.0000"), rounding=ROUND_DOWN)

        await insert_binance_position(
            position_uid=position_uid,
            strategy_id=strategy_id,
            symbol=symbol,
            direction=direction,
            entry_price=float(entry_price),
            entry_time=entry_time,
            leverage=leverage,
            position_side=position_side,
            executed_qty=float(qty),
            notional_value=float(notional_value),
            raw_data=order
        )

    except Exception as e:
        log.exception(f"❌ Ошибка записи позиции {position_uid} в базу: {e}")

    # 🔸 TP-уровни (для логирования — не для расчёта ордеров)
    tp_levels = config.get("tp_levels", {})
    for level, tp in sorted(tp_levels.items()):
        if tp["tp_type"] != "percent":
            continue

        try:
            percent = Decimal(str(tp["tp_value"])) / Decimal("100")
            volume = qty * Decimal(str(tp["volume_percent"])) / Decimal("100")

            if direction == "long":
                tp_price = entry_price * (Decimal("1") + percent)
            else:
                tp_price = entry_price * (Decimal("1") - percent)

            tp_price = tp_price.quantize(Decimal("1." + "0" * price_precision), rounding=ROUND_DOWN)
            log.info(f"🔸 TP{level}: {tp_price:.{price_precision}f} | {tp['volume_percent']}% → {volume:.4f}")

        except Exception as e:
            log.warning(f"⚠️ Ошибка расчёта TP{level}: {e}")

    # 🔸 SL-цена
    try:
        sl_percent = Decimal(str(config.get("sl_value", 0))) / Decimal("100")
        if direction == "long":
            sl_price = entry_price * (Decimal("1") - sl_percent)
        else:
            sl_price = entry_price * (Decimal("1") + sl_percent)

        sl_price = sl_price.quantize(Decimal("1." + "0" * price_precision), rounding=ROUND_DOWN)
        log.info(f"🔸 SL (initial): {sl_price:.{price_precision}f} ({sl_percent * 100}%)")

    except Exception as e:
        log.warning(f"⚠️ Ошибка расчёта SL: {e}")

    # 🔸 Размещение TP/SL ордеров
    await place_tp_sl_orders(
        symbol=symbol,
        direction=direction,
        entry_price=float(entry_price),
        qty=float(qty),
        strategy_id=strategy_id,
        position_uid=position_uid
    )

    # 🔸 Очистка буфера — удаляем использованный order_id
    filled_order_map.pop(order_id, None)    
# 🔸 Размещение TP и SL ордеров после открытия позиции
async def place_tp_sl_orders(
    symbol: str,
    direction: str,
    qty: float,
    entry_price: float,
    strategy_id: int,
    position_uid: str
):
    config = get_strategy_config(strategy_id)
    if not config:
        log.warning(f"⚠️ Стратегия {strategy_id} не найдена в кеше для размещения TP/SL")
        return

    tp_levels = config.get("tp_levels", {})
    price_precision = get_price_precision_for_symbol(symbol)
    qty_precision = get_precision_for_symbol(symbol)
    tick = Decimal(str(get_tick_size_for_symbol(symbol)))

    entry_price_d = Decimal(str(entry_price))
    qty_d = Decimal(str(qty))

    total_tp_volume = Decimal('0')
    sorted_tp = sorted(tp_levels.items())
    num_tp = len(sorted_tp)

    # 🔸 TP ордера
    for i, (level, tp) in enumerate(sorted_tp):
        if tp["tp_type"] != "percent":
            continue

        percent = Decimal(str(tp["tp_value"])) / Decimal('100')
        volume_percent = Decimal(str(tp["volume_percent"]))

        if i < num_tp - 1:
            volume = qty_d * volume_percent / Decimal('100')
            total_tp_volume += volume
        else:
            volume = qty_d - total_tp_volume  # остаток

        if direction == "long":
            tp_price = entry_price_d * (Decimal('1') + percent)
            side = "SELL"
        else:
            tp_price = entry_price_d * (Decimal('1') - percent)
            side = "BUY"

        tp_price = tp_price.quantize(tick, rounding=ROUND_DOWN)
        volume = volume.quantize(Decimal('1.' + '0' * qty_precision), rounding=ROUND_DOWN)
        volume_str = f"{volume:.{qty_precision}f}"

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

            try:
                await insert_binance_order(
                    position_uid=position_uid,
                    strategy_id=strategy_id,
                    symbol=symbol,
                    binance_order_id=resp["orderId"],
                    side=side,
                    type_="LIMIT",
                    status="NEW",
                    purpose="tp",
                    level=level,
                    price=float(tp_price),
                    quantity=float(volume),
                    reduce_only=True,
                    close_position=False,
                    time_in_force="GTC",
                    raw_data=resp
                )
            except Exception as db_exc:
                log.exception(f"⚠️ Ошибка записи TP{level} ордера в БД: {db_exc}")

        except Exception as e:
            log.warning(f"⚠️ Ошибка размещения TP{level}: {e}")

    # 🔸 SL ордер
    sl_value = Decimal(str(config.get("sl_value", 0))) / Decimal('100')

    if direction == "long":
        sl_price = entry_price_d * (Decimal('1') - sl_value)
        side = "SELL"
    else:
        sl_price = entry_price_d * (Decimal('1') + sl_value)
        side = "BUY"

    sl_price = sl_price.quantize(tick, rounding=ROUND_DOWN)
    qty_str = f"{qty_d:.{qty_precision}f}"

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

        try:
            await insert_binance_order(
                position_uid=position_uid,
                strategy_id=strategy_id,
                symbol=symbol,
                binance_order_id=resp["orderId"],
                side=side,
                type_="STOP_MARKET",
                status="NEW",
                purpose="sl",
                level=None,
                price=None,
                quantity=float(qty_d),
                reduce_only=True,
                close_position=False,
                time_in_force=None,
                raw_data=resp
            )
        except Exception as db_exc:
            log.exception(f"⚠️ Ошибка записи SL ордера в БД: {db_exc}")

    except Exception as e:
        log.warning(f"⚠️ Ошибка размещения SL: {e}")