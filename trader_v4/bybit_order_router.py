# bybit_order_router.py — маршрутизатор: сбор и отправка заявки (entry + TP как reduceOnly + SL как trading-stop)

# 🔸 Импорты
import os
import logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from bybit_http_client import BybitHttpClient, BybitAPIError
from trader_config import config

# 🔸 Логгер
log = logging.getLogger("BYBIT_ROUTER")

# 🔸 Параметры пилота/масштабирования
BYBIT_SIZE_PCT = os.getenv("BYBIT_SIZE_PCT", "10")  # проценты от внутреннего объёма
CATEGORY = "linear"                                 # деривативы USDT-perp
TIF_TP = "GTC"                                      # TP — Good-Till-Cancel
POSITION_MODE = "one-way"                           # фиксируем соглашение
MARGIN_MODE = "isolated"                            # фиксируем соглашение


# 🔸 Публичная точка: создать внешний «вход» + выставить TP и SL (по политике)
async def route_entry_with_tpsl(
    *,
    client: BybitHttpClient,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    direction: str,                   # 'long' | 'short'
    qty_internal: Decimal,            # внутренний объём (в контрактах/единицах инструмента)
    tp_targets: List[Dict[str, Any]], # [{'level':int, 'price':Decimal|str, 'quantity':Decimal|str}, ...]
    sl_targets: List[Dict[str, Any]], # [{'price':Decimal|str, ...}] (берём первый актуальный)
    leverage: Optional[Decimal] = None,
    ensure_leverage: bool = False,    # при необходимости выставить плечо на символе
) -> Dict[str, Any]:
    # коэффициент масштабирования пилота
    R = _ratio_from_env(BYBIT_SIZE_PCT)

    # детали тикера для округлений
    tick = _get_ticker_meta(symbol)
    if not tick:
        raise ValueError(f"Ticker meta not found in config.tickers for {symbol}")

    # стороны для входа и закрытия
    side_entry, side_close = _sides_from_direction(direction)

    # рассчитать внешний qty (масштаб + округление; обеспечить min_qty)
    qty_ex = _scale_and_round_qty(symbol, qty_internal, R, tick)

    # защищённая проверка: если после округления слишком мало — не отправляем
    if qty_ex is None:
        raise ValueError(f"Scaled qty below min step for {symbol}: internal={qty_internal} R={R}")

    # (опцион.) установить плечо на символе (one-way/isolated ожидаются на аккаунте заранее)
    if ensure_leverage and leverage is not None:
        try:
            # Bybit требует строковые значения плеча; ставим одинаковые buy/sell
            await client.set_leverage(category=CATEGORY, symbol=symbol, buy_leverage=str(leverage), sell_leverage=str(leverage))
            log.info("BYBIT_ROUTER: leverage set %s %s", symbol, leverage)
        except BybitAPIError as e:
            log.info("BYBIT_ROUTER: set_leverage skipped/fail (%s): %s", symbol, e)

    # 1) ENTRY (Market): orderLinkId = position_uid
    entry_res = await _safe_create_order(
        client,
        symbol=symbol,
        side=side_entry,
        order_type="Market",
        qty=_qty_to_str(qty_ex),
        order_link_id=position_uid,
    )
    entry_order_id = _extract_order_id(entry_res)

    # 2) TP как отдельные reduceOnly Limit (GTC), масштабировать qty тем же коэффициентом
    tp_results: List[Dict[str, Any]] = []
    for idx, tp in enumerate(sorted(tp_targets or [], key=lambda t: (t.get("level") is None, t.get("level") or 10**9))):
        lvl = tp.get("level") if tp.get("level") is not None else (idx + 1)
        raw_tp_qty = _to_decimal(tp.get("quantity"))
        raw_tp_price = _to_decimal(tp.get("price"))

        if raw_tp_qty is None or raw_tp_price is None:
            log.info("BYBIT_ROUTER: skip TP level=%s (qty/price missing) for %s", lvl, symbol)
            continue

        tp_qty_ex = _scale_and_round_qty(symbol, raw_tp_qty, R, tick)
        tp_price_ex = _round_price(symbol, raw_tp_price, tick)

        if tp_qty_ex is None:
            log.info("BYBIT_ROUTER: skip TP level=%s — qty below min step after scaling (sym=%s)", lvl, symbol)
            continue

        tp_link_id = f"{position_uid}:tp{lvl}"
        tp_res = await _safe_create_order(
            client,
            symbol=symbol,
            side=side_close,             # закрывающая сторона
            order_type="Limit",
            qty=_qty_to_str(tp_qty_ex),
            price=_price_to_str(tp_price_ex),
            time_in_force=TIF_TP,
            reduce_only=True,
            order_link_id=tp_link_id,
        )
        tp_results.append({
            "level": lvl,
            "order_link_id": tp_link_id,
            "result": tp_res,
            "order_id": _extract_order_id(tp_res),
        })

    # 3) SL как position-level trading stop (берём первый живой уровень из sl_targets)
    sl_result: Optional[Dict[str, Any]] = None
    sl_price = _pick_sl_price(sl_targets)
    if sl_price is not None:
        sl_price_ex = _round_price(symbol, sl_price, tick)
        try:
            sl_result = await client.set_trading_stop(category=CATEGORY, symbol=symbol, stop_loss=_price_to_str(sl_price_ex))
        except BybitAPIError as e:
            log.info("BYBIT_ROUTER: set_trading_stop fail (%s): %s", symbol, e)
            sl_result = {"error": str(e)}

    # сводка
    return {
        "entry": {"order_link_id": position_uid, "order_id": entry_order_id, "result": entry_res},
        "tp": tp_results,
        "sl": sl_result,
        "meta": {
            "symbol": symbol,
            "direction": direction,
            "qty_internal": str(qty_internal),
            "qty_exchange": _qty_to_str(qty_ex),
            "scale_pct": BYBIT_SIZE_PCT,
            "position_mode": POSITION_MODE,
            "margin_mode": MARGIN_MODE,
            "category": CATEGORY,
        },
    }


# 🔸 Защищённое создание ордера с идемпотентностью по orderLinkId
async def _safe_create_order(
    client: BybitHttpClient,
    *,
    symbol: str,
    side: str,
    order_type: str,
    qty: str,
    price: Optional[str] = None,
    time_in_force: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    order_link_id: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        return await client.create_order(
            category=CATEGORY,
            symbol=symbol,
            side=side,
            order_type=order_type,
            qty=qty,
            price=price,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            order_link_id=order_link_id,
        )
    except BybitAPIError as e:
        # идемпотентность: если линк уже использован, пытаемся найти существующий ордер
        if order_link_id:
            try:
                found = await _find_order_by_link_id(client, symbol=symbol, order_link_id=order_link_id)
                if found:
                    log.info("BYBIT_ROUTER: reuse existing order for linkId=%s", order_link_id)
                    return {"retCode": 0, "retMsg": "reuse-existing", "result": found}
            except Exception as ee:
                log.info("BYBIT_ROUTER: lookup existing failed for linkId=%s: %s", order_link_id, ee)
        # иных случаев — пробрасываем
        raise


# 🔸 Поиск уже существующего ордера по orderLinkId (open/history)
async def _find_order_by_link_id(client: BybitHttpClient, *, symbol: str, order_link_id: str) -> Optional[Dict[str, Any]]:
    # сначала среди открытых
    lst = await client.get_orders_list(category=CATEGORY, symbol=symbol, open_only=True, limit=50)
    items = ((lst.get("result") or {}).get("list") or [])
    for it in items:
        if str(it.get("orderLinkId") or "") == order_link_id:
            return it
    # затем в истории (одна страница)
    lst = await client.get_orders_list(category=CATEGORY, symbol=symbol, open_only=False, limit=50)
    items = ((lst.get("result") or {}).get("list") or [])
    for it in items:
        if str(it.get("orderLinkId") or "") == order_link_id:
            return it
    return None


# 🔸 Вспомогательные функции округления/преобразований

def _ratio_from_env(txt: str) -> Decimal:
    try:
        v = Decimal(str(txt))
        if v <= 0:
            return Decimal("0")
        if v > 100:
            v = Decimal("100")
        return (v / Decimal("100"))
    except Exception:
        return Decimal("0.10")  # дефолт 10%

def _get_ticker_meta(symbol: str) -> Optional[Dict[str, Any]]:
    # ожидаем в config.tickers[symbol] поля: ticksize, min_qty, precision_price, precision_qty
    row = config.tickers.get(symbol)
    return dict(row) if row else None

def _to_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))
    except Exception:
        return None

def _sides_from_direction(direction: str) -> Tuple[str, str]:
    d = (direction or "").lower()
    if d == "long":
        return "Buy", "Sell"
    return "Sell", "Buy"

def _scale_and_round_qty(symbol: str, qty_internal: Decimal, R: Decimal, tick: Dict[str, Any]) -> Optional[Decimal]:
    try:
        q = (qty_internal * R)
        prec_q = tick.get("precision_qty")
        min_qty = _to_decimal(tick.get("min_qty")) or Decimal("0")

        if prec_q is not None:
            q = q.quantize(Decimal(10) ** -int(prec_q), rounding=ROUND_DOWN)

        # если есть min_qty — доводим до минимума, если q > 0
        if q <= 0:
            return None
        if min_qty > 0 and q < min_qty:
            q = min_qty

        return q
    except InvalidOperation:
        return None

def _round_price(symbol: str, price: Decimal, tick: Dict[str, Any]) -> Decimal:
    # приоритет — ticksize; если нет, используем precision_price
    ts = _to_decimal(tick.get("ticksize"))
    if ts and ts > 0:
        # Quantize к кратному ticksize (ROUND_DOWN — консервативно)
        q = (price / ts).quantize(Decimal("1"), rounding=ROUND_DOWN) * ts
        return q
    prec_p = tick.get("precision_price")
    if prec_p is not None:
        return price.quantize(Decimal(10) ** -int(prec_p), rounding=ROUND_DOWN)
    return price

def _qty_to_str(q: Decimal) -> str:
    # Удаляем лишние нули, как в TG-форматтерах
    s = f"{q:f}"
    return s

def _price_to_str(p: Decimal) -> str:
    s = f"{p:f}"
    return s

def _extract_order_id(payload: Dict[str, Any]) -> Optional[str]:
    # ожидаем v5-формат: {'result': {'orderId': '...'}} либо reuse-existing: payload['result'] — это уже item
    res = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(res, dict):
        return None
    if "orderId" in res:
        return str(res.get("orderId"))
    # если reuse-existing — формат элемента из order/list
    if "orderId" in res.keys() or "orderId" in (res.get("list", [{}])[0] if isinstance(res.get("list"), list) and res.get("list") else {}):
        try:
            return str(res["orderId"])
        except Exception:
            pass
    return None

def _pick_sl_price(sl_targets: List[Dict[str, Any]]) -> Optional[Decimal]:
    for s in sl_targets or []:
        pr = _to_decimal(s.get("price"))
        if pr is not None:
            return pr
    return None