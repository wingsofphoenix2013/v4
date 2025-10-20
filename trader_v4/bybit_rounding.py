# bybit_rounding.py — утилиты округления цены/объёма по метаданным тикера (ticksize / precision / min_qty)

# 🔸 Импорты
import logging
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from trader_config import config

# 🔸 Логгер
log = logging.getLogger("BYBIT_ROUNDING")


# 🔸 Публичные функции округления

def round_price(symbol: str, price: Decimal | str | float | int, *, mode: str = "down") -> Decimal:
    """
    Округляет цену по правилам тикера:
    1) приоритет: ticksize (кратно шагу);
    2) иначе precision_price (кол-во знаков).
    mode: 'down' (по умолчанию) | 'up' | 'nearest'
    """
    px = _to_decimal(price)
    meta = _get_meta(symbol)
    if meta is None:
        log.info("BYBIT_ROUNDING: meta not found for %s — return as is", symbol)
        return px

    ticksize = _to_decimal(meta.get("ticksize"))
    prec_p = meta.get("precision_price")

    if ticksize and ticksize > 0:
        return _round_to_step(px, ticksize, mode=mode)
    if prec_p is not None:
        return _round_to_precision(px, int(prec_p), mode=mode)

    return px


def round_qty(
    symbol: str,
    qty: Decimal | str | float | int,
    *,
    mode: str = "down",
    enforce_min: bool = True
) -> Optional[Decimal]:
    """
    Округляет количество по precision_qty; при enforce_min=True доводит до min_qty (если >0).
    Возвращает None, если после округления qty <= 0 (или < min_qty при enforce_min=False).
    mode: 'down' | 'up' | 'nearest'
    """
    q = _to_decimal(qty)
    if q is None:
        return None

    meta = _get_meta(symbol)
    if meta is None:
        log.info("BYBIT_ROUNDING: meta not found for %s — cannot round qty", symbol)
        return None

    prec_q = meta.get("precision_qty")
    min_qty = _to_decimal(meta.get("min_qty")) or Decimal("0")

    # округление по количеству знаков
    if prec_q is not None:
        q = _round_to_precision(q, int(prec_q), mode=mode)

    if q <= 0:
        return None

    # доведение до минимума при enforce_min
    if enforce_min and min_qty > 0 and q < min_qty:
        q = min_qty

    # если min_qty не enforced и результат меньше минимума — сигнализируем None
    if not enforce_min and min_qty > 0 and q < min_qty:
        return None

    return q


def min_qty_ok(symbol: str, qty: Decimal | str | float | int) -> bool:
    """Проверяет, что qty ≥ min_qty для символа (если min_qty задан)."""
    q = _to_decimal(qty)
    meta = _get_meta(symbol)
    if q is None or meta is None:
        return False
    min_q = _to_decimal(meta.get("min_qty")) or Decimal("0")
    return q >= min_q if min_q > 0 else q > 0


def normalize_price_qty(
    symbol: str,
    price: Decimal | str | float | int,
    qty: Decimal | str | float | int,
    *,
    price_mode: str = "down",
    qty_mode: str = "down",
    enforce_min_qty: bool = True
) -> Tuple[Decimal, Optional[Decimal]]:
    """
    Комбо-утилита: округляет (price, qty) по метаданным тикера.
    Возвращает (price_dec, qty_dec_or_None).
    """
    return round_price(symbol, price, mode=price_mode), round_qty(symbol, qty, mode=qty_mode, enforce_min=enforce_min_qty)


# 🔸 Внутренние утилиты

def _get_meta(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Ждём в config.tickers[symbol] как минимум:
      - ticksize (numeric | str | Decimal) — шаг цены;
      - precision_price (int) — знаки цены;
      - precision_qty (int) — знаки количества;
      - min_qty (numeric) — минимальное количество.
    """
    row = config.tickers.get(symbol)
    return dict(row) if row else None


def _to_decimal(x: Any) -> Optional[Decimal]:
    try:
        if x is None:
            return None
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _round_to_step(value: Decimal, step: Decimal, *, mode: str = "down") -> Decimal:
    """
    Округляет value к кратному step:
      - 'down': в меньшую сторону,
      - 'up': в большую,
      - 'nearest': к ближайшему (0.5 вверх).
    """
    if step <= 0:
        return value

    # приводим к количеству шагов
    steps = value / step
    if mode == "up":
        steps_q = steps.to_integral_value(rounding=ROUND_UP)
    elif mode == "nearest":
        steps_q = steps.to_integral_value(rounding=ROUND_HALF_UP)
    else:  # down
        steps_q = steps.to_integral_value(rounding=ROUND_DOWN)

    return steps_q * step


def _round_to_precision(value: Decimal, digits: int, *, mode: str = "down") -> Decimal:
    """
    Округляет value до digits знаков после запятой:
      - 'down' → ROUND_DOWN, 'up' → ROUND_UP, 'nearest' → ROUND_HALF_UP.
    """
    if digits < 0:
        return value
    quant = Decimal(10) ** -digits
    rounding = ROUND_DOWN if mode == "down" else ROUND_UP if mode == "up" else ROUND_HALF_UP
    return value.quantize(quant, rounding=rounding)