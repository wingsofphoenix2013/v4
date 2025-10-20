# bybit_intents.py — схемы и билдеры intent’ов для стрима bybit_order_intents (entry + TP/SL, amend SL, close, cancel)

# 🔸 Импорты
import json
import logging
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Literal

# 🔸 Логгер
log = logging.getLogger("BYBIT_INTENTS")

# 🔸 Константы стрима/экшены
STREAM_NAME = "bybit_order_intents"
ACTION_CREATE_ENTRY_TPSL: Literal["create_entry_tpsl"] = "create_entry_tpsl"
ACTION_AMEND_SL: Literal["amend_sl"] = "amend_sl"
ACTION_CLOSE_MARKET: Literal["close_market"] = "close_market"
ACTION_CANCEL_GENERIC: Literal["cancel_generic"] = "cancel_generic"


# 🔸 Базовые типы (канонический вид TP/SL целей)
@dataclass
class TPTarget:
    level: Optional[int]   # None → авто-нумерация/не используется
    price: Decimal
    quantity: Decimal

    def to_wire(self) -> Dict[str, Any]:
        # сериализация для стрима (worker читает JSON-строки и парсит как list[dict])
        return {
            "level": self.level if self.level is not None else None,
            "price": _dec_to_str(self.price),
            "quantity": _dec_to_str(self.quantity),
        }

@dataclass
class SLTarget:
    price: Decimal

    def to_wire(self) -> Dict[str, Any]:
        return {"price": _dec_to_str(self.price)}


# 🔸 Intent: вход + TP + SL (единый)
@dataclass
class CreateEntryTPSLIntent:
    action: Literal["create_entry_tpsl"]
    position_uid: str
    strategy_id: int
    symbol: str
    direction: Literal["long", "short"]
    qty_internal: Decimal
    tp_targets: List[TPTarget]
    sl_targets: List[SLTarget]
    leverage: Optional[Decimal] = None
    ensure_leverage: bool = False

    def to_stream_payload(self) -> Dict[str, Any]:
        # worker ждёт JSON-строки в полях tp_targets/sl_targets
        return {
            "action": self.action,
            "position_uid": self.position_uid,
            "strategy_id": str(self.strategy_id),
            "symbol": self.symbol,
            "direction": self.direction,
            "qty_internal": _dec_to_str(self.qty_internal),
            "tp_targets": json.dumps([t.to_wire() for t in self.tp_targets], ensure_ascii=False, separators=(",", ":")),
            "sl_targets": json.dumps([s.to_wire() for s in self.sl_targets], ensure_ascii=False, separators=(",", ":")),
            "leverage": _dec_to_str(self.leverage) if self.leverage is not None else "",
            "ensure_leverage": "true" if self.ensure_leverage else "false",
        }


# 🔸 Intent: изменить SL (position-level trading stop)
@dataclass
class AmendSLIntent:
    action: Literal["amend_sl"]
    position_uid: str
    symbol: str
    new_sl_price: Decimal  # абсолютная цена SL

    def to_stream_payload(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "position_uid": self.position_uid,
            "symbol": self.symbol,
            "new_sl_price": _dec_to_str(self.new_sl_price),
        }


# 🔸 Intent: закрыть остаток reduceOnly Market
@dataclass
class CloseMarketIntent:
    action: Literal["close_market"]
    position_uid: str
    symbol: str
    direction: Literal["long", "short"]  # для вычисления обратной стороны (worker)
    qty_internal: Decimal                # внутренний остаток (будет масштабирован worker’ом)

    def to_stream_payload(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "position_uid": self.position_uid,
            "symbol": self.symbol,
            "direction": self.direction,
            "qty_internal": _dec_to_str(self.qty_internal),
        }


# 🔸 Intent: отмена по order_link_id
@dataclass
class CancelGenericIntent:
    action: Literal["cancel_generic"]
    symbol: str
    order_link_id: str

    def to_stream_payload(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "symbol": self.symbol,
            "order_link_id": self.order_link_id,
        }


# 🔸 Билдеры intent’ов (удобные фабрики)

def build_create_entry_tpsl(
    *,
    position_uid: str,
    strategy_id: int,
    symbol: str,
    direction: str,
    qty_internal: Decimal | str | float | int,
    tp_levels: List[Dict[str, Any]] | List[TPTarget],
    sl_levels: List[Dict[str, Any]] | List[SLTarget],
    leverage: Optional[Decimal | str | float | int] = None,
    ensure_leverage: bool = False,
) -> CreateEntryTPSLIntent:
    d = _normalize_direction(direction)
    q = _to_decimal(qty_internal, field="qty_internal")
    tp = [_ensure_tp_obj(x) for x in (tp_levels or [])]
    sl = [_ensure_sl_obj(x) for x in (sl_levels or [])]
    lev = _to_decimal(leverage) if leverage is not None else None
    return CreateEntryTPSLIntent(
        action=ACTION_CREATE_ENTRY_TPSL,
        position_uid=position_uid,
        strategy_id=int(strategy_id),
        symbol=symbol,
        direction=d,
        qty_internal=q,
        tp_targets=tp,
        sl_targets=sl,
        leverage=lev,
        ensure_leverage=bool(ensure_leverage),
    )


def build_amend_sl(*, position_uid: str, symbol: str, new_sl_price: Decimal | str | float | int) -> AmendSLIntent:
    return AmendSLIntent(
        action=ACTION_AMEND_SL,
        position_uid=position_uid,
        symbol=symbol,
        new_sl_price=_to_decimal(new_sl_price, field="new_sl_price"),
    )


def build_close_market(
    *,
    position_uid: str,
    symbol: str,
    direction: str,
    qty_internal: Decimal | str | float | int
) -> CloseMarketIntent:
    return CloseMarketIntent(
        action=ACTION_CLOSE_MARKET,
        position_uid=position_uid,
        symbol=symbol,
        direction=_normalize_direction(direction),
        qty_internal=_to_decimal(qty_internal, field="qty_internal"),
    )


def build_cancel_generic(*, symbol: str, order_link_id: str) -> CancelGenericIntent:
    return CancelGenericIntent(
        action=ACTION_CANCEL_GENERIC,
        symbol=symbol,
        order_link_id=order_link_id,
    )


# 🔸 Утилиты

def _to_decimal(x: Any, *, field: str | None = None) -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        fname = f" in field '{field}'" if field else ""
        raise ValueError(f"Invalid decimal value{fname}: {x!r}")

def _dec_to_str(x: Decimal | str | float | int | None) -> str:
    if x is None:
        return ""
    if isinstance(x, Decimal):
        return f"{x:f}"
    try:
        return f"{Decimal(str(x)):f}"
    except Exception:
        return str(x)

def _normalize_direction(d: str) -> Literal["long", "short"]:
    v = (d or "").strip().lower()
    if v not in ("long", "short"):
        raise ValueError(f"direction must be 'long' or 'short', got: {d!r}")
    return v  # type: ignore[return-value]

def _ensure_tp_obj(x: Any) -> TPTarget:
    if isinstance(x, TPTarget):
        return x
    if isinstance(x, dict):
        lvl = x.get("level")
        pr = _to_decimal(x.get("price"), field="tp.price")
        qt = _to_decimal(x.get("quantity"), field="tp.quantity")
        return TPTarget(level=int(lvl) if lvl is not None else None, price=pr, quantity=qt)
    raise ValueError(f"Invalid TP target: {x!r}")

def _ensure_sl_obj(x: Any) -> SLTarget:
    if isinstance(x, SLTarget):
        return x
    if isinstance(x, dict):
        pr = _to_decimal(x.get("price"), field="sl.price")
        return SLTarget(price=pr)
    raise ValueError(f"Invalid SL target: {x!r}")