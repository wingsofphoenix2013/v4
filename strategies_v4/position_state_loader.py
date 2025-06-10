# position_state_loader.py

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional
from datetime import datetime

from infra import infra

# 🔸 Логгер состояния позиций
log = logging.getLogger("POSITION_STATE")

# 🔸 Структура цели TP или SL
@dataclass
class Target:
    type: str
    level: int
    price: Optional[Decimal]
    quantity: Decimal
    hit: bool
    hit_at: Optional[datetime]
    id: Optional[int] = None
    canceled: bool = False

# 🔸 Структура позиции
@dataclass
class PositionState:
    uid: str
    strategy_id: int
    symbol: str
    direction: str
    entry_price: Decimal
    quantity: Decimal
    quantity_left: Decimal
    status: str
    created_at: datetime
    exit_price: Optional[Decimal]
    closed_at: Optional[datetime]
    close_reason: Optional[str]
    pnl: Optional[Decimal]
    planned_risk: Optional[Decimal]
    route: str
    tp_targets: list[Target]
    sl_targets: list[Target]
    log_uid: Optional[str]
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

# 🔸 Глобальный реестр позиций
position_registry: dict[tuple[int, str], PositionState] = {}

# 🔸 Загрузка всех открытых позиций и их целей
async def load_position_state():
    log.info("📥 Загрузка открытых позиций из БД")

    positions = await infra.pg_pool.fetch(
        "SELECT * FROM positions_v4 WHERE status IN ('open', 'partial')"
    )

    if not positions:
        log.info("ℹ️ Открытых позиций не найдено")
        return

    uids = [p["position_uid"] for p in positions]
    targets_raw = await infra.pg_pool.fetch(
        "SELECT * FROM position_targets_v4 WHERE position_uid = ANY($1)", uids
    )

    # Группировка целей по позиции
    targets_by_uid = {}
    for t in targets_raw:
        target = Target(
            type=t["type"],
            level=t["level"],
            price=t["price"],
            quantity=t["quantity"],
            hit=t["hit"] or False,
            hit_at=t["hit_at"],
            id=t["id"],
            canceled=t["canceled"] or False
        )
        targets_by_uid.setdefault(t["position_uid"], []).append(target)

    loaded, skipped = 0, 0

    for p in positions:
        uid = p["position_uid"]
        strategy_id = p["strategy_id"]
        symbol = p["symbol"]

        t_all = targets_by_uid.get(uid, [])

        tp = [t for t in t_all if t.type == "tp"]
        sl = [t for t in t_all if t.type == "sl"]

        if not tp and not sl:
            log.error(f"❌ Пропущена позиция без целей: {uid} ({symbol})")
            skipped += 1
            continue

        state = PositionState(
            uid=uid,
            strategy_id=strategy_id,
            symbol=symbol,
            direction=p["direction"],
            entry_price=p["entry_price"],
            quantity=p["quantity"],
            quantity_left=p["quantity_left"],
            status=p["status"],
            created_at=p["created_at"],
            exit_price=p["exit_price"],
            closed_at=p["closed_at"],
            close_reason=p["close_reason"],
            pnl=p["pnl"],
            planned_risk=p["planned_risk"],
            route=p["route"],
            tp_targets=tp,
            sl_targets=sl,
            log_uid=p["log_uid"]
        )

        position_registry[(strategy_id, symbol)] = state
        loaded += 1

    log.info(f"✅ Загружено позиций: {loaded}, пропущено (без целей): {skipped}")