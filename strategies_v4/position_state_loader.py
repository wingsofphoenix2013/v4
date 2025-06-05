# position_state_loader.py

import logging
import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
from decimal import Decimal
from datetime import datetime

from infra import infra  # 🔸 доступ к infra.pg_pool

# 🔸 Логгер для загрузчика состояния позиций
log = logging.getLogger("POSITION_LOADER")

# 🔸 Структура цели позиции (TP/SL)
@dataclass
class Target:
    type: str  # 'tp' or 'sl'
    level: int
    price: Decimal
    quantity: Decimal
    hit: bool
    hit_at: Optional[datetime]
    id: Optional[int] = None
    canceled: bool = False
    source: str = "price"

# 🔸 Структура позиции с вложенными целями
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
    pnl: Decimal
    planned_risk: Decimal
    route: str
    tp_targets: List[Target]
    sl_targets: List[Target]
    log_id: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

# 🔸 Глобальные in-memory хранилища состояний
position_registry: Dict[Tuple[int, str], PositionState] = {}

# 🔸 Загрузка активных позиций и целей из базы данных
async def load_position_state():
    pool = infra.pg_pool
    if pool is None:
        raise RuntimeError("❌ PostgreSQL pool не инициализирован")

    async with pool.acquire() as conn:
        log.debug("📥 Загрузка активных позиций из PG")

        positions = await conn.fetch(
            """
            SELECT * FROM positions_v4
            WHERE status IN ('open', 'partial')
            """
        )
        position_uids = [r['position_uid'] for r in positions]

        if not position_uids:
            log.debug("ℹ️ Нет активных позиций для восстановления")
            return

        log.debug(f"🔍 Найдено позиций: {len(position_uids)}")

        targets = await conn.fetch(
            """
            SELECT id, type, level, price, quantity, hit, hit_at, canceled, source, position_uid
            FROM position_targets_v4
            WHERE position_uid = ANY($1)
            """,
            position_uids
        )

    # 🔸 Группировка целей по позиции
    target_map: Dict[str, List[Target]] = {}
    for row in targets:
        target = Target(
            id=row['id'],
            type=row['type'],
            level=row['level'],
            price=row['price'],
            quantity=row['quantity'],
            hit=row['hit'],
            hit_at=row['hit_at'],
            canceled=row['canceled'],
            source=row["source"]
        )
        target_map.setdefault(row['position_uid'], []).append(target)

    # 🔸 Построение объектов PositionState
    for row in positions:
        uid = row['position_uid']
        all_targets = target_map.get(uid, [])
        tp_targets = [t for t in all_targets if t.type == 'tp']
        sl_targets = [t for t in all_targets if t.type == 'sl']

        position = PositionState(
            uid=uid,
            strategy_id=row['strategy_id'],
            symbol=row['symbol'],
            direction=row['direction'],
            entry_price=row['entry_price'],
            quantity=row['quantity'],
            quantity_left=row['quantity_left'],
            status=row['status'],
            created_at=row['created_at'],
            exit_price=row['exit_price'],
            closed_at=row['closed_at'],
            close_reason=row['close_reason'],
            pnl=row['pnl'],
            planned_risk=row['planned_risk'],
            route=row['route'],
            tp_targets=tp_targets,
            sl_targets=sl_targets,
            log_id=row['log_id']
        )

        # 🔸 Индексация по (strategy_id, symbol)
        position_registry[(position.strategy_id, position.symbol)] = position

    log.info(f"✅ Загружено и проиндексировано {len(position_registry)} позиций")