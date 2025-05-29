# position_state_loader.py

import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from decimal import Decimal
from datetime import datetime

from infra import infra  # 🔸 доступ к infra.pg_pool

# 🔸 Логгер для загрузчика состояния позиций
log = logging.getLogger("POSITION_LOADER")

# 🔸 Структура цели позиции (TP/SL)
@dataclass
class Target:
    id: int
    type: str  # 'tp' or 'sl'
    level: int
    price: Decimal
    quantity: Decimal
    hit: bool
    hit_at: Optional[datetime]
    canceled: bool

# 🔸 Структура позиции с вложенными целями
@dataclass
class PositionState:
    id: int
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
    tp_targets: List[Target]
    sl_targets: List[Target]

# 🔸 Глобальные in-memory хранилища состояний
position_registry: Dict[int, PositionState] = {}
position_index: Dict[Tuple[int, str], int] = {}

# 🔸 Загрузка активных позиций и целей из базы данных
async def load_position_state():
    pool = infra.pg_pool
    if pool is None:
        raise RuntimeError("❌ PostgreSQL pool не инициализирован")

    async with pool.acquire() as conn:
        log.info("📥 Загрузка активных позиций из PG")

        positions = await conn.fetch(
            """
            SELECT * FROM positions_v4
            WHERE status IN ('open', 'partial')
            """
        )
        position_ids = [r['id'] for r in positions]

        if not position_ids:
            log.info("ℹ️ Нет активных позиций для восстановления")
            return

        log.info(f"🔍 Найдено позиций: {len(position_ids)}")

        targets = await conn.fetch(
            """
            SELECT * FROM position_targets_v4
            WHERE position_id = ANY($1)
            """,
            position_ids
        )

    # 🔸 Группировка целей по позиции
    target_map: Dict[int, List[Target]] = {}
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
        )
        target_map.setdefault(row['position_id'], []).append(target)

    # 🔸 Построение объектов PositionState
    for row in positions:
        pos_id = row['id']
        all_targets = target_map.get(pos_id, [])
        tp_targets = [t for t in all_targets if t.type == 'tp']
        sl_targets = [t for t in all_targets if t.type == 'sl']

        position = PositionState(
            id=pos_id,
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
            tp_targets=tp_targets,
            sl_targets=sl_targets,
        )

        position_registry[pos_id] = position
        position_index[(position.strategy_id, position.symbol)] = pos_id

    log.info(f"✅ Загружено и проиндексировано {len(position_registry)} позиций")