# 🔸 Загрузчик входных данных: активные тесты/параметры/стратегии, план запусков, итератор закрытых позиций

import logging
from typing import List, Tuple, AsyncIterator
from datetime import datetime
import laboratory_v4_infra as infra

log = logging.getLogger("LAB_LOADER")


# 🔸 Загрузка активных тестов (labs)
async def load_active_labs() -> List[dict]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id AS lab_id, name, min_trade_type, min_trade_value, min_winrate
            FROM laboratory_instances_v4
            WHERE active = true
            ORDER BY id ASC
            """
        )
    return [dict(r) for r in rows]


# 🔸 Загрузка параметров теста (по lab_id)
async def load_lab_parameters(lab_id: int) -> List[dict]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, lab_id, test_name, test_type, test_tf, param_spec, ema_len
            FROM laboratory_parameters_v4
            WHERE lab_id = $1
            ORDER BY test_name ASC, test_type ASC, test_tf ASC NULLS LAST
            """,
            lab_id,
        )
    return [dict(r) for r in rows]


# 🔸 Загрузка активных стратегий (enabled + market_watcher, не архивные)
async def load_active_strategies() -> List[int]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id
            FROM strategies_v4
            WHERE enabled = true
              AND (archived IS NOT TRUE)
              AND market_watcher = true
            ORDER BY id ASC
            """
        )
    return [int(r["id"]) for r in rows]


# 🔸 Формирование плана запусков (lab_id × strategy_id)
async def build_run_plan() -> List[Tuple[int, int]]:
    labs = await load_active_labs()
    strategies = await load_active_strategies()

    plan: List[Tuple[int, int]] = []
    for lab in labs:
        for sid in strategies:
            plan.append((int(lab["lab_id"]), sid))

    log.info("План запусков: labs=%d, strategies=%d, tasks=%d", len(labs), len(strategies), len(plan))
    return plan


# 🔸 Итератор закрытых позиций (до cutoff, по batch_size)
async def iter_closed_positions_uids(strategy_id: int, cutoff_ts: datetime, batch_size: int) -> AsyncIterator[str]:
    last_id = 0

    # внутренний запрос пачки по id
    async def fetch_batch(after_id: int):
        return await infra.pg_pool.fetch(
            """
            SELECT id, position_uid
            FROM positions_v4
            WHERE strategy_id = $1
              AND status = 'closed'
              AND closed_at <= $2
              AND id > $3
            ORDER BY id ASC
            LIMIT $4
            """,
            strategy_id, cutoff_ts, after_id, batch_size,
        )

    while True:
        rows = await fetch_batch(last_id)
        if not rows:
            break

        for r in rows:
            yield r["position_uid"]

        last_id = int(rows[-1]["id"])