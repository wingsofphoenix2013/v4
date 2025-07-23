# king_marker_worker.py

import logging
from datetime import datetime

import asyncpg
import pandas as pd

import infra

log = logging.getLogger("KING_MARKER")


async def run_king_marker_worker():
    log.info("[KING_MARKER] 🔁 Маркировка позиций с привязкой к правлению Короля")

    # 🔹 Загружаем интервалы правления Королей
    query_kings = """
        SELECT
            strategy_id,
            ts_recorded,
            LEAD(ts_recorded) OVER (ORDER BY ts_recorded) AS next_ts
        FROM strategies_active_v4
        ORDER BY ts_recorded
    """

    async with infra.pg_pool.acquire() as conn:
        kings = await conn.fetch(query_kings)

    if not kings:
        log.warning("[KING_MARKER] ❗ В таблице strategies_active_v4 нет записей")
        return

    df_kings = pd.DataFrame(kings, columns=["strategy_id", "ts_recorded", "next_ts"])
    df_kings["ts_recorded"] = pd.to_datetime(df_kings["ts_recorded"])
    df_kings["next_ts"] = pd.to_datetime(df_kings["next_ts"])
    min_ts = df_kings["ts_recorded"].min()

    # 🔹 Загружаем непроверенные позиции
    query_positions = """
        SELECT id, strategy_id, created_at
        FROM positions_v4
        WHERE king_checked = false
          AND created_at >= $1
        ORDER BY created_at
        LIMIT 1000
    """

    async with infra.pg_pool.acquire() as conn:
        positions = await conn.fetch(query_positions, min_ts)

    if not positions:
        log.info("[KING_MARKER] ✅ Нет новых позиций для маркировки")
        return

    df_pos = pd.DataFrame(positions, columns=["id", "strategy_id", "created_at"])
    df_pos["created_at"] = pd.to_datetime(df_pos["created_at"])

    updates = []
    skip_ids = []

    for pos in df_pos.itertuples():
        pos_time = pos.created_at
        strategy_id = pos.strategy_id

        matched_king = df_kings[
            (df_kings["strategy_id"] == strategy_id) &
            (df_kings["ts_recorded"] <= pos_time) &
            ((df_kings["next_ts"].isna()) | (pos_time < df_kings["next_ts"]))
        ]

        if not matched_king.empty:
            updates.append((True, strategy_id, pos.id))  # opened_by_king, king_id, position_id
        else:
            skip_ids.append(pos.id)

    async with infra.pg_pool.acquire() as conn:
        # 🔹 Обновляем подходящие позиции
        for opened_by_king, king_id, pos_id in updates:
            await conn.execute(
                """
                UPDATE positions_v4
                SET
                    opened_by_king = $1,
                    king_strategy_id = $2,
                    king_checked = TRUE
                WHERE id = $3
                """,
                opened_by_king, king_id, pos_id
            )

        # 🔹 Отмечаем оставшиеся как проверенные, но без Короля
        for pos_id in skip_ids:
            await conn.execute(
                """
                UPDATE positions_v4
                SET king_checked = TRUE
                WHERE id = $1
                """,
                pos_id
            )

    log.info(
        f"[KING_MARKER] ✅ Обновлено {len(updates)} позиций как открытые при Короле, "
        f"{len(skip_ids)} отмечены как проверенные без совпадения"
    )