# king_marker_worker.py

import logging
from datetime import datetime

import asyncpg
import pandas as pd

import infra

log = logging.getLogger("KING_MARKER")


# 🔸 Воркер маркировки позиций, открытых при Короле (dry-run)
async def run_king_marker_worker():
    log.info("[KING_MARKER] 🔁 Запуск dry-run маркировки позиций")

    # 🔹 Получаем минимальное время ts_recorded
    query_min_ts = "SELECT MIN(ts_recorded) FROM strategies_active_v4"

    async with infra.pg_pool.acquire() as conn:
        min_ts = await conn.fetchval(query_min_ts)

    if min_ts is None:
        log.warning("[KING_MARKER] ❗ Нет истории Королей — обработка пропущена")
        return

    # 🔹 Загружаем активных Королей с next_ts
    query_kings = """
        SELECT
            strategy_id,
            ts_recorded,
            LEAD(ts_recorded) OVER (ORDER BY ts_recorded) AS next_ts
        FROM strategies_active_v4
        ORDER BY ts_recorded
    """

    # 🔹 Загружаем непроверенные позиции после первого Короля
    query_positions = """
        SELECT id, strategy_id, created_at
        FROM positions_v4
        WHERE king_checked = false
          AND created_at >= $1
        ORDER BY created_at
        LIMIT 1000
    """

    async with infra.pg_pool.acquire() as conn:
        kings = await conn.fetch(query_kings)
        positions = await conn.fetch(query_positions, min_ts)

    if not positions:
        log.info("[KING_MARKER] ✅ Нет новых позиций для анализа")
        return

    # 🔹 Преобразование в DataFrame
    df_kings = pd.DataFrame(kings, columns=["strategy_id", "ts_recorded", "next_ts"])
    df_pos = pd.DataFrame(positions, columns=["id", "strategy_id", "created_at"])
    df_pos["created_at"] = pd.to_datetime(df_pos["created_at"])
    df_kings["ts_recorded"] = pd.to_datetime(df_kings["ts_recorded"])
    df_kings["next_ts"] = pd.to_datetime(df_kings["next_ts"])

    matched = 0
    unmatched = 0

    for row in df_pos.itertuples():
        pos_time = row.created_at
        matched_king = df_kings[
            (df_kings["ts_recorded"] <= pos_time) &
            ((df_kings["next_ts"].isna()) | (pos_time < df_kings["next_ts"]))
        ]

        if not matched_king.empty:
            king_id = matched_king.iloc[0]["strategy_id"]
            log.info(f"[KING_MARKER] ✅ Позиция {row.id} — открыта при Короле {king_id}")
            matched += 1
        else:
            log.info(f"[KING_MARKER] ⚠️ Позиция {row.id} — не совпадает ни с одним Королём")
            unmatched += 1

    log.info(f"[KING_MARKER] 🧾 Обработка завершена: {len(df_pos)} позиций, {matched} совпали, {unmatched} без Короля")