# king_marker_worker.py

import logging
from datetime import datetime

import asyncpg
import pandas as pd

import infra

log = logging.getLogger("KING_MARKER")


# 🔸 Dry-run воркер: логирует позиции, открытые стратегией-королем в интервале её правления
async def run_king_marker_worker():
    log.info("[KING_MARKER] 🔁 Dry-run: поиск позиций, открытых стратегией-королем")

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

    # 🔹 Получаем минимальное время вступления первого Короля
    min_ts = df_kings["ts_recorded"].min()

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
        positions = await conn.fetch(query_positions, min_ts)

    if not positions:
        log.info("[KING_MARKER] ✅ Нет новых позиций для анализа")
        return

    df_pos = pd.DataFrame(positions, columns=["id", "strategy_id", "created_at"])
    df_pos["created_at"] = pd.to_datetime(df_pos["created_at"])

    matched = 0
    unmatched = 0

    for pos in df_pos.itertuples():
        pos_time = pos.created_at
        strategy_id = pos.strategy_id

        matched_king = df_kings[
            (df_kings["strategy_id"] == strategy_id) &
            (df_kings["ts_recorded"] <= pos_time) &
            ((df_kings["next_ts"].isna()) | (pos_time < df_kings["next_ts"]))
        ]

        if not matched_king.empty:
            king_id = matched_king.iloc[0]["strategy_id"]
            log.info(f"[KING_MARKER] ✅ Позиция {pos.id} — открыта стратегией {king_id} во время её правления")
            matched += 1
        else:
            log.info(f"[KING_MARKER] ⚠️ Позиция {pos.id} — стратегия {strategy_id} не была Королём на момент открытия")
            unmatched += 1

    log.info(f"[KING_MARKER] 🧾 Завершено: {len(df_pos)} позиций, {matched} совпали, {unmatched} без Короля")