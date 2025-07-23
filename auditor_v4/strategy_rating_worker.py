# strategy_rating_worker.py

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy.stats import linregress
import asyncpg

import infra

# 🔸 Логгер
log = logging.getLogger("STRATEGY_RATER")


# strategy_rating_worker.py

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from scipy.stats import linregress
import asyncpg

import infra

# 🔸 Логгер
log = logging.getLogger("STRATEGY_RATER")


# 🔸 Основной воркер
async def run_strategy_rating_worker():
    start = datetime.utcnow()
    log.info("[STRATEGY_RATER] 🔁 Запуск расчёта рейтингов стратегий")

    # 🔹 Временные окна
    now = datetime.utcnow()
    ts_now = now.replace(second=0, microsecond=0)

    from_ts_3h = now - timedelta(hours=3)
    from_ts_12h = now - timedelta(hours=12)

    # 🔹 Загрузка позиций за 12ч
    query = """
        SELECT strategy_id, pnl, notional_value, created_at, closed_at
        FROM positions_v4
        WHERE closed_at BETWEEN $1 AND $2
          AND status = 'closed'
          AND pnl IS NOT NULL
          AND notional_value IS NOT NULL
          AND created_at IS NOT NULL
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query, from_ts_12h, now)

    if not rows:
        log.warning("[STRATEGY_RATER] ❗ Нет закрытых позиций за последние 12 часов")
        return

    # 🔹 Преобразование в DataFrame
    df = pd.DataFrame(rows, columns=rows[0].keys())
    df["pnl"] = df["pnl"].astype(float)
    df["notional_value"] = df["notional_value"].astype(float)
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["closed_at"] = pd.to_datetime(df["closed_at"])

    # 🔹 Расчёт метрик за 12ч
    df_12h = df.copy()
    df_12h["pnl_pct"] = df_12h["pnl"] / df_12h["notional_value"] * 100

    grouped_12h = df_12h.groupby("strategy_id")
    metrics_12h = grouped_12h.agg({
        "pnl_pct": "mean",
        "pnl": "count"
    }).rename(columns={
        "pnl_pct": "pnl_pct_12h",
        "pnl": "trade_count_12h"
    })

    # 🔹 Медианный pnl_pct по всем стратегиям
    median_pnl = metrics_12h["pnl_pct_12h"].median()

    # 🔹 Получение всех активных стратегий (enabled = true)
    total_strategies = [
        sid for sid, strategy in infra.enabled_strategies.items()
        if strategy.get("enabled") is True
    ]

    passed = []
    rejected = []

    for sid in total_strategies:
        if sid not in metrics_12h.index:
            rejected.append((sid, "нет сделок за 12ч"))
            continue

        row = metrics_12h.loc[sid]
        pnl = row["pnl_pct_12h"]
        trades = row["trade_count_12h"]

        passed_by_pnl = pnl >= median_pnl
        passed_by_trades = trades >= 10

        if passed_by_pnl or passed_by_trades:
            reason_parts = []
            if passed_by_pnl:
                reason_parts.append("по pnl")
            if passed_by_trades:
                reason_parts.append("по сделкам")
            passed.append((sid, pnl, trades, reason_parts))
        else:
            reason = f"pnl={pnl:.2f}, trades={trades} — ниже медианы и < 10"
            rejected.append((sid, reason))

    if not passed:
        log.warning("[STRATEGY_RATER] ❌ Ни одна стратегия не прошла фильтр допуска (12ч)")
        return

    log.info(f"[STRATEGY_RATER] ✅ К допуску прошли {len(passed)} стратегий (из {len(total_strategies)} включённых в системе)")

    log.info("[STRATEGY_RATER] 📄 Список допущенных стратегий:")
    for sid, pnl, trades, reason_parts in passed:
        log.info(
            f"[STRATEGY_RATER] • Стратегия {sid} — pnl={pnl:.2f}, trades={trades} — допущена ({' и '.join(reason_parts)})"
        )

    log.info(f"[STRATEGY_RATER] ❌ Отклонено {len(rejected)} стратегий:")
    for sid, reason in rejected:
        log.info(f"[STRATEGY_RATER] • Стратегия {sid} — {reason}")

    # 🔹 Оставляем только допущенные стратегии
    passed_ids = [sid for sid, *_ in passed]
    df = df[df["strategy_id"].isin(passed_ids)]

    if df.empty:
        log.warning("[STRATEGY_RATER] ❌ После фильтрации не осталось данных за 3ч")
        return

    # 🔹 Отсечение данных до 3ч
    df = df[df["closed_at"] >= from_ts_3h]