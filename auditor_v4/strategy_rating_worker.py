# strategy_rating_worker.py

import logging
from datetime import datetime, timedelta

import asyncpg
import numpy as np
import pandas as pd
from scipy.stats import linregress

import infra

# 🔸 Логгер
log = logging.getLogger("STRATEGY_RATER")

# 🔸 Основной воркер
async def run_strategy_rating_worker():
    start = datetime.utcnow()
    log.info("[STRATEGY_RATER] 🔁 Запуск расчёта рейтингов стратегий")

    # 🔹 Временное окно: последние 3 часа
    now = datetime.utcnow()
    from_ts = now - timedelta(hours=3)

    # 🔹 Получение позиций из БД
    query = """
        SELECT strategy_id, closed_at, pnl, notional_value
        FROM positions_v4
        WHERE closed_at BETWEEN $1 AND $2
          AND status = 'closed'
          AND pnl IS NOT NULL
          AND notional_value IS NOT NULL
    """

    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(query, from_ts, now)

    if not rows:
        log.warning("[STRATEGY_RATER] ❗ Нет закрытых позиций за последние 3 часа — расчёт пропущен")
        return

    # 🔹 Перевод в DataFrame
    df = pd.DataFrame(rows, columns=rows[0].keys())
    df["closed_at"] = pd.to_datetime(df["closed_at"])
    df["pnl"] = df["pnl"].astype(float)
    df["notional_value"] = df["notional_value"].astype(float)

    # 🔹 Группировка по strategy_id
    grouped = df.groupby("strategy_id")
    results = []

    for strategy_id, group in grouped:
        trade_count = len(group)

        # 🔸 PnL %
        total_pnl = group["pnl"].sum()
        total_value = group["notional_value"].sum()
        pnl_pct = (total_pnl / total_value * 100) if total_value > 0 else 0.0

        # 🔸 Profit factor
        profit = group[group["pnl"] > 0]["pnl"].sum()
        loss = group[group["pnl"] < 0]["pnl"].sum()
        profit_factor = profit / abs(loss) if loss < 0 else None

        # 🔸 Win rate
        win_rate = (group["pnl"] > 0).mean()

        # 🔸 Volatility
        volatility = group["pnl"].std(ddof=0)

        # 🔸 Trend slope
        equity = group.sort_values("closed_at")["pnl"].cumsum()
        minutes = (group["closed_at"] - group["closed_at"].min()).dt.total_seconds() / 60
        slope = linregress(minutes, equity).slope if len(group) >= 2 else 0.0

        # 🔸 Max drawdown
        balance = equity
        peak = balance.cummax()
        drawdown = peak - balance
        max_drawdown = drawdown.max()

        results.append({
            "strategy_id": strategy_id,
            "pnl_pct": pnl_pct,
            "profit_factor": profit_factor,
            "win_rate": win_rate,
            "volatility": volatility,
            "trend_slope": slope,
            "max_drawdown": max_drawdown,
            "trade_count": trade_count,
        })

    # 🔹 Нормализация и рейтинг
    metrics_df = pd.DataFrame(results)
    EPSILON = 1e-9

    def normalize(series):
        min_val = series.min()
        max_val = series.max()
        return (series - min_val) / (max_val - min_val + EPSILON)

    metrics_df["norm_pnl_pct"]       = normalize(metrics_df["pnl_pct"])
    metrics_df["norm_trend_slope"]   = normalize(metrics_df["trend_slope"])
    metrics_df["norm_profit_factor"] = normalize(metrics_df["profit_factor"].fillna(0))
    metrics_df["norm_win_rate"]      = normalize(metrics_df["win_rate"])
    metrics_df["norm_max_drawdown"]  = normalize(metrics_df["max_drawdown"].fillna(0))
    metrics_df["norm_volatility"]    = normalize(metrics_df["volatility"].fillna(0))

    metrics_df["rating"] = (
        0.30 * metrics_df["norm_pnl_pct"] +
        0.20 * metrics_df["norm_trend_slope"] +
        0.20 * metrics_df["norm_profit_factor"] +
        0.10 * metrics_df["norm_win_rate"] -
        0.10 * metrics_df["norm_max_drawdown"] -
        0.10 * metrics_df["norm_volatility"]
    )

    # 🔹 Получение предыдущих метрик
    ts_now = now.replace(second=0, microsecond=0)

    query_prev = """
        SELECT strategy_id, pnl_pct, rating
        FROM strategies_metrics_v4
        WHERE ts = (
            SELECT MAX(ts) FROM strategies_metrics_v4
            WHERE ts < $1
        )
    """

    async with infra.pg_pool.acquire() as conn:
        prev_rows = await conn.fetch(query_prev, ts_now)

    prev_map = {r["strategy_id"]: r for r in prev_rows}

    # 🔹 Δ rating и pnl_diff_pct
    metrics_df["pnl_diff_pct"] = metrics_df.apply(
        lambda row: row["pnl_pct"] - prev_map.get(row["strategy_id"], {}).get("pnl_pct", 0),
        axis=1
    )
    metrics_df["delta_rating"] = metrics_df.apply(
        lambda row: row["rating"] - prev_map.get(row["strategy_id"], {}).get("rating", 0),
        axis=1
    )

    # 🔹 Запись в БД
    insert_query = """
        INSERT INTO strategies_metrics_v4 (
            strategy_id, ts,
            pnl_pct, trend_slope, profit_factor, win_rate,
            max_drawdown, volatility, trade_count,
            pnl_diff_pct, rating
        )
        VALUES (
            $1, $2,
            $3, $4, $5, $6,
            $7, $8, $9,
            $10, $11
        )
    """

    async with infra.pg_pool.acquire() as conn:
        for row in metrics_df.itertuples():
            await conn.execute(
                insert_query,
                row.strategy_id, ts_now,
                row.pnl_pct, row.trend_slope, row.profit_factor, row.win_rate,
                row.max_drawdown, row.volatility, row.trade_count,
                row.pnl_diff_pct, row.rating
            )

    # 🔹 Финальное логирование
    for row in metrics_df.itertuples():
        delta = row.delta_rating
        log.info(
            f"[STRATEGY_RATER] ⭐ Стратегия {row.strategy_id} — "
            f"rating: {row.rating:.4f}, Δ rating: {delta:+.4f}"
        )

    elapsed = datetime.utcnow() - start
    log.info(f"[STRATEGY_RATER] ✅ Расчёт завершён за {elapsed.total_seconds():.2f} сек")