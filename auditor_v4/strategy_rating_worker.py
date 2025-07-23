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

    # 🔹 Расчёт метрик за 12ч (с учётом левереджа)
    df_12h = df.copy()

    # 🔸 Добавляем левередж из enabled_strategies
    df_12h["leverage"] = df_12h["strategy_id"].map(
        lambda sid: float(infra.enabled_strategies.get(sid, {}).get("leverage", 1))
    )

    # 🔸 Пересчёт доходности: pnl / (notional / leverage) * 100
    df_12h["pnl_pct"] = df_12h["pnl"] * df_12h["leverage"] / df_12h["notional_value"] * 100

    # 🔸 Группировка по стратегии
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
        passed_by_trades = trades >= 5  # изменено с 10 → 5

        if passed_by_pnl and passed_by_trades:
            passed.append((sid, pnl, trades))
        else:
            reasons = []
            if not passed_by_pnl:
                reasons.append("ниже медианы по pnl")
            if not passed_by_trades:
                reasons.append("менее 5 сделок")
            reason = f"pnl={pnl:.2f}, trades={trades} — " + ", ".join(reasons)
            rejected.append((sid, reason))

    if not passed:
        log.critical("[STRATEGY_RATER] ❌ Ни одна стратегия не прошла фильтр допуска (оба условия). Торги должны быть остановлены.")
        return

    log.info(f"[STRATEGY_RATER] ✅ К допуску прошли {len(passed)} стратегий (из {len(total_strategies)} включённых в системе)")

    log.debug("[STRATEGY_RATER] 📄 Список допущенных стратегий:")
    for sid, pnl, trades in passed:
        log.debug(
            f"[STRATEGY_RATER] • Стратегия {sid} — pnl={pnl:.2f}, trades={trades} — допущена (по двум условиям)"
        )

    log.info(f"[STRATEGY_RATER] ❌ Отклонено {len(rejected)} стратегий:")
    for sid, reason in rejected:
        log.debug(f"[STRATEGY_RATER] • Стратегия {sid} — {reason}")

    # 🔹 Оставляем только допущенные стратегии
    passed_ids = [sid for sid, *_ in passed]
    df = df[df["strategy_id"].isin(passed_ids)]

    if df.empty:
        log.warning("[STRATEGY_RATER] ❌ После фильтрации не осталось данных за 3ч")
        return

    # 🔹 Отсечение данных до 3ч
    df = df[df["closed_at"] >= from_ts_3h]

    # 🔹 Расчёт метрик за 3ч (core set)
    grouped_3h = df.groupby("strategy_id")
    results = []

    for strategy_id, group in grouped_3h:
        trade_count = len(group)

        # 🔸 Доходность с учётом левереджа
        leverage = float(infra.enabled_strategies.get(strategy_id, {}).get("leverage", 1))
        pnl_pct = (group["pnl"] * leverage / group["notional_value"] * 100).mean()

        # 🔸 Profit factor
        profit = group[group["pnl"] > 0]["pnl"].sum()
        loss = group[group["pnl"] < 0]["pnl"].sum()
        profit_factor = float(profit / abs(loss)) if loss < 0 else 0.0

        # 🔸 Win rate
        win_rate = float((group["pnl"] > 0).mean())

        # 🔸 Avg holding time (в секундах)
        holding_durations = (group["closed_at"] - group["created_at"]).dt.total_seconds()
        avg_holding_time = holding_durations.mean() if not holding_durations.empty else 0.0

        # 🔸 Trend slope (наклон equity кривой)
        equity = group.sort_values("closed_at")["pnl"].cumsum()
        minutes = (group["closed_at"] - group["closed_at"].min()).dt.total_seconds() / 60
        slope = float(linregress(minutes, equity).slope) if len(group) >= 2 else 0.0

        # 🔸 Max drawdown
        peak = equity.cummax()
        drawdown = peak - equity
        max_drawdown = float(drawdown.max()) if not drawdown.empty else 0.0

        results.append({
            "strategy_id": strategy_id,
            "pnl_pct": pnl_pct,
            "trend_slope": slope,
            "profit_factor": profit_factor,
            "win_rate": win_rate,
            "max_drawdown": max_drawdown,
            "avg_holding_time": avg_holding_time,
            "trade_count": trade_count
        })

    if not results:
        log.warning("[STRATEGY_RATER] ❌ Ни одной стратегии не осталось для анализа за 3ч")
        return

    # 🔹 Преобразуем в DataFrame и логируем
    metrics_df = pd.DataFrame(results)
    avg_trade_count = metrics_df["trade_count"].mean()
    metrics_df["avg_trade_count"] = avg_trade_count

    log.debug(f"[STRATEGY_RATER] 📊 Метрики рассчитаны для {len(metrics_df)} стратегий (3ч окно)")

    for row in metrics_df.itertuples():
        log.debug(
            f"[STRATEGY_RATER] • Стратегия {row.strategy_id} — "
            f"pnl={row.pnl_pct:.2f}%, trades={row.trade_count}, "
            f"win={row.win_rate:.2f}, pf={row.profit_factor:.2f}, "
            f"slope={row.trend_slope:.2f}, ddraw={row.max_drawdown:.2f}, "
            f"hold={row.avg_holding_time:.1f}s"
        )

    # 🔹 Расчёт speed_factor и reliability_weight
    metrics_df["speed_factor"] = metrics_df["trade_count"].clip(upper=20) / 20.0
    metrics_df["reliability_weight"] = (
        np.log1p(metrics_df["trade_count"]) / np.log1p(metrics_df["avg_trade_count"])
    ).clip(upper=1.0)

    # 🔹 Нормализация
    def normalize(series):
        min_val = series.min()
        max_val = series.max()
        return (series - min_val) / (max_val - min_val + 1e-9)

    metrics_df["norm_pnl"] = normalize(metrics_df["pnl_pct"])
    metrics_df["norm_trend"] = normalize(metrics_df["trend_slope"])
    metrics_df["norm_pf"] = normalize(metrics_df["profit_factor"].fillna(0))
    metrics_df["norm_win"] = normalize(metrics_df["win_rate"])
    metrics_df["norm_ddraw"] = normalize(metrics_df["max_drawdown"])
    metrics_df["norm_hold"] = normalize(metrics_df["avg_holding_time"])

    # 🔹 Рейтинг
    metrics_df["raw_rating"] = (
        0.40 * metrics_df["norm_pnl"] * metrics_df["speed_factor"] +
        0.20 * metrics_df["norm_trend"] +
        0.15 * metrics_df["norm_pf"] +
        0.10 * metrics_df["norm_win"] -
        0.10 * metrics_df["norm_ddraw"] -
        0.05 * metrics_df["norm_hold"]
    )

    metrics_df["final_rating"] = metrics_df["raw_rating"] * metrics_df["reliability_weight"]

    # 🔹 Отсекаем слабые по reliability
    before = len(metrics_df)
    metrics_df = metrics_df[metrics_df["reliability_weight"] >= 0.3]
    after = len(metrics_df)

    log.info(
        f"[STRATEGY_RATER] ⚖️ Оставлено {after} стратегий после фильтра reliability_weight >= 0.3 (из {before})"
    )

    log.info("[STRATEGY_RATER] 🧮 Рейтинги стратегий после фильтра:")
    for row in metrics_df.itertuples():
        log.info(
            f"[STRATEGY_RATER] • Стратегия {row.strategy_id} — "
            f"raw={row.raw_rating:.4f}, final={row.final_rating:.4f}, "
            f"weight={row.reliability_weight:.2f}, speed={row.speed_factor:.2f}"
        )
    # 🔹 Сохраняем метрики в strategies_metrics_v4
    insert_query = """
        INSERT INTO strategies_metrics_v4 (
            strategy_id, ts,
            pnl_pct_3h, trend_slope_3h, profit_factor_3h, win_rate_3h,
            max_drawdown_3h, avg_holding_time_3h,
            trade_count_3h, avg_trade_count_3h,
            speed_factor, reliability_weight,
            raw_rating, final_rating
        )
        VALUES (
            $1, $2,
            $3, $4, $5, $6,
            $7, $8,
            $9, $10,
            $11, $12,
            $13, $14
        )
        ON CONFLICT DO NOTHING
    """

    async with infra.pg_pool.acquire() as conn:
        for row in metrics_df.itertuples():
            await conn.execute(
                insert_query,
                row.strategy_id, ts_now,
                float(row.pnl_pct), float(row.trend_slope), float(row.profit_factor or 0), float(row.win_rate),
                float(row.max_drawdown), float(row.avg_holding_time),
                int(row.trade_count), int(row.avg_trade_count),
                float(row.speed_factor), float(row.reliability_weight),
                float(row.raw_rating), float(row.final_rating)
            )

    log.info(f"[STRATEGY_RATER] 💾 Сохранены метрики в strategies_metrics_v4: {len(metrics_df)} строк")
    # 🔹 Определение лучшей стратегии (по final_rating)
    best_row = metrics_df.sort_values("final_rating", ascending=False).iloc[0]
    best_id = best_row.strategy_id
    best_rating = best_row.final_rating

    # 🔹 Запросы
    query_last_active = """
        SELECT ts, ts_switch, strategy_id, rating
        FROM strategies_active_v4
        ORDER BY ts DESC
        LIMIT 1
    """

    insert_active = """
        INSERT INTO strategies_active_v4 (
            ts, ts_switch, strategy_id, rating, previous_strategy_id, reason
        )
        VALUES ($1, $1, $2, $3, $4, $5)
    """

    update_active = """
        UPDATE strategies_active_v4
        SET ts = $1, rating = $2, reason = $3
        WHERE strategy_id = $4
    """

    reset_top_flag = "UPDATE strategies_v4 SET top_strategy = FALSE"
    set_top_flag = "UPDATE strategies_v4 SET top_strategy = TRUE WHERE id = $1"

    async with infra.pg_pool.acquire() as conn:
        last_entry = await conn.fetchrow(query_last_active)

        if last_entry is None:
            # 🟢 Первый запуск
            await conn.execute(insert_active, ts_now, best_id, best_rating, None, "initial_selection")
            await conn.execute(reset_top_flag)
            await conn.execute(set_top_flag, best_id)
            log.info(f"[STRATEGY_RATER] 👑 Стратегия {best_id} зафиксирована как 'Король' — причина: initial_selection")

        else:
            previous_id = last_entry["strategy_id"]
            minutes_passed = (ts_now - last_entry["ts_switch"]).total_seconds() / 60

            previous_rating_row = metrics_df.loc[metrics_df["strategy_id"] == previous_id, "final_rating"]
            previous_rating = float(previous_rating_row.iloc[0]) if not previous_rating_row.empty else float(last_entry["rating"])
            rating_diff = best_rating - previous_rating

            if best_id == previous_id:
                # 🟢 Король остался тем же — подтверждение
                await conn.execute(update_active, ts_now, best_rating, "confirmed", best_id)
                await conn.execute(reset_top_flag)
                await conn.execute(set_top_flag, best_id)
                log.info(f"[STRATEGY_RATER] 👑 Стратегия {best_id} подтверждена как 'Король' — обновлён рейтинг и время")

            elif rating_diff >= 0.15 and minutes_passed >= 15:
                # 🟢 Новый лидер → переключаем
                reason = f"switched (Δ={rating_diff:.4f}, {minutes_passed:.1f} мин)"
                await conn.execute(insert_active, ts_now, best_id, best_rating, previous_id, reason)
                await conn.execute(reset_top_flag)
                await conn.execute(set_top_flag, best_id)
                log.info(f"[STRATEGY_RATER] 👑 Стратегия {best_id} зафиксирована как 'Король' — причина: {reason}")

            else:
                # 🟡 Новый лидер, но не прошёл условия → подтверждаем старого
                await conn.execute(update_active, ts_now, previous_rating, "retained", previous_id)
                await conn.execute(reset_top_flag)
                await conn.execute(set_top_flag, previous_id)
                log.info(
                    f"[STRATEGY_RATER] 👑 Стратегия {best_id} — лидер, но не зафиксирован "
                    f"(Δ rating: {rating_diff:.4f}, прошло: {minutes_passed:.1f} мин — "
                    f"нужно Δ ≥ 0.15 и ≥ 15 мин). Король {previous_id} сохранён."
                )