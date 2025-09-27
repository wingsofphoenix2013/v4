# 🔸 oracle_mw_confidence_night.py — ночной тюнер: автокалибровка весов (wR,wP,wC,wS) per-strategy/per-window раз в сутки

import asyncio
import logging
from typing import Dict, List, Tuple, Optional
import math
import time

import infra
# используем готовые утилиты/константы из основного воркера confidence
from oracle_mw_confidence import (
    WINDOW_STEPS, Z, BASELINE_WR,
    _wilson_lower_bound, _wilson_bounds,
    _persistence_metrics, _cross_window_coherence, _stability_key_dynamic,
    _ecdf_rank, _median, _mad, _iqr,
)

log = logging.getLogger("ORACLE_CONFIDENCE_NIGHT")

# 🔸 Параметры запуска воркера (задержка старта и интервал в часах — подключаются через run_periodic в main)
INITIAL_DELAY_H = 24        # первый запуск через 24 часа после старта сервиса
INTERVAL_H      = 24        # затем раз в 24 часа

# 🔸 Параметры обучения/отбора
MIN_SAMPLES_PER_STRATEGY = 200     # минимальное число строк-образцов для обучения на стратегию/окно
HOLDOUT_FRACTION         = 0.15    # доля последних «пар отчётов» на holdout-проверку (по времени)
WEIGHT_CLIP_MIN          = 0.05    # минимальный вес компоненты
WEIGHT_CLIP_MAX          = 0.35    # максимальный вес компоненты (жёстко ограничиваем, чтобы C не доминировал)
WEIGHTS_TOLERANCE        = 1e-9    # защита от деления на ноль при нормировке


# 🔸 Точка входа воркера (вызывается из oracle_v4_main через run_periodic(..., initial_delay=..., interval=...))
async def run_oracle_confidence_night():
    # условия достаточности
    if infra.pg_pool is None:
        log.info("❌ Пропуск ночного тюнера: нет PG-пула")
        return

    # получаем список стратегий для тюнинга (активные и market_watcher=true)
    strategies = await _load_target_strategies()
    if not strategies:
        log.info("ℹ️ Нечего тюнить: нет стратегий с market_watcher=true")
        return

    # перебор стратегий и окон (7d/14d/28d)
    updated_total = 0
    for sid in strategies:
        for tf in ("7d", "14d", "28d"):
            try:
                ok = await _train_and_activate_weights(strategy_id=sid, time_frame=tf)
                if ok:
                    updated_total += 1
            except Exception:
                log.exception("❌ Ошибка тюнинга весов: strategy_id=%s, time_frame=%s", sid, tf)

    log.info("✅ Ночной тюнер завершён: обновлено активных весов для %d пар (strategy_id × time_frame)", updated_total)


# 🔸 Загрузка целевых стратегий
async def _load_target_strategies() -> List[int]:
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id
            FROM strategies_v4
            WHERE enabled = true
              AND (archived IS NOT TRUE)
              AND market_watcher = true
            """
        )
    return [int(r["id"]) for r in rows]


# 🔸 Обучение и активация весов для одной пары (strategy_id, time_frame)
async def _train_and_activate_weights(strategy_id: int, time_frame: str) -> bool:
    async with infra.pg_pool.acquire() as conn:
        # собираем список отчётов по стратегии/окну по времени (ASC), берём запас: длина окна + ещё окно
        limit_reports = int(WINDOW_STEPS.get(time_frame, 42) * 2)
        reports = await conn.fetch(
            """
            SELECT id, created_at
            FROM oracle_report_stat
            WHERE strategy_id = $1 AND time_frame = $2
            ORDER BY created_at ASC
            LIMIT $3
            """,
            strategy_id, time_frame, limit_reports
        )
        if len(reports) < 3:
            log.info("ℹ️ strategy=%s tf=%s: недостаточно отчётов (%d < 3)", strategy_id, time_frame, len(reports))
            return False

        # формируем пары (t, t+1) по времени; для каждой пары — дата t и t+1
        pairs: List[Tuple[Tuple[int, str], Tuple[int, str]]] = []
        for i in range(len(reports) - 1):
            pairs.append(
                ((int(reports[i]["id"]), str(reports[i]["created_at"])),
                 (int(reports[i+1]["id"]), str(reports[i+1]["created_at"])))
            )

        # собираем датасет: для всех ключей, присутствующих в t,
        # считаем признаки на t (R,P,C,S) и целевую метку y по t+1 (персистентность знака wr относительно baseline)
        X: List[Tuple[float, float, float, float]] = []
        Y: List[int] = []

        # Для ускорения кэшируем когорты (по (report_id, direction, timeframe, agg_type, agg_base))
        cohort_cache: Dict[Tuple, List[dict]] = {}

        for (rep_id_t, created_t), (rep_id_n, created_n) in pairs:
            # выбираем все строки T (признаки на момент t)
            rows_t = await conn.fetch(
                """
                SELECT
                  id, report_id, strategy_id, time_frame, direction, timeframe,
                  agg_type, agg_base, agg_state, trades_total, trades_wins, winrate, avg_pnl_per_trade, report_created_at
                FROM v_mw_aggregated_with_time
                WHERE report_id = $1
                """,
                rep_id_t
            )
            if not rows_t:
                continue

            # мапа для быстрого доступа ко второй точке (t+1) по ключу
            rows_n = await conn.fetch(
                """
                SELECT
                  id, report_id, strategy_id, time_frame, direction, timeframe,
                  agg_type, agg_base, agg_state, trades_total, trades_wins, winrate, report_created_at
                FROM v_mw_aggregated_with_time
                WHERE report_id = $1
                """,
                rep_id_n
            )
            key2row_n: Dict[Tuple, dict] = {}
            for rn in rows_n:
                kn = (
                    rn["direction"], rn["timeframe"], rn["agg_type"], rn["agg_base"], rn["agg_state"]
                )
                key2row_n[kn] = dict(rn)

            # считаем признаки/цель
            for rt in rows_t:
                row_t = dict(rt)
                key = (row_t["direction"], row_t["timeframe"], row_t["agg_type"], row_t["agg_base"], row_t["agg_state"])
                row_next = key2row_n.get(key)
                # цель определяем только если в t+1 ключ присутствует
                if not row_next:
                    continue

                # кэш когорты для t
                cohort_key = (
                    row_t["strategy_id"], row_t["time_frame"], row_t["direction"],
                    row_t["timeframe"], row_t["agg_type"], row_t["agg_base"], row_t["report_created_at"]
                )
                if cohort_key not in cohort_cache:
                    cohort_cache[cohort_key] = await _fetch_cohort_for(conn, row_t)

                # R (на t)
                n_t = int(row_t["trades_total"] or 0)
                w_t = int(row_t["trades_wins"] or 0)
                R_t = _wilson_lower_bound(w_t, n_t, Z) if n_t > 0 else 0.0

                # P (на t)
                L = int(WINDOW_STEPS.get(time_frame, 42))
                presence_rate_t, growth_hist_t, _hist_n_t = await _persistence_metrics(conn, row_t, L)
                P_t = 0.6 * presence_rate_t + 0.4 * growth_hist_t

                # C (на t) — использует текущую реализацию с требованием ≥ 2 уверенных окон
                C_t = await _cross_window_coherence(conn, row_t)

                # S (на t)
                S_t, _len_hist, _meta = await _stability_key_dynamic(conn, row_t, L, cohort_cache[cohort_key])

                # цель y: знак wr относительно baseline на t и t+1 должны совпадать, и t+1 должен быть «уверенным» (интервал Вильсона не пересекает baseline)
                y = _target_same_sign_next(row_t, row_next)

                X.append((R_t, P_t, C_t, S_t))
                Y.append(y)

        samples = len(Y)
        if samples < MIN_SAMPLES_PER_STRATEGY:
            log.info("ℹ️ strategy=%s tf=%s: мало данных для тюнинга (samples=%d < %d)",
                     strategy_id, time_frame, samples, MIN_SAMPLES_PER_STRATEGY)
            return False

        # делим на train/holdout по времени пар (просто последние HOLDOUT_FRACTION доли — holdout)
        holdout = max(1, int(samples * HOLDOUT_FRACTION))
        train = samples - holdout
        X_train, Y_train = X[:train], Y[:train]
        X_hold, Y_hold = X[train:], Y[train:]

        # оцениваем «важность» признаков на train: берём абсолютную point-biserial корреляцию (упрощённая корреляция Пирсона с бинарной меткой)
        imp = _feature_importance_corr(X_train, Y_train)  # dict {"wR":..., "wP":..., "wC":..., "wS":...}

        # нормируем до суммирования в 1, клиппим по границам и снова нормируем
        weights = _normalize_weights(imp, clip_min=WEIGHT_CLIP_MIN, clip_max=WEIGHT_CLIP_MAX)

        # доп. политика: гарантируем минимальную долю R и ограничиваем C сверху (как в рантайме)
        # это делает поведение стабильным даже при «шумной» истории
        min_R = 0.25
        max_C = 0.35
        wR = max(weights["wR"], min_R)
        wC = min(weights["wC"], max_C)
        wP = weights["wP"]
        wS = weights["wS"]
        s = wR + wP + wC + wS
        weights = {"wR": wR / s, "wP": wP / s, "wC": wC / s, "wS": wS / s}

        # при желании можно оценить «качество» на holdout как корреляцию прогнозов с меткой,
        # но для простоты — просто логируем размер holdout
        log.info(
            "📊 Тюнинг strategy=%s tf=%s: samples=%d (train=%d, holdout=%d) → weights=%s",
            strategy_id, time_frame, samples, train, holdout, weights
        )

        # активируем новые веса в БД (деактивируем старые по паре strategy/tf)
        await conn.execute(
            """
            UPDATE oracle_conf_model
               SET is_active = false
             WHERE is_active = true
               AND COALESCE(strategy_id, -1) = $1
               AND COALESCE(time_frame, '') = $2
            """,
            int(strategy_id), str(time_frame)
        )
        await conn.execute(
            """
            INSERT INTO oracle_conf_model (name, strategy_id, time_frame, weights, opts, is_active)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, true)
            """,
            f"auto_{time.strftime('%Y%m%d_%H%M%S')}",
            int(strategy_id),
            str(time_frame),
            # храним только 4 веса; baseline_mode оставляем neutral
            json.dumps(weights),
            '{"baseline_mode":"neutral"}',
        )
        log.info("✅ Активированы новые веса для strategy=%s tf=%s: %s", strategy_id, time_frame, weights)
        return True


# 🔸 Когорта для одного отчёта (все состояния внутри среза; используется для S и ECDF(n) при расчётах в обучении)
async def _fetch_cohort_for(conn, row: dict) -> List[dict]:
    rows = await conn.fetch(
        """
        SELECT id, trades_total, trades_wins, winrate, avg_pnl_per_trade
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND time_frame  = $2
          AND direction   = $3
          AND timeframe   = $4
          AND agg_type    = $5
          AND agg_base    = $6
          AND report_created_at = $7
        """,
        row["strategy_id"], row["time_frame"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["report_created_at"]
    )
    return [dict(x) for x in rows]


# 🔸 Целевая метка: «персистентность знака wr относительно baseline» на следующем отчёте
def _target_same_sign_next(row_t: dict, row_next: dict) -> int:
    # знак в t
    n_t = int(row_t["trades_total"] or 0); w_t = int(row_t["trades_wins"] or 0)
    lb_t, ub_t = _wilson_bounds(w_t, n_t, Z) if n_t > 0 else (0.0, 0.0)
    sign_t = 0
    if lb_t > BASELINE_WR:
        sign_t = +1
    elif ub_t < BASELINE_WR:
        sign_t = -1

    # знак в t+1
    n_n = int(row_next["trades_total"] or 0); w_n = int(row_next["trades_wins"] or 0)
    lb_n, ub_n = _wilson_bounds(w_n, n_n, Z) if n_n > 0 else (0.0, 0.0)
    sign_n = 0
    if lb_n > BASELINE_WR:
        sign_n = +1
    elif ub_n < BASELINE_WR:
        sign_n = -1

    # целевая логика: оба знака должны быть «уверенными» и одинаковыми
    if sign_t == 0 or sign_n == 0:
        return 0
    return 1 if (sign_t == sign_n) else 0


# 🔸 Важность признаков: point-biserial correlation (упрощённая корреляция Пирсона с бинарной меткой)
def _feature_importance_corr(X: List[Tuple[float, float, float, float]], Y: List[int]) -> Dict[str, float]:
    if not X or not Y or len(X) != len(Y):
        return {"wR": 0.25, "wP": 0.25, "wC": 0.25, "wS": 0.25}

    n = len(Y)
    mean_y = sum(Y) / n if n > 0 else 0.0

    # агрегаторы по признакам
    sums = [0.0, 0.0, 0.0, 0.0]
    sums2 = [0.0, 0.0, 0.0, 0.0]
    covs = [0.0, 0.0, 0.0, 0.0]

    for i in range(n):
        xi = X[i]
        yi = Y[i]
        for j in range(4):
            x = float(xi[j])
            sums[j] += x
            sums2[j] += x * x
            covs[j] += x * yi

    imps: List[float] = []
    for j in range(4):
        mean_x = sums[j] / n
        var_x = max(0.0, (sums2[j] / n) - (mean_x * mean_x))
        std_x = math.sqrt(var_x)
        cov_xy = (covs[j] / n) - (mean_x * mean_y)
        # дисперсия бинарной метки: p*(1-p)
        std_y = math.sqrt(max(1e-12, mean_y * (1.0 - mean_y)))
        corr = 0.0 if std_x < 1e-12 else (cov_xy / (std_x * std_y))
        imps.append(abs(corr))

    # именованные важности
    return {"wR": imps[0], "wP": imps[1], "wC": imps[2], "wS": imps[3]}


# 🔸 Нормализация и клиппинг весов (с последующей нормировкой)
def _normalize_weights(imp: Dict[str, float], clip_min: float, clip_max: float) -> Dict[str, float]:
    # начальная нормализация
    total = sum(max(v, 0.0) for v in imp.values())
    if total <= WEIGHTS_TOLERANCE:
        base = {"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}
        return base

    weights = {k: (max(v, 0.0) / total) for k, v in imp.items()}

    # клиппинг
    for k in weights:
        weights[k] = min(max(weights[k], clip_min), clip_max)

    # повторная нормировка до 1
    s2 = sum(weights.values())
    if s2 <= WEIGHTS_TOLERANCE:
        return {"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}

    return {k: (v / s2) for k, v in weights.items()}