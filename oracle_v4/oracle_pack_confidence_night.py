# oracle_pack_confidence_night.py — ночной тюнер PACK: автокалибровка весов (wR,wP,wC,wS) per-strategy/per-window раз в сутки

# 🔸 Импорты
import asyncio
import logging
from typing import Dict, List, Tuple, Optional
import math
import time
import json

import infra
# 🔸 базовые константы/утилиты берём из MW-конфиденса (универсальны)
from oracle_mw_confidence import (
    WINDOW_STEPS, Z, BASELINE_WR,
    _wilson_lower_bound, _wilson_bounds,
    _ecdf_rank, _median, _mad, _iqr,
)
# 🔸 стабильность S для PACK берём из рантайм-воркера PACK-confidence
from oracle_pack_confidence import _stability_key_dynamic_pack

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_CONFIDENCE_NIGHT")

# 🔸 Параметры запуска воркера (запускается через run_periodic в oracle_v4_main)
INITIAL_DELAY_H = 25        # первый запуск через 24 часа после старта сервиса
INTERVAL_H      = 24        # затем раз в 24 часа

# 🔸 Параметры обучения/отбора
MIN_SAMPLES_PER_STRATEGY = 200     # минимальное число строк-образцов для обучения на стратегию/окно
HOLDOUT_FRACTION         = 0.15    # доля последних «пар отчётов» на holdout-проверку (по времени)
WEIGHT_CLIP_MIN          = 0.05    # минимальный вес компоненты
WEIGHT_CLIP_MAX          = 0.35    # максимальный вес компоненты (жёстко ограничиваем, чтобы C не доминировал)
WEIGHTS_TOLERANCE        = 1e-9    # защита от деления на ноль при нормировке


# 🔸 Точка входа воркера (вызывается из oracle_v4_main → run_periodic(...))
async def run_oracle_pack_confidence_night():
    # условия достаточности
    if infra.pg_pool is None:
        log.debug("❌ Пропуск ночного PACK-тюнера: нет PG-пула")
        return

    # список стратегий для тюнинга (активные и market_watcher=true)
    strategies = await _load_target_strategies()
    if not strategies:
        log.debug("ℹ️ Нечего тюнить: нет стратегий с market_watcher=true")
        return

    # перебор стратегий и окон (7d/14d/28d)
    updated_total = 0
    async with infra.pg_pool.acquire() as conn:
        for sid in strategies:
            for tf in ("7d", "14d", "28d"):
                try:
                    ok = await _train_and_activate_weights_pack(conn, strategy_id=sid, time_frame=tf)
                    if ok:
                        updated_total += 1
                except Exception:
                    log.exception("❌ Ошибка PACK-тюнинга весов: strategy_id=%s, time_frame=%s", sid, tf)

    log.info("✅ Ночной PACK-тюнер завершён: обновлено активных весов для %d пар (strategy_id × time_frame)", updated_total)


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


# 🔸 Обучение и активация весов для одной пары (strategy_id, time_frame) — PACK
async def _train_and_activate_weights_pack(conn, strategy_id: int, time_frame: str) -> bool:
    # набираем по времени пары отчётов (t, t+1), ограничиваемся ~2*L для свежести
    limit_reports = int(WINDOW_STEPS.get(time_frame, 42) * 2)
    reports = await conn.fetch(
        """
        SELECT id, created_at
        FROM oracle_report_stat
        WHERE strategy_id = $1 AND time_frame = $2 AND source = 'pack'
        ORDER BY created_at ASC
        LIMIT $3
        """,
        strategy_id, time_frame, limit_reports
    )
    if len(reports) < 3:
        log.debug("ℹ️ PACK strategy=%s tf=%s: недостаточно отчётов (%d < 3)", strategy_id, time_frame, len(reports))
        return False

    # пары (t, t+1)
    pairs: List[Tuple[Tuple[int, str], Tuple[int, str]]] = []
    for i in range(len(reports) - 1):
        pairs.append(
            ((int(reports[i]["id"]), str(reports[i]["created_at"])),
             (int(reports[i+1]["id"]), str(reports[i+1]["created_at"])))
        )

    # датасет признаков и меток
    X: List[Tuple[float, float, float, float]] = []  # (R,P,C,S)
    Y: List[int] = []

    # кэш когорт (на уровне одного отчёта)
    cohort_cache: Dict[Tuple, List[dict]] = {}

    for (rep_id_t, created_t), (rep_id_n, created_n) in pairs:
        # строки отчёта T
        rows_t = await conn.fetch(
            """
            SELECT
              id, report_id, strategy_id, time_frame, direction, timeframe,
              pack_base, agg_type, agg_key, agg_value,
              trades_total, trades_wins, winrate, avg_pnl_per_trade, report_created_at
            FROM v_pack_aggregated_with_time
            WHERE report_id = $1
            """,
            rep_id_t
        )
        if not rows_t:
            continue

        # строки отчёта T+1 (для целевой метки)
        rows_n = await conn.fetch(
            """
            SELECT
              id, report_id, strategy_id, time_frame, direction, timeframe,
              pack_base, agg_type, agg_key, agg_value,
              trades_total, trades_wins, winrate, report_created_at
            FROM v_pack_aggregated_with_time
            WHERE report_id = $1
            """,
            rep_id_n
        )
        key2row_n: Dict[Tuple, dict] = {}
        for rn in rows_n:
            kn = (rn["direction"], rn["timeframe"], rn["pack_base"], rn["agg_type"], rn["agg_key"], rn["agg_value"])
            key2row_n[kn] = dict(rn)

        # находим «тройку отчётов» (7d/14d/28d) с тем же window_end, что у T — для расчёта C
        hdr_t = await conn.fetchrow(
            "SELECT strategy_id, window_end FROM oracle_report_stat WHERE id = $1",
            rep_id_t
        )
        trio_rows = await conn.fetch(
            """
            SELECT id, time_frame
            FROM oracle_report_stat
            WHERE strategy_id = $1
              AND window_end  = $2
              AND time_frame  IN ('7d','14d','28d')
              AND source = 'pack'
            """,
            int(hdr_t["strategy_id"]), hdr_t["window_end"]
        )
        trio_ids = {str(r["time_frame"]): int(r["id"]) for r in trio_rows}

        for rt in rows_t:
            row_t = dict(rt)
            key = (row_t["direction"], row_t["timeframe"], row_t["pack_base"], row_t["agg_type"], row_t["agg_key"], row_t["agg_value"])
            row_next = key2row_n.get(key)
            if not row_next:
                continue

            # когорта для T (все agg_value внутри оси на одном отчёте)
            cohort_key = (
                row_t["strategy_id"], row_t["time_frame"], row_t["direction"], row_t["timeframe"],
                row_t["pack_base"], row_t["agg_type"], row_t["agg_key"], row_t["report_created_at"]
            )
            if cohort_key not in cohort_cache:
                cohort_cache[cohort_key] = await _fetch_pack_cohort(conn, row_t)

            # R (на t)
            n_t = int(row_t["trades_total"] or 0)
            w_t = int(row_t["trades_wins"] or 0)
            R_t = _wilson_lower_bound(w_t, n_t, Z) if n_t > 0 else 0.0

            # P (на t)
            L = int(WINDOW_STEPS.get(time_frame, 42))
            presence_rate_t, growth_hist_t = await _persistence_metrics_pack(conn, row_t, L)
            P_t = 0.6 * presence_rate_t + 0.4 * growth_hist_t

            # C (на t) — по трём отчётам с тем же window_end; если найдено <2 окон — C=0.0
            if len(trio_ids) >= 2:
                C_t = await _cross_window_coherence_by_ids_pack(conn, row_t, trio_ids)
            else:
                C_t = 0.0

            # S (на t) — робастная стабильность wr
            S_t, _len_hist, _meta = _stability_key_dynamic_pack(row_t, L, cohort_cache[cohort_key])

            # целевая метка y: оба знака относительно baseline уверенные и совпадают
            y = _target_same_sign_next_pack(row_t, row_next)

            X.append((R_t, P_t, C_t, S_t))
            Y.append(y)

    samples = len(Y)
    if samples < MIN_SAMPLES_PER_STRATEGY:
        log.debug("ℹ️ PACK strategy=%s tf=%s: мало данных для тюнинга (samples=%d < %d)",
                 strategy_id, time_frame, samples, MIN_SAMPLES_PER_STRATEGY)
        return False

    # разбиение на train/holdout по времени (последние пары — holdout)
    holdout = max(1, int(samples * HOLDOUT_FRACTION))
    train = samples - holdout
    X_train, Y_train = X[:train], Y[:train]
    # X_hold, Y_hold = X[train:], Y[train:]  # резерв под метрику, если понадобится

    # важность признаков (point-biserial corr)
    imp = _feature_importance_corr(X_train, Y_train)

    # нормировка + клиппинг + повторная нормировка
    weights = _normalize_weights(imp, clip_min=WEIGHT_CLIP_MIN, clip_max=WEIGHT_CLIP_MAX)

    # политика: гарантируем минимальную долю R и ограничиваем C сверху
    min_R = 0.25
    max_C = 0.35
    wR = max(weights["wR"], min_R)
    wC = min(weights["wC"], max_C)
    wP = weights["wP"]
    wS = weights["wS"]
    s = wR + wP + wC + wS
    weights = {"wR": wR / s, "wP": wP / s, "wC": wC / s, "wS": wS / s}

    log.debug("📊 PACK-тюнинг strategy=%s tf=%s: samples=%d (train=%d, holdout=%d) → weights=%s",
             strategy_id, time_frame, samples, train, holdout, weights)

    # активируем новые веса (деактивируем старые для пары strategy/tf)
    await conn.execute(
        """
        UPDATE oracle_pack_conf_model
           SET is_active = false
         WHERE is_active = true
           AND COALESCE(strategy_id, -1) = $1
           AND COALESCE(time_frame, '') = $2
        """,
        int(strategy_id), str(time_frame)
    )
    await conn.execute(
        """
        INSERT INTO oracle_pack_conf_model (name, strategy_id, time_frame, weights, opts, is_active)
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, true)
        """,
        f"pack_auto_{time.strftime('%Y%m%d_%H%M%S')}",
        int(strategy_id),
        str(time_frame),
        json.dumps(weights),
        '{"baseline_mode":"neutral"}',
    )
    log.info("✅ Активированы новые PACK-веса для strategy=%s tf=%s: %s", strategy_id, time_frame, weights)
    return True


# 🔸 Когорта (PACK) для одного отчёта: все состояния внутри оси, используется для S/ECDF
async def _fetch_pack_cohort(conn, row: dict) -> List[dict]:
    rows = await conn.fetch(
        """
        SELECT id, trades_total, trades_wins, winrate, avg_pnl_per_trade
        FROM v_pack_aggregated_with_time
        WHERE strategy_id = $1
          AND time_frame  = $2
          AND direction   = $3
          AND timeframe   = $4
          AND pack_base   = $5
          AND agg_type    = $6
          AND agg_key     = $7
          AND report_created_at = $8
        """,
        row["strategy_id"], row["time_frame"], row["direction"], row["timeframe"],
        row["pack_base"], row["agg_type"], row["agg_key"], row["report_created_at"]
    )
    return [dict(x) for x in rows]


# 🔸 Persistence-метрики (PACK): presence_rate и growth_hist
async def _persistence_metrics_pack(conn, row: dict, L: int) -> Tuple[float, float]:
    last_rows = await conn.fetch(
        """
        WITH last_reports AS (
          SELECT id, created_at
          FROM oracle_report_stat
          WHERE strategy_id = $1
            AND time_frame  = $2
            AND created_at <= $3
            AND source = 'pack'
          ORDER BY created_at DESC
          LIMIT $4
        )
        SELECT lr.created_at,
               a.trades_total
        FROM last_reports lr
        LEFT JOIN oracle_pack_aggregated_stat a
          ON a.report_id = lr.id
         AND a.strategy_id = $1
         AND a.time_frame  = $2
         AND a.direction   = $5
         AND a.timeframe   = $6
         AND a.pack_base   = $7
         AND a.agg_type    = $8
         AND a.agg_key     = $9
         AND a.agg_value   = $10
        ORDER BY lr.created_at DESC
        """,
        row["strategy_id"],
        row["time_frame"],
        row["report_created_at"],
        int(L),
        row["direction"],
        row["timeframe"],
        row["pack_base"],
        row["agg_type"],
        row["agg_key"],
        row["agg_value"],
    )

    present_flags = [1 if r["trades_total"] is not None and int(r["trades_total"]) > 0 else 0 for r in last_rows]
    L_eff = len(present_flags) if present_flags else 0
    presence_rate = (sum(present_flags) / L_eff) if L_eff > 0 else 0.0

    hist_n = [int(r["trades_total"]) for r in last_rows if r["trades_total"] is not None]
    growth_hist = _ecdf_rank(int(row["trades_total"] or 0), hist_n) if hist_n else 0.0

    return presence_rate, growth_hist


# 🔸 C по конкретному комплекту report_id (7d/14d/28d) для PACK-ключа строки
async def _cross_window_coherence_by_ids_pack(conn, row: dict, report_ids: Dict[str, int]) -> float:
    rows = await conn.fetch(
        """
        SELECT a.time_frame, a.trades_total, a.trades_wins
        FROM oracle_pack_aggregated_stat a
        WHERE a.report_id = ANY($1::bigint[])
          AND a.strategy_id = $2
          AND a.direction   = $3
          AND a.timeframe   = $4
          AND a.pack_base   = $5
          AND a.agg_type    = $6
          AND a.agg_key     = $7
          AND a.agg_value   = $8
        """,
        list(report_ids.values()),
        row["strategy_id"], row["direction"], row["timeframe"],
        row["pack_base"], row["agg_type"], row["agg_key"], row["agg_value"]
    )
    if not rows:
        return 0.0

    signs: List[int] = []
    weights: List[float] = []

    for r in rows:
        n = int(r["trades_total"] or 0)
        w = int(r["trades_wins"] or 0)
        if n <= 0:
            continue
        lb, ub = _wilson_bounds(w, n, Z)
        if lb > BASELINE_WR:
            dist = max(lb - BASELINE_WR, ub - BASELINE_WR)
            if dist > 0:
                signs.append(+1); weights.append(dist)
        elif ub < BASELINE_WR:
            dist = max(BASELINE_WR - lb, BASELINE_WR - ub)
            if dist > 0:
                signs.append(-1); weights.append(dist)

    total_weight = sum(weights)
    if total_weight <= 0.0 or len(weights) < 2:
        return 0.0

    signed_weight = sum(s * w for s, w in zip(signs, weights))
    C = abs(signed_weight) / total_weight
    return float(max(0.0, min(1.0, C)))


# 🔸 Целевая метка: «персистентность знака wr относительно baseline» на следующем отчёте (PACK)
def _target_same_sign_next_pack(row_t: dict, row_next: dict) -> int:
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
        std_y = math.sqrt(max(1e-12, mean_y * (1.0 - mean_y)))  # дисперсия бинарной метки p(1-p)
        corr = 0.0 if std_x < 1e-12 else (cov_xy / (std_x * std_y))
        imps.append(abs(corr))

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