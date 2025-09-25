# 🔸 oracle_mw_confidence.py — воркер confidence: динамическая модель доверия (R, P, C, S, ECDF) + загрузка весов из БД

import asyncio
import logging
import json
import math
import time
from typing import Dict, List, Tuple, Optional

import infra

log = logging.getLogger("ORACLE_CONFIDENCE")

# 🔸 Константы воркера (Redis Stream)
REPORT_STREAM = "oracle:mw:reports_ready"
REPORT_CONSUMER_GROUP = "oracle_confidence_group"
REPORT_CONSUMER_NAME = "oracle_confidence_worker"

# 🔸 Геометрия окна (шаг 4 часа → 6 прогонов в сутки)
WINDOW_STEPS = {"7d": 7 * 6, "14d": 14 * 6, "28d": 28 * 6}

# 🔸 Параметры статистики
Z = 1.96            # уровень доверия для Wilson (95%)
BASELINE_WR = 0.5   # нейтральная доля успехов (RR 1:1), без учёта pnl

# 🔸 Кэш весов модели (strategy_id,time_frame) → (weights, opts, ts)
_weights_cache: Dict[Tuple[Optional[int], Optional[str]], Tuple[Dict[str, float], Dict, float]] = {}
WEIGHTS_TTL_SEC = 15 * 60  # 15 минут


# 🔸 Точка входа воркера (вызывается из oracle_v4_main через run_safe_loop)
async def run_oracle_confidence():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORT_STREAM, groupname=REPORT_CONSUMER_GROUP, id="$", mkstream=True
        )
        log.info("📡 Создана группа потребителей в Redis Stream: %s", REPORT_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.info("🚀 Старт воркера confidence (динамическая модель)")

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=REPORT_CONSUMER_GROUP,
                consumername=REPORT_CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=32,
                block=30_000,
            )
            if not resp:
                continue

            # обработка сообщений
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))
                        time_frame = payload.get("time_frame")
                        await _process_report(report_id, strategy_id, time_frame)
                        await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения из Redis Stream")

        except asyncio.CancelledError:
            log.info("⏹️ Воркер confidence остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла confidence — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка всего отчёта (по report_id)
async def _process_report(report_id: int, strategy_id: int, time_frame: str):
    async with infra.pg_pool.acquire() as conn:
        # выборка всех строк отчёта
        rows = await conn.fetch(
            """
            SELECT
              id,
              report_id,
              strategy_id,
              time_frame,
              direction,
              timeframe,
              agg_type,
              agg_base,
              agg_state,
              trades_total,
              trades_wins,
              winrate,
              avg_pnl_per_trade,
              report_created_at
            FROM v_mw_aggregated_with_time
            WHERE report_id = $1
            """,
            report_id,
        )
        if not rows:
            log.info("ℹ️ Для report_id=%s нет агрегатов", report_id)
            return

        # когорты для адаптивных нормировок считаем один раз на отчёт и срез
        cohort_cache: Dict[Tuple, List[dict]] = {}

        updated = 0
        for r in rows:
            row = dict(r)

            # ключ когорты: внутри неё сравниваем n (для N_effect)
            cohort_key = (
                row["strategy_id"], row["time_frame"], row["direction"],
                row["timeframe"], row["agg_type"], row["agg_base"], row["report_created_at"]
            )
            if cohort_key not in cohort_cache:
                cohort_cache[cohort_key] = await _fetch_cohort(conn, row)

            # загружаем активные веса для данной стратегии/окна (с кэшем)
            weights, opts = await _get_active_weights(conn, row["strategy_id"], row["time_frame"])

            try:
                confidence, inputs = await _calc_confidence(conn, row, cohort_cache[cohort_key], weights, opts)
                await conn.execute(
                    """
                    UPDATE oracle_mw_aggregated_stat
                       SET confidence = $2,
                           confidence_inputs = $3,
                           confidence_updated_at = now()
                     WHERE id = $1
                    """,
                    int(row["id"]),
                    float(confidence),
                    json.dumps(inputs, separators=(",", ":")),
                )

                # аудит (если таблица есть — вставка успешна; если нет — проигнорируется исключением)
                try:
                    await conn.execute(
                        """
                        INSERT INTO oracle_mw_confidence_audit (
                          aggregated_id, report_id, strategy_id, time_frame, direction, timeframe,
                          agg_type, agg_base, agg_state, confidence, components
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                        """,
                        int(row["id"]), int(row["report_id"]), int(row["strategy_id"]), str(row["time_frame"]),
                        str(row["direction"]), str(row["timeframe"]), str(row["agg_type"]), str(row["agg_base"]),
                        str(row["agg_state"]), float(confidence), json.dumps(inputs, separators=(",", ":"))
                    )
                except Exception:
                    # не валим основной процесс, просто лог на debug
                    log.debug("Аудит недоступен или вставка пропущена (aggregated_id=%s)", row["id"])

                updated += 1
            except Exception:
                log.exception("❌ Ошибка обновления confidence для aggregated_id=%s", row["id"])

        log.info(
            "✅ Обновлён confidence для report_id=%s (strategy_id=%s, time_frame=%s): %d строк",
            report_id, strategy_id, time_frame, updated
        )


# 🔸 Выборка когорты (все состояния agg_state внутри одного среза и времени отчёта)
async def _fetch_cohort(conn, row: dict) -> List[dict]:
    rows = await conn.fetch(
        """
        SELECT
          id,
          trades_total,
          trades_wins,
          winrate,
          avg_pnl_per_trade
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


# 🔸 Загрузка активных весов из БД (с простым кэшем)
async def _get_active_weights(conn, strategy_id: int, time_frame: str) -> Tuple[Dict[str, float], Dict]:
    now = time.time()

    # попытка: (strategy_id,time_frame)
    key = (strategy_id, time_frame)
    w = _weights_cache.get(key)
    if w and (now - w[2] < WEIGHTS_TTL_SEC):
        return w[0], w[1]

    # попытка: (strategy_id,NULL)
    key2 = (strategy_id, None)
    w2 = _weights_cache.get(key2)
    if w2 and (now - w2[2] < WEIGHTS_TTL_SEC):
        return w2[0], w2[1]

    # попытка: (NULL,NULL)
    key3 = (None, None)
    w3 = _weights_cache.get(key3)
    if w3 and (now - w3[2] < WEIGHTS_TTL_SEC):
        return w3[0], w3[1]

    # запрос в БД: приоритет — точное совпадение, затем по стратегии, затем глобально
    row = await conn.fetchrow(
        """
        SELECT weights, COALESCE(opts,'{}'::jsonb) AS opts
          FROM oracle_conf_model
         WHERE is_active = true
           AND (
                 (strategy_id = $1 AND time_frame = $2)
              OR (strategy_id = $1 AND time_frame IS NULL)
              OR (strategy_id IS NULL AND time_frame IS NULL)
           )
         ORDER BY
           CASE WHEN strategy_id = $1 AND time_frame = $2 THEN 1
                WHEN strategy_id = $1 AND time_frame IS NULL THEN 2
                ELSE 3 END,
           created_at DESC
         LIMIT 1
        """,
        strategy_id, time_frame
    )

    if row:
        weights = dict(row["weights"])
        opts = dict(row["opts"])
    else:
        # дефолтные веса
        weights = {"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}
        opts = {"baseline_mode": "neutral"}

    # запись в кэш для всех трёх ключей, чтобы реже дёргать БД
    ts = time.time()
    _weights_cache[(strategy_id, time_frame)] = (weights, opts, ts)
    _weights_cache[(strategy_id, None)] = (weights, opts, ts)
    _weights_cache[(None, None)] = (weights, opts, ts)

    return weights, opts


# 🔸 Расчёт confidence (динамическая модель)
async def _calc_confidence(
    conn,
    row: dict,
    cohort_rows: List[dict],
    weights: Dict[str, float],
    opts: Dict,
) -> Tuple[float, dict]:
    n = int(row["trades_total"] or 0)
    wins = int(row["trades_wins"] or 0)
    wr = float(row["winrate"] or 0.0)

    # Reliability (R): нижняя граница Wilson
    R = _wilson_lower_bound(wins, n, Z) if n > 0 else 0.0

    # Persistence (P): presence_rate по последним L отчётов и growth_hist через ECDF по историческим n
    L = WINDOW_STEPS.get(str(row["time_frame"]), 42)
    presence_rate, growth_hist, hist_n = await _persistence_metrics(conn, row, L)
    P = 0.6 * presence_rate + 0.4 * growth_hist

    # Cross-window coherence (C): согласованность знака wr относительно baseline (без PnL)
    C = await _cross_window_coherence(conn, row)

    # Stability (S): робастная устойчивость wr с динамической шкалой
    S_key, len_hist, dyn_scale_used = await _stability_key_dynamic(conn, row, L, cohort_rows)

    # Адаптивная нормировка по «массе» внутри когорты: ECDF по n
    cohort_n = [int(x["trades_total"] or 0) for x in cohort_rows]
    ecdf_cohort = _ecdf_rank(n, cohort_n)
    # если когорта маленькая, смешиваем с исторической ECDF по ключу
    ecdf_hist = _ecdf_rank(n, hist_n) if hist_n else 0.0
    if len(cohort_n) < 5:
        N_effect = 0.5 * ecdf_cohort + 0.5 * ecdf_hist
    else:
        N_effect = ecdf_cohort
    # динамический нижний порог: 1/(size+1), чтобы полностью не занулить
    floor = 1.0 / float(max(2, len(cohort_n)) + 1)
    N_effect = max(N_effect, floor)
    N_effect = float(max(0.0, min(1.0, N_effect)))

    # Загруженные веса
    wR = float(weights.get("wR", 0.4))
    wP = float(weights.get("wP", 0.25))
    wC = float(weights.get("wC", 0.2))
    wS = float(weights.get("wS", 0.15))

    # Итоговый скор (клиппинг на всякий случай)
    raw = wR * R + wP * P + wC * C + wS * S_key
    confidence = round(max(0.0, min(1.0, raw * N_effect)), 4)

    inputs = {
        "R": round(R, 6),
        "P": round(P, 6),
        "C": round(C, 6),
        "S": round(S_key, 6),
        "N_effect": round(N_effect, 6),
        "weights": {"wR": wR, "wP": wP, "wC": wC, "wS": wS},
        "n": n,
        "wins": wins,
        "wr": wr,
        "presence_rate": round(presence_rate, 6),
        "growth_hist": round(growth_hist, 6),
        "hist_points": len(hist_n),
        "dyn_scale_used": dyn_scale_used,
        "baseline_wr": BASELINE_WR,
        "formula": "(wR*R + wP*P + wC*C + wS*S) * N_effect",
    }
    return confidence, inputs


# 🔸 Persistence-метрики: presence_rate и growth_hist (ECDF по историческим n)
async def _persistence_metrics(conn, row: dict, L: int) -> Tuple[float, float, List[int]]:
    # получаем последние L отчётов (created_at) по стратегии/окну до и включая текущий отчёт
    last_rows = await conn.fetch(
        """
        WITH last_reports AS (
          SELECT id, created_at
          FROM oracle_report_stat
          WHERE strategy_id = $1
            AND time_frame  = $2
            AND created_at <= $3
          ORDER BY created_at DESC
          LIMIT $4
        )
        SELECT lr.created_at,
               a.trades_total
        FROM last_reports lr
        LEFT JOIN oracle_mw_aggregated_stat a
          ON a.report_id = lr.id
         AND a.strategy_id = $1
         AND a.time_frame  = $2
         AND a.direction   = $5
         AND a.timeframe   = $6
         AND a.agg_type    = $7
         AND a.agg_base    = $8
         AND a.agg_state   = $9
        ORDER BY lr.created_at DESC
        """,
        row["strategy_id"],
        row["time_frame"],
        row["report_created_at"],
        int(L),
        row["direction"],
        row["timeframe"],
        row["agg_type"],
        row["agg_base"],
        row["agg_state"],
    )

    # presence_rate: доля отчётов, где ключ присутствовал
    present_flags = [1 if r["trades_total"] is not None else 0 for r in last_rows]
    L_eff = len(present_flags) if present_flags else 0
    presence_rate = (sum(present_flags) / L_eff) if L_eff > 0 else 0.0

    # growth_hist: ECDF текущего n относительно истории n (где ключ присутствовал)
    hist_n = [int(r["trades_total"]) for r in last_rows if r["trades_total"] is not None]
    growth_hist = _ecdf_rank(int(row["trades_total"] or 0), hist_n) if hist_n else 0.0

    return presence_rate, growth_hist, hist_n


# 🔸 Cross-window coherence: согласованность знака wr относительно BASELINE_WR (без PnL)
async def _cross_window_coherence(conn, row: dict) -> float:
    rows = await conn.fetch(
        """
        SELECT time_frame, trades_total, trades_wins, winrate
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND direction   = $2
          AND timeframe   = $3
          AND agg_type    = $4
          AND agg_base    = $5
          AND agg_state   = $6
          AND report_created_at = $7
        """,
        row["strategy_id"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["agg_state"],
        row["report_created_at"],
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

        # уверенно выше baseline → +1, вес = дистанция CI от baseline
        if lb > BASELINE_WR:
            dist = max(lb - BASELINE_WR, ub - BASELINE_WR)
            if dist > 0:
                signs.append(+1)
                weights.append(dist)
        # уверенно ниже baseline → -1, вес = дистанция CI от baseline
        elif ub < BASELINE_WR:
            dist = max(BASELINE_WR - lb, BASELINE_WR - ub)
            if dist > 0:
                signs.append(-1)
                weights.append(dist)
        # иначе — окно неопределённое, не учитываем

    total_weight = sum(weights)
    if total_weight <= 0.0:
        return 0.0

    signed_weight = sum(s * w for s, w in zip(signs, weights))
    C = abs(signed_weight) / total_weight
    return float(max(0.0, min(1.0, C)))


# 🔸 Стабильность ключа: робастный z по истории wr с динамической шкалой
async def _stability_key_dynamic(
    conn,
    row: dict,
    L: int,
    cohort_rows: List[dict],
) -> Tuple[float, int, dict]:
    rows = await conn.fetch(
        """
        SELECT winrate
        FROM v_mw_aggregated_with_time
        WHERE strategy_id = $1
          AND time_frame  = $2
          AND direction   = $3
          AND timeframe   = $4
          AND agg_type    = $5
          AND agg_base    = $6
          AND agg_state   = $7
          AND report_created_at <= $8
        ORDER BY report_created_at DESC
        LIMIT $9
        """,
        row["strategy_id"], row["time_frame"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["agg_state"],
        row["report_created_at"], int(L)
    )
    wr_hist = [float(r["winrate"] or 0.0) for r in rows]
    wr_now = float(row["winrate"] or 0.0)
    if len(wr_hist) < 2:
        return 1.0, len(wr_hist), {"mode": "short_hist", "scale": None}

    med = _median(wr_hist)
    mad = _mad(wr_hist, med)
    iqr = _iqr(wr_hist)

    # когортная шкала по текущему отчёту (для подстраховки)
    wr_cohort = [float(x["winrate"] or 0.0) for x in cohort_rows] if cohort_rows else []
    cohort_mad = _mad(wr_cohort, _median(wr_cohort)) if len(wr_cohort) >= 3 else 0.0

    # базовые кандидаты шкалы
    cand = []
    if mad > 0:
        cand.append(mad / 0.6745)
    if iqr > 0:
        cand.append(iqr / 1.349)
    if cohort_mad > 0:
        cand.append(cohort_mad / 0.6745)

    # статистический минимум с учётом длины истории
    n_hist = len(wr_hist)
    cand.append(1.0 / math.sqrt(max(1.0, float(n_hist))))

    # если история идеально ровная: S=1.0
    if all(c <= 0 for c in cand[:-1]) and abs(wr_now - med) < 1e-12:
        return 1.0, n_hist, {"mode": "flat_hist", "scale": 0.0}

    scale = max(cand) if cand else 1e-6
    z = abs(wr_now - med) / (scale + 1e-12)
    S_key = 1.0 / (1.0 + z)

    return S_key, n_hist, {"mode": "dynamic", "scale": round(scale, 6), "median": round(med, 6)}


# 🔸 Wilson lower bound (биномиальная пропорция)
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    return max(0.0, min(1.0, lb))


# 🔸 Wilson bounds: нижняя и верхняя границы
def _wilson_bounds(wins: int, n: int, z: float) -> tuple[float, float]:
    if n <= 0:
        return 0.0, 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    ub = (center + adj) / denom
    return max(0.0, min(1.0, lb)), max(0.0, min(1.0, ub))


# 🔸 ECDF-ранг: доля значений ≤ x (если список пуст, 0.0)
def _ecdf_rank(x: int, values: List[int]) -> float:
    if not values:
        return 0.0
    cnt = sum(1 for v in values if v <= x)
    return cnt / len(values)


# 🔸 Медиана
def _median(arr: List[float]) -> float:
    n = len(arr)
    if n == 0:
        return 0.0
    s = sorted(arr)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])


# 🔸 MAD (median absolute deviation)
def _mad(arr: List[float], med: float) -> float:
    if not arr:
        return 0.0
    dev = [abs(x - med) for x in arr]
    return _median(dev)


# 🔸 IQR (межквартильный размах)
def _iqr(arr: List[float]) -> float:
    n = len(arr)
    if n < 4:
        return 0.0
    s = sorted(arr)
    q1 = _percentile(s, 25.0)
    q3 = _percentile(s, 75.0)
    return max(0.0, q3 - q1)


# 🔸 Персентиль (линейная интерполяция)
def _percentile(sorted_arr: List[float], p: float) -> float:
    if not sorted_arr:
        return 0.0
    if p <= 0:
        return sorted_arr[0]
    if p >= 100:
        return sorted_arr[-1]
    k = (len(sorted_arr) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_arr[int(k)]
    d0 = sorted_arr[int(f)] * (c - k)
    d1 = sorted_arr[int(c)] * (k - f)
    return d0 + d1