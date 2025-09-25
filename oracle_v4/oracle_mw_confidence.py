# 🔸 oracle_mw_confidence.py — воркер confidence: адаптивная оценка доверия (R, P, C, S) без ручных порогов

import asyncio
import logging
import json
import math
from typing import List, Tuple

import infra

log = logging.getLogger("ORACLE_CONFIDENCE")

# 🔸 Константы воркера (стрим отчётов)
REPORT_STREAM = "oracle:mw:reports_ready"
REPORT_CONSUMER_GROUP = "oracle_confidence_group"
REPORT_CONSUMER_NAME = "oracle_confidence_worker"

# 🔸 Геометрия окна (шаг 4 часа → 6 прогонов в сутки)
WINDOW_STEPS = {"7d": 7 * 6, "14d": 14 * 6, "28d": 28 * 6}

# 🔸 Параметры статистики
Z = 1.96  # Wilson 95%


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

    log.info("🚀 Старт воркера confidence (адаптивная модель)")

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=REPORT_CONSUMER_GROUP,
                consumername=REPORT_CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=16,
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
        cohort_cache = {}

        updated = 0
        for r in rows:
            row = dict(r)
            # ключ когорты: все поля, кроме agg_state (внутри него сравниваемся)
            cohort_key = (
                row["strategy_id"], row["time_frame"], row["direction"],
                row["timeframe"], row["agg_type"], row["agg_base"], row["report_created_at"]
            )
            if cohort_key not in cohort_cache:
                cohort_cache[cohort_key] = await _fetch_cohort(conn, row)

            try:
                confidence, inputs = await _calc_confidence(conn, row, cohort_cache[cohort_key])
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


# 🔸 Расчёт confidence (адаптивная модель)
async def _calc_confidence(conn, row: dict, cohort_rows: List[dict]) -> Tuple[float, dict]:
    n = int(row["trades_total"] or 0)
    wins = int(row["trades_wins"] or 0)
    wr = float(row["winrate"] or 0.0)
    pnl = float(row["avg_pnl_per_trade"] or 0.0)

    # Reliability (R): Wilson lower bound
    R = _wilson_lower_bound(wins, n, Z) if n > 0 else 0.0

    # Persistence (P): presence_rate по последним L отчётам и growth_hist через ECDF по историческим n
    L = WINDOW_STEPS.get(str(row["time_frame"]), 42)
    presence_rate, growth_hist, hist_n = await _persistence_metrics(conn, row, L)
    P = 0.6 * presence_rate + 0.4 * growth_hist

    # Cross-window coherence (C): согласованность 7d/14d/28d, веса = R окна
    C = await _cross_window_coherence(conn, row)

    # Stability (S): робастная оценка (MAD) по истории ключа;
    # при малой истории добавляем shrinkage к «когортной» стабильности текущего прогона
    S_key, len_hist = await _stability_key(conn, row, L)
    S_cohort = _stability_cohort_now(row, cohort_rows)
    w = min(1.0, max(0.0, len_hist / max(1, L)))  # вес истории ключа в [0..1]
    S = w * S_key + (1 - w) * S_cohort

    # Адаптивная нормировка по «массе» внутри когорты: ECDF по n
    N_effect = _ecdf_rank(n, [int(x["trades_total"] or 0) for x in cohort_rows])
    N_effect = 0.05 + 0.95 * N_effect  # мягкая пружина от нуля

    # Итоговый скор
    raw = 0.4 * R + 0.25 * P + 0.2 * C + 0.15 * S
    confidence = round(max(0.0, min(1.0, raw * N_effect)), 4)

    inputs = {
        "R": round(R, 6),
        "P": round(P, 6),
        "C": round(C, 6),
        "S": round(S, 6),
        "N_effect": round(N_effect, 6),
        "n": n,
        "wins": wins,
        "wr": wr,
        "avg_pnl_per_trade": pnl,
        "presence_rate": round(presence_rate, 6),
        "growth_hist": round(growth_hist, 6),
        "hist_points": len(hist_n),
        "formula": "(0.4*R + 0.25*P + 0.2*C + 0.15*S) * N_effect",
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


# 🔸 Cross-window coherence: взвешенное согласие окон (веса = R окна)
async def _cross_window_coherence(conn, row: dict) -> float:
    rows = await conn.fetch(
        """
        SELECT time_frame, trades_total, trades_wins, winrate, avg_pnl_per_trade
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

    num = 0.0
    den = 0.0
    for r in rows:
        n = int(r["trades_total"] or 0)
        w = int(r["trades_wins"] or 0)
        pnl = float(r["avg_pnl_per_trade"] or 0.0)
        if n <= 0:
            continue
        Rw = _wilson_lower_bound(w, n, Z)
        den += Rw
        aligned = 1.0 if (Rw > 0.5 and pnl > 0.0) else 0.0
        num += Rw * aligned

    return (num / den) if den > 0 else 0.0


# 🔸 Стабильность ключа: робастный z на истории ключа (median/MAD)
async def _stability_key(conn, row: dict, L: int) -> Tuple[float, int]:
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
    if len(wr_hist) < 2:
        return 1.0, len(wr_hist)

    wr_now = float(row["winrate"] or 0.0)
    med = _median(wr_hist)
    mad = _mad(wr_hist, med)
    scale = (mad / 0.6745) if mad > 0 else (abs(med) * 0.1 + 1e-6)
    robust_z = abs(wr_now - med) / (scale + 1e-12)
    S_key = 1.0 / (1.0 + robust_z)
    return S_key, len(wr_hist)


# 🔸 «Когортная» стабильность: насколько wr_now выбивается из распределения wr в текущем прогоне
def _stability_cohort_now(row: dict, cohort_rows: List[dict]) -> float:
    wr_now = float(row["winrate"] or 0.0)
    wr_cohort = [float(x["winrate"] or 0.0) for x in cohort_rows]
    if len(wr_cohort) < 3:
        return 1.0
    med = _median(wr_cohort)
    mad = _mad(wr_cohort, med)
    scale = (mad / 0.6745) if mad > 0 else (abs(med) * 0.1 + 1e-6)
    robust_z = abs(wr_now - med) / (scale + 1e-12)
    return 1.0 / (1.0 + robust_z)


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