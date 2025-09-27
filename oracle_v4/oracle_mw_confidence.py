# 🔸 oracle_mw_confidence.py — воркер confidence: пакетный расчёт по комплекту окон (7d+14d+28d) для одного window_end + идемпотентность

import asyncio
import logging
import json
import math
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import infra

log = logging.getLogger("ORACLE_CONFIDENCE")

# 🔸 Константы воркера (Redis Stream)
REPORT_STREAM = "oracle:mw:reports_ready"
REPORT_CONSUMER_GROUP = "oracle_confidence_group"
REPORT_CONSUMER_NAME = "oracle_confidence_worker"

# 🔸 Стрим «готово для sense»
SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
SENSE_REPORT_READY_MAXLEN = 10000

# 🔸 Геометрия окна (шаг 4 часа → 6 прогонов в сутки)
WINDOW_STEPS = {"7d": 7 * 6, "14d": 14 * 6, "28d": 28 * 6}

# 🔸 Параметры статистики
Z = 1.96
BASELINE_WR = 0.5

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

    log.info("🚀 Старт воркера confidence (пакет по window_end)")

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=REPORT_CONSUMER_GROUP,
                consumername=REPORT_CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            # обработка сообщений
            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        # из сообщения берём strategy_id и window_end — это наш ключ комплекта
                        strategy_id = int(payload.get("strategy_id", 0))
                        window_end = payload.get("window_end")
                        if not (strategy_id and window_end):
                            log.debug("ℹ️ Пропуск сообщения: нет strategy_id/window_end: %s", payload)
                            await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, msg_id)
                            continue

                        await _process_window_batch(strategy_id, window_end)
                        await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения из Redis Stream")

        except asyncio.CancelledError:
            log.info("⏹️ Воркер confidence остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла confidence — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Публикация события «отчёт готов для sense» в Redis Stream
async def _emit_sense_report_ready(
    *,
    report_ids: Dict[str, int],
    strategy_id: int,
    window_end: str,
    aggregate_rows: int,
):
    # собираем пейлоад
    payload = {
        "strategy_id": int(strategy_id),
        "time_frames": {"7d": report_ids.get("7d"), "14d": report_ids.get("14d"), "28d": report_ids.get("28d")},
        "window_end": window_end,
        "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
        "aggregate_rows": int(aggregate_rows),
    }
    fields = {"data": json.dumps(payload, separators=(",", ":"))}
    await infra.redis_client.xadd(
        name=SENSE_REPORT_READY_STREAM,
        fields=fields,
        maxlen=SENSE_REPORT_READY_MAXLEN,
        approximate=True,
    )
    log.info("[SENSE_REPORT_READY] sid=%s window_end=%s rows=%d", strategy_id, window_end, aggregate_rows)


# 🔸 Пакетная обработка одного комплекта окон (ключ = strategy_id + window_end)
async def _process_window_batch(strategy_id: int, window_end_iso: str):
    # идемпотентность: отметим комплект как «обрабатывается впервые»
    async with infra.pg_pool.acquire() as conn:
        # сначала проверим, что комплект (7d/14d/28d) вообще собран
        rows = await conn.fetch(
            """
            SELECT id, time_frame, created_at
            FROM oracle_report_stat
            WHERE strategy_id = $1
              AND window_end = $2::timestamp
              AND time_frame IN ('7d','14d','28d')
            """,
            int(strategy_id), str(window_end_iso)
        )
        if len(rows) < 3:
            log.debug("⌛ Комплект не готов: sid=%s window_end=%s (нашли %d из 3)", strategy_id, window_end_iso, len(rows))
            return

        # вставка-маркер: если уже есть запись — выходим (идемпотентность)
        inserted = await conn.fetchrow(
            """
            INSERT INTO oracle_conf_processed (strategy_id, window_end)
            VALUES ($1, $2::timestamp)
            ON CONFLICT DO NOTHING
            RETURNING 1
            """,
            int(strategy_id), str(window_end_iso)
        )
        if not inserted:
            log.info("⏭️ Пропуск: комплект уже обработан (sid=%s window_end=%s)", strategy_id, window_end_iso)
            return

        report_ids = {str(r["time_frame"]): int(r["id"]) for r in rows}

        # локальный «снэпшот» весов на время батча (стабильность в пределах комплекта)
        batch_weights: Dict[str, Tuple[Dict[str, float], Dict]] = {}
        for tf in ("7d", "14d", "28d"):
            w, o = await _get_active_weights(conn, strategy_id, tf)
            batch_weights[tf] = (w, o)

        # берём все строки агрегатов по трём отчётам
        agg_rows = await conn.fetch(
            """
            SELECT
              a.id,
              a.report_id,
              a.strategy_id,
              a.time_frame,
              a.direction,
              a.timeframe,
              a.agg_type,
              a.agg_base,
              a.agg_state,
              a.trades_total,
              a.trades_wins,
              a.winrate,
              a.avg_pnl_per_trade,
              r.created_at AS report_created_at
            FROM oracle_mw_aggregated_stat a
            JOIN oracle_report_stat r ON r.id = a.report_id
            WHERE a.report_id = ANY($1::bigint[])
            """,
            list(report_ids.values())
        )
        if not agg_rows:
            log.debug("ℹ️ Нет агрегатов для комплекта: sid=%s window_end=%s", strategy_id, window_end_iso)
            return

        # кэши когорты на один отчёт (срез) — ключ без agg_state
        cohort_cache: Dict[Tuple, List[dict]] = {}

        updated = 0
        for r in agg_rows:
            row = dict(r)

            # ключ когорты для ECDF(n)/S (на уровне одного отчёта)
            cohort_key = (
                row["strategy_id"], row["time_frame"], row["direction"],
                row["timeframe"], row["agg_type"], row["agg_base"], row["report_created_at"]
            )
            if cohort_key not in cohort_cache:
                cohort_cache[cohort_key] = await _fetch_cohort(conn, row)

            # берём веса из локального снэпшота для конкретного окна строки
            weights, opts = batch_weights.get(row["time_frame"], ({"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}, {"baseline_mode": "neutral"}))

            try:
                confidence, inputs = await _calc_confidence_by_window_end(
                    conn=conn,
                    row=row,
                    cohort_rows=cohort_cache[cohort_key],
                    weights=weights,
                    opts=opts,
                    window_end_iso=window_end_iso,
                    report_ids=report_ids,
                )
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
                # аудит (best-effort)
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
                    log.debug("Аудит недоступен или пропущен (aggregated_id=%s)", row["id"])

                updated += 1
            except Exception:
                log.exception("❌ Ошибка обновления confidence для aggregated_id=%s", row["id"])

        log.info("✅ Обновлён confidence (пакет): sid=%s window_end=%s rows=%d", strategy_id, window_end_iso, updated)

    # публикация «готово для sense» — только если мы реально обработали комплект (и дошли сюда)
    try:
        await _emit_sense_report_ready(
            report_ids=report_ids,
            strategy_id=strategy_id,
            window_end=window_end_iso,
            aggregate_rows=updated,
        )
    except Exception:
        log.exception("❌ Ошибка публикации события в %s (sid=%s window_end=%s)", SENSE_REPORT_READY_STREAM, strategy_id, window_end_iso)


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


# 🔸 Загрузка активных весов из БД (с простым кэшем и безопасным парсингом JSON)
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

    # дефолты
    defaults_w = {"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}
    defaults_o = {"baseline_mode": "neutral"}

    def _parse_json_like(x, default):
        if isinstance(x, dict):
            return x
        if isinstance(x, (bytes, bytearray, memoryview)):
            try:
                return json.loads(bytes(x).decode("utf-8"))
            except Exception:
                log.exception("⚠️ Не удалось распарсить JSON из bytes/memoryview")
                return default
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                log.exception("⚠️ Не удалось распарсить JSON из строки")
                return default
        return default

    if row:
        raw_w = row["weights"]
        raw_o = row["opts"]
        weights = _parse_json_like(raw_w, defaults_w)
        opts = _parse_json_like(raw_o, defaults_o)
    else:
        weights = defaults_w
        opts = defaults_o

    # приведение и валидация весов + мягкие ограничения
    wR = float(weights.get("wR", defaults_w["wR"]))
    wP = float(weights.get("wP", defaults_w["wP"]))
    wC = float(weights.get("wC", defaults_w["wC"]))
    wS = float(weights.get("wS", defaults_w["wS"]))

    # клиппинг, чтобы C не доминировал, а R не деградировал
    wC = min(wC, 0.35)
    wR = max(wR, 0.25)

    total = wR + wP + wC + wS
    if not math.isfinite(total) or total <= 0:
        wR, wP, wC, wS = defaults_w["wR"], defaults_w["wP"], defaults_w["wC"], defaults_w["wS"]
        total = wR + wP + wC + wS

    wR, wP, wC, wS = (wR / total, wP / total, wC / total, wS / total)
    weights_norm = {"wR": wR, "wP": wP, "wC": wC, "wS": wS}

    ts = time.time()
    _weights_cache[(strategy_id, time_frame)] = (weights_norm, opts, ts)
    _weights_cache[(strategy_id, None)] = (weights_norm, opts, ts)
    _weights_cache[(None, None)] = (weights_norm, opts, ts)

    return weights_norm, opts


# 🔸 Расчёт confidence для строки (по комплекту окон с общим window_end)
async def _calc_confidence_by_window_end(
    *,
    conn,
    row: dict,
    cohort_rows: List[dict],
    weights: Dict[str, float],
    opts: Dict,
    window_end_iso: str,
    report_ids: Dict[str, int],
) -> Tuple[float, dict]:
    n = int(row["trades_total"] or 0)
    wins = int(row["trades_wins"] or 0)
    wr = float(row["winrate"] or 0.0)

    # Reliability (R)
    R = _wilson_lower_bound(wins, n, Z) if n > 0 else 0.0

    # Persistence (P): по текущему окну строки
    L = WINDOW_STEPS.get(str(row["time_frame"]), 42)
    presence_rate, growth_hist, hist_n = await _persistence_metrics(conn, row, L)
    P = 0.6 * presence_rate + 0.4 * growth_hist

    # Cross-window coherence (C) по трём отчётам из одного window_end
    C = await _cross_window_coherence_by_ids(conn, row, report_ids)

    # Stability (S): робастная устойчивость wr (динамическая шкала)
    S_key, _len_hist, dyn_scale_used = await _stability_key_dynamic(conn, row, L, cohort_rows)

    # Масса / N_effect: ECDF по когорте + абсолютная масса относительно медианы
    cohort_n = [int(x["trades_total"] or 0) for x in cohort_rows]
    ecdf_cohort = _ecdf_rank(n, cohort_n)
    ecdf_hist = _ecdf_rank(n, hist_n) if hist_n else 0.0
    if len(cohort_n) < 5:
        N_effect = 0.5 * ecdf_cohort + 0.5 * ecdf_hist
    else:
        N_effect = ecdf_cohort
    floor = 1.0 / float(max(2, len(cohort_n)) + 1)
    N_effect = max(N_effect, floor)
    N_effect = float(max(0.0, min(1.0, N_effect)))
    n_pos = [v for v in cohort_n if v > 0]
    n_med = _median([float(v) for v in n_pos]) if n_pos else 1.0
    abs_mass = math.sqrt(n / (n + n_med)) if (n_med > 0 and n >= 0) else 0.0
    N_effect = float(max(0.0, min(1.0, N_effect * abs_mass)))

    # Веса
    wR = float(weights.get("wR", 0.4))
    wP = float(weights.get("wP", 0.25))
    wC = float(weights.get("wC", 0.2))
    wS = float(weights.get("wS", 0.15))

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
        "window_end": window_end_iso,
        "formula": "(wR*R + wP*P + wC*C + wS*S) * N_effect",
    }
    return confidence, inputs


# 🔸 C по конкретному комплекту report_id (7d/14d/28d) для ключа строки
async def _cross_window_coherence_by_ids(conn, row: dict, report_ids: Dict[str, int]) -> float:
    # собираем для текущего ключа строки (direction/timeframe/agg_type/base/state) показатели по каждому report_id
    rows = await conn.fetch(
        """
        SELECT a.time_frame, a.trades_total, a.trades_wins
        FROM oracle_mw_aggregated_stat a
        WHERE a.report_id = ANY($1::bigint[])
          AND a.strategy_id = $2
          AND a.direction   = $3
          AND a.timeframe   = $4
          AND a.agg_type    = $5
          AND a.agg_base    = $6
          AND a.agg_state   = $7
        """,
        list(report_ids.values()),
        row["strategy_id"], row["direction"], row["timeframe"],
        row["agg_type"], row["agg_base"], row["agg_state"]
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
        # уверенно выше baseline → +1
        if lb > BASELINE_WR:
            dist = max(lb - BASELINE_WR, ub - BASELINE_WR)
            if dist > 0:
                signs.append(+1); weights.append(dist)
        # уверенно ниже baseline → -1
        elif ub < BASELINE_WR:
            dist = max(BASELINE_WR - lb, BASELINE_WR - ub)
            if dist > 0:
                signs.append(-1); weights.append(dist)
        # иначе окно неопределённое — пропускаем

    total_weight = sum(weights)
    # требуем минимум 2 уверенных окна
    if total_weight <= 0.0 or len(weights) < 2:
        return 0.0

    signed_weight = sum(s * w for s, w in zip(signs, weights))
    C = abs(signed_weight) / total_weight
    return float(max(0.0, min(1.0, C)))


# 🔸 Persistence-метрики: presence_rate и growth_hist (ECDF по историческим n)
async def _persistence_metrics(conn, row: dict, L: int) -> Tuple[float, float, List[int]]:
    # последние L отчётов (created_at) по стратегии/окну до и включая текущий отчёт
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

    present_flags = [1 if r["trades_total"] is not None else 0 for r in last_rows]
    L_eff = len(present_flags) if present_flags else 0
    presence_rate = (sum(present_flags) / L_eff) if L_eff > 0 else 0.0

    hist_n = [int(r["trades_total"]) for r in last_rows if r["trades_total"] is not None]
    growth_hist = _ecdf_rank(int(row["trades_total"] or 0), hist_n) if hist_n else 0.0

    return presence_rate, growth_hist, hist_n


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

    wr_cohort = [float(x["winrate"] or 0.0) for x in cohort_rows] if cohort_rows else []
    cohort_mad = _mad(wr_cohort, _median(wr_cohort)) if len(wr_cohort) >= 3 else 0.0

    cand = []
    if mad > 0:
        cand.append(mad / 0.6745)
    if iqr > 0:
        cand.append(iqr / 1.349)
    if cohort_mad > 0:
        cand.append(cohort_mad / 0.6745)

    n_hist = len(wr_hist)
    cand.append(1.0 / math.sqrt(max(1.0, float(n_hist))))

    if all(c <= 0 for c in cand[:-1]) and abs(wr_now - med) < 1e-12:
        return 1.0, n_hist, {"mode": "flat_hist", "scale": 0.0}

    scale = max(cand) if cand else 1e-6
    z = abs(wr_now - med) / (scale + 1e-12)
    S_key = 1.0 / (1.0 + z)

    return S_key, n_hist, {"mode": "dynamic", "scale": round(scale, 6), "median": round(med, 6)}


# 🔸 Wilson lower bound / bounds
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    return max(0.0, min(1.0, lb))


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


# 🔸 ECDF-ранг / базовые статистики
def _ecdf_rank(x: int, values: List[int]) -> float:
    if not values:
        return 0.0
    cnt = sum(1 for v in values if v <= x)
    return cnt / len(values)


def _median(arr: List[float]) -> float:
    n = len(arr)
    if n == 0:
        return 0.0
    s = sorted(arr)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])


def _mad(arr: List[float], med: float) -> float:
    if not arr:
        return 0.0
    dev = [abs(x - med) for x in arr]
    return _median(dev)


def _iqr(arr: List[float]) -> float:
    n = len(arr)
    if n < 4:
        return 0.0
    s = sorted(arr)
    q1 = _percentile(s, 25.0)
    q3 = _percentile(s, 75.0)
    return max(0.0, q3 - q1)


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