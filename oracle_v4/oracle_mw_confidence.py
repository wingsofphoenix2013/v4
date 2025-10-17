# oracle_mw_confidence.py — воркер MW-confidence: пакетный расчёт R/P/C/S по комплекту (7d+14d+28d), батч-SQL, параллелизм (max=2), идемпотентность «второго шанса»

# 🔸 Импорты
import asyncio
import logging
import json
import math
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_CONFIDENCE")

# 🔸 Стримы
REPORT_STREAM = "oracle:mw:reports_ready"
REPORT_CONSUMER_GROUP = "oracle_confidence_group"
REPORT_CONSUMER_NAME = "oracle_confidence_worker"

SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
SENSE_REPORT_READY_MAXLEN = 10_000

# 🔸 Параллелизм
MAX_CONCURRENT_STRATEGIES = 2  # одновременно обрабатываем до 2 стратегий (комплектов window_end)

# 🔸 Геометрия окна (шаг 6 часов → 4 прогона в сутки)
WINDOW_STEPS = {"7d": 7 * 4, "14d": 14 * 4, "28d": 28 * 4}

# 🔸 Параметры статистики
Z = 1.96
BASELINE_WR = 0.55

# 🔸 Кэш весов модели (strategy_id,time_frame) → (weights, opts, ts)
_weights_cache: Dict[Tuple[Optional[int], Optional[str]], Tuple[Dict[str, float], Dict, float]] = {}
WEIGHTS_TTL_SEC = 15 * 60  # 15 минут


# 🔸 Публичная точка входа воркера (переписанная версия с параллелизмом и батчами)
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
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", REPORT_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    sem = asyncio.Semaphore(MAX_CONCURRENT_STRATEGIES)
    log.debug("🚀 Старт воркера MW-confidence (max_parallel_strategies=%d)", MAX_CONCURRENT_STRATEGIES)

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=REPORT_CONSUMER_GROUP,
                consumername=REPORT_CONSUMER_NAME,
                streams={REPORT_STREAM: ">"},
                count=128,
                block=30_000,
            )
            if not resp:
                continue

            # группируем сообщения по (strategy_id, window_end)
            batches: Dict[Tuple[int, str], List[Tuple[str, dict]]] = {}
            for _, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        strategy_id = int(payload.get("strategy_id", 0))
                        window_end = payload.get("window_end")
                        if not (strategy_id and window_end):
                            await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, msg_id)
                            continue
                        batches.setdefault((strategy_id, window_end), []).append((msg_id, payload))
                    except Exception:
                        log.exception("❌ Ошибка парсинга сообщения из Redis Stream")

            tasks = []
            for (sid, window_end), items in batches.items():
                tasks.append(_process_window_batch_guard(sem, items, sid, window_end))
            if tasks:
                await asyncio.gather(*tasks)

        except asyncio.CancelledError:
            log.debug("⏹️ Воркер confidence остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла confidence — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Гард: семафор + ловим исключения
async def _process_window_batch_guard(sem: asyncio.Semaphore, items: List[Tuple[str, dict]], strategy_id: int, window_end_iso: str):
    async with sem:
        try:
            await _process_window_batch(items, strategy_id, window_end_iso)
        except Exception:
            log.exception("❌ Сбой обработки комплекта sid=%s window_end=%s", strategy_id, window_end_iso)


# 🔸 Пакетная обработка одного комплекта (ключ = strategy_id + window_end)
async def _process_window_batch(items: List[Tuple[str, dict]], strategy_id: int, window_end_iso: str):
    # привести ISO-строку к datetime (UTC-naive)
    try:
        window_end_dt = datetime.fromisoformat(window_end_iso.replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        # ACK всех сообщений комплекта, чтобы не зависали без шансов
        await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, *[mid for (mid, _) in items])
        return

    async with infra.pg_pool.acquire() as conn:
        # ранний гейт идемпотентности — второй шанс, если появились строки без confidence
        already = await conn.fetchval(
            """
            SELECT 1 FROM oracle_conf_processed
            WHERE strategy_id = $1 AND window_end = $2
            """,
            int(strategy_id), window_end_dt
        )
        if already:
            missing = await conn.fetchval(
                """
                SELECT COUNT(*)::int
                  FROM oracle_mw_aggregated_stat a
                  JOIN oracle_report_stat r ON r.id = a.report_id
                 WHERE r.strategy_id = $1
                   AND r.window_end  = $2
                   AND (a.confidence IS NULL OR a.confidence_inputs IS NULL)
                """,
                int(strategy_id), window_end_dt
            )
            if int(missing or 0) == 0:
                await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, *[mid for (mid, _) in items])
                log.debug("⏭️ Пропуск комплекта: уже обработан, недостающих confidence нет (sid=%s window_end=%s)", strategy_id, window_end_iso)
                return
            # иначе продолжаем расчёт повторно (добиваем пропуски)

        # проверяем, что комплект 7d/14d/28d готов по шапкам (source='mw')
        rows = await conn.fetch(
            """
            SELECT id, time_frame, created_at
              FROM oracle_report_stat
             WHERE strategy_id = $1
               AND window_end  = $2
               AND time_frame IN ('7d','14d','28d')
               AND source = 'mw'
            """,
            int(strategy_id), window_end_dt
        )
        if len(rows) < 3:
            log.debug("⌛ Комплект не готов (шапки): sid=%s window_end=%s (нашли %d из 3)", strategy_id, window_end_iso, len(rows))
            return

        # три report_id по окнам
        report_ids: Dict[str, int] = {str(r["time_frame"]): int(r["id"]) for r in rows}

        # проверяем, что у каждого окна есть строки в oracle_mw_aggregated_stat
        cnt_rows = await conn.fetch(
            """
            SELECT report_id, COUNT(*)::int AS cnt
              FROM oracle_mw_aggregated_stat
             WHERE report_id = ANY($1::bigint[])
             GROUP BY report_id
            """,
            list(report_ids.values())
        )
        counts = {int(r["report_id"]): int(r["cnt"]) for r in cnt_rows}
        if any(counts.get(rid, 0) <= 0 for rid in report_ids.values()):
            log.debug("⌛ Комплект не готов (агрегаты): sid=%s window_end=%s cnts=%s", strategy_id, window_end_iso, counts)
            return

        # предзагрузка активных весов (стабильны в рамках комплекта)
        weights_by_tf: Dict[str, Tuple[Dict[str, float], Dict]] = {}
        for tf in ("7d", "14d", "28d"):
            w, o = await _get_active_weights(conn, strategy_id, tf)
            weights_by_tf[tf] = (w, o)

        # забираем ВСЕ агрегаты по трём отчётам одним запросом
        agg_rows = await conn.fetch(
            """
            SELECT
              a.id, a.report_id, a.strategy_id, a.time_frame,
              a.direction, a.timeframe, a.agg_type, a.agg_base, a.agg_state,
              a.trades_total, a.trades_wins, a.winrate, a.avg_pnl_per_trade,
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

        # подготовим когорты: ключ = без agg_state + report_created_at
        # (strategy_id, time_frame, direction, timeframe, agg_type, agg_base, report_created_at)
        cohort_keys: List[Tuple] = []
        for r in agg_rows:
            cohort_keys.append((
                r["strategy_id"], r["time_frame"], r["direction"], r["timeframe"],
                r["agg_type"], r["agg_base"], r["report_created_at"]
            ))
        cohort_keys = list({ck for ck in cohort_keys})

        # загрузим когорты батчами через UNNEST из v_mw_aggregated_with_time
        cohort_cache: Dict[Tuple, List[dict]] = {}
        if cohort_keys:
            BATCH = 200
            for i in range(0, len(cohort_keys), BATCH):
                chunk = cohort_keys[i:i+BATCH]
                sid_a       = [ck[0] for ck in chunk]
                tf_a        = [ck[1] for ck in chunk]
                dir_a       = [ck[2] for ck in chunk]
                timeframe_a = [ck[3] for ck in chunk]
                aggt_a      = [ck[4] for ck in chunk]
                aggb_a      = [ck[5] for ck in chunk]
                rcat_a      = [ck[6] for ck in chunk]

                rows_coh = await conn.fetch(
                    """
                    WITH keys AS (
                      SELECT
                        unnest($1::int[])        AS k_sid,
                        unnest($2::text[])       AS k_tf,
                        unnest($3::text[])       AS k_dir,
                        unnest($4::text[])       AS k_timeframe,
                        unnest($5::text[])       AS k_aggt,
                        unnest($6::text[])       AS k_aggb,
                        unnest($7::timestamp[])  AS k_rcat
                    )
                    SELECT
                      v.strategy_id, v.time_frame, v.direction, v.timeframe,
                      v.agg_type, v.agg_base, v.report_created_at,
                      v.id, v.trades_total, v.trades_wins, v.winrate, v.avg_pnl_per_trade
                    FROM v_mw_aggregated_with_time v
                    JOIN keys k ON
                         v.strategy_id       = k.k_sid
                     AND v.time_frame        = k.k_tf
                     AND v.direction         = k.k_dir
                     AND v.timeframe         = k.k_timeframe
                     AND v.agg_type          = k.k_aggt
                     AND v.agg_base          = k.k_aggb
                     AND v.report_created_at = k.k_rcat
                    """,
                    sid_a, tf_a, dir_a, timeframe_a, aggt_a, aggb_a, rcat_a
                )
                for rr in rows_coh:
                    ck = (
                        rr["strategy_id"], rr["time_frame"], rr["direction"], rr["timeframe"],
                        rr["agg_type"], rr["agg_base"], rr["report_created_at"]
                    )
                    cohort_cache.setdefault(ck, []).append(dict(rr))

        # persistence-матрицы (последние L отчётов) для каждого окна
        persistence_by_tf: Dict[str, Dict[Tuple, List[Optional[int]]]] = {}
        for tf in ("7d", "14d", "28d"):
            rep_created = None
            for r in rows:
                if str(r["time_frame"]) == tf:
                    rep_created = r["created_at"]; break
            L = int(WINDOW_STEPS.get(tf, 42))
            persistence_by_tf[tf] = await _persistence_matrix_mw(conn, strategy_id, tf, rep_created, L) if rep_created else {}

        # сгруппируем строки по ключу для C (кросс-оконная согласованность)
        rows_by_key: Dict[Tuple, List[dict]] = {}
        agg_list = [dict(r) for r in agg_rows]
        for r in agg_list:
            kC = (
                r["strategy_id"], r["direction"], r["timeframe"],
                r["agg_type"], r["agg_base"], r["agg_state"]
            )
            rows_by_key.setdefault(kC, []).append(r)

        # расчёт и запись — атомарно
        updated_per_report: Dict[int, int] = {rid: 0 for rid in report_ids.values()}
        ids, confs, inputs = [], [], []

        for r in agg_list:
            n = int(r["trades_total"] or 0)
            w = int(r["trades_wins"] or 0)
            wr = float(r["winrate"] or 0.0)

            # веса на окно
            weights, opts = weights_by_tf.get(
                str(r["time_frame"]),
                ({"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}, {"baseline_mode": "neutral"})
            )

            # R
            R = _wilson_lower_bound(w, n, Z) if n > 0 else 0.0

            # P
            L = int(WINDOW_STEPS.get(str(r["time_frame"]), 42))
            keyP = (r["direction"], r["timeframe"], r["agg_type"], r["agg_base"], r["agg_state"])
            hist_n = persistence_by_tf.get(str(r["time_frame"]), {}).get(keyP, [])
            present_flags = [1 if v is not None and int(v) > 0 else 0 for v in (hist_n or [])]
            L_eff = len(present_flags)
            presence_rate = (sum(present_flags) / L_eff) if L_eff > 0 else 0.0
            hist_vals = [int(v) for v in (hist_n or []) if v is not None]
            growth_hist = _ecdf_rank(int(n), hist_vals) if hist_vals else 0.0
            P = 0.6 * presence_rate + 0.4 * growth_hist

            # C
            C = _coherence_from_rows_mw(rows_by_key.get((
                r["strategy_id"], r["direction"], r["timeframe"],
                r["agg_type"], r["agg_base"], r["agg_state"]
            ), []))

            # S (стабильность) — по когорте (все agg_state в одной оси на одном отчёте)
            cohort_key = (
                r["strategy_id"], r["time_frame"], r["direction"], r["timeframe"],
                r["agg_type"], r["agg_base"], r["report_created_at"]
            )
            cohort_rows = cohort_cache.get(cohort_key, [])
            S, _hist_len, dyn_meta = _stability_key_dynamic_mw(r, L, cohort_rows)

            # N_effect (масса)
            cohort_n = [int(x.get("trades_total") or 0) for x in cohort_rows]
            ecdf_cohort = _ecdf_rank(n, cohort_n) if cohort_n else 0.0
            ecdf_hist = _ecdf_rank(int(n), hist_vals) if hist_vals else 0.0
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

            # веса (безопасная нормировка)
            wR = float(weights.get("wR", 0.4)); wP = float(weights.get("wP", 0.25))
            wC = float(weights.get("wC", 0.2));  wS = float(weights.get("wS", 0.15))
            total_w = wR + wP + wC + wS
            if not math.isfinite(total_w) or total_w <= 0:
                wR, wP, wC, wS = 0.4, 0.25, 0.2, 0.15
                total_w = 1.0
            wR, wP, wC, wS = (wR / total_w, wP / total_w, wC / total_w, wS / total_w)

            raw = wR * R + wP * P + wC * C + wS * S
            confidence = round(max(0.0, min(1.0, raw * N_effect)), 4)

            inputs_json = {
                "R": round(R, 6), "P": round(P, 6), "C": round(C, 6), "S": round(S, 6),
                "N_effect": round(N_effect, 6),
                "weights": {"wR": wR, "wP": wP, "wC": wC, "wS": wS},
                "n": n, "wins": w, "wr": round(wr, 6),
                "presence_rate": round(presence_rate, 6), "growth_hist": round(growth_hist, 6),
                "hist_points": len(hist_vals), "dyn_scale_used": dyn_meta,
                "baseline_wr": BASELINE_WR, "window_end": window_end_iso,
                "formula": "(wR*R + wP*P + wC*C + wS*S) * N_effect",
            }

            ids.append(int(r["id"]))
            confs.append(float(confidence))
            try:
                inputs.append(json.dumps(inputs_json, separators=(",", ":")))
            except Exception:
                inputs.append(json.dumps({"error": "encode_failed"}))

            updated_per_report[int(r["report_id"])] = updated_per_report.get(int(r["report_id"]), 0) + 1

        # атомарная запись: UPDATE + маркер processed
        async with conn.transaction():
            if ids:
                await conn.executemany(
                    """
                    UPDATE oracle_mw_aggregated_stat
                       SET confidence = $2,
                           confidence_inputs = $3,
                           confidence_updated_at = now()
                     WHERE id = $1
                    """,
                    list(zip(ids, confs, inputs))
                )
            await conn.execute(
                """
                INSERT INTO oracle_conf_processed (strategy_id, window_end)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                int(strategy_id), window_end_dt
            )

        # лог
        log.debug(
            "✅ Обновлён confidence (MW): sid=%s window_end=%s rows_total=%d rows_7d=%d rows_14d=%d rows_28d=%d",
            strategy_id, window_end_iso, len(ids),
            updated_per_report.get(report_ids.get("7d", -1), 0),
            updated_per_report.get(report_ids.get("14d", -1), 0),
            updated_per_report.get(report_ids.get("28d", -1), 0),
        )

        # публикация ТРЁХ событий — по одному на каждый отчёт (для sense-воркера)
        try:
            for tf in ("7d", "14d", "28d"):
                rid = report_ids[tf]
                _rows = updated_per_report.get(rid, 0)
                payload = {
                    "report_id": int(rid),
                    "strategy_id": int(strategy_id),
                    "time_frame": tf,
                    "window_end": window_end_iso,
                    "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                    "aggregate_rows": int(_rows),
                }
                await infra.redis_client.xadd(
                    name=SENSE_REPORT_READY_STREAM,
                    fields={"data": json.dumps(payload, separators=(",", ":"))},
                    maxlen=SENSE_REPORT_READY_MAXLEN,
                    approximate=True,
                )
            log.debug("[MW_SENSE_REPORT_READY] sid=%s window_end=%s done", strategy_id, window_end_iso)
        except Exception:
            log.exception("❌ Ошибка публикации событий в %s", SENSE_REPORT_READY_STREAM)

        # ACK всех сообщений комплекта — только после успешного commit
        await infra.redis_client.xack(REPORT_STREAM, REPORT_CONSUMER_GROUP, *[mid for (mid, _) in items])


# 🔸 Загрузка активных весов из БД (с простым кэшем и безопасным парсингом JSON)
async def _get_active_weights(conn, strategy_id: int, time_frame: str) -> Tuple[Dict[str, float], Dict]:
    now = time.time()

    for key in [(strategy_id, time_frame), (strategy_id, None), (None, None)]:
        w = _weights_cache.get(key)
        if w and (now - w[2] < WEIGHTS_TTL_SEC):
            return w[0], w[1]

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

    defaults_w = {"wR": 0.4, "wP": 0.25, "wC": 0.2, "wS": 0.15}
    defaults_o = {"baseline_mode": "neutral"}

    def _parse_json_like(x, default):
        if isinstance(x, dict): return x
        if isinstance(x, (bytes, bytearray, memoryview)):
            try: return json.loads(bytes(x).decode("utf-8"))
            except Exception:
                log.exception("⚠️ Не удалось распарсить JSON из bytes/memoryview"); return default
        if isinstance(x, str):
            try: return json.loads(x)
            except Exception:
                log.exception("⚠️ Не удалось распарсить JSON из строки"); return default
        return default

    if row:
        weights = _parse_json_like(row["weights"], defaults_w)
        opts    = _parse_json_like(row["opts"], defaults_o)
    else:
        weights = defaults_w; opts = defaults_o

    # мягкие ограничения + нормировка
    wR = float(weights.get("wR", defaults_w["wR"]))
    wP = float(weights.get("wP", defaults_w["wP"]))
    wC = float(weights.get("wC", defaults_w["wC"]))
    wS = float(weights.get("wS", defaults_w["wS"]))

    wC = min(wC, 0.35)
    wR = max(wR, 0.25)

    total = wR + wP + wC + wS
    if not math.isfinite(total) or total <= 0:
        wR, wP, wC, wS = defaults_w["wR"], defaults_w["wP"], defaults_w["wC"], defaults_w["wS"]
        total = 1.0

    wR, wP, wC, wS = (wR / total, wP / total, wC / total, wS / total)
    weights_norm = {"wR": wR, "wP": wP, "wC": wC, "wS": wS}

    ts = time.time()
    _weights_cache[(strategy_id, time_frame)] = (weights_norm, opts, ts)
    _weights_cache[(strategy_id, None)]      = (weights_norm, opts, ts)
    _weights_cache[(None, None)]             = (weights_norm, opts, ts)
    return weights_norm, opts


# 🔸 Матрица persistence (MW): для каждого ключа → список n за последние L отчётов до cutoff
async def _persistence_matrix_mw(conn, strategy_id: int, time_frame: str, cutoff_created_at, L: int) -> Dict[Tuple, List[Optional[int]]]:
    if cutoff_created_at is None:
        return {}

    last_reports = await conn.fetch(
        """
        SELECT id, created_at
          FROM oracle_report_stat
         WHERE strategy_id = $1
           AND time_frame  = $2
           AND created_at <= $3
           AND source = 'mw'
         ORDER BY created_at DESC
         LIMIT $4
        """,
        int(strategy_id), str(time_frame), cutoff_created_at, int(L)
    )
    if not last_reports:
        return {}

    rep_ids = [int(r["id"]) for r in last_reports]

    rows = await conn.fetch(
        """
        SELECT
          a.report_id,
          a.direction, a.timeframe, a.agg_type, a.agg_base, a.agg_state,
          a.trades_total
        FROM oracle_mw_aggregated_stat a
        WHERE a.report_id = ANY($1::bigint[])
          AND a.strategy_id = $2
        """,
        rep_ids, int(strategy_id)
    )

    rep_order = {int(r["id"]): idx for idx, r in enumerate(last_reports)}  # 0 — самый свежий

    mat: Dict[Tuple, List[Optional[int]]] = {}
    for r in rows:
        key = (r["direction"], r["timeframe"], r["agg_type"], r["agg_base"], r["agg_state"])
        arr = mat.setdefault(key, [None] * len(last_reports))
        pos = rep_order.get(int(r["report_id"]))
        if pos is not None:
            arr[pos] = int(r["trades_total"] or 0)

    return mat


# 🔸 Coherence C для MW (по одному ключу на трёх окнах)
def _coherence_from_rows_mw(rows: List[dict]) -> float:
    if not rows:
        return 0.0
    signs: List[int] = []
    weights: List[float] = []
    for r in rows:
        n = int(r.get("trades_total") or 0)
        w = int(r.get("trades_wins") or 0)
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


# 🔸 Стабильность ключа (MW): робастный z по истории wr с динамической шкалой
def _stability_key_dynamic_mw(row: dict, L: int, cohort_rows: List[dict]) -> Tuple[float, int, dict]:
    wr_hist = [float(r.get("winrate") or 0.0) for r in cohort_rows]
    wr_now = float(row.get("winrate") or 0.0)
    if len(wr_hist) < 2:
        return 1.0, len(wr_hist), {"mode": "short_hist", "scale": None}

    med = _median(wr_hist)
    mad = _mad(wr_hist, med)
    iqr = _iqr(wr_hist)

    # MAD по когорте
    cohort_mad = _mad(wr_hist, med) if len(wr_hist) >= 3 else 0.0

    cand = []
    if mad > 0: cand.append(mad / 0.6745)
    if iqr > 0: cand.append(iqr / 1.349)
    if cohort_mad > 0: cand.append(cohort_mad / 0.6745)

    n_hist = len(wr_hist)
    cand.append(1.0 / math.sqrt(max(1.0, float(n_hist))))

    if all(c <= 0 for c in cand[:-1]) and abs(wr_now - med) < 1e-12:
        return 1.0, n_hist, {"mode": "flat_hist", "scale": 0.0}

    scale = max(cand) if cand else 1e-6
    z = abs(wr_now - med) / (scale + 1e-12)
    S_key = 1.0 / (1.0 + z)
    return S_key, n_hist, {"mode": "dynamic", "scale": round(scale, 6), "median": round(med, 6)}


# 🔸 Базовые статистики и утилиты
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n <= 0: return 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    return max(0.0, min(1.0, lb))

def _wilson_bounds(wins: int, n: int, z: float) -> tuple[float, float]:
    if n <= 0: return 0.0, 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    ub = (center + adj) / denom
    return max(0.0, min(1.0, lb)), max(0.0, min(1.0, ub))

def _ecdf_rank(x: int, values: List[int]) -> float:
    if not values: return 0.0
    cnt = sum(1 for v in values if v <= x)
    return cnt / len(values)

def _median(arr: List[float]) -> float:
    n = len(arr)
    if n == 0: return 0.0
    s = sorted(arr); mid = n // 2
    if n % 2 == 1: return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])

def _mad(arr: List[float], med: float) -> float:
    if not arr: return 0.0
    dev = [abs(x - med) for x in arr]
    return _median(dev)

def _iqr(arr: List[float]) -> float:
    n = len(arr)
    if n < 4: return 0.0
    s = sorted(arr)
    q1 = _percentile(s, 25.0)
    q3 = _percentile(s, 75.0)
    return max(0.0, q3 - q1)

def _percentile(sorted_arr: List[float], p: float) -> float:
    if not sorted_arr: return 0.0
    if p <= 0: return sorted_arr[0]
    if p >= 100: return sorted_arr[-1]
    k = (len(sorted_arr) - 1) * (p / 100.0)
    f = math.floor(k); c = math.ceil(k)
    if f == c: return sorted_arr[int(k)]
    d0 = sorted_arr[int(f)] * (c - k)
    d1 = sorted_arr[int(c)] * (k - f)
    return d0 + d1