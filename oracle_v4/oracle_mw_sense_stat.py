# oracle_mw_sense_stat.py — воркер sense-stat: оценка «разделяющей силы» agg_base (0..1) и формирование whitelist (7d)

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_SENSE_STAT")

# 🔸 Константы Redis Stream (инициация по готовности отчётов ДЛЯ sense)
SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
SENSE_CONSUMER_GROUP = "oracle_sense_stat_group"
SENSE_CONSUMER_NAME = "oracle_sense_stat_worker"

# 🔸 Константы Redis Stream для whitelist
WHITELIST_READY_STREAM = "oracle:mw_whitelist:reports_ready"
WHITELIST_READY_MAXLEN = 10_000

# 🔸 Константы расчёта sense
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")
AGG_BASES = (
    # solo
    "trend", "volatility", "extremes", "momentum",
    # pairs
    "trend_volatility", "trend_extremes", "trend_momentum",
    "volatility_extremes", "volatility_momentum",
    "extremes_momentum",
    # triples
    "trend_volatility_extremes",
    "trend_volatility_momentum",
    "trend_extremes_momentum",
    "volatility_extremes_momentum",
    # quadruple
    "trend_volatility_extremes_momentum",
)
SMOOTH_HISTORY_N = 5
CONF_THRESHOLD_SENSE = 0.1  # включаем в расчёт sense только состояния с confidence > 0.1
EPS = 1e-12

# 🔸 Пороговые значения для whitelist (настраиваются перезапуском сервиса)
SCORE_SENSE_MIN = 0.5    # база попадает в WL только если score_smoothed > 0.5
CONF_THRESHOLD_WL = 0.25 # строка агрегата (agg_state) — confidence > 0.25
WL_WR_MIN = 0.55         # минимальный winrate для попадания строки в whitelist (>= 0.55)

# 🔸 Пороговые значения для вычисления confirmation по confidence
CONFIRM_T0 = 0.75  # => confirmation = 0
CONFIRM_T1 = 0.50  # => confirmation = 1  (если conf в [0.50, 0.75))
CONFIRM_T2 = 0.25  # => confirmation = 2  (если conf в [0.25, 0.50))

# 🔸 Публичная точка входа воркера
async def run_oracle_sense_stat():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно, только новые сообщения)
    try:
        await infra.redis_client.xgroup_create(
            name=SENSE_REPORT_READY_STREAM,
            groupname=SENSE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана группа потребителей в Redis Stream: %s", SENSE_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера sense-stat")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=SENSE_CONSUMER_GROUP,
                consumername=SENSE_CONSUMER_NAME,
                streams={SENSE_REPORT_READY_STREAM: ">"},
                count=64,
                block=30_000,
            )
            if not resp:
                continue

            for stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        payload = json.loads(fields.get("data", "{}"))
                        report_id = int(payload.get("report_id", 0))
                        strategy_id = int(payload.get("strategy_id", 0))
                        time_frame = payload.get("time_frame")
                        window_end = payload.get("window_end")

                        if not (report_id and strategy_id and time_frame and window_end):
                            log.debug("ℹ️ Пропуск сообщения: недостаточно данных %s", payload)
                            await infra.redis_client.xack(SENSE_REPORT_READY_STREAM, SENSE_CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report(report_id, strategy_id, time_frame, window_end)
                        await infra.redis_client.xack(SENSE_REPORT_READY_STREAM, SENSE_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в sense-stat")

        except asyncio.CancelledError:
            log.debug("⏹️ Воркер sense-stat остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла sense-stat — пауза 5 секунд")
            await asyncio.sleep(5)

# 🔸 Обработка одного отчёта: расчёт sense-stat + (если 7d) построение whitelist
async def _process_report(report_id: int, strategy_id: int, time_frame: str, window_end_iso: str):
    # парсинг window_end
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # выборка агрегатов текущего отчёта (confidence > 0.1 для sense)
        rows = await conn.fetch(
            """
            SELECT timeframe, direction, agg_base, agg_state,
                   trades_total, trades_wins, winrate, confidence
              FROM oracle_mw_aggregated_stat
             WHERE report_id = $1
               AND confidence > $2
            """,
            report_id, CONF_THRESHOLD_SENSE
        )

        if not rows:
            log.debug("ℹ️ Нет строк (confidence>%s) для report_id=%s (sid=%s tf=%s)",
                     CONF_THRESHOLD_SENSE, report_id, strategy_id, time_frame)
            return

        # группировка по (timeframe, direction, agg_base)
        data: Dict[Tuple[str, str, str], List[dict]] = {}
        for r in rows:
            key = (r["timeframe"], r["direction"], r["agg_base"])
            data.setdefault(key, []).append({
                "agg_state": r["agg_state"],
                "n": int(r["trades_total"] or 0),
                "w": int(r["trades_wins"] or 0),
                "p": float(r["winrate"] or 0.0),
            })

        updated = 0
        for tf in TF_LIST:
            for direction in DIRECTIONS:
                for base in AGG_BASES:
                    states = data.get((tf, direction, base), [])
                    if not states:
                        continue  # только если есть хотя бы одно состояние

                    score_current, states_used, components = _compute_score(states)

                    # сглаживание по истории (≤5 предыдущих прогонов)
                    prev_vals = await conn.fetch(
                        """
                        SELECT score_current
                          FROM oracle_mw_sense_stat
                         WHERE strategy_id = $1
                           AND time_frame  = $2
                           AND timeframe   = $3
                           AND direction   = $4
                           AND agg_base    = $5
                           AND window_end  < $6
                         ORDER BY window_end DESC
                         LIMIT $7
                        """,
                        strategy_id, time_frame, tf, direction, base,
                        window_end_dt, int(SMOOTH_HISTORY_N)
                    )
                    hist = [float(x["score_current"]) for x in prev_vals] if prev_vals else []
                    score_smoothed = _smooth_mean(score_current, hist)

                    # запись/обновление строки sense
                    await conn.execute(
                        """
                        INSERT INTO oracle_mw_sense_stat (
                            report_id, strategy_id, time_frame, window_end,
                            timeframe, direction, agg_base,
                            states_used, score_current, score_smoothed, components,
                            created_at, updated_at
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),now()
                        )
                        ON CONFLICT (report_id, timeframe, direction, agg_base)
                        DO UPDATE SET
                            states_used    = EXCLUDED.states_used,
                            score_current  = EXCLUDED.score_current,
                            score_smoothed = EXCLUDED.score_smoothed,
                            components     = EXCLUDED.components,
                            updated_at     = now()
                        """,
                        report_id, strategy_id, time_frame, window_end_dt,
                        tf, direction, base,
                        int(states_used), float(score_current), float(score_smoothed),
                        json.dumps(components, separators=(",", ":"))
                    )
                    updated += 1

        log.debug("✅ sense-stat готов: report_id=%s sid=%s tf=%s window_end=%s — строк=%d",
                 report_id, strategy_id, time_frame, window_end_iso, updated)

        # формирование whitelist только для 7d
        if str(time_frame) == "7d":
            inserted = await _build_whitelist_for_7d(conn, report_id, strategy_id, window_end_dt)
            log.debug("✅ whitelist обновлён (7d): report_id=%s sid=%s rows=%d", report_id, strategy_id, inserted)
            # событие о готовности whitelist
            try:
                payload = {
                    "strategy_id": int(strategy_id),
                    "report_id": int(report_id),
                    "time_frame": "7d",
                    "window_end": window_end_dt.isoformat(),
                    "rows_inserted": int(inserted),
                    "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                }
                await infra.redis_client.xadd(
                    name=WHITELIST_READY_STREAM,
                    fields={"data": json.dumps(payload, separators=(",", ":"))},
                    maxlen=WHITELIST_READY_MAXLEN,
                    approximate=True,
                )
                log.debug("[WHITELIST_READY] sid=%s report_id=%s rows=%d", strategy_id, report_id, inserted)
            except Exception:
                log.exception("❌ Ошибка публикации события в %s", WHITELIST_READY_STREAM)

# 🔸 Построение whitelist для 7d: очищаем по стратегии и заполняем свежим набором
async def _build_whitelist_for_7d(conn, report_id: int, strategy_id: int, window_end_dt: datetime) -> int:
    # список баз (по данному report_id), у которых score_smoothed > SCORE_SENSE_MIN
    bases_rows = await conn.fetch(
        """
        SELECT timeframe, direction, agg_base
          FROM oracle_mw_sense_stat
         WHERE report_id = $1
           AND time_frame = '7d'
           AND score_smoothed > $2
        """,
        report_id, float(SCORE_SENSE_MIN)
    )
    if not bases_rows:
        # актуализируем пустым срезом для стратегии
        async with conn.transaction():
            await conn.execute("DELETE FROM oracle_mw_whitelist WHERE strategy_id = $1", strategy_id)
        return 0

    selectors = {(r["timeframe"], r["direction"], r["agg_base"]) for r in bases_rows}

    # выборка кандидатов из агрегатов (по выбранным базам)
    cand_rows = await conn.fetch(
        """
        SELECT
            a.id          AS aggregated_id,
            a.strategy_id AS strategy_id,
            a.direction   AS direction,
            a.timeframe   AS timeframe,
            a.agg_base    AS agg_base,
            a.agg_state   AS agg_state,
            a.winrate     AS winrate,
            a.confidence  AS confidence
        FROM oracle_mw_aggregated_stat a
        WHERE a.report_id = $1
          AND a.time_frame = '7d'
          AND a.strategy_id = $2
          AND a.confidence > $3
          AND a.winrate >= $4
        """,
        report_id, strategy_id, float(CONF_THRESHOLD_WL), float(WL_WR_MIN)
    )

    # фильтрация по выбранным базам (score_smoothed > SCORE_SENSE_MIN)
    filtered = [
        dict(r) for r in cand_rows
        if (r["timeframe"], r["direction"], r["agg_base"]) in selectors
    ]

    # вычисляем confirmation по confidence и готовим батч на вставку
    to_insert = []
    for r in filtered:
        wr = float(r["winrate"] or 0.0)
        conf_val = float(r["confidence"] or 0.0)

        # дополнительная защита по winrate (SQL уже отфильтровал)
        if wr < WL_WR_MIN:
            continue

        # вычисление confirmation по confidence
        confm = _confirmation_by_confidence(conf_val)

        to_insert.append({
            "aggregated_id": int(r["aggregated_id"]),
            "strategy_id": int(r["strategy_id"]),
            "direction": str(r["direction"]),
            "timeframe": str(r["timeframe"]),
            "agg_base": str(r["agg_base"]),
            "agg_state": str(r["agg_state"]),
            "winrate": float(wr),
            "confidence": conf_val,
            "confirmation": int(confm),
        })

    # атомарно обновляем срез для стратегии
    async with conn.transaction():
        await conn.execute("DELETE FROM oracle_mw_whitelist WHERE strategy_id = $1", strategy_id)

        if to_insert:
            await conn.executemany(
                """
                INSERT INTO oracle_mw_whitelist (
                    aggregated_id, strategy_id, direction, timeframe,
                    agg_base, agg_state, winrate, confidence, confirmation
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9
                )
                """,
                [
                    (
                        row["aggregated_id"],
                        row["strategy_id"],
                        row["direction"],
                        row["timeframe"],
                        row["agg_base"],
                        row["agg_state"],
                        row["winrate"],
                        row["confidence"],
                        row["confirmation"],
                    )
                    for row in to_insert
                ]
            )

    return len(to_insert)

# 🔸 Расчёт разделяющей силы (winrate по состояниям внутри базы)
def _compute_score(states: List[dict]) -> Tuple[float, int, Dict]:
    # должно быть минимум 2 состояния с n>0
    if len([s for s in states if s["n"] > 0]) < 2:
        comps = {"k_states": len(states), "n_total": sum(int(s["n"]) for s in states), "reason": "insufficient_states"}
        return 0.0, len(states), comps

    n_total = sum(int(s["n"]) for s in states if s["n"] > 0)
    if n_total <= 0:
        comps = {"k_states": len(states), "n_total": 0, "reason": "no_mass"}
        return 0.0, len(states), comps

    p_bar = sum(float(s["p"]) * int(s["n"]) for s in states if s["n"] > 0) / max(1, n_total)
    ss_between = 0.0
    ss_within = 0.0
    for s in states:
        n_i = int(s["n"])
        if n_i <= 0:
            continue
        p_i = float(s["p"])
        # межгрупповая дисперсия: вклад состояния
        ss_between += n_i * (p_i - p_bar) ** 2
        # внутригрупповая вариативность: сумма p(1-p) как аппроксимация
        ss_within += p_i * (1.0 - p_i)

    score = ss_between / (ss_between + ss_within + EPS)
    score = max(0.0, min(1.0, float(round(score, 4))))

    comps = {
        "k_states": len(states),
        "n_total": n_total,
        "p_bar": round(p_bar, 6),
        "ss_between": round(ss_between, 6),
        "ss_within": round(ss_within, 6),
        "formula": "score = SS_between / (SS_between + SS_within)",
    }
    return score, len(states), comps

# 🔸 Сглаживание (среднее по текущему и ≤5 предыдущим)
def _smooth_mean(current: float, history: List[float]) -> float:
    vals = [float(current)] + [float(x) for x in history if x is not None]
    if not vals:
        return float(current)
    sm = sum(vals) / len(vals)
    sm = max(0.0, min(1.0, float(round(sm, 4))))
    return sm

# 🔸 Вычисление confirmation по confidence
def _confirmation_by_confidence(conf: float) -> int:
    if conf >= CONFIRM_T0:
        return 0
    if conf >= CONFIRM_T1:
        return 1
    if conf >= CONFIRM_T2:
        return 2
    return 2