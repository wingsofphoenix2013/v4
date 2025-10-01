# oracle_pack_sense_stat.py — воркер PACK-sense: оценка «разделяющей силы» по pack_base+agg_type+agg_key (0..1), сглаживание по 5 прогонам

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_SENSE")

# 🔸 Константы Redis Stream (инициация по готовности PACK-confidence для отчёта)
PACK_SENSE_REPORT_READY_STREAM = "oracle:pack_sense:reports_ready"
PACK_SENSE_CONSUMER_GROUP = "oracle_pack_sense_group"
PACK_SENSE_CONSUMER_NAME = "oracle_pack_sense_worker"

# 🔸 Стрим для сборки списков после завершения sense
PACK_LISTS_BUILD_READY_STREAM = "oracle:pack_lists:build_ready"
PACK_LISTS_BUILD_READY_MAXLEN = 10_000

# 🔸 Константы расчёта
TF_LIST = ("m5", "m15", "h1")
DIRECTIONS = ("long", "short")
CONF_THRESHOLD = 0.1           # включаем в расчёт sense только состояния с confidence > 0.1
SMOOTH_HISTORY_N = 5           # сглаживание по текущему и ≤5 предыдущим
EPS = 1e-12

# 🔸 Публичная точка входа воркера
async def run_oracle_pack_sense():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_SENSE_REPORT_READY_STREAM,
            groupname=PACK_SENSE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана группа потребителей Redis Stream: %s", PACK_SENSE_CONSUMER_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.debug("🚀 Старт воркера PACK-sense")

    # основной цикл
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_SENSE_CONSUMER_GROUP,
                consumername=PACK_SENSE_CONSUMER_NAME,
                streams={PACK_SENSE_REPORT_READY_STREAM: ">"},
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
                            log.debug("ℹ️ Пропуск PACK-sense сообщения: недостаточно данных %s", payload)
                            await infra.redis_client.xack(PACK_SENSE_REPORT_READY_STREAM, PACK_SENSE_CONSUMER_GROUP, msg_id)
                            continue

                        await _process_report(report_id, strategy_id, time_frame, window_end)
                        await infra.redis_client.xack(PACK_SENSE_REPORT_READY_STREAM, PACK_SENSE_CONSUMER_GROUP, msg_id)
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения в PACK-sense")

        except asyncio.CancelledError:
            log.debug("⏹️ Воркер PACK-sense остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK-sense — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного отчёта: расчёт sense по pack_base+agg_type+agg_key
async def _process_report(report_id: int, strategy_id: int, time_frame: str, window_end_iso: str):
    # парсинг window_end
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    async with infra.pg_pool.acquire() as conn:
        # выбираем ВСЕ агрегаты этого отчёта (для базового перечня ключей pack_base+agg_type+agg_key)
        rows_all = await conn.fetch(
            """
            SELECT timeframe, direction, pack_base, agg_type, agg_key
            FROM oracle_pack_aggregated_stat
            WHERE report_id = $1
            GROUP BY timeframe, direction, pack_base, agg_type, agg_key
            """,
            report_id
        )
        if not rows_all:
            log.debug("ℹ️ PACK-sense: нет агрегатов для report_id=%s", report_id)
            return

        # выбираем строки, которые проходят фильтр confidence>0.1 (states/agg_value уровня)
        rows = await conn.fetch(
            """
            SELECT timeframe, direction, pack_base, agg_type, agg_key, agg_value,
                   trades_total, trades_wins, winrate
            FROM oracle_pack_aggregated_stat
            WHERE report_id = $1
              AND confidence > $2
            """,
            report_id, float(CONF_THRESHOLD)
        )
        # группировка по ключу базы sense: pack_base + agg_type + agg_key (+tf, dir)
        data: Dict[Tuple[str, str, str, str, str], List[dict]] = {}
        for r in rows:
            key = (r["timeframe"], r["direction"], r["pack_base"], r["agg_type"], r["agg_key"])
            data.setdefault(key, []).append({
                "agg_value": r["agg_value"],
                "n": int(r["trades_total"] or 0),
                "w": int(r["trades_wins"] or 0),
                "p": float(r["winrate"] or 0.0),
            })

        updated = 0
        # проходим по ВСЕМ базовым ключам, даже если после фильтра они пустые → score=0
        for r in rows_all:
            tf = r["timeframe"]; direction = r["direction"]
            pbase = r["pack_base"]; atype = r["agg_type"]; akey = r["agg_key"]
            states = data.get((tf, direction, pbase, atype, akey), [])

            score_current, states_used, components = _compute_score(states)

            # сглаживание по истории (≤5 предыдущих)
            prev_vals = await conn.fetch(
                """
                SELECT score_current
                FROM oracle_pack_sense_stat
                WHERE strategy_id = $1
                  AND time_frame  = $2
                  AND timeframe   = $3
                  AND direction   = $4
                  AND pack_base   = $5
                  AND agg_type    = $6
                  AND agg_key     = $7
                  AND window_end  < $8
                ORDER BY window_end DESC
                LIMIT $9
                """,
                strategy_id, time_frame, tf, direction, pbase, atype, akey,
                window_end_dt, int(SMOOTH_HISTORY_N)
            )
            hist = [float(x["score_current"]) for x in prev_vals] if prev_vals else []
            score_smoothed = _smooth_mean(score_current, hist)

            await conn.execute(
                """
                INSERT INTO oracle_pack_sense_stat (
                    report_id, strategy_id, time_frame, window_end,
                    timeframe, direction, pack_base, agg_type, agg_key,
                    states_used, score_current, score_smoothed, components,
                    created_at, updated_at
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,
                    $10,$11,$12,$13,
                    now(), now()
                )
                ON CONFLICT (report_id, timeframe, direction, pack_base, agg_type, agg_key)
                DO UPDATE SET
                    states_used    = EXCLUDED.states_used,
                    score_current  = EXCLUDED.score_current,
                    score_smoothed = EXCLUDED.score_smoothed,
                    components     = EXCLUDED.components,
                    updated_at     = now()
                """,
                report_id, strategy_id, time_frame, window_end_dt,
                tf, direction, pbase, atype, akey,
                int(states_used), float(score_current), float(score_smoothed),
                json.dumps(components, separators=(",", ":"))
            )
            updated += 1

        log.debug("✅ PACK-sense готов: report_id=%s sid=%s tf=%s window_end=%s — строк=%d",
                 report_id, strategy_id, time_frame, window_end_iso, updated)

        # после записи sense — отправляем событие для сборки списков
        try:
            payload = {
                "strategy_id": int(strategy_id),
                "report_id": int(report_id),
                "time_frame": str(time_frame),
                "window_end": window_end_dt.isoformat(),
                "generated_at": datetime.utcnow().replace(tzinfo=None).isoformat(),
                "axes_updated": int(updated),
            }
            await infra.redis_client.xadd(
                name=PACK_LISTS_BUILD_READY_STREAM,
                fields={"data": json.dumps(payload, separators=(",", ":"))},
                maxlen=PACK_LISTS_BUILD_READY_MAXLEN,
                approximate=True,
            )
            log.debug("[PACK_LISTS_BUILD_READY] sid=%s report_id=%s tf=%s axes=%d",
                      strategy_id, report_id, time_frame, updated)
        except Exception:
            log.exception("❌ Ошибка публикации события в %s", PACK_LISTS_BUILD_READY_STREAM)

# 🔸 Расчёт разделяющей силы по списку states (agg_value: p, n)
def _compute_score(states: List[dict]) -> Tuple[float, int, Dict]:
    # если нет ни одного состояния (после фильтра) — score=0, но возвращаем k/n для компонентов
    if not states or len([s for s in states if s["n"] > 0]) < 2:
        comps = {
            "k_states": len(states),
            "n_total": sum(int(s["n"]) for s in states),
            "reason": "insufficient_states" if states else "no_states_after_conf_filter",
            "p_bar": 0.0,
            "ss_between": 0.0,
            "ss_within": 0.0,
            "formula": "score = SS_between / (SS_between + SS_within)"
        }
        return 0.0, len(states), comps

    # взвешенная средняя winrate по сделкам
    n_total = sum(int(s["n"]) for s in states if s["n"] > 0)
    if n_total <= 0:
        comps = {"k_states": len(states), "n_total": 0, "reason": "no_mass", "p_bar": 0.0, "ss_between": 0.0, "ss_within": 0.0, "formula": "score = SS_between / (SS_between + SS_within)"}
        return 0.0, len(states), comps

    p_bar = sum(float(s["p"]) * int(s["n"]) for s in states if s["n"] > 0) / max(1, n_total)

    # межгрупповая/внутригрупповая «дисперсии»
    ss_between = 0.0
    ss_within = 0.0
    for s in states:
        n_i = int(s["n"])
        if n_i <= 0:
            continue
        p_i = float(s["p"])
        ss_between += n_i * (p_i - p_bar) ** 2
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