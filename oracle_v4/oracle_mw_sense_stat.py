# oracle_mw_sense_stat.py — воркер sense-stat: оценка «разделяющей силы» agg_base (0..1) по winrate, с учётом последних 5 прогонов

# 🔸 Импорты
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_SENSE_STAT")

# 🔸 Константы Redis Stream (инициируемся по готовности отчётов ДЛЯ sense)
SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
SENSE_CONSUMER_GROUP = "oracle_sense_stat_group"
SENSE_CONSUMER_NAME = "oracle_sense_stat_worker"

# 🔸 Константы расчёта
TF_LIST = ("m5", "m15", "h1")  # считаем изолированно по каждому TF
DIRECTIONS = ("long", "short")
AGG_BASES = (
    # solo
    "trend", "volatility", "extremes", "momentum",
    # pairs
    "trend_volatility", "trend_extremes", "trend_momentum",
    "volatility_extremes", "volatility_momentum",
    "extremes_momentum",
    # triplets
    "trend_volatility_extremes",
    "trend_volatility_momentum",
    "trend_extremes_momentum",
    "volatility_extremes_momentum",
    # quadruplet
    "trend_volatility_extremes_momentum",
)
SMOOTH_HISTORY_N = 5  # сглаживание по текущему и предыдущим ≤5 значениям (итого до 6 точек)
EPS = 1e-12           # числовая стабильность

# 🔸 Публичная точка входа воркера (включается из oracle_v4_main.py → run_safe_loop)
async def run_oracle_sense_stat():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно, читаем только НОВЫЕ сообщения)
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
            # группа уже есть — ок
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.info("🚀 Старт воркера sense-stat")

    # основной цикл чтения стрима
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

            # обработка сообщений
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


# 🔸 Обработка одного отчёта (report_id + time_frame + window_end)
async def _process_report(report_id: int, strategy_id: int, time_frame: str, window_end_iso: str):
    # парсим window_end в UTC-naive (по инвариантам системы)
    try:
        window_end_dt = datetime.fromisoformat(str(window_end_iso).replace("Z", ""))
    except Exception:
        log.exception("❌ Неверный формат window_end: %r", window_end_iso)
        return

    # выборка агрегатов текущего отчёта (фильтр confidence > 0)
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              timeframe,
              direction,
              agg_base,
              agg_state,
              trades_total,
              trades_wins,
              winrate,
              confidence
            FROM oracle_mw_aggregated_stat
            WHERE report_id = $1
              AND confidence > 0
            """,
            report_id
        )

        if not rows:
            log.info("ℹ️ Нет строк (confidence>0) для report_id=%s (sid=%s tf=%s)", report_id, strategy_id, time_frame)
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
        # обход по всем комбинациям TF × direction × agg_base
        for tf in TF_LIST:
            for direction in DIRECTIONS:
                for base in AGG_BASES:
                    states = data.get((tf, direction, base), [])
                    score_current, states_used, components = _compute_score(states)

                    # расчёт сглаженного значения по истории (до 5 предыдущих прогона)
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
                        strategy_id, time_frame, tf, direction, base, window_end_dt, int(SMOOTH_HISTORY_N)
                    )
                    hist = [float(x["score_current"]) for x in prev_vals] if prev_vals else []
                    score_smoothed = _smooth_mean(score_current, hist)

                    # upsert в oracle_mw_sense_stat
                    await conn.execute(
                        """
                        INSERT INTO oracle_mw_sense_stat (
                            report_id, strategy_id, time_frame, window_end,
                            timeframe, direction, agg_base,
                            states_used, score_current, score_smoothed, components,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4,
                            $5, $6, $7,
                            $8, $9, $10, $11,
                            now(), now()
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
                        int(states_used), float(score_current), float(score_smoothed), json.dumps(components, separators=(",", ":"))
                    )
                    updated += 1

        # итоговый лог по отчёту
        log.info(
            "✅ sense-stat готов: report_id=%s sid=%s tf=%s window_end=%s — записано комбинаций=%d",
            report_id, strategy_id, time_frame, window_end_iso, updated
        )


# 🔸 Расчёт «разделяющей силы» внутри agg_base по списку состояний (winrate)
def _compute_score(states: List[dict]) -> Tuple[float, int, Dict]:
    # если меньше двух состояний — база ничего не разделяет
    if not states or len([s for s in states if s["n"] > 0]) < 2:
        comps = {
            "k_states": len(states),
            "n_total": sum(int(s["n"]) for s in states),
            "reason": "insufficient_states"
        }
        return 0.0, len(states), comps

    # взвешенная средняя winrate по сделкам
    n_total = sum(int(s["n"]) for s in states if s["n"] > 0)
    if n_total <= 0:
        comps = {
            "k_states": len(states),
            "n_total": n_total,
            "reason": "no_mass"
        }
        return 0.0, len(states), comps

    p_bar = sum(float(s["p"]) * int(s["n"]) for s in states if s["n"] > 0) / max(1, n_total)

    # межгрупповая дисперсия (SS_between) и «внутригрупповая» аппрокс. (SS_within)
    ss_between = 0.0
    ss_within = 0.0
    for s in states:
        n_i = int(s["n"])
        if n_i <= 0:
            continue
        p_i = float(s["p"])
        # межгруппа: вклад состояния в отличие от общего среднего
        ss_between += n_i * (p_i - p_bar) ** 2
        # внутри: суммарная неопределённость пропорции (биномиальная аппроксимация)
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


# 🔸 Сглаживание: среднее текущего и до N прошлых значений
def _smooth_mean(current: float, history: List[float]) -> float:
    vals = [float(current)] + [float(x) for x in history if x is not None]
    if not vals:
        return float(current)
    sm = sum(vals) / len(vals)
    sm = max(0.0, min(1.0, float(round(sm, 4))))
    return sm