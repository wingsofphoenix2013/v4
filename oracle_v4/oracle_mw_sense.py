# oracle_mw_sense.py — расчёт метрик полезности agg_base (Coverage / Discrimination / Net Effect) и запись в oracle_mw_sense

# 🔸 Импорты
import asyncio
import json
import logging
import math
from collections import defaultdict
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_SENSE")

# 🔸 Константы Redis Stream (читает события о готовности отчётов для sense)
SENSE_REPORT_READY_STREAM = "oracle:mw_sense:reports_ready"
SENSE_GROUP = "oracle_mw_sense_group"
SENSE_CONSUMER = "oracle_mw_sense_worker"

# 🔸 Параметры
BASELINE_WR = 0.535  # жёсткая точка безубыточности по системе
Z = 1.96             # Wilson 95%


# 🔸 Публичная точка входа воркера (запускать через oracle_v4_main.py → run_safe_loop)
async def run_oracle_mw_sense():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.info("❌ Пропуск ORACLE_MW_SENSE: PG/Redis не инициализированы")
        return

    # создание группы потребителей (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=SENSE_REPORT_READY_STREAM, groupname=SENSE_GROUP, id="$", mkstream=True
        )
        log.info("📡 Создана группа потребителей в Redis Stream: %s", SENSE_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка инициализации группы Redis Stream")
            return

    log.info("🚀 Старт воркера ORACLE_MW_SENSE (метрики agg_base)")

    # основной цикл чтения стрима
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=SENSE_GROUP,
                consumername=SENSE_CONSUMER,
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
                        # условия достаточности
                        data_raw = fields.get("data", "{}")
                        payload = json.loads(data_raw) if isinstance(data_raw, str) else {}
                        report_id = int(payload.get("report_id") or 0)
                        strategy_id = int(payload.get("strategy_id") or 0)
                        time_frame = str(payload.get("time_frame") or "")
                        if not (report_id and strategy_id and time_frame):
                            log.info("ℹ️ Пропуск сообщения: неполный payload: %s", payload)
                            await infra.redis_client.xack(SENSE_REPORT_READY_STREAM, SENSE_GROUP, msg_id)
                            continue

                        # расчёт по одному report_id
                        await _process_report(report_id=report_id, strategy_id=strategy_id, time_frame=time_frame)

                        # ack сообщения
                        await infra.redis_client.xack(SENSE_REPORT_READY_STREAM, SENSE_GROUP, msg_id)

                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения ORACLE_MW_SENSE")

        except asyncio.CancelledError:
            log.info("⏹️ Воркер ORACLE_MW_SENSE остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла ORACLE_MW_SENSE — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 Обработка одного отчёта: группировка по timeframe → agg_base, расчёт и UPSERT
async def _process_report(*, report_id: int, strategy_id: int, time_frame: str):
    # читаем все агрегаты одного отчёта (по всем TF и базам)
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              timeframe,
              agg_base,
              agg_state,
              trades_total,
              trades_wins,
              winrate
            FROM oracle_mw_aggregated_stat
            WHERE report_id = $1
            """,
            int(report_id),
        )
        if not rows:
            log.info("[SENSE] report_id=%s sid=%s tf=%s: агрегатов нет — пропуск", report_id, strategy_id, time_frame)
            return

        # группировка: timeframe → agg_base → список строк состояний
        groups: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
        for r in rows:
            timeframe = str(r["timeframe"])
            base = str(r["agg_base"])
            groups[timeframe][base].append(
                {
                    "state": str(r["agg_state"]),
                    "n": int(r["trades_total"] or 0),
                    "w": int(r["trades_wins"] or 0),
                    "wr": float(r["winrate"] or 0.0),
                }
            )

        total_written = 0
        # обход TF → agg_base
        for timeframe, base_map in groups.items():
            for agg_base, state_rows in base_map.items():
                # вычисление метрик
                metrics, inputs_json = _compute_metrics_for_base(state_rows)
                # парсинг базы на арность/компоненты
                agg_arity, agg_components = _parse_base_components(agg_base)

                # UPSERT результата
                await conn.execute(
                    """
                    INSERT INTO oracle_mw_sense (
                        report_id, strategy_id, time_frame, timeframe, agg_base,
                        coverage_entropy_norm, discrimination_cramers_v,
                        net_effect_loose, net_effect_strict,
                        coverage_included_loose, coverage_included_strict,
                        trades_total_all_states, states_used_count, baseline_wr,
                        inputs_json, agg_arity, agg_components,
                        created_at, updated_at
                    )
                    VALUES (
                        $1,$2,$3,$4,$5,
                        $6,$7,
                        $8,$9,
                        $10,$11,
                        $12,$13,$14,
                        $15,$16,$17,
                        now(), now()
                    )
                    ON CONFLICT (report_id, strategy_id, time_frame, timeframe, agg_base)
                    DO UPDATE SET
                        coverage_entropy_norm    = EXCLUDED.coverage_entropy_norm,
                        discrimination_cramers_v = EXCLUDED.discrimination_cramers_v,
                        net_effect_loose         = EXCLUDED.net_effect_loose,
                        net_effect_strict        = EXCLUDED.net_effect_strict,
                        coverage_included_loose  = EXCLUDED.coverage_included_loose,
                        coverage_included_strict = EXCLUDED.coverage_included_strict,
                        trades_total_all_states  = EXCLUDED.trades_total_all_states,
                        states_used_count        = EXCLUDED.states_used_count,
                        baseline_wr              = EXCLUDED.baseline_wr,
                        inputs_json              = EXCLUDED.inputs_json,
                        agg_arity                = EXCLUDED.agg_arity,
                        agg_components           = EXCLUDED.agg_components,
                        updated_at               = now()
                    """,
                    int(report_id),
                    int(strategy_id),
                    str(time_frame),
                    str(timeframe),
                    str(agg_base),
                    float(metrics["coverage_entropy_norm"]),
                    float(metrics["discrimination_cramers_v"]),
                    float(metrics["net_effect_loose"]),
                    float(metrics["net_effect_strict"]),
                    float(metrics["coverage_included_loose"]),
                    float(metrics["coverage_included_strict"]),
                    int(metrics["trades_total_all_states"]),
                    int(metrics["states_used_count"]),
                    float(BASELINE_WR),
                    json.dumps(inputs_json, separators=(",", ":")),
                    int(agg_arity),
                    agg_components,
                )
                total_written += 1

        log.info(
            "✅ [SENSE] report_id=%s sid=%s tf=%s → записано баз: %d (TF=%d)",
            report_id, strategy_id, time_frame, total_written, len(groups),
        )


# 🔸 Расчёт метрик по одному agg_base (в рамках одного report_id × timeframe)
def _compute_metrics_for_base(state_rows: List[dict]) -> Tuple[Dict[str, float], Dict[str, dict]]:
    # подготовка входов
    # фильтруем состояния без сделок
    rows = [r for r in state_rows if (r.get("n", 0) or 0) > 0]
    N = sum(r["n"] for r in rows)
    S = len(rows)

    # inputs_json для аудита
    inputs_json = {r["state"]: {"n": int(r["n"]), "w": int(r["w"]), "wr": float(r["wr"])} for r in rows}

    # граничные случаи
    if N <= 0 or S <= 0:
        return (
            {
                "coverage_entropy_norm": 0.0,
                "discrimination_cramers_v": 0.0,
                "net_effect_loose": 0.0,
                "net_effect_strict": 0.0,
                "coverage_included_loose": 0.0,
                "coverage_included_strict": 0.0,
                "trades_total_all_states": 0,
                "states_used_count": 0,
            },
            inputs_json,
        )

    # Coverage: нормированная энтропия по долям p_s
    ps = [r["n"] / N for r in rows]
    H = -sum(p * math.log(p) for p in ps if p > 0)
    Hmax = math.log(S) if S > 1 else 1.0
    coverage_entropy_norm = 0.0 if S <= 1 else (H / Hmax if Hmax > 0 else 0.0)
    coverage_entropy_norm = float(max(0.0, min(1.0, coverage_entropy_norm)))

    # Discrimination: Cramér’s V для таблицы |S|×2 (win/loss)
    wins = [r["w"] for r in rows]
    losses = [r["n"] - r["w"] for r in rows]
    total_wins = sum(wins)
    total_losses = sum(losses)
    chi2 = 0.0
    for i in range(S):
        exp_w = (rows[i]["n"] * total_wins) / N if N > 0 else 0.0
        exp_l = (rows[i]["n"] * total_losses) / N if N > 0 else 0.0
        if exp_w > 0:
            chi2 += ((wins[i] - exp_w) ** 2) / exp_w
        if exp_l > 0:
            chi2 += ((losses[i] - exp_l) ** 2) / exp_l
    k = 1 if S > 1 else 0  # c-1 = 1 для win/loss
    V = math.sqrt(chi2 / (N * k)) if (N > 0 and k > 0) else 0.0
    discrimination_cramers_v = float(max(0.0, min(1.0, V)))

    # Net Effect (loose/strict) и coverage включённых
    ne_loose = 0.0
    ne_strict = 0.0
    cov_loose = 0.0
    cov_strict = 0.0
    for r in rows:
        p = r["n"] / N
        wr = r["wr"]
        # loose: по сырому WR
        if wr >= BASELINE_WR:
            ne_loose += p * (wr - BASELINE_WR)
            cov_loose += p
        # strict: по нижней границе Wilson
        lb = _wilson_lower_bound(r["w"], r["n"], Z)
        if lb > BASELINE_WR:
            ne_strict += p * (wr - BASELINE_WR)
            cov_strict += p

    metrics = {
        "coverage_entropy_norm": coverage_entropy_norm,
        "discrimination_cramers_v": discrimination_cramers_v,
        "net_effect_loose": float(ne_loose),
        "net_effect_strict": float(ne_strict),
        "coverage_included_loose": float(min(1.0, max(0.0, cov_loose))),
        "coverage_included_strict": float(min(1.0, max(0.0, cov_strict))),
        "trades_total_all_states": int(N),
        "states_used_count": int(S),
    }
    return metrics, inputs_json


# 🔸 Парсинг базы в арность и список компонент (для удобных срезов)
def _parse_base_components(agg_base: str) -> Tuple[int, List[str]]:
    parts = [p for p in str(agg_base).split("_") if p]
    arity = max(1, len(parts))
    components = parts  # сохраняем естественный порядок формирования
    return arity, components


# 🔸 Wilson lower bound (для strict-отбора)
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    return float(max(0.0, min(1.0, lb)))