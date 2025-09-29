# oracle_mw_sense.py — расчёт метрик полезности agg_base (Coverage / Discrimination / Net Effect) + формирование актуального Whitelist

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
BASELINE_WR = 0.535  # минимальный winrate (планка); можно поднять, и всё подстроится
Z = 1.96             # Wilson 95%

# 🔸 Пороги «сбалансированного» профиля для Allowlist баз (решают, какие agg_base вообще годятся)
ALLOWLIST_MIN_NE_STRICT = 0.0
ALLOWLIST_MIN_CRAMERS_V = 0.15
ALLOWLIST_MIN_COVERAGE_INCLUDED_STRICT = 0.20
ALLOWLIST_MIN_TRADES = 300
ALLOWLIST_MIN_STATES_USED = 4
ALLOWLIST_MIN_ENTROPY_NORM = 0.60


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

    log.info("🚀 Старт воркера ORACLE_MW_SENSE (метрики agg_base + whitelist)")

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
                        # распаковка payload
                        data_raw = fields.get("data", "{}")
                        payload = json.loads(data_raw) if isinstance(data_raw, str) else {}
                        report_id = int(payload.get("report_id") or 0)
                        strategy_id = int(payload.get("strategy_id") or 0)
                        time_frame = str(payload.get("time_frame") or "")  # '7d' | '14d' | '28d'
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


# 🔸 Обработка одного отчёта: расчёт метрик по базам + построение Whitelist (для 7d)
async def _process_report(*, report_id: int, strategy_id: int, time_frame: str):
    # читаем все агрегаты одного отчёта (по всем TF, базам, состояниям, направлениям)
    async with infra.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
              timeframe,
              direction,
              agg_type,
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

        # группировка для метрик баз: timeframe → agg_base → [state rows без учёта direction]
        groups_for_metrics: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
        # группировка для whitelist: timeframe → agg_base → direction → [state rows]
        groups_for_wl: Dict[str, Dict[str, Dict[str, List[dict]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for r in rows:
            rec = {
                "state": str(r["agg_state"]),
                "n": int(r["trades_total"] or 0),
                "w": int(r["trades_wins"] or 0),
                "wr": float(r["winrate"] or 0.0),
                "direction": str(r["direction"]),
                "agg_type": str(r["agg_type"]),
            }
            tf = str(r["timeframe"])
            base = str(r["agg_base"])
            groups_for_metrics[tf][base].append(rec)
            groups_for_wl[tf][base][rec["direction"]].append(rec)

        total_bases_written = 0
        wl_rows_written_total = 0
        tf_count = 0

        # обход TF → agg_base для расчёта метрик и записи в oracle_mw_sense
        for timeframe, base_map in groups_for_metrics.items():
            tf_count += 1
            allowlist_bases: List[str] = []

            for agg_base, state_rows in base_map.items():
                # вычисление метрик для базы
                metrics, inputs_json = _compute_metrics_for_base(state_rows)

                # парсинг базы на арность/компоненты
                agg_arity, agg_components = _parse_base_components(agg_base)

                # UPSERT метрик в oracle_mw_sense
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
                total_bases_written += 1

                # формирование Allowlist по «сбалансированному» профилю
                if (
                    float(metrics["net_effect_strict"]) > ALLOWLIST_MIN_NE_STRICT
                    and float(metrics["discrimination_cramers_v"]) >= ALLOWLIST_MIN_CRAMERS_V
                    and float(metrics["coverage_included_strict"]) >= ALLOWLIST_MIN_COVERAGE_INCLUDED_STRICT
                    and int(metrics["trades_total_all_states"]) >= ALLOWLIST_MIN_TRADES
                    and int(metrics["states_used_count"]) >= ALLOWLIST_MIN_STATES_USED
                    and float(metrics["coverage_entropy_norm"]) >= ALLOWLIST_MIN_ENTROPY_NORM
                ):
                    allowlist_bases.append(agg_base)

            # если окно 7d — строим Whitelist (только для 7d, по твоим правилам)
            if time_frame == "7d":
                # перед вставкой — чистим старые WL для этой стратегии/TF/окна, чтобы таблица была всегда актуальной
                await conn.execute(
                    """
                    DELETE FROM oracle_mw_sense_whitelist
                    WHERE strategy_id = $1 AND time_frame = $2 AND timeframe = $3
                    """,
                    int(strategy_id), str(time_frame), str(timeframe)
                )

                # проходим по разрешённым базам и собираем WL по обоим направлениям
                wl_rows_written_tf = 0
                bases_map_for_tf = groups_for_wl.get(timeframe, {})
                for base in allowlist_bases:
                    dir_map = bases_map_for_tf.get(base, {})
                    for direction, rows_dir in dir_map.items():
                        # сумма сделок в базе для данного направления (для p_share)
                        N_dir_base = sum(int(r["n"]) for r in rows_dir if int(r["n"]) > 0)
                        if N_dir_base <= 0:
                            continue

                        # тип базы → для agg_type (solo/combo)
                        agg_type = "solo" if _base_arity_from_name(base) == 1 else "combo"

                        # по всем состояниям этой базы+направления — отбираем LB > BASELINE_WR
                        for r in rows_dir:
                            n = int(r["n"])
                            w = int(r["w"])
                            if n <= 0:
                                continue
                            lb = _wilson_lower_bound(w, n, Z)
                            if lb <= BASELINE_WR:
                                continue

                            wr = float(r["wr"])
                            p_share = float(n) / float(N_dir_base) if N_dir_base > 0 else 0.0
                            delta_wr = wr - BASELINE_WR

                            # вставка в Whitelist
                            await conn.execute(
                                """
                                INSERT INTO oracle_mw_sense_whitelist (
                                    report_id, strategy_id, time_frame, timeframe, direction,
                                    agg_base, agg_state,
                                    trades_total, trades_wins, winrate, wilson_lb, delta_wr, p_share_in_base,
                                    agg_arity, agg_components, baseline_wr,
                                    created_at, updated_at
                                )
                                VALUES (
                                    $1,$2,$3,$4,$5,
                                    $6,$7,
                                    $8,$9,$10,$11,$12,$13,
                                    $14,$15,$16,
                                    now(), now()
                                )
                                ON CONFLICT (report_id, strategy_id, time_frame, timeframe, direction, agg_base, agg_state)
                                DO UPDATE SET
                                    trades_total    = EXCLUDED.trades_total,
                                    trades_wins     = EXCLUDED.trades_wins,
                                    winrate         = EXCLUDED.winrate,
                                    wilson_lb       = EXCLUDED.wilson_lb,
                                    delta_wr        = EXCLUDED.delta_wr,
                                    p_share_in_base = EXCLUDED.p_share_in_base,
                                    agg_arity       = EXCLUDED.agg_arity,
                                    agg_components  = EXCLUDED.agg_components,
                                    baseline_wr     = EXCLUDED.baseline_wr,
                                    updated_at      = now()
                                """,
                                int(report_id),
                                int(strategy_id),
                                str(time_frame),
                                str(timeframe),
                                str(direction),
                                str(base),
                                str(r["state"]),
                                int(n),
                                int(w),
                                float(_clip01(wr)),
                                float(_clip01(lb)),
                                float(round(delta_wr, 6)),
                                float(round(_clip01(p_share), 6)),
                                int(_base_arity_from_name(base)),
                                _base_components_from_name(base),
                                float(BASELINE_WR),
                            )
                            wl_rows_written_tf += 1

                wl_rows_written_total += wl_rows_written_tf
                log.info(
                    "✅ [WL] sid=%s report=%s win=%s tf=%s: allowlist_bases=%d, wl_rows=%d",
                    strategy_id, report_id, time_frame, timeframe, len(allowlist_bases), wl_rows_written_tf
                )

        log.info(
            "✅ [SENSE] report_id=%s sid=%s win=%s → записано баз: %d (TF=%d), WL_rows_total=%d",
            report_id, strategy_id, time_frame, total_bases_written, tf_count, wl_rows_written_total
        )


# 🔸 Расчёт метрик по одному agg_base (в рамках одного report_id × timeframe, без учёта направления)
def _compute_metrics_for_base(state_rows: List[dict]) -> Tuple[Dict[str, float], Dict[str, dict]]:
    # подготовка входов — объединяем по состояниям, игнорируя direction
    # состояние идентифицируем по строке 'state'
    acc: Dict[str, Dict[str, float]] = {}
    for r in state_rows:
        s = r["state"]
        v = acc.setdefault(s, {"n": 0, "w": 0})
        v["n"] += int(r["n"])
        v["w"] += int(r["w"])

    rows = [{"state": s, "n": int(v["n"]), "w": int(v["w"]), "wr": (float(v["w"]) / float(v["n"])) if v["n"] > 0 else 0.0}
            for s, v in acc.items() if int(v["n"]) > 0]

    N = sum(r["n"] for r in rows)
    S = len(rows)

    inputs_json = {r["state"]: {"n": int(r["n"]), "w": int(r["w"]), "wr": float(round(_clip01(r["wr"]), 6))} for r in rows}

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

    # Coverage: нормированная энтропия
    ps = [r["n"] / N for r in rows]
    H = -sum(p * math.log(p) for p in ps if p > 0)
    Hmax = math.log(S) if S > 1 else 1.0
    coverage_entropy_norm = 0.0 if S <= 1 else (H / Hmax if Hmax > 0 else 0.0)
    coverage_entropy_norm = float(max(0.0, min(1.0, coverage_entropy_norm)))

    # Discrimination: Cramér’s V по таблице |S|×2 (win/loss)
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
    k = 1 if S > 1 else 0  # c-1 = 1 (win/loss)
    V = math.sqrt(chi2 / (N * k)) if (N > 0 and k > 0) else 0.0
    discrimination_cramers_v = float(max(0.0, min(1.0, V)))

    # Net Effect (loose/strict) и coverage включённых (по строкам без направления)
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
        lb = _wilson_lower_bound(int(r["w"]), int(r["n"]), Z)
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


# 🔸 Вспомогательное: арность по имени базы
def _base_arity_from_name(agg_base: str) -> int:
    parts = [p for p in str(agg_base).split("_") if p]
    return max(1, len(parts))


# 🔸 Вспомогательное: компоненты по имени базы
def _base_components_from_name(agg_base: str) -> List[str]:
    return [p for p in str(agg_base).split("_") if p]


# 🔸 Wilson lower bound (для strict-отбора/Whitelist)
def _wilson_lower_bound(wins: int, n: int, z: float) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    denom = 1.0 + (z * z) / n
    center = p + (z * z) / (2.0 * n)
    adj = z * math.sqrt((p * (1.0 - p) / n) + (z * z) / (4.0 * n * n))
    lb = (center - adj) / denom
    return float(max(0.0, min(1.0, lb)))


# 🔸 Вспомогательное: клип к [0,1]
def _clip01(x: float) -> float:
    try:
        return float(max(0.0, min(1.0, x)))
    except Exception:
        return 0.0