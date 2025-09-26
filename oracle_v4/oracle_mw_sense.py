# oracle_mw_sense.py — воркер расчёта sense_score по MW-агрегатам (онлайн-адаптация, сглаживание по последним 7 значениям)

# 🔸 Импорты
import asyncio
import json
import math
import logging
from collections import defaultdict
from typing import Dict, Tuple, List, Optional
from datetime import datetime

import infra

log = logging.getLogger("ORACLE_MW_SENSE")

# 🔸 Константы воркера / параметры исполнения
STREAM = "oracle:mw:reports_ready"      # источник событий о готовых MW-отчётах
GROUP = "oracle_mw_sense_v1"            # своя consumer group, чтобы не мешать другим
CONSUMER = "sense_worker_1"             # имя потребителя
BLOCK_MS = 5000                         # таймаут ожидания XREADGROUP (мс)
BATCH_COUNT = 64                        # сколько сообщений за раз читаем

SMOOTH_WINDOW_N = 7                     # сглаживание по 7 последним значениям (включая текущее)
METHOD_VERSION = "sense_v1_online_w7"   # версия методики (для трассировки)

# 🔸 Вспомогательные математические функции
def _safe_div(a: float, b: float) -> float:
    # защита от деления на ноль
    return (a / b) if b else 0.0

def _bin_entropy(q: float) -> float:
    # бинарная энтропия в натуральных логарифмах; 0<=q<=1
    if q <= 0.0 or q >= 1.0:
        return 0.0
    return -(q * math.log(q) + (1.0 - q) * math.log(1.0 - q))

def _geom_mean(values: List[float]) -> float:
    # геометрическое среднее по [0..1]; если список пуст — 0
    if not values:
        return 0.0
    prod = 1.0
    n = 0
    for v in values:
        v_clamped = max(0.0, min(1.0, float(v)))
        prod *= v_clamped
        n += 1
        if prod == 0.0:
            return 0.0
    return prod ** (1.0 / n)

# 🔸 Парсер ISO-времени из события → datetime (UTC-naive под схему timestamp)
def _to_dt(x: Optional[str]) -> Optional[datetime]:
    # условия достаточности
    if not x:
        return None
    s = str(x)
    # поддержка суффикса 'Z' (UTC)
    if s.endswith("Z"):
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
    # стандартный ISO (с или без микросекунд, с таймзоной или без)
    try:
        dt = datetime.fromisoformat(s)
        # если aware — делаем naive (UTC по инвариантам системы)
        return dt.replace(tzinfo=None)
    except Exception:
        # запасной вариант: откусываем смещение/суффиксы
        base = s.split("+")[0].split("Z")[0]
        try:
            return datetime.fromisoformat(base).replace(tzinfo=None)
        except Exception:
            return None

# 🔸 Инициализация consumer group
async def _ensure_group(redis):
    # создаём группу, если её ещё нет
    try:
        await redis.xgroup_create(name=STREAM, groupname=GROUP, id="$", mkstream=True)
        log.info("Создана consumer group '%s' на стриме '%s'", GROUP, STREAM)
    except Exception:
        # вероятно, уже существует
        pass

# 🔸 Публичная точка запуска воркера (встраивается в oracle_v4_main.py через run_safe_loop)
async def run_oracle_mw_sense():
    # условия достаточности
    if infra.redis_client is None or infra.pg_pool is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    await _ensure_group(infra.redis_client)
    log.info("📡 ORACLE_MW_SENSE запущен (group=%s, consumer=%s)", GROUP, CONSUMER)

    # основной цикл чтения событий
    while True:
        try:
            messages = await infra.redis_client.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM: ">"},
                count=BATCH_COUNT,
                block=BLOCK_MS,
            )
            if not messages:
                continue

            # формат ответов: [(stream, [(id, {fields}), ...])]
            for _stream, entries in messages:
                for msg_id, fields in entries:
                    try:
                        data = json.loads(fields.get("data", "{}"))
                        await _process_report_event(data)
                        await infra.redis_client.xack(STREAM, GROUP, msg_id)
                    except asyncio.CancelledError:
                        log.info("⏹️ ORACLE_MW_SENSE остановлен по сигналу (msg_id=%s)", msg_id)
                        raise
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения sense, msg_id=%s", msg_id)
                        # не ack — останется в pending для ретрая
        except asyncio.CancelledError:
            log.info("⏹️ ORACLE_MW_SENSE остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка основного цикла ORACLE_MW_SENSE")
            await asyncio.sleep(2.0)

# 🔸 Обработка одного события REPORT_READY
async def _process_report_event(evt: Dict):
    # условия достаточности события
    if not evt or "report_id" not in evt:
        return

    report_id = int(evt["report_id"])
    strategy_id = int(evt.get("strategy_id", 0))
    time_frame = str(evt.get("time_frame", ""))  # '7d'|'14d'|'28d'
    window_start = _to_dt(evt.get("window_start"))
    window_end = _to_dt(evt.get("window_end"))

    # логируем заголовок
    log.info("[SENSE] обработка report_id=%s strategy_id=%s time_frame=%s", report_id, strategy_id, time_frame)

    async with infra.pg_pool.acquire() as conn:
        # читаем агрегаты отчёта
        rows = await conn.fetch(
            """
            SELECT direction, timeframe, agg_base, agg_state,
                   trades_total, trades_wins, winrate, confidence
              FROM oracle_mw_aggregated_stat
             WHERE report_id = $1
            """,
            report_id,
        )
        if not rows:
            log.info("[SENSE] report_id=%s — агрегатов нет, пропуск", report_id)
            return

        # группируем по (direction, timeframe, agg_base) → внутри по agg_state
        group_map: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
        for r in rows:
            key = (r["direction"], r["timeframe"], r["agg_base"])
            group_map[key].append(
                {
                    "agg_state": r["agg_state"],
                    "t": int(r["trades_total"]),
                    "w": int(r["trades_wins"]),
                    "wr": float(r["winrate"]),
                    "conf": float(r["confidence"]),
                }
            )

        # получим число закрытых сделок по направлению (для coverage)
        t_all_by_dir: Dict[str, int] = {"long": 0, "short": 0}
        if window_start and window_end and strategy_id:
            t_all_rows = await conn.fetch(
                """
                SELECT direction, COUNT(*)::int AS cnt
                  FROM positions_v4
                 WHERE strategy_id = $1
                   AND status = 'closed'
                   AND closed_at >= $2
                   AND closed_at <  $3
                 GROUP BY direction
                """,
                strategy_id, window_start, window_end,
            )
            for rr in t_all_rows:
                t_all_by_dir[str(rr["direction"])] = int(rr["cnt"])

        # обработка каждой группы → компоненты и итоговый sense
        for (direction, timeframe, agg_base), items in group_map.items():
            # суммарные t и w
            T = sum(x["t"] for x in items)
            W = sum(x["w"] for x in items)
            wr_overall = _safe_div(W, T)

            # coverage: доля сделок по направлению, покрытых данной базой
            T_all_dir = t_all_by_dir.get(direction, 0)
            coverage = max(0.0, min(1.0, _safe_div(T, T_all_dir))) if T_all_dir else 0.0

            # распределение p_s и энтропия
            K_obs = sum(1 for x in items if x["t"] > 0)
            if T > 0 and K_obs > 1:
                p = [x["t"] / T for x in items if x["t"] > 0]
                H = -sum(pi * math.log(pi) for pi in p)
                H_max = math.log(len(p))
                entropy_norm = _safe_div(H, H_max)
            else:
                entropy_norm = 0.0

            # information gain (нормированный)
            if T > 0:
                H_y = _bin_entropy(wr_overall)
                H_y_s = 0.0
                for x in items:
                    if x["t"] <= 0:
                        continue
                    ps = x["t"] / T
                    H_y_s += ps * _bin_entropy(x["wr"])
                IG = max(0.0, H_y - H_y_s)
                ig_norm = _safe_div(IG, H_y) if H_y > 0 else 0.0
            else:
                ig_norm = 0.0

            # средневзвешенный confidence
            if T > 0:
                confidence_avg = sum((x["t"] / T) * max(0.0, min(1.0, x["conf"])) for x in items if x["t"] > 0)
            else:
                confidence_avg = 0.0

            # итоговый raw sense — без ручных весов (геометрическое среднее 4 факторов)
            sense_raw = _geom_mean([coverage, entropy_norm, ig_norm, confidence_avg])

            # сглаживание по последним N=7 значений (включая текущее): простое скользящее среднее
            prev_rows = await conn.fetch(
                """
                SELECT sense_score_raw
                  FROM oracle_mw_sense_stat
                 WHERE strategy_id = $1
                   AND time_frame  = $2
                   AND timeframe   = $3
                   AND direction   = $4
                   AND agg_base    = $5
                 ORDER BY computed_at DESC
                 LIMIT $6
                """,
                strategy_id, time_frame, timeframe, direction, agg_base, SMOOTH_WINDOW_N - 1,
            )
            history = [float(r["sense_score_raw"]) for r in prev_rows]
            smooth_vals = [sense_raw] + history
            sense_smooth = sum(smooth_vals) / len(smooth_vals)

            # сохраняем в БД (UPSERT по уникальному ключу репорта)
            await conn.execute(
                """
                INSERT INTO oracle_mw_sense_stat (
                    report_id, strategy_id, time_frame, timeframe, direction, agg_base,
                    sense_score_raw, sense_score_smooth,
                    coverage, entropy_norm, ig_norm, confidence_avg,
                    trades_total, states_count,
                    method_version, smoothing_window_n, inputs
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8,
                    $9, $10, $11, $12,
                    $13, $14,
                    $15, $16, $17
                )
                ON CONFLICT (report_id, strategy_id, time_frame, timeframe, direction, agg_base)
                DO UPDATE SET
                    sense_score_raw     = EXCLUDED.sense_score_raw,
                    sense_score_smooth  = EXCLUDED.sense_score_smooth,
                    coverage            = EXCLUDED.coverage,
                    entropy_norm        = EXCLUDED.entropy_norm,
                    ig_norm             = EXCLUDED.ig_norm,
                    confidence_avg      = EXCLUDED.confidence_avg,
                    trades_total        = EXCLUDED.trades_total,
                    states_count        = EXCLUDED.states_count,
                    method_version      = EXCLUDED.method_version,
                    smoothing_window_n  = EXCLUDED.smoothing_window_n,
                    inputs              = EXCLUDED.inputs,
                    computed_at         = now()
                """,
                report_id,
                strategy_id,
                time_frame,
                timeframe,
                direction,
                agg_base,
                round(float(sense_raw), 4),
                round(float(sense_smooth), 4),
                round(float(coverage), 4),
                round(float(entropy_norm), 4),
                round(float(ig_norm), 4),
                round(float(confidence_avg), 4),
                int(T),
                int(K_obs),
                METHOD_VERSION,
                SMOOTH_WINDOW_N,
                json.dumps(
                    {
                        "window_start": window_start.isoformat() if window_start else None,
                        "window_end": window_end.isoformat() if window_end else None,
                        "components": {
                            "coverage": coverage,
                            "entropy_norm": entropy_norm,
                            "ig_norm": ig_norm,
                            "confidence_avg": confidence_avg,
                        },
                        "totals": {"T": T, "W": W, "wr_overall": wr_overall},
                        "states": [
                            {
                                "agg_state": x["agg_state"],
                                "t": x["t"],
                                "w": x["w"],
                                "wr": x["wr"],
                                "conf": x["conf"],
                                "p": _safe_div(x["t"], T) if T > 0 else 0.0,
                            }
                            for x in items
                        ],
                        "version": METHOD_VERSION,
                        "smooth": {"window_n": SMOOTH_WINDOW_N},
                    },
                    separators=(",", ":"),
                ),
            )

            # лог на результат по ключу
            log.info(
                "[SENSE] sid=%s win=%s tf=%s dir=%s base=%s | raw=%.4f smooth=%.4f | cov=%.4f ent=%.4f ig=%.4f conf=%.4f | T=%d K=%d",
                strategy_id, time_frame, timeframe, direction, agg_base,
                round(sense_raw, 4), round(sense_smooth, 4),
                round(coverage, 4), round(entropy_norm, 4), round(ig_norm, 4), round(confidence_avg, 4),
                int(T), int(K_obs),
            )

    # финальный лог по отчёту
    log.info("[SENSE] report_id=%s — расчёт sense завершён", report_id)