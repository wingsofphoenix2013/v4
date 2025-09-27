# oracle_mw_sense.py — воркер расчёта sense_score по MW-агрегатам (подписка на confidence_ready, окно из БД, сглаживание, Redis KV)

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
STREAM = "oracle:mw_sense:reports_ready"   # стрим-сигнал от oracle_mw_confidence.py
GROUP = "oracle_mw_sense_v1"               # consumer group для sense
CONSUMER = "sense_worker_1"                # имя потребителя
BLOCK_MS = 5000                            # таймаут ожидания XREADGROUP (мс)
BATCH_COUNT = 64                           # сколько сообщений за раз читаем

SMOOTH_WINDOW_N = 7                        # сглаживание по 7 последним значениям (включая текущее)
METHOD_VERSION = "sense_v1_online_w7"      # версия методики (для трассировки)

# 🔸 Настройки Redis KV сводки
KV_TTL_SEC = 5 * 60 * 60                   # 5 часов
KV_PREFIX = "oracle:sense:summary"         # ключ: oracle:sense:summary:{sid}:{win}:{tf}:{dir}:{base}:{state}


# 🔸 Вспомогательные математические функции
def _safe_div(a: float, b: float) -> float:
    return (a / b) if b else 0.0

def _bin_entropy(q: float) -> float:
    if q <= 0.0 or q >= 1.0:
        return 0.0
    return -(q * math.log(q) + (1.0 - q) * math.log(1.0 - q))

def _geom_mean(values: List[float]) -> float:
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


# 🔸 Инициализация consumer group
async def _ensure_group(redis):
    try:
        await redis.xgroup_create(name=STREAM, groupname=GROUP, id="$", mkstream=True)
        log.info("Создана consumer group '%s' на стриме '%s'", GROUP, STREAM)
    except Exception:
        # уже существует
        pass


# 🔸 Публичная точка запуска воркера (встраивается в oracle_v4_main.py через run_safe_loop)
async def run_oracle_mw_sense():
    # условия достаточности
    if infra.redis_client is None or infra.pg_pool is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    await _ensure_group(infra.redis_client)
    log.info("📡 ORACLE_MW_SENSE запущен (stream=%s, group=%s, consumer=%s)", STREAM, GROUP, CONSUMER)

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

            for _stream, entries in messages:
                for msg_id, fields in entries:
                    try:
                        data = json.loads(fields.get("data", "{}"))
                        # маршрутизация форматов payload
                        await _route_event(data)
                        await infra.redis_client.xack(STREAM, GROUP, msg_id)
                    except asyncio.CancelledError:
                        log.info("⏹️ ORACLE_MW_SENSE остановлен по сигналу (msg_id=%s)", msg_id)
                        raise
                    except Exception:
                        log.exception("❌ Ошибка обработки сообщения sense, msg_id=%s payload=%r", msg_id, fields)
                        # не ack — останется в pending
        except asyncio.CancelledError:
            log.info("⏹️ ORACLE_MW_SENSE остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка основного цикла ORACLE_MW_SENSE")
            await asyncio.sleep(2.0)


# 🔸 Маршрутизация события: поддержка двух форматов payload
async def _route_event(evt: Dict):
    if not evt:
        log.warning("[SENSE] пустой payload — пропуск")
        return

    # современный формат: единичный отчёт
    if "report_id" in evt:
        rid = int(evt.get("report_id"))
        sid = int(evt.get("strategy_id", 0))
        tf_win = str(evt.get("time_frame", ""))
        log.info("[SENSE] start report_id=%s strategy_id=%s time_frame=%s", rid, sid, tf_win)
        await _process_report(rid)
        log.info("[SENSE] done report_id=%s", rid)
        return

    # совместимость: сводный пакет по окну без report_id
    if "strategy_id" in evt and "time_frames" in evt:
        sid = int(evt.get("strategy_id", 0))
        time_frames = evt.get("time_frames") or {}
        win_end_iso = evt.get("window_end")
        win_end_dt = _to_dt_aware_or_naive(win_end_iso)

        for tf_win in ("7d", "14d", "28d"):
            if tf_win not in time_frames:
                continue
            rid = await _find_report_id_by_sid_win(sid, tf_win, win_end_dt)
            if rid:
                log.info("[SENSE] start report_id=%s strategy_id=%s time_frame=%s (compat)", rid, sid, tf_win)
                await _process_report(rid)
                log.info("[SENSE] done report_id=%s (compat)", rid)
            else:
                log.warning("[SENSE] report_id не найден по sid=%s time_frame=%s window_end=%s", sid, tf_win, win_end_iso)
        return

    log.warning("[SENSE] некорректный payload (нет report_id / time_frames): %r", evt)


# (вложенный) условия достаточности
def _to_dt_aware_or_naive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        try:
            base = s.split("+")[0].split("Z")[0]
            return datetime.fromisoformat(base)
        except Exception:
            return None


# 🔸 Поиск отчёта по (strategy_id, time_frame, window_end)
async def _find_report_id_by_sid_win(strategy_id: int, time_frame: str, window_end: Optional[datetime]) -> Optional[int]:
    async with infra.pg_pool.acquire() as conn:
        if window_end:
            row = await conn.fetchrow(
                """
                SELECT id
                  FROM oracle_report_stat
                 WHERE strategy_id = $1
                   AND time_frame  = $2
                   AND window_end  = $3
                 LIMIT 1
                """,
                strategy_id, time_frame, window_end.replace(tzinfo=None)
            )
            if row:
                return int(row["id"])
        # fallback: последний отчёт по окну
        row = await conn.fetchrow(
            """
            SELECT id
              FROM oracle_report_stat
             WHERE strategy_id = $1
               AND time_frame  = $2
             ORDER BY created_at DESC
             LIMIT 1
            """,
            strategy_id, time_frame
        )
        return int(row["id"]) if row else None


# 🔸 Обработка одного отчёта по report_id (окно всегда читаем из БД)
async def _process_report(report_id: int):
    async with infra.pg_pool.acquire() as conn:
        # шапка отчёта — источник истины
        hdr = await conn.fetchrow(
            """
            SELECT strategy_id, time_frame, window_start, window_end
              FROM oracle_report_stat
             WHERE id = $1
            """,
            report_id,
        )
        if not hdr:
            log.warning("[SENSE] report_id=%s — не найден в oracle_report_stat", report_id)
            return

        strategy_id = int(hdr["strategy_id"])
        time_frame = str(hdr["time_frame"])
        window_start: Optional[datetime] = hdr["window_start"]
        window_end: Optional[datetime] = hdr["window_end"]

        # агрегаты отчёта (confidence уже посчитан)
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

        # группировка по базе (dir, tf, base)
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

        # coverage и сводка по направлению — строго по окну из oracle_report_stat
        closed_by_dir: Dict[str, Dict[str, float]] = {"long": {"t": 0, "w": 0}, "short": {"t": 0, "w": 0}}
        if window_start and window_end:
            dir_rows = await conn.fetch(
                """
                SELECT direction,
                       COUNT(*)::int AS cnt,
                       COALESCE(SUM((pnl > 0)::int),0)::int AS wins
                  FROM positions_v4
                 WHERE strategy_id = $1
                   AND status = 'closed'
                   AND closed_at >= $2
                   AND closed_at <  $3
                 GROUP BY direction
                """,
                strategy_id, window_start, window_end,
            )
            for rr in dir_rows:
                d = str(rr["direction"])
                closed_by_dir[d] = {"t": int(rr["cnt"]), "w": int(rr["wins"])}
        else:
            log.warning("[SENSE] report_id=%s — окно без дат (window_start/window_end NULL), coverage может быть некорректен", report_id)

        # расчёт по каждой базе
        for (direction, timeframe, agg_base), items in group_map.items():
            T = sum(x["t"] for x in items)
            W = sum(x["w"] for x in items)
            wr_overall = _safe_div(W, T)

            # coverage
            T_all_dir = int(closed_by_dir.get(direction, {}).get("t", 0))
            coverage = max(0.0, min(1.0, _safe_div(T, T_all_dir))) if T_all_dir else 0.0

            # entropy_norm
            K_obs = sum(1 for x in items if x["t"] > 0)
            if T > 0 and K_obs > 1:
                p = [x["t"] / T for x in items if x["t"] > 0]
                H = -sum(pi * math.log(pi) for pi in p)
                H_max = math.log(len(p))
                entropy_norm = _safe_div(H, H_max)
            else:
                entropy_norm = 0.0

            # ig_norm
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

            # confidence_avg
            confidence_avg = sum((x["t"] / T) * max(0.0, min(1.0, x["conf"])) for x in items if T > 0 and x["t"] > 0) if T > 0 else 0.0

            # итоговые sense
            sense_raw = _geom_mean([coverage, entropy_norm, ig_norm, confidence_avg])

            # сглаживание (SMA по N=7)
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

            # UPSERT
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

            # лог результата
            log.info(
                "[SENSE] sid=%s win=%s tf=%s dir=%s base=%s | raw=%.4f smooth=%.4f | cov=%.4f ent=%.4f ig=%.4f conf=%.4f | T=%d K=%d",
                strategy_id, time_frame, timeframe, direction, agg_base,
                round(sense_raw, 4), round(sense_smooth, 4),
                round(coverage, 4), round(entropy_norm, 4), round(ig_norm, 4), round(confidence_avg, 4),
                int(T), int(K_obs),
            )

            # публикация Redis KV по каждому agg_state
            closed_total_dir = int(closed_by_dir.get(direction, {}).get("t", 0))
            wins_total_dir = int(closed_by_dir.get(direction, {}).get("w", 0))
            winrate_dir = _safe_div(wins_total_dir, closed_total_dir)

            for x in items:
                kv_key = f"{KV_PREFIX}:{strategy_id}:{time_frame}:{timeframe}:{direction}:{agg_base}:{x['agg_state']}"
                kv_val = {
                    "strategy_id": strategy_id,
                    "time_frame": time_frame,
                    "timeframe": timeframe,
                    "direction": direction,
                    "agg_base": agg_base,
                    "agg_state": x["agg_state"],
                    "report_id": report_id,
                    "window_start": window_start.isoformat() if window_start else None,
                    "window_end": window_end.isoformat() if window_end else None,
                    "closed_total": closed_total_dir,
                    "winrate": round(winrate_dir, 4),
                    "confidence_score": round(max(0.0, min(1.0, float(x["conf"]))), 4),
                    "sense_score_raw": round(float(sense_raw), 4),
                    "sense_score_smooth": round(float(sense_smooth), 4),
                }
                await infra.redis_client.set(kv_key, json.dumps(kv_val, separators=(",", ":")), ex=KV_TTL_SEC)

                log.info(
                    "[SENSE_KV] set key=%s | closed_total=%d winrate=%.4f conf=%.4f sense_raw=%.4f sense_smooth=%.4f",
                    kv_key, closed_total_dir, round(winrate_dir, 4),
                    round(float(x["conf"]), 4), round(float(sense_raw), 4), round(float(sense_smooth), 4)
                )