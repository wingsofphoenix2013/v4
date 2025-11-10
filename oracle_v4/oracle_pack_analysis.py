# oracle_pack_analysis.py — воркер RAW-анализа PACK-комбо (7d): пишет oracle_pack_analysis и перестраивает oracle_pack_active

# 🔸 Импорты
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

import infra
from oracle_pack_snapshot import PACK_COMBOS  # используем допустимые комбо из существующего контура

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_ANALYSIS")

# 🔸 Константы воркера / стримы
PACK_REPORT_STREAM = "oracle:pack:reports_ready"    # источник событий (готов PACK-репорт)
PACK_ANALYSIS_CONSUMER_GROUP = "oracle_pack_analysis_group"
PACK_ANALYSIS_CONSUMER_NAME  = "oracle_pack_analysis_worker"
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Таймфреймы и окно
TF_ORDER = ("m5", "m15", "h1")
WINDOW_SIZE_7D = timedelta(days=7)

# 🔸 Пороговые параметры для принятия решения (RAW)
MIN_WITH = 50              # минимум сделок в группе "with"
MIN_WITHOUT = 50           # минимум сделок в группе "without"
MIN_PRESENCE = 0.05        # нижняя граница наличия паттерна
MAX_PRESENCE = 0.95        # верхняя граница наличия паттерна
MIN_DELTA_ROI = 0.001      # минимум прироста ROI (в долях депозита за окно)
MIN_DELTA_WR  = 0.01       # минимум прироста winrate (в абсолютных долях)
Z_THRESHOLD   = 2.0        # порог z-теста для разности долей (~p<0.045)

# 🔸 Публичная точка входа воркера (ивент-драйв: слушаем PACK reports_ready → считаем RAW-эффекты)
async def run_oracle_pack_analysis():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск PACK_ANALYSIS: PG/Redis не инициализированы")
        return

    # создание consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=PACK_REPORT_STREAM,
            groupname=PACK_ANALYSIS_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана consumer group для %s", PACK_REPORT_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка создания consumer group для %s", PACK_REPORT_STREAM)
            return

    log.info("🚀 PACK RAW analysis слушает %s (group=%s, consumer=%s)",
             PACK_REPORT_STREAM, PACK_ANALYSIS_CONSUMER_GROUP, PACK_ANALYSIS_CONSUMER_NAME)

    # основной цикл чтения событий
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=PACK_ANALYSIS_CONSUMER_GROUP,
                consumername=PACK_ANALYSIS_CONSUMER_NAME,
                streams={PACK_REPORT_STREAM: ">"},
                count=READ_COUNT,
                block=READ_BLOCK_MS,
            )
            if not resp:
                continue

            acks: List[str] = []
            async with infra.pg_pool.acquire() as conn:
                for _stream_name, msgs in resp:
                    for msg_id, fields in msgs:
                        try:
                            payload = json.loads(fields.get("data", "{}"))
                            sid = int(payload.get("strategy_id", 0))
                            time_frame = str(payload.get("time_frame", "")).lower()
                            win_end_iso = payload.get("window_end")
                            win_start_iso = payload.get("window_start")  # может отсутствовать

                            # фильтр: только валидные события и только 7d
                            if not (sid and win_end_iso and time_frame == "7d"):
                                acks.append(msg_id)
                                continue

                            # стратегия должна быть актуальна (market_watcher=true)
                            if infra.market_watcher_strategies and sid not in infra.market_watcher_strategies:
                                acks.append(msg_id)
                                continue

                            t_end = _parse_iso_utcnaive(win_end_iso)
                            if t_end is None:
                                acks.append(msg_id)
                                continue

                            # гард «последний 7d (source='pack')»
                            if not await _is_latest_or_equal_7d_pack(conn, sid, t_end):
                                acks.append(msg_id)
                                continue

                            # окно расчёта
                            if win_start_iso:
                                t_start = _parse_iso_utcnaive(win_start_iso) or (t_end - WINDOW_SIZE_7D)
                            else:
                                t_start = t_end - WINDOW_SIZE_7D

                            # основной расчёт по стратегии
                            try:
                                totals = await _process_strategy_raw_7d(conn, sid, t_start, t_end)
                                acks.append(msg_id)
                                # лог итога по стратегии
                                log.info("[SID=%s] PACK RAW 7d [%s .. %s): analysis_rows=%d, active_on=%d (m5=%d, m15=%d, h1=%d)",
                                         sid, t_start.isoformat(), t_end.isoformat(),
                                         totals["analysis_rows_total"], totals["active_total"],
                                         totals["active_m5"], totals["active_m15"], totals["active_h1"])
                            except Exception:
                                log.exception("❌ Ошибка PACK RAW анализа по sid=%s", sid)
                                acks.append(msg_id)

                        except Exception:
                            log.exception("❌ Ошибка разбора/обработки PACK_ANALYSIS сообщения")
                            acks.append(msg_id)

            # подтверждаем обработанные сообщения
            if acks:
                try:
                    await infra.redis_client.xack(PACK_REPORT_STREAM, PACK_ANALYSIS_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ Ошибка ACK для %s", PACK_REPORT_STREAM)

        except asyncio.CancelledError:
            log.debug("⏹️ PACK_ANALYSIS остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла PACK_ANALYSIS — пауза 5 секунд")
            await asyncio.sleep(5)

# 🔸 Обработка стратегии по окну 7d: расчёт RAW-эффектов по всем TF, перестройка ACTIVE
async def _process_strategy_raw_7d(conn, strategy_id: int, win_start: datetime, win_end: datetime) -> Dict[str, int]:
    # депозит стратегии (нормировка ROI)
    deposit = await conn.fetchval("SELECT COALESCE(deposit,0)::float8 FROM strategies_v4 WHERE id = $1", int(strategy_id))
    dep = float(deposit or 0.0)
    if dep <= 0.0:
        dep = 1.0

    # агрегаторы итогов
    analysis_rows_total = 0
    active_total = 0
    active_by_tf = {"m5": 0, "m15": 0, "h1": 0}

    # предварительно читаем предыдущие ACTIVE (для устойчивости решения)
    prev_active = await _load_prev_active_map(conn, strategy_id)

    for tf in TF_ORDER:
        try:
            # расчёт и запись истории
            rows_metrics = await _calc_raw_metrics_for_tf(conn, strategy_id, tf, win_start, win_end, dep)
            if not rows_metrics:
                # нет позиций → очищаем ACTIVE по этому TF
                await conn.execute(
                    "DELETE FROM oracle_pack_active WHERE strategy_id = $1 AND timeframe = $2",
                    int(strategy_id), str(tf)
                )
                log.info("[SID=%s][%s] total=0 — ACTIVE очищен", strategy_id, tf)
                continue

            # upsert истории в oracle_pack_analysis
            await _upsert_analysis_batch(conn, strategy_id, tf, win_start, win_end, dep, rows_metrics)
            analysis_rows_total += len(rows_metrics)

            # отбор победителей в ACTIVE по базам
            winners = _select_active_winners(strategy_id, tf, rows_metrics)
            # пересборка ACTIVE (с учётом устойчивости)
            inserted = await _rebuild_active_for_tf(conn, strategy_id, tf, win_start, win_end, winners, prev_active)
            active_total += inserted
            active_by_tf[tf] = inserted

            # лог по TF
            log.info("[SID=%s][%s] analysis_rows=%d, active_on=%d, deposit=%.4f",
                     strategy_id, tf, len(rows_metrics), inserted, dep)
        except Exception:
            log.exception("❌ Ошибка PACK RAW по sid=%s tf=%s", strategy_id, tf)

    return {
        "analysis_rows_total": int(analysis_rows_total),
        "active_total": int(active_total),
        "active_m5": int(active_by_tf["m5"]),
        "active_m15": int(active_by_tf["m15"]),
        "active_h1": int(active_by_tf["h1"]),
    }

# 🔸 Расчёт метрик RAW по одному TF (возвращает список строк-метрик по (pack_base, agg_key))
async def _calc_raw_metrics_for_tf(
    conn,
    strategy_id: int,
    tf: str,
    win_start: datetime,
    win_end: datetime,
    deposit_used: float,
) -> List[Dict[str, Any]]:
    # выбираем все закрытые позиции окна (без фильтра по TF, как в snapshot-пайплайнах)
    # затем «presence» на TF через oracle_pack_positions → oracle_pack_dict (фильтрация по TF/направлению)
    rows = await conn.fetch(
        """
        WITH base_p AS (
          SELECT position_uid, direction, COALESCE(pnl,0)::float8 AS pnl
          FROM positions_v4
          WHERE strategy_id = $1
            AND status = 'closed'
            AND closed_at >= $2 AND closed_at < $3
        ),
        totals AS (
          SELECT
            COUNT(*)::int              AS n_total,
            COALESCE(SUM((pnl > 0)::int),0)::int AS wins_total,
            COALESCE(SUM(pnl),0)::float8        AS pnl_total
          FROM base_p
        ),
        present AS (
          SELECT DISTINCT p.position_uid, pd.pack_base, pd.agg_key
          FROM base_p p
          JOIN oracle_pack_positions opp ON opp.position_uid = p.position_uid
          JOIN oracle_pack_dict pd       ON pd.id = opp.pack_dict_id
          WHERE pd.strategy_id = $1
            AND pd.timeframe   = $4
            AND pd.direction   = p.direction
        ),
        with_by AS (
          SELECT
            pr.pack_base,
            pr.agg_key,
            COUNT(*)::int                                         AS n_with,
            COALESCE(SUM((bp.pnl > 0)::int),0)::int               AS wins_with,
            COALESCE(SUM(bp.pnl),0)::float8                       AS pnl_with
          FROM present pr
          JOIN base_p bp ON bp.position_uid = pr.position_uid
          GROUP BY pr.pack_base, pr.agg_key
        )
        SELECT
          wb.pack_base,
          wb.agg_key,
          t.n_total,
          wb.n_with,
          (t.n_total - wb.n_with)         AS n_without,
          wb.wins_with,
          (t.wins_total - wb.wins_with)   AS wins_without,
          wb.pnl_with,
          (t.pnl_total - wb.pnl_with)     AS pnl_without
        FROM with_by wb
        CROSS JOIN totals t
        """,
        int(strategy_id), win_start, win_end, str(tf)
    )

    # если нет вообще позиций (totals = 0) — rows будет пустым
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        n_total = int(r["n_total"] or 0)
        if n_total <= 0:
            continue

        pack_base = str(r["pack_base"])
        agg_key   = str(r["agg_key"])
        n_with    = int(r["n_with"] or 0)
        n_without = int(r["n_without"] or 0)
        wins_with = int(r["wins_with"] or 0)
        wins_without = int(r["wins_without"] or 0)
        pnl_with  = float(r["pnl_with"] or 0.0)
        pnl_without = float(r["pnl_without"] or 0.0)

        # базовые метрики
        winrate_with = round((wins_with / n_with), 4) if n_with > 0 else 0.0
        winrate_without = round((wins_without / n_without), 4) if n_without > 0 else 0.0
        roi_with = round((pnl_with / deposit_used), 6) if deposit_used > 0 else 0.0
        roi_without = round((pnl_without / deposit_used), 6) if deposit_used > 0 else 0.0
        delta_winrate = round(winrate_with - winrate_without, 4)
        delta_roi = round(roi_with - roi_without, 6)
        presence_rate = round((n_with / n_total), 4) if n_total > 0 else 0.0

        # z-тест для разности долей (приближение нормальное)
        total_wins = wins_with + wins_without
        total_n = n_with + n_without
        if n_with > 0 and n_without > 0 and total_n > 0:
            p = total_wins / total_n
            denom = (p * (1.0 - p) * (1.0 / n_with + 1.0 / n_without)) ** 0.5
            z_winrate = (delta_winrate / denom) if denom > 0 else None
        else:
            z_winrate = None

        # флаги качества/эффекта
        support_ok = (n_with >= MIN_WITH) and (n_without >= MIN_WITHOUT) and (MIN_PRESENCE <= presence_rate <= MAX_PRESENCE)
        effect_ok = (delta_roi >= MIN_DELTA_ROI) and (delta_winrate >= MIN_DELTA_WR) and ((z_winrate or 0.0) >= Z_THRESHOLD)

        out.append({
            "pack_base": pack_base,
            "agg_key": agg_key,
            "n_total": n_total,
            "n_with": n_with,
            "n_without": n_without,
            "wins_with": wins_with,
            "wins_without": wins_without,
            "pnl_with": round(pnl_with, 4),
            "pnl_without": round(pnl_without, 4),
            "winrate_with": winrate_with,
            "winrate_without": winrate_without,
            "roi_with": roi_with,
            "roi_without": roi_without,
            "delta_winrate": delta_winrate,
            "delta_roi": delta_roi,
            "presence_rate": presence_rate,
            "z_winrate": round(float(z_winrate), 4) if isinstance(z_winrate, (int, float)) else None,
            "support_ok": support_ok,
            "effect_ok": effect_ok,
        })

    return out

# 🔸 UPSERT батча истории в oracle_pack_analysis
async def _upsert_analysis_batch(
    conn,
    strategy_id: int,
    tf: str,
    win_start: datetime,
    win_end: datetime,
    deposit_used: float,
    rows_metrics: List[Dict[str, Any]],
):
    # подготовка массивов для UNNEST
    args = {
        "strategy_id": [],
        "time_frame": [],
        "timeframe": [],
        "pack_base": [],
        "agg_key": [],
        "window_start": [],
        "window_end": [],
        "n_total": [],
        "n_with": [],
        "n_without": [],
        "wins_with": [],
        "wins_without": [],
        "pnl_with": [],
        "pnl_without": [],
        "winrate_with": [],
        "winrate_without": [],
        "roi_with": [],
        "roi_without": [],
        "delta_winrate": [],
        "delta_roi": [],
        "presence_rate": [],
        "z_winrate": [],
        "support_ok": [],
        "effect_ok": [],
        "deposit_used": [],
    }

    for m in rows_metrics:
        args["strategy_id"].append(int(strategy_id))
        args["time_frame"].append("7d")
        args["timeframe"].append(str(tf))
        args["pack_base"].append(str(m["pack_base"]))
        args["agg_key"].append(str(m["agg_key"]))
        args["window_start"].append(win_start)
        args["window_end"].append(win_end)

        args["n_total"].append(int(m["n_total"]))
        args["n_with"].append(int(m["n_with"]))
        args["n_without"].append(int(m["n_without"]))
        args["wins_with"].append(int(m["wins_with"]))
        args["wins_without"].append(int(m["wins_without"]))
        args["pnl_with"].append(float(m["pnl_with"]))
        args["pnl_without"].append(float(m["pnl_without"]))

        args["winrate_with"].append(float(m["winrate_with"]))
        args["winrate_without"].append(float(m["winrate_without"]))
        args["roi_with"].append(float(m["roi_with"]))
        args["roi_without"].append(float(m["roi_without"]))
        args["delta_winrate"].append(float(m["delta_winrate"]))
        args["delta_roi"].append(float(m["delta_roi"]))
        args["presence_rate"].append(float(m["presence_rate"]))
        # NULL для z_winrate допускается
        z_val = m["z_winrate"]
        args["z_winrate"].append(None if z_val is None else float(z_val))
        args["support_ok"].append(bool(m["support_ok"]))
        args["effect_ok"].append(bool(m["effect_ok"]))
        args["deposit_used"].append(float(deposit_used))

    await conn.execute(
        """
        WITH data AS (
          SELECT
            unnest($1::int[])        AS strategy_id,
            unnest($2::text[])       AS time_frame,
            unnest($3::text[])       AS timeframe,
            unnest($4::text[])       AS pack_base,
            unnest($5::text[])       AS agg_key,
            unnest($6::timestamp[])  AS window_start,
            unnest($7::timestamp[])  AS window_end,

            unnest($8::int[])        AS n_total,
            unnest($9::int[])        AS n_with,
            unnest($10::int[])       AS n_without,
            unnest($11::int[])       AS wins_with,
            unnest($12::int[])       AS wins_without,
            unnest($13::numeric[])   AS pnl_with,
            unnest($14::numeric[])   AS pnl_without,

            unnest($15::numeric[])   AS winrate_with,
            unnest($16::numeric[])   AS winrate_without,
            unnest($17::numeric[])   AS roi_with,
            unnest($18::numeric[])   AS roi_without,
            unnest($19::numeric[])   AS delta_winrate,
            unnest($20::numeric[])   AS delta_roi,
            unnest($21::numeric[])   AS presence_rate,
            unnest($22::numeric[])   AS z_winrate,
            unnest($23::bool[])      AS support_ok,
            unnest($24::bool[])      AS effect_ok,
            unnest($25::numeric[])   AS deposit_used
        )
        INSERT INTO oracle_pack_analysis (
          strategy_id, time_frame, timeframe, pack_base, agg_key, window_start, window_end,
          n_total, n_with, n_without, wins_with, wins_without, pnl_with, pnl_without,
          winrate_with, winrate_without, roi_with, roi_without, delta_winrate, delta_roi,
          presence_rate, z_winrate, support_ok, effect_ok, deposit_used, computed_at
        )
        SELECT
          strategy_id, time_frame, timeframe, pack_base, agg_key, window_start, window_end,
          n_total, n_with, n_without, wins_with, wins_without, pnl_with, pnl_without,
          winrate_with, winrate_without, roi_with, roi_without, delta_winrate, delta_roi,
          presence_rate, z_winrate, support_ok, effect_ok, deposit_used, now()
        FROM data
        ON CONFLICT (strategy_id, time_frame, timeframe, pack_base, agg_key, window_start, window_end)
        DO UPDATE SET
          n_total        = EXCLUDED.n_total,
          n_with         = EXCLUDED.n_with,
          n_without      = EXCLUDED.n_without,
          wins_with      = EXCLUDED.wins_with,
          wins_without   = EXCLUDED.wins_without,
          pnl_with       = EXCLUDED.pnl_with,
          pnl_without    = EXCLUDED.pnl_without,
          winrate_with   = EXCLUDED.winrate_with,
          winrate_without= EXCLUDED.winrate_without,
          roi_with       = EXCLUDED.roi_with,
          roi_without    = EXCLUDED.roi_without,
          delta_winrate  = EXCLUDED.delta_winrate,
          delta_roi      = EXCLUDED.delta_roi,
          presence_rate  = EXCLUDED.presence_rate,
          z_winrate      = EXCLUDED.z_winrate,
          support_ok     = EXCLUDED.support_ok,
          effect_ok      = EXCLUDED.effect_ok,
          deposit_used   = EXCLUDED.deposit_used,
          computed_at    = now()
        """,
        args["strategy_id"], args["time_frame"], args["timeframe"], args["pack_base"], args["agg_key"],
        args["window_start"], args["window_end"], args["n_total"], args["n_with"], args["n_without"],
        args["wins_with"], args["wins_without"], args["pnl_with"], args["pnl_without"],
        args["winrate_with"], args["winrate_without"], args["roi_with"], args["roi_without"],
        args["delta_winrate"], args["delta_roi"], args["presence_rate"], args["z_winrate"],
        args["support_ok"], args["effect_ok"], args["deposit_used"],
    )

# 🔸 Выбор победителей для ACTIVE по базам (на основе rows_metrics одного TF)
def _select_active_winners(strategy_id: int, tf: str, rows_metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # группируем по базе
    by_base: Dict[str, List[Dict[str, Any]]] = {}
    for m in rows_metrics:
        by_base.setdefault(m["pack_base"], []).append(m)

    winners: List[Dict[str, Any]] = []
    for pack_base, items in by_base.items():
        # определяем семейство и допустимые комбо (ограничиваем поиск осмысленными вариантами)
        family = _pack_family_from_base(pack_base)
        allowed_keys = set(PACK_COMBOS.get(family, []))

        # кандидаты: прошли support_ok & effect_ok, комбо допустимо
        candidates = [
            m for m in items
            if m["support_ok"] and m["effect_ok"] and m["agg_key"] in allowed_keys
        ]
        if not candidates:
            continue

        # выбираем победителя по delta_roi (далее z_winrate, затем n_with)
        candidates.sort(
            key=lambda x: (float(x["delta_roi"]), float(x.get("z_winrate") or 0.0), int(x["n_with"])),
            reverse=True
        )
        best = candidates[0]
        winners.append({
            "strategy_id": strategy_id,
            "timeframe": tf,
            "pack_base": pack_base,
            "agg_key_selected": best["agg_key"],
            "delta_winrate": best["delta_winrate"],
            "delta_roi": best["delta_roi"],
            "n_with": best["n_with"],
            "n_without": best["n_without"],
            "presence_rate": best["presence_rate"],
            "z_winrate": best["z_winrate"],
        })

    return winners

# 🔸 Перестроение ACTIVE по TF: полное обновление (с учётом устойчивости решения)
async def _rebuild_active_for_tf(
    conn,
    strategy_id: int,
    tf: str,
    win_start: datetime,
    win_end: datetime,
    winners: List[Dict[str, Any]],
    prev_active_map: Dict[Tuple[int, str, str], int],
) -> int:
    # удаляем прошлый ACTIVE для стратегии и TF
    await conn.execute(
        "DELETE FROM oracle_pack_active WHERE strategy_id = $1 AND timeframe = $2",
        int(strategy_id), str(tf)
    )
    if not winners:
        return 0

    # подготавливаем вставку (считаем устойчивость)
    rows_to_insert: List[Tuple[Any, ...]] = []
    now_ts = datetime.utcnow().replace(tzinfo=None)
    inserted = 0

    for w in winners:
        key = (int(strategy_id), str(tf), str(w["pack_base"]))
        prev_streak = int(prev_active_map.get(key, 0))
        streak = prev_streak + 1

        rows_to_insert.append((
            int(strategy_id),
            str(tf),
            str(w["pack_base"]),
            str(w["agg_key_selected"]),
            True,  # use_indicator
            float(w["delta_winrate"]),
            float(w["delta_roi"]),
            int(w["n_with"]),
            int(w["n_without"]),
            float(w["presence_rate"]),
            None if w["z_winrate"] is None else float(w["z_winrate"]),
            int(streak),
            win_start, win_end,
            now_ts, now_ts,
        ))
        inserted += 1

    # массовая вставка ACTIVE
    await conn.executemany(
        """
        INSERT INTO oracle_pack_active (
          strategy_id, timeframe, pack_base, agg_key_selected, use_indicator,
          delta_winrate, delta_roi, n_with, n_without, presence_rate, z_winrate,
          decision_sustained_windows, window_start, window_end, computed_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,
          $6,$7,$8,$9,$10,$11,
          $12,$13,$14,$15,$16
        )
        """,
        rows_to_insert
    )
    return inserted

# 🔸 Загрузка предыдущего ACTIVE (для подсчёта устойчивости)
async def _load_prev_active_map(conn, strategy_id: int) -> Dict[Tuple[int, str, str], int]:
    rows = await conn.fetch(
        """
        SELECT strategy_id, timeframe, pack_base, decision_sustained_windows
        FROM oracle_pack_active
        WHERE strategy_id = $1
        """,
        int(strategy_id)
    )
    out: Dict[Tuple[int, str, str], int] = {}
    for r in rows:
        key = (int(r["strategy_id"]), str(r["timeframe"]), str(r["pack_base"]))
        out[key] = int(r["decision_sustained_windows"] or 0)
    return out

# 🔸 Утилиты

def _parse_iso_utcnaive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None

async def _is_latest_or_equal_7d_pack(conn, strategy_id: int, window_end: datetime) -> bool:
    last = await conn.fetchval(
        """
        SELECT MAX(window_end) FROM oracle_report_stat
        WHERE strategy_id = $1 AND time_frame = '7d' AND source = 'pack'
        """,
        int(strategy_id)
    )
    if last is None:
        return True
    return window_end >= last

def _pack_family_from_base(pack_base: str) -> str:
    s = (pack_base or "").strip().lower()
    # bb → 'bb', adx_dmi* → 'adx_dmi', иначе — алфавитный префикс до первой не-буквы
    if s.startswith("bb"):
        return "bb"
    if s.startswith("adx_dmi"):
        return "adx_dmi"
    out = []
    for ch in s:
        if ch.isalpha():
            out.append(ch)
        else:
            break
    return "".join(out)