# 🔸 oracle_mw_snapshot.py — воркер MW-отчётов: батч-агрегация по СОСТОЯНИЯМ (solo/combos), публикация KV

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import json

import infra

log = logging.getLogger("ORACLE_MW_SNAPSHOT")

# 🔸 Константы воркера / параметры исполнения
INITIAL_DELAY_SEC = 90                    # первый запуск через 90 секунд
INTERVAL_SEC = 4 * 60 * 60                # периодичность — каждые 4 часа
REDIS_TTL_SEC = 8 * 60 * 60               # TTL KV публикаций — 8 часов
BATCH_SIZE = 500                          # размер батча по позициям
WINDOW_TAGS = ("7d", "14d", "28d")        # метки окон
WINDOW_SIZES = {
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "28d": timedelta(days=28),
}
TF_ORDER = ("m5", "m15", "h1")            # последовательная обработка TF
MW_BASES = ("trend", "volatility", "extremes", "momentum")  # фиксированный порядок для combo


# 🔸 Публичная точка запуска воркера (вызывается из oracle_v4_main.py → run_periodic)
async def run_oracle_mw_snapshot():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    strategies = sorted(infra.market_watcher_strategies or [])
    if not strategies:
        log.debug("ℹ️ Стратегий с market_watcher=true нет — нечего обрабатывать")
        return

    t_ref = datetime.utcnow().replace(tzinfo=None)  # UTC-naive по инвариантам системы
    log.debug("🚀 Старт MW-отчёта t0=%s, стратегий=%d", t_ref.isoformat(), len(strategies))

    # последовательная обработка стратегий
    async with infra.pg_pool.acquire() as conn:
        for sid in strategies:
            try:
                await _process_strategy(conn, sid, t_ref)
            except Exception:
                log.exception("❌ Ошибка обработки strategy_id=%s", sid)

    log.debug("✅ Завершено формирование MW-отчётов (стратегий=%d)", len(strategies))


# 🔸 Полный проход по стратегии: все окна → по каждому окну все TF последовательно
async def _process_strategy(conn, strategy_id: int, t_ref: datetime):
    for tag in WINDOW_TAGS:
        win_start = t_ref - WINDOW_SIZES[tag]
        win_end = t_ref

        # шапка отчёта: создаём черновик
        report_id = await _create_report_header(conn, strategy_id, tag, win_start, win_end)

        # агрегаты для шапки — одним SQL
        closed_total, closed_wins, pnl_sum_total, pnl_sum_wins = await _calc_report_head_metrics(
            conn, strategy_id, win_start, win_end
        )

        days_in_window = WINDOW_SIZES[tag].total_seconds() / 86400.0
        winrate = round((closed_wins / closed_total) if closed_total else 0.0, 4)
        avg_pnl_per_trade = round((pnl_sum_total / closed_total) if closed_total else 0.0, 4)
        avg_trades_per_day = round(closed_total / days_in_window, 4)

        await _finalize_report_header(
            conn=conn,
            report_id=report_id,
            closed_total=closed_total,
            closed_wins=closed_wins,
            winrate=winrate,
            pnl_sum_total=pnl_sum_total,
            pnl_sum_wins=pnl_sum_wins,
            avg_pnl_per_trade=avg_pnl_per_trade,
            avg_trades_per_day=avg_trades_per_day,
        )

        if closed_total == 0:
            log.debug("[REPORT] sid=%s win=%s total=0 — пропуск TF/агрегации", strategy_id, tag)
            continue

        # последовательный проход по TF
        for tf in TF_ORDER:
            try:
                await _process_timeframe(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
            except Exception:
                log.exception("❌ Ошибка агрегации sid=%s win=%s tf=%s", strategy_id, tag, tf)

        # публикация KV сводок по этому report
        try:
            await _publish_kv_bulk(conn, infra.redis_client, report_id, strategy_id, tag)
        except Exception:
            log.exception("❌ Ошибка публикации KV sid=%s win=%s", strategy_id, tag)

        log.debug(
            "[REPORT] sid=%s win=%s report_id=%s total=%d wins=%d wr=%.4f pnl_sum=%.4f avg_pnl=%.4f avg_tpd=%.4f",
            strategy_id, tag, report_id, closed_total, closed_wins, winrate, pnl_sum_total, avg_pnl_per_trade, avg_trades_per_day
        )


# 🔸 Создание черновика шапки отчёта
async def _create_report_header(conn, strategy_id: int, time_frame: str, win_start: datetime, win_end: datetime) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO oracle_report_stat (strategy_id, time_frame, window_start, window_end)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        strategy_id, time_frame, win_start, win_end
    )
    return int(row["id"])


# 🔸 Расчёт агрегатов для шапки (одним SQL)
async def _calc_report_head_metrics(conn, strategy_id: int, win_start: datetime, win_end: datetime):
    r = await conn.fetchrow(
        """
        SELECT
            COUNT(*)::int                         AS closed_total,
            COALESCE(SUM( (pnl > 0)::int ), 0)::int AS closed_wins,
            COALESCE(SUM(pnl), 0)::numeric(24,4)    AS pnl_sum_total,
            COALESCE(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END), 0)::numeric(24,4) AS pnl_sum_wins
        FROM positions_v4
        WHERE strategy_id = $1
          AND status = 'closed'
          AND closed_at >= $2
          AND closed_at <  $3
        """,
        strategy_id, win_start, win_end
    )
    return int(r["closed_total"]), int(r["closed_wins"]), float(r["pnl_sum_total"]), float(r["pnl_sum_wins"])


# 🔸 Финализация шапки отчёта (update метрик)
async def _finalize_report_header(
    conn,
    report_id: int,
    closed_total: int,
    closed_wins: int,
    winrate: float,
    pnl_sum_total: float,
    pnl_sum_wins: float,
    avg_pnl_per_trade: float,
    avg_trades_per_day: float,
):
    await conn.execute(
        """
        UPDATE oracle_report_stat
           SET closed_total       = $2,
               closed_wins        = $3,
               winrate            = $4,
               pnl_sum_total      = $5,
               pnl_sum_wins       = $6,
               avg_pnl_per_trade  = $7,
               avg_trades_per_day = $8
         WHERE id = $1
        """,
        report_id,
        int(closed_total),
        int(closed_wins),
        round(float(winrate), 4),
        round(float(pnl_sum_total), 4),
        round(float(pnl_sum_wins), 4),
        round(float(avg_pnl_per_trade), 4),
        round(float(avg_trades_per_day), 4),
    )

# 🔸 Обработка одного TF: выбор позиций окна → батч-агрегация MW-STATE → upsert агрегатов
async def _process_timeframe(
    conn,
    report_id: int,
    strategy_id: int,
    time_frame: str,
    timeframe: str,
    win_start: datetime,
    win_end: datetime,
    days_in_window: float,
):
    # выбираем закрытые позиции этого окна (direction, pnl)
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1
           AND status = 'closed'
           AND closed_at >= $2
           AND closed_at <  $3
        """,
        strategy_id, win_start, win_end
    )
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[TF] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    total = len(positions)
    ok_rows = 0
    batch_count = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for bi in range(batch_count):
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # читаем MW (включая ошибки) → агрегируем только status='ok' на текущем TF
        # states_tf приводим к ТЕКСТУ, чтобы предсказуемо парсить JSON далее
        rows_mw = await conn.fetch(
            """
            WITH mw AS (
              SELECT position_uid, timeframe, param_base, value_text, status
                FROM indicator_position_stat
               WHERE position_uid = ANY($1::text[])
                 AND param_type = 'marketwatch'
            )
            SELECT
              m.position_uid,
              bool_or(m.status = 'error') AS has_error,
              (jsonb_object_agg(m.param_base, m.value_text)
                 FILTER (WHERE m.timeframe = $2 AND m.status = 'ok' AND m.param_base = ANY($3::text[])))::text AS states_tf
            FROM mw m
            GROUP BY m.position_uid
            """,
            uid_list, timeframe, list(MW_BASES),
        )

        # подготовим агрегаты батча в памяти
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        if not rows_mw:
            continue

        # заготовки комбо (фиксированный порядок)
        combos_2 = (
            ("trend", "volatility"),
            ("trend", "extremes"),
            ("trend", "momentum"),
            ("volatility", "extremes"),
            ("volatility", "momentum"),
            ("extremes", "momentum"),
        )
        combos_3 = (
            ("trend", "volatility", "extremes"),
            ("trend", "volatility", "momentum"),
            ("trend", "extremes", "momentum"),
            ("volatility", "extremes", "momentum"),
        )
        combos_4 = (tuple(MW_BASES),)

        # обходим MW-строки
        for r in rows_mw:
            uid = r["position_uid"]
            has_error = bool(r["has_error"])
            raw_states = r["states_tf"]

            # парсим JSON надёжно
            if not raw_states or has_error:
                continue
            if isinstance(raw_states, dict):
                states_tf = raw_states
            else:
                try:
                    # raw_states — строка вида '{"trend":"down_weak", ...}'
                    states_tf = json.loads(raw_states)
                except Exception:
                    log.debug("[TF] skip uid=%s: states_tf JSON parse error: %r", uid, raw_states)
                    continue

            if not isinstance(states_tf, dict) or not states_tf:
                continue

            # нормализуем только допустимые базы
            states_tf = {k: v for k, v in states_tf.items() if k in MW_BASES and isinstance(v, str) and v}

            if not states_tf:
                continue

            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            is_win = pnl > 0.0

            # solo: по каждой доступной базе фиксируем её state
            for base in MW_BASES:
                state = states_tf.get(base)
                if not state:
                    continue
                k = (report_id, strategy_id, time_frame, direction, timeframe, "solo", base, state)
                inc = inc_map.setdefault(k, {"t": 0, "w": 0, "pt": 0.0, "pw": 0.0})
                inc["t"] += 1
                if is_win:
                    inc["w"] += 1
                    inc["pw"] = round(inc["pw"] + pnl, 4)
                inc["pt"] = round(inc["pt"] + pnl, 4)

            # combos: формируем в фиксированном порядке с join состояния
            def _touch_combo(combo: Tuple[str, ...]):
                # условия достаточности
                for b in combo:
                    if b not in states_tf:
                        return
                agg_base = "_".join(combo)
                agg_state = "|".join(f"{b}:{states_tf[b]}" for b in combo)  # 'trend:down_weak|volatility:expanding|...'
                k = (report_id, strategy_id, time_frame, direction, timeframe, "combo", agg_base, agg_state)
                inc = inc_map.setdefault(k, {"t": 0, "w": 0, "pt": 0.0, "pw": 0.0})
                inc["t"] += 1
                if is_win:
                    inc["w"] += 1
                    inc["pw"] = round(inc["pw"] + pnl, 4)
                inc["pt"] = round(inc["pt"] + pnl, 4)

            for c in combos_2:
                _touch_combo(c)
            for c in combos_3:
                _touch_combo(c)
            for c in combos_4:
                _touch_combo(c)

        # батчевый UPSERT
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    log.debug("[TF] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)

# 🔸 Батчевый UPSERT агрегатов (UNNEST + ON CONFLICT) с пересчётом метрик
async def _upsert_aggregates_batch(conn, inc_map: Dict[Tuple, Dict[str, float]], days_in_window: float):
    # готовим массивы полей (соответствует новому uq-ключу с agg_state)
    report_ids, strategy_ids, time_frames, directions = [], [], [], []
    timeframes, agg_types, agg_bases, agg_states = [], [], [], []
    trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc = [], [], [], []

    for (report_id, strategy_id, time_frame, direction, timeframe, agg_type, agg_base, agg_state), v in inc_map.items():
        report_ids.append(report_id)
        strategy_ids.append(strategy_id)
        time_frames.append(time_frame)
        directions.append(direction)
        timeframes.append(timeframe)
        agg_types.append(agg_type)
        agg_bases.append(agg_base)
        agg_states.append(agg_state)
        trades_inc.append(int(v["t"]))
        wins_inc.append(int(v["w"]))
        pnl_total_inc.append(round(float(v["pt"]), 4))
        pnl_wins_inc.append(round(float(v["pw"]), 4))

    await conn.execute(
        """
        WITH data AS (
          SELECT
            unnest($1::bigint[])   AS report_id,
            unnest($2::int[])      AS strategy_id,
            unnest($3::text[])     AS time_frame,
            unnest($4::text[])     AS direction,
            unnest($5::text[])     AS timeframe,
            unnest($6::text[])     AS agg_type,
            unnest($7::text[])     AS agg_base,
            unnest($8::text[])     AS agg_state,
            unnest($9::int[])      AS t_inc,
            unnest($10::int[])     AS w_inc,
            unnest($11::numeric[]) AS pt_inc,
            unnest($12::numeric[]) AS pw_inc
        )
        INSERT INTO oracle_mw_aggregated_stat (
            report_id, strategy_id, time_frame, direction, timeframe, agg_type, agg_base, agg_state,
            trades_total, trades_wins, winrate,
            pnl_sum_total, pnl_sum_wins,
            avg_pnl_per_trade, avg_trades_per_day
        )
        SELECT
            report_id, strategy_id, time_frame, direction, timeframe, agg_type, agg_base, agg_state,
            t_inc, w_inc,
            ROUND(CASE WHEN t_inc > 0 THEN w_inc::numeric / t_inc::numeric ELSE 0 END, 4),
            pt_inc, pw_inc,
            ROUND(CASE WHEN t_inc > 0 THEN pt_inc::numeric / t_inc::numeric ELSE 0 END, 4),
            ROUND(t_inc::numeric / $13::numeric, 4)
        FROM data
        ON CONFLICT (report_id, strategy_id, time_frame, direction, timeframe, agg_type, agg_base, agg_state)
        DO UPDATE SET
            trades_total       = oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total,
            trades_wins        = oracle_mw_aggregated_stat.trades_wins  + EXCLUDED.trades_wins,
            pnl_sum_total      = ROUND(oracle_mw_aggregated_stat.pnl_sum_total + EXCLUDED.pnl_sum_total, 4),
            pnl_sum_wins       = ROUND(oracle_mw_aggregated_stat.pnl_sum_wins  + EXCLUDED.pnl_sum_wins,  4),
            winrate            = ROUND(
                                   CASE
                                     WHEN (oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total) > 0
                                       THEN (oracle_mw_aggregated_stat.trades_wins + EXCLUDED.trades_wins)::numeric
                                            / (oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric
                                     ELSE 0
                                   END, 4),
            avg_pnl_per_trade  = ROUND(
                                   CASE
                                     WHEN (oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total) > 0
                                       THEN (oracle_mw_aggregated_stat.pnl_sum_total + EXCLUDED.pnl_sum_total)::numeric
                                            / (oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric
                                     ELSE 0
                                   END, 4),
            avg_trades_per_day = ROUND(
                                   ( (oracle_mw_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric / $13::numeric ),
                                   4),
            updated_at         = now()
        """,
        report_ids, strategy_ids, time_frames, directions, timeframes, agg_types, agg_bases, agg_states,
        trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc,
        days_in_window,
    )


# 🔸 Публикация KV сводок для отчёта (по последним строкам на direction+base+state)
async def _publish_kv_bulk(conn, redis, report_id: int, strategy_id: int, time_frame: str):
    row_rep = await conn.fetchrow("SELECT closed_total FROM oracle_report_stat WHERE id = $1", report_id)
    if not row_rep:
        return
    closed_total = int(row_rep["closed_total"] or 0)

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (direction, agg_base, agg_state)
               direction, agg_base, agg_state, trades_total, winrate
          FROM oracle_mw_aggregated_stat
         WHERE report_id = $1
         ORDER BY direction, agg_base, agg_state, updated_at DESC
        """,
        report_id,
    )
    if not rows:
        return

    pipe = redis.pipeline()
    for r in rows:
        direction = r["direction"]
        agg_base = r["agg_base"]
        agg_state = r["agg_state"]
        trades_total = int(r["trades_total"] or 0)
        winrate = float(r["winrate"] or 0.0)

        key = f"oracle:mw:{strategy_id}:{direction}:{agg_base}:{agg_state}:{time_frame}"
        payload = {
            "strategy_id": strategy_id,
            "direction": direction,
            "agg_base": agg_base,
            "agg_state": agg_state,
            "time_frame": time_frame,
            "report_id": report_id,
            "closed_total": closed_total,
            "agg_trades_total": trades_total,
            "winrate": f"{winrate:.4f}",
        }
        pipe.set(key, str(payload), ex=REDIS_TTL_SEC)

    await pipe.execute()