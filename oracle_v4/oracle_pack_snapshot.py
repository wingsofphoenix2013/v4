# 🔸 oracle_pack_snapshot.py — воркер PACK-отчётов: агрегация по RSI (solo/combos), каркас под остальные PACK-и

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import infra

log = logging.getLogger("ORACLE_PACK_SNAPSHOT")

# 🔸 Константы воркера / параметры исполнения
INITIAL_DELAY_SEC = 90                    # первый запуск через 90 секунд
INTERVAL_SEC = 4 * 60 * 60                # периодичность — каждые 4 часа
REDIS_TTL_SEC = 8 * 60 * 60               # TTL KV публикаций — 8 часов
BATCH_SIZE = 500                          # размер батча по позициям
WINDOW_TAGS = ("7d", "14d", "28d")
WINDOW_SIZES = {
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "28d": timedelta(days=28),
}
TF_ORDER = ("m5", "m15", "h1")

# 🔸 Белые списки полей по PACK (ориентир для агрегации)
PACK_FIELDS = {
    "rsi":     ["bucket_low", "trend"],  # <— реализовано
    "mfi":     ["bucket_low", "trend"],  # TODO
    "bb":      ["bucket", "bucket_delta", "bw_trend_strict", "bw_trend_smooth"],  # TODO
    "lr":      ["bucket", "bucket_delta", "angle_trend"],  # TODO
    "atr":     ["bucket", "bucket_delta"],  # TODO
    "adx_dmi": ["adx_bucket_low", "adx_dynamic_strict", "adx_dynamic_smooth", "gap_bucket_low", "gap_dynamic_strict", "gap_dynamic_smooth"],  # TODO
    "ema":     ["side", "dynamic", "dynamic_strict", "dynamic_smooth"],  # TODO
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct", "hist_trend_strict", "hist_trend_smooth"],  # TODO
}

# 🔸 Комбинации полей внутри PACK (пока реализуем только для rsi)
PACK_COMBOS = {
    "rsi": [
        ("bucket_low", "trend"),
    ],
    # "mfi": [...],
    # "bb": [...],
    # ...
}

# 🔸 Публичная точка запуска воркера (используется из oracle_v4_main.py → run_periodic)
async def run_oracle_pack_snapshot():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.info("❌ Пропуск: PG/Redis не инициализированы")
        return

    strategies = sorted(infra.market_watcher_strategies or [])
    if not strategies:
        log.info("ℹ️ Стратегий с market_watcher=true нет — нечего обрабатывать")
        return

    t_ref = datetime.utcnow().replace(tzinfo=None)  # UTC-naive
    log.info("🚀 Старт PACK-отчёта t0=%s, стратегий=%d", t_ref.isoformat(), len(strategies))

    async with infra.pg_pool.acquire() as conn:
        for sid in strategies:
            try:
                await _process_strategy(conn, sid, t_ref)
            except Exception:
                log.exception("❌ Ошибка PACK обработки strategy_id=%s", sid)

    log.info("✅ Завершено формирование PACK-отчётов (стратегий=%d)", len(strategies))


# 🔸 Полный проход по стратегии: все окна → по каждому окну все TF последовательно
async def _process_strategy(conn, strategy_id: int, t_ref: datetime):
    for tag in WINDOW_TAGS:
        win_start = t_ref - WINDOW_SIZES[tag]
        win_end = t_ref

        # создаём (или переиспользуем) шапку отчёта — та же таблица oracle_report_stat
        report_id = await _create_report_header(conn, strategy_id, tag, win_start, win_end)

        # общие метрики по окну — одним SQL
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
            log.info("[PACK REPORT] sid=%s win=%s total=0 — пропуск TF/агрегации", strategy_id, tag)
            continue

        # последовательный проход по TF — считаем ПАКИ (сейчас только RSI)
        for tf in TF_ORDER:
            try:
                await _process_timeframe_rsi(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # 🔸 Заглушки под остальные паки (реализовать позже)
                # await _process_timeframe_mfi(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_bb(conn,   report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_lr(conn,   report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_atr(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_adx(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_ema(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                # await _process_timeframe_macd(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
            except Exception:
                log.exception("❌ Ошибка PACK агрегации sid=%s win=%s tf=%s", strategy_id, tag, tf)

        # публикация KV сводок по этому report (на pack-ключи)
        try:
            await _publish_kv_bulk(conn, infra.redis_client, report_id, strategy_id, tag)
        except Exception:
            log.exception("❌ Ошибка публикации PACK KV sid=%s win=%s", strategy_id, tag)

        log.info(
            "[PACK REPORT] sid=%s win=%s report_id=%s total=%d wins=%d wr=%.4f pnl_sum=%.4f avg_pnl=%.4f avg_tpd=%.4f",
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


# 🔸 Обработка TF: PACK=RSI (solo + combo внутри RSI)
async def _process_timeframe_rsi(
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
        log.info("[PACK-RSI] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    total = len(positions)
    ok_rows = 0
    batch_count = (total + BATCH_SIZE - 1) // BATCH_SIZE

    # подсобные множества/порядки
    rsi_fields = PACK_FIELDS["rsi"]
    rsi_combos = PACK_COMBOS["rsi"]

    for bi in range(batch_count):
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # забираем PACK-значения только для RSI (param_base LIKE 'rsi%'), на текущем TF
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, timeframe, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack'
               AND timeframe = $2
               AND param_base LIKE 'rsi%%'
               AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, rsi_fields,
        )

        # группируем: uid → pack_base (rsi14/21/...) → { field: value(str) }
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        has_error: Dict[str, set] = {}  # uid -> set(pack_base) с ошибками

        for r in rows_pack:
            if r["status"] != "ok":
                # ошибка по конкретному pack_base: исключаем его (но не всю позицию)
                has_error.setdefault(r["position_uid"], set()).add(r["param_base"])
                continue

            uid = r["position_uid"]
            base = r["param_base"]           # rsi14, rsi21...
            name = r["param_name"]           # bucket_low | trend
            # нормализуем значение в строку
            if r["value_text"] is not None:
                val = str(r["value_text"])
            else:
                # численное — храним как компактную строку (без засорения)
                num = float(r["value_num"] or 0.0)
                # RSI bucket_low — как правило целое кратно 5 → формат без лишних нулей
                val = f"{num:.8f}".rstrip('0').rstrip('.') if '.' in f"{num:.8f}" else f"{int(num)}"

            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # формируем инкременты по batch (solo+combo)
        inc_map: Dict[Tuple, Dict[str, float]] = {}

        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            is_win = pnl > 0.0

            for base, fields in base_map.items():
                # пропускаем pack_base с error
                if base in has_error.get(uid, set()):
                    continue

                # SOLO: по каждому присутствующему полю
                for fname in rsi_fields:
                    if fname not in fields:
                        continue
                    fval = fields[fname]
                    k = (report_id, strategy_id, time_frame, direction, timeframe, base, "solo", fname, fval)
                    inc = inc_map.setdefault(k, {"t": 0, "w": 0, "pt": 0.0, "pw": 0.0})
                    inc["t"] += 1
                    if is_win:
                        inc["w"] += 1
                        inc["pw"] = round(inc["pw"] + pnl, 4)
                    inc["pt"] = round(inc["pt"] + pnl, 4)

                # COMBO: по заданным наборам полей (в порядке из PACK_COMBOS['rsi'])
                for combo in rsi_combos:
                    if not all(f in fields for f in combo):
                        continue
                    agg_key = "|".join(combo)  # 'bucket_low|trend'
                    agg_value = "|".join(f"{f}:{fields[f]}" for f in combo)  # 'bucket_low:50|trend:up'
                    k = (report_id, strategy_id, time_frame, direction, timeframe, base, "combo", agg_key, agg_value)
                    inc = inc_map.setdefault(k, {"t": 0, "w": 0, "pt": 0.0, "pw": 0.0})
                    inc["t"] += 1
                    if is_win:
                        inc["w"] += 1
                        inc["pw"] = round(inc["pw"] + pnl, 4)
                    inc["pt"] = round(inc["pt"] + pnl, 4)

        # батчевый UPSERT
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    log.info("[PACK-RSI] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Батчевый UPSERT (UNNEST + ON CONFLICT) с пересчётом метрик
async def _upsert_aggregates_batch(conn, inc_map: Dict[Tuple, Dict[str, float]], days_in_window: float):
    # ключ: (report_id, strategy_id, time_frame, direction, timeframe, pack_base, agg_type, agg_key, agg_value)
    report_ids, strategy_ids, time_frames, directions = [], [], [], []
    timeframes, pack_bases, agg_types, agg_keys, agg_values = [], [], [], [], []
    trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc = [], [], [], []

    for (report_id, strategy_id, time_frame, direction, timeframe, pack_base, agg_type, agg_key, agg_value), v in inc_map.items():
        report_ids.append(report_id)
        strategy_ids.append(strategy_id)
        time_frames.append(time_frame)
        directions.append(direction)
        timeframes.append(timeframe)
        pack_bases.append(pack_base)
        agg_types.append(agg_type)
        agg_keys.append(agg_key)
        agg_values.append(agg_value)
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
            unnest($6::text[])     AS pack_base,
            unnest($7::text[])     AS agg_type,
            unnest($8::text[])     AS agg_key,
            unnest($9::text[])     AS agg_value,
            unnest($10::int[])     AS t_inc,
            unnest($11::int[])     AS w_inc,
            unnest($12::numeric[]) AS pt_inc,
            unnest($13::numeric[]) AS pw_inc
        )
        INSERT INTO oracle_pack_aggregated_stat (
            report_id, strategy_id, time_frame, direction, timeframe, pack_base, agg_type, agg_key, agg_value,
            trades_total, trades_wins, winrate,
            pnl_sum_total, pnl_sum_wins,
            avg_pnl_per_trade, avg_trades_per_day
        )
        SELECT
            report_id, strategy_id, time_frame, direction, timeframe, pack_base, agg_type, agg_key, agg_value,
            t_inc, w_inc,
            ROUND(CASE WHEN t_inc > 0 THEN w_inc::numeric / t_inc::numeric ELSE 0 END, 4),
            pt_inc, pw_inc,
            ROUND(CASE WHEN t_inc > 0 THEN pt_inc::numeric / t_inc::numeric ELSE 0 END, 4),
            ROUND(t_inc::numeric / $14::numeric, 4)
        FROM data
        ON CONFLICT (report_id, strategy_id, time_frame, direction, timeframe, pack_base, agg_type, agg_key, agg_value)
        DO UPDATE SET
            trades_total       = oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total,
            trades_wins        = oracle_pack_aggregated_stat.trades_wins  + EXCLUDED.trades_wins,
            pnl_sum_total      = ROUND(oracle_pack_aggregated_stat.pnl_sum_total + EXCLUDED.pnl_sum_total, 4),
            pnl_sum_wins       = ROUND(oracle_pack_aggregated_stat.pnl_sum_wins  + EXCLUDED.pnl_sum_wins,  4),
            winrate            = ROUND(
                                   CASE
                                     WHEN (oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total) > 0
                                       THEN (oracle_pack_aggregated_stat.trades_wins + EXCLUDED.trades_wins)::numeric
                                            / (oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric
                                     ELSE 0
                                   END, 4),
            avg_pnl_per_trade  = ROUND(
                                   CASE
                                     WHEN (oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total) > 0
                                       THEN (oracle_pack_aggregated_stat.pnl_sum_total + EXCLUDED.pnl_sum_total)::numeric
                                            / (oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric
                                     ELSE 0
                                   END, 4),
            avg_trades_per_day = ROUND(
                                   ( (oracle_pack_aggregated_stat.trades_total + EXCLUDED.trades_total)::numeric / $14::numeric ),
                                   4),
            updated_at         = now()
        """,
        report_ids, strategy_ids, time_frames, directions, timeframes, pack_bases, agg_types, agg_keys, agg_values,
        trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc,
        days_in_window,
    )


# 🔸 Публикация KV сводок
async def _publish_kv_bulk(conn, redis, report_id: int, strategy_id: int, time_frame: str):
    row_rep = await conn.fetchrow("SELECT closed_total FROM oracle_report_stat WHERE id = $1", report_id)
    if not row_rep:
        return
    closed_total = int(row_rep["closed_total"] or 0)

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (direction, pack_base, agg_key, agg_value)
               direction, pack_base, agg_key, agg_value, trades_total, winrate
          FROM oracle_pack_aggregated_stat
         WHERE report_id = $1
         ORDER BY direction, pack_base, agg_key, agg_value, updated_at DESC
        """,
        report_id,
    )
    if not rows:
        return

    pipe = redis.pipeline()
    for r in rows:
        direction = r["direction"]
        pack_base = r["pack_base"]
        agg_key = r["agg_key"]
        agg_value = r["agg_value"]
        trades_total = int(r["trades_total"] or 0)
        winrate = float(r["winrate"] or 0.0)

        key = f"oracle:pack:{strategy_id}:{direction}:{pack_base}:{agg_key}:{agg_value}:{time_frame}"
        payload = {
            "strategy_id": strategy_id,
            "direction": direction,
            "pack_base": pack_base,
            "agg_key": agg_key,
            "agg_value": agg_value,
            "time_frame": time_frame,
            "report_id": report_id,
            "closed_total": closed_total,
            "agg_trades_total": trades_total,
            "winrate": f"{winrate:.4f}",
        }
        # можно заменить на json.dumps(payload) при желании
        pipe.set(key, json.dumps(payload), ex=REDIS_TTL_SEC)

    await pipe.execute()