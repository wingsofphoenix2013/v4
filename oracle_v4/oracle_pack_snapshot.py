# oracle_pack_snapshot.py — воркер PACK-отчётов (ограниченные COMBO): агрегация по PACK и публикация события в стрим

# 🔸 Импорты
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_PACK_SNAPSHOT")

# 🔸 Константы воркера / параметры исполнения
INITIAL_DELAY_SEC = 90                    # первый запуск через 90 секунд
INTERVAL_SEC = 3 * 60 * 60                # периодичность — каждые 3 часа
BATCH_SIZE = 500                          # размер батча по позициям
WINDOW_TAGS = ("7d", "14d", "28d")
WINDOW_SIZES = {
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "28d": timedelta(days=28),
}
TF_ORDER = ("m5", "m15", "h1")

# 🔸 Whitelist полей и КОМБИНАЦИЙ (solo НЕ пишем, только перечисленные combo)
PACK_FIELDS = {
    "rsi":     ["bucket_low", "trend"],
    "mfi":     ["bucket_low", "trend"],
    "bb":      ["bucket", "bucket_delta", "bw_trend_smooth"],
    "lr":      ["bucket", "bucket_delta", "angle_trend"],
    "atr":     ["bucket", "bucket_delta"],
    "adx_dmi": ["adx_bucket_low", "gap_bucket_low", "adx_dynamic_smooth", "gap_dynamic_smooth"],
    "ema":     ["side", "dynamic", "dynamic_smooth"],
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct", "hist_trend_smooth"],
}

# 🔸 Разрешённые COMBO (строки — имена полей через "|")
PACK_COMBOS = {
    "rsi": ["bucket_low|trend"],
    "mfi": ["bucket_low|trend"],
    "atr": ["bucket|bucket_delta"],

    "bb": [
        "bucket|bucket_delta",
        "bucket|bw_trend_smooth",
        "bucket_delta|bw_trend_smooth",
        "bucket|bucket_delta|bw_trend_smooth",
    ],

    "lr": [
        "bucket|bucket_delta",
        "bucket|angle_trend",
        "bucket_delta|angle_trend",
        "bucket|bucket_delta|angle_trend",
    ],

    "adx_dmi": [
        "adx_bucket_low|adx_dynamic_smooth",
        "gap_bucket_low|gap_dynamic_smooth",
        "adx_dynamic_smooth|gap_dynamic_smooth",
    ],

    "ema": [
        "side|dynamic_smooth",
        "side|dynamic",
    ],

    "macd": [
        "mode|cross",
        "mode|hist_trend_smooth",
        "mode|hist_bucket_low_pct",
        "cross|hist_trend_smooth",
        "mode|zero_side",
    ],
}

# 🔸 Настройки Redis Stream (сигнал «отчёт готов» по PACK)
REPORT_READY_STREAM = "oracle:pack:reports_ready"
REPORT_READY_MAXLEN = 10_000  # XADD MAXLEN ~


# 🔸 Публичная точка запуска воркера (используется из oracle_v4_main.py → run_periodic)
async def run_oracle_pack_snapshot():
    # условия достаточности
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # набор стратегий для обработки
    strategies = sorted(infra.market_watcher_strategies or [])
    if not strategies:
        log.debug("ℹ️ Стратегий с market_watcher=true нет — нечего обрабатывать")
        return

    # момент отсчёта окна
    t_ref = datetime.utcnow().replace(tzinfo=None)  # UTC-naive
    log.debug("🚀 Старт PACK-отчёта t0=%s, стратегий=%d", t_ref.isoformat(), len(strategies))

    # последовательная обработка стратегий
    async with infra.pg_pool.acquire() as conn:
        for sid in strategies:
            try:
                await _process_strategy(conn, sid, t_ref)
            except Exception:
                log.exception("❌ Ошибка PACK обработки strategy_id=%s", sid)

    # завершение
    log.debug("✅ Завершено формирование PACK-отчётов (стратегий=%d)", len(strategies))


# 🔸 Полный проход по стратегии: все окна → по каждому окну все TF последовательно
async def _process_strategy(conn, strategy_id: int, t_ref: datetime):
    # цикл по окнам (7d/14d/28d)
    for tag in WINDOW_TAGS:
        # расчёт границ окна
        win_start = t_ref - WINDOW_SIZES[tag]
        win_end = t_ref

        # шапка отчёта (source='pack')
        report_id = await _create_report_header(conn, strategy_id, tag, win_start, win_end)

        # метрики «шапки» (closed_total/wins/pnl/avg-*)
        closed_total, closed_wins, pnl_sum_total, pnl_sum_wins = await _calc_report_head_metrics(
            conn, strategy_id, win_start, win_end
        )
        days_in_window = WINDOW_SIZES[tag].total_seconds() / 86400.0
        winrate = round((closed_wins / closed_total) if closed_total else 0.0, 4)
        avg_pnl_per_trade = round((pnl_sum_total / closed_total) if closed_total else 0.0, 4)
        avg_trades_per_day = round(closed_total / days_in_window, 4)

        # финализация «шапки»
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

        # если сделок нет — сразу уведомляем downstream
        if closed_total == 0:
            log.debug("[PACK REPORT] sid=%s win=%s total=0 — пропуск TF/агрегации", strategy_id, tag)
            try:
                await _emit_report_ready(
                    redis=infra.redis_client,
                    report_id=report_id,
                    strategy_id=strategy_id,
                    time_frame=tag,
                    window_start=win_start,
                    window_end=win_end,
                    aggregate_rows=0,
                    tf_done=[],
                    generated_at=datetime.utcnow().replace(tzinfo=None),
                )
            except Exception:
                log.exception("❌ Ошибка публикации PACK REPORT_READY sid=%s win=%s (total=0)", strategy_id, tag)
            continue

        # последовательный проход по TF — считаем ПАКи ТОЛЬКО по разрешённым COMBO
        tf_done: List[str] = []
        for tf in TF_ORDER:
            # обработка одного TF по каждому индикатору
            try:
                await _process_timeframe_rsi(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_mfi(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_bb(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_lr(conn,  report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_atr(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_adx(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_ema(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                await _process_timeframe_macd(conn,report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
                tf_done.append(tf)
            except Exception:
                log.exception("❌ Ошибка PACK агрегации sid=%s win=%s tf=%s", strategy_id, tag, tf)

        # публикация события «отчёт готов» (для PACK-confidence)
        try:
            # число агрегатных строк
            row_count = await conn.fetchval(
                "SELECT COUNT(*)::int FROM oracle_pack_aggregated_stat WHERE report_id = $1",
                report_id,
            )
            await _emit_report_ready(
                redis=infra.redis_client,
                report_id=report_id,
                strategy_id=strategy_id,
                time_frame=tag,
                window_start=win_start,
                window_end=win_end,
                aggregate_rows=int(row_count or 0),
                tf_done=tf_done,
                generated_at=datetime.utcnow().replace(tzinfo=None),
            )
        except Exception:
            log.exception("❌ Ошибка публикации PACK REPORT_READY sid=%s win=%s", strategy_id, tag)

        # логирование итогов по окну
        log.debug(
            "[PACK REPORT] sid=%s win=%s report_id=%s total=%d wins=%d wr=%.4f pnl_sum=%.4f avg_pnl=%.4f avg_tpd=%.4f",
            strategy_id, tag, report_id, closed_total, closed_wins, winrate, pnl_sum_total, avg_pnl_per_trade, avg_trades_per_day
        )


# 🔸 Создание (или возврат id) шапки отчёта (source='pack')
async def _create_report_header(conn, strategy_id: int, time_frame: str, win_start: datetime, win_end: datetime) -> int:
    # вставка с идемпотентностью по (sid, tf, window_start, window_end, source)
    row = await conn.fetchrow(
        """
        INSERT INTO oracle_report_stat (strategy_id, time_frame, window_start, window_end, source)
        VALUES ($1, $2, $3, $4, 'pack')
        ON CONFLICT (strategy_id, time_frame, window_start, window_end, source)
        DO UPDATE SET created_at = oracle_report_stat.created_at
        RETURNING id
        """,
        strategy_id, time_frame, win_start, win_end
    )
    # возврат id созданной/найденной шапки
    return int(row["id"])


# 🔸 Расчёт агрегатов для шапки (одним SQL)
async def _calc_report_head_metrics(conn, strategy_id: int, win_start: datetime, win_end: datetime):
    # суммарные метрики по закрытым позициям в окне
    r = await conn.fetchrow(
        """
        SELECT
            COUNT(*)::int                            AS closed_total,
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
    # возврат кортежа метрик
    return int(r["closed_total"]), int(r["closed_wins"]), float(r["pnl_sum_total"]), float(r["pnl_sum_wins"])


# 🔸 Финализация «шапки» (update метрик)
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
    # обновление агрегированных полей отчёта
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


# 🔸 Вспомогательная функция: построение combo-инкремента
def _emit_combo_inc(
    inc_map: Dict[Tuple, Dict[str, float]],
    *,
    report_id: int,
    strategy_id: int,
    time_frame: str,
    direction: str,
    timeframe: str,
    base: str,
    fields: Dict[str, str],
    combos: List[str],
    pnl: float,
):
    # добавить инкременты ТОЛЬКО по разрешённым combo ('field1|field2|...')
    is_win = pnl > 0.0
    for combo_str in combos:
        parts = combo_str.split("|")
        if not all(f in fields for f in parts):
            continue
        agg_key = combo_str
        agg_value = "|".join(f"{f}:{fields[f]}" for f in parts)
        k = (report_id, strategy_id, time_frame, direction, timeframe, base, "combo", agg_key, agg_value)
        inc = inc_map.setdefault(k, {"t": 0, "w": 0, "pt": 0.0, "pw": 0.0})
        inc["t"] += 1
        if is_win:
            inc["w"] += 1
            inc["pw"] = round(inc["pw"] + pnl, 4)
        inc["pt"] = round(inc["pt"] + pnl, 4)


# 🔸 Обработка TF: PACK=RSI
async def _process_timeframe_rsi(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-RSI] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для RSI
    total, ok_rows = len(positions), 0
    rsi_fields = PACK_FIELDS["rsi"]
    rsi_combos = PACK_COMBOS["rsi"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений RSI
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'rsi%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, rsi_fields,
        )

        # группировка значений по uid → base → {field: value}
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # накапливаем инкременты только по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=rsi_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-RSI] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=MFI
async def _process_timeframe_mfi(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-MFI] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для MFI
    total, ok_rows = len(positions), 0
    mfi_fields = PACK_FIELDS["mfi"]
    mfi_combos = PACK_COMBOS["mfi"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений MFI
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'mfi%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, mfi_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=mfi_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-MFI] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=BB
async def _process_timeframe_bb(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-BB] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для BB
    total, ok_rows = len(positions), 0
    bb_fields = PACK_FIELDS["bb"]
    bb_combos = PACK_COMBOS["bb"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений BB
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'bb%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, bb_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=bb_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-BB] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=LR
async def _process_timeframe_lr(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-LR] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для LR
    total, ok_rows = len(positions), 0
    lr_fields = PACK_FIELDS["lr"]
    lr_combos = PACK_COMBOS["lr"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений LR
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'lr%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, lr_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=lr_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-LR] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=ATR
async def _process_timeframe_atr(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-ATR] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для ATR
    total, ok_rows = len(positions), 0
    atr_fields = PACK_FIELDS["atr"]
    atr_combos = PACK_COMBOS["atr"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений ATR
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'atr%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, atr_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=atr_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-ATR] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=ADX_DMI
async def _process_timeframe_adx(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-ADX_DMI] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для ADX_DMI
    total, ok_rows = len(positions), 0
    adx_fields = PACK_FIELDS["adx_dmi"]
    adx_combos = PACK_COMBOS["adx_dmi"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений ADX_DMI
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'adx_dmi%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, adx_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=adx_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-ADX_DMI] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=EMA
async def _process_timeframe_ema(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-EMA] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для EMA
    total, ok_rows = len(positions), 0
    ema_fields = PACK_FIELDS["ema"]
    ema_combos = PACK_COMBOS["ema"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений EMA
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'ema%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, ema_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=ema_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-EMA] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Обработка TF: PACK=MACD
async def _process_timeframe_macd(conn, report_id, strategy_id, time_frame, timeframe, win_start, win_end, days_in_window):
    # выбор закрытых позиций
    rows = await conn.fetch(
        """
        SELECT position_uid, direction, pnl
          FROM positions_v4
         WHERE strategy_id = $1 AND status = 'closed'
           AND closed_at >= $2 AND closed_at < $3
        """,
        strategy_id, win_start, win_end
    )
    # нет позиций — выходим
    positions = [dict(r) for r in rows]
    if not positions:
        log.debug("[PACK-MACD] sid=%s win=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    # параметры для MACD
    total, ok_rows = len(positions), 0
    macd_fields = PACK_FIELDS["macd"]
    macd_combos = PACK_COMBOS["macd"]

    # по батчам
    for bi in range((total + BATCH_SIZE - 1) // BATCH_SIZE):
        # срез батча
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # чтение PACK-значений MACD
        rows_pack = await conn.fetch(
            """
            SELECT position_uid, param_base, param_name, value_num, value_text, status
              FROM indicator_position_stat
             WHERE position_uid = ANY($1::text[])
               AND param_type = 'pack' AND timeframe = $2
               AND param_base LIKE 'macd%' AND param_name = ANY($3::text[])
            """,
            uid_list, timeframe, macd_fields,
        )

        # группировка значений
        by_uid: Dict[str, Dict[str, Dict[str, str]]] = {}
        bad: Dict[str, set] = {}
        for r in rows_pack:
            uid, base, status = r["position_uid"], r["param_base"], r["status"]
            if status != "ok":
                bad.setdefault(uid, set()).add(base)
                continue
            name = r["param_name"]
            val = str(r["value_text"]) if r["value_text"] is not None else (
                f"{float(r['value_num'] or 0.0):.8f}".rstrip('0').rstrip('.')
            )
            by_uid.setdefault(uid, {}).setdefault(base, {})[name] = val

        # инкременты по разрешённым combo
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        for uid, base_map in by_uid.items():
            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            for base, fields in base_map.items():
                if base in bad.get(uid, set()):
                    continue
                _emit_combo_inc(
                    inc_map,
                    report_id=report_id, strategy_id=strategy_id, time_frame=time_frame,
                    direction=direction, timeframe=timeframe, base=base, fields=fields,
                    combos=macd_combos, pnl=pnl,
                )

        # запись батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    # лог по TF
    log.debug("[PACK-MACD] sid=%s win=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Батчевый UPSERT (UNNEST + ON CONFLICT) с пересчётом метрик
async def _upsert_aggregates_batch(conn, inc_map: Dict[Tuple, Dict[str, float]], days_in_window: float):
    # подготовка массивов полей под UNNEST
    report_ids, strategy_ids, time_frames, directions = [], [], [], []
    timeframes, pack_bases, agg_types, agg_keys, agg_values = [], [], [], [], []
    trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc = [], [], [], []

    # сбор данных из inc_map
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

    # UPSERT + пересчёт производных
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


# 🔸 Публикация события «отчёт готов» (PACK)
async def _emit_report_ready(
    redis,
    *,
    report_id: int,
    strategy_id: int,
    time_frame: str,
    window_start: datetime,
    window_end: datetime,
    aggregate_rows: int,
    tf_done: List[str],
    generated_at: datetime,
):
    # формирование payload
    payload = {
        "report_id": int(report_id),
        "strategy_id": int(strategy_id),
        "time_frame": str(time_frame),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "generated_at": generated_at.isoformat(),
        "aggregate_rows": int(aggregate_rows),
        "tf_done": list(tf_done or []),
    }
    fields = {"data": json.dumps(payload, separators=(",", ":"))}

    # отправка в Redis Stream
    await redis.xadd(
        name=REPORT_READY_STREAM,
        fields=fields,
        maxlen=REPORT_READY_MAXLEN,
        approximate=True,
    )

    # лог результата
    log.debug(
        "[PACK_REPORT_READY] sid=%s win=%s report_id=%s rows=%d tf_done=%s",
        strategy_id, time_frame, report_id, aggregate_rows, ",".join(tf_done) if tf_done else "-",
    )