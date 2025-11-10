# oracle_mw_snapshot.py — воркер MW-отчётов: событийный запуск от reports_start, UPSERT шапок по окну, батч-агрегация (solo/combos), публикация «отчёт готов»

# 🔸 Импорты
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json

import infra

# 🔸 Логгер
log = logging.getLogger("ORACLE_MW_SNAPSHOT")

# 🔸 Константы воркера / параметры исполнения
INITIAL_DELAY_SEC = 90                        # первый запуск fallback (если используешь) через 90 секунд
INTERVAL_SEC = 3 * 60 * 60                    # периодичность fallback — каждые 3 часа
BATCH_SIZE = 250                              # размер батча по позициям
WINDOW_TAGS = ("7d", "14d", "28d")            # метки окон
WINDOW_SIZES = {
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "28d": timedelta(days=28),
}
TF_ORDER = ("m5", "m15", "h1")                # последовательная обработка TF

# 🔸 Наборы баз для MW
# Порядок важен для построения комбо; базовый канонический порядок: trend → volatility → extremes → momentum.
MW_BASES_FETCH = (
    "trend",
    "volatility",
    "extremes",
    "momentum",
    "mom_align",       # derived: aligned/countertrend/flat
)

# Какие solo-базы реально пишем в агрегаты (соло анализ держим узким, чтобы не распылять статистику)
SOLO_BASES = ("trend",)

# Комбинации, которые пишем (всегда с 'trend' первым)
COMBOS_2_ALLOWED = (
    ("trend", "volatility"),
    ("trend", "extremes"),
    ("trend", "momentum"),
    ("trend", "mom_align"),
)

COMBOS_3_ALLOWED = (
    ("trend", "volatility", "extremes"),
    ("trend", "volatility", "momentum"),
    ("trend", "extremes", "momentum"),
    ("trend", "volatility", "mom_align"),
)

# Квартет оставляем только в исходном виде (иначе размерность становится избыточной)
COMBOS_4_ALLOWED = (("trend", "volatility", "extremes", "momentum"),)

# 🔸 Настройки Redis Stream
REPORTS_START_STREAM = "oracle:mw:reports_start"    # источник: сигнал «старт отчётов» от oracle_positions_analyzer
START_CONSUMER_GROUP = "oracle_mw_snapshot_group"
START_CONSUMER_NAME  = "oracle_mw_snapshot_worker"
READ_COUNT = 128
READ_BLOCK_MS = 30_000

# 🔸 Настройки Redis Stream для сигнала «отчёт готов»
REPORT_READY_STREAM = "oracle:mw:reports_ready"     # имя стрима с уведомлениями о готовности отчёта
REPORT_READY_MAXLEN = 10_000                        # мягкое ограничение длины стрима (XADD ... MAXLEN ~)


# 🔸 Публичная точка запуска (ивент-драйв: слушаем reports_start)
async def run_oracle_mw_snapshot():
    # условия достаточности окружения
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск: PG/Redis не инициализированы")
        return

    # создаём consumer group (идемпотентно)
    try:
        await infra.redis_client.xgroup_create(
            name=REPORTS_START_STREAM,
            groupname=START_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug("📡 Создана consumer group для %s", REPORTS_START_STREAM)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            pass
        else:
            log.exception("❌ Ошибка создания consumer group для %s", REPORTS_START_STREAM)
            return

    log.info("🚀 MW snapshot слушает %s", REPORTS_START_STREAM)

    # основной цикл чтения событий
    while True:
        try:
            resp = await infra.redis_client.xreadgroup(
                groupname=START_CONSUMER_GROUP,
                consumername=START_CONSUMER_NAME,
                streams={REPORTS_START_STREAM: ">"},
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
                            win_end_iso = payload.get("window_end")
                            win_start_iso = payload.get("window_start")  # опционально (для 7d)

                            if not (sid and win_end_iso):
                                log.debug("ℹ️ Пропуск события (недостаточно данных): %s", payload)
                                acks.append(msg_id)
                                continue

                            # стратегия должна быть актуальна для MW
                            if infra.market_watcher_strategies and sid not in infra.market_watcher_strategies:
                                log.debug("ℹ️ Пропуск sid=%s: не в кэше market_watcher", sid)
                                acks.append(msg_id)
                                continue

                            t_ref = _parse_iso_utcnaive(win_end_iso)
                            if t_ref is None:
                                log.debug("ℹ️ Пропуск sid=%s: неверный window_end=%r", sid, win_end_iso)
                                acks.append(msg_id)
                                continue

                            # гард «последний отчёт» (устаревшие окна не считаем)
                            if not await _is_latest_or_equal_7d(conn, sid, t_ref):
                                log.debug("⏭️ Пропуск sid=%s: устаревшее окно window_end=%s", sid, t_ref.isoformat())
                                acks.append(msg_id)
                                continue

                            # формируем окна от t_ref; для 7d используем заданный window_start, если пришёл
                            windows = _build_windows_from_ref(t_ref, win_start_iso)

                            # последовательная обработка трёх окон
                            for tag, (w_start, w_end) in windows.items():
                                try:
                                    await _process_window(conn, sid, tag, w_start, w_end)
                                except Exception:
                                    log.exception("❌ Ошибка обработки sid=%s tag=%s", sid, tag)

                            acks.append(msg_id)

                        except Exception:
                            log.exception("❌ Ошибка разбора/обработки сообщения snapshot")
                            acks.append(msg_id)

            # подтверждаем обработанные сообщения
            if acks:
                try:
                    await infra.redis_client.xack(REPORTS_START_STREAM, START_CONSUMER_GROUP, *acks)
                except Exception:
                    log.exception("⚠️ Ошибка ACK для %s", REPORTS_START_STREAM)

        except asyncio.CancelledError:
            log.debug("⏹️ MW snapshot остановлен по сигналу")
            raise
        except Exception:
            log.exception("❌ Ошибка цикла MW snapshot — пауза 5 секунд")
            await asyncio.sleep(5)


# 🔸 (Опционально) Fallback-проход (редко): обрабатывает все стратегии по now()
async def run_oracle_mw_snapshot_fallback():
    if infra.pg_pool is None or infra.redis_client is None:
        log.debug("❌ Пропуск fallback: PG/Redis не инициализированы")
        return

    strategies = sorted(infra.market_watcher_strategies or [])
    if not strategies:
        log.debug("ℹ️ Fallback: нет стратегий MW")
        return

    t_ref = datetime.utcnow().replace(tzinfo=None)
    windows = _build_windows_from_ref(t_ref, win_start_7d_iso=None)

    async with infra.pg_pool.acquire() as conn:
        for sid in strategies:
            # гард «последний 7d»
            if not await _is_latest_or_equal_7d(conn, sid, t_ref):
                continue
            for tag, (w_start, w_end) in windows.items():
                try:
                    await _process_window(conn, sid, tag, w_start, w_end)
                except Exception:
                    log.exception("❌ Ошибка fallback обработки sid=%s tag=%s", sid, tag)


# 🔸 Полный проход по одному окну (шапка → TF-агрегации → сигнал готовности)
async def _process_window(conn, strategy_id: int, tag: str, win_start: datetime, win_end: datetime):
    # шапка отчёта: UPSERT по окну → получаем report_id
    report_id = await _upsert_report_header(conn, strategy_id, tag, win_start, win_end)

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
        log.debug("[REPORT] sid=%s tag=%s total=0 — пропуск TF/агрегации", strategy_id, tag)
        # даже при total=0 отправляем событие «готов»
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
            log.exception("❌ Ошибка публикации REPORT_READY sid=%s tag=%s (total=0)", strategy_id, tag)
        return

    # последовательный проход по TF
    tf_done: List[str] = []
    for tf in TF_ORDER:
        try:
            await _process_timeframe(conn, report_id, strategy_id, tag, tf, win_start, win_end, days_in_window)
            tf_done.append(tf)
        except Exception:
            log.exception("❌ Ошибка агрегации sid=%s tag=%s tf=%s", strategy_id, tag, tf)

    # после завершения TF — отправляем событие «отчёт готов»
    try:
        row_count = await conn.fetchval(
            "SELECT COUNT(*)::int FROM oracle_mw_aggregated_stat WHERE report_id = $1",
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
        log.exception("❌ Ошибка публикации REPORT_READY sid=%s tag=%s", strategy_id, tag)

    log.debug(
        "[REPORT] sid=%s tag=%s report_id=%s total=%d wins=%d wr=%.4f pnl_sum=%.4f avg_pnl=%.4f avg_tpd=%.4f",
        strategy_id, tag, report_id, closed_total, closed_wins, winrate, pnl_sum_total, avg_pnl_per_trade, avg_trades_per_day
    )


# 🔸 UPSERT шапки отчёта (source='mw') по окну → возвращает report_id
async def _upsert_report_header(conn, strategy_id: int, time_frame: str, win_start: datetime, win_end: datetime) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO oracle_report_stat (strategy_id, time_frame, window_start, window_end, source)
        VALUES ($1, $2, $3, $4, 'mw')
        ON CONFLICT (strategy_id, time_frame, window_start, window_end, source)
        DO UPDATE SET source = EXCLUDED.source
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
        log.debug("[TF] sid=%s tag=%s tf=%s total=0", strategy_id, time_frame, timeframe)
        return

    total = len(positions)
    ok_rows = 0
    batch_count = (total + BATCH_SIZE - 1) // BATCH_SIZE

    for bi in range(batch_count):
        batch = positions[bi * BATCH_SIZE : (bi + 1) * BATCH_SIZE]
        uid_list = [p["position_uid"] for p in batch]
        uid_meta = {p["position_uid"]: (p["direction"], float(p["pnl"] or 0.0)) for p in batch}

        # читаем MW (включая ошибки) → агрегируем только status='ok' на текущем TF
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
            uid_list, timeframe, list(MW_BASES_FETCH),
        )

        # подготовим агрегаты батча в памяти
        inc_map: Dict[Tuple, Dict[str, float]] = {}
        if not rows_mw:
            continue

        combos_2 = COMBOS_2_ALLOWED
        combos_3 = COMBOS_3_ALLOWED
        combos_4 = COMBOS_4_ALLOWED

        # обходим MW-строки
        for r in rows_mw:
            uid = r["position_uid"]
            has_error = bool(r["has_error"])
            raw_states = r["states_tf"]

            # парсим JSON
            if not raw_states or has_error:
                continue
            if isinstance(raw_states, dict):
                states_tf = raw_states
            else:
                try:
                    states_tf = json.loads(raw_states)  # '{"trend":"down_weak", ...}'
                except Exception:
                    log.debug("[TF] skip uid=%s: states_tf JSON parse error: %r", uid, raw_states)
                    continue

            if not isinstance(states_tf, dict) or not states_tf:
                continue

            # фильтруем только допустимые базы (включая derived)
            states_tf = {k: v for k, v in states_tf.items() if k in MW_BASES_FETCH and isinstance(v, str) and v}
            if not states_tf:
                continue

            direction, pnl = uid_meta.get(uid, ("long", 0.0))
            is_win = pnl > 0.0

            # solo
            for base in SOLO_BASES:
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

            # combos
            def _touch_combo(combo: Tuple[str, ...]):
                # проверка наличия всех баз
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

        # UPSERT батча
        if inc_map:
            await _upsert_aggregates_batch(conn, inc_map, days_in_window)
            ok_rows += sum(v["t"] for v in inc_map.values())

    log.debug("[TF] sid=%s tag=%s tf=%s positions=%d agg_rows=%d", strategy_id, time_frame, timeframe, total, ok_rows)


# 🔸 Батчевый UPSERT агрегатов (UNNEST + ON CONFLICT) с пересчётом метрик
async def _upsert_aggregates_batch(conn, inc_map: Dict[Tuple, Dict[str, float]], days_in_window: float):
    # готовим массивы полей
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
        *[
            report_ids, strategy_ids, time_frames, directions,
            timeframes, agg_types, agg_bases, agg_states,
            trades_inc, wins_inc, pnl_total_inc, pnl_wins_inc,
            days_in_window,
        ],
    )


# 🔸 Публикация события «отчёт готов» в Redis Stream
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
    # собираем пейлоад
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

    # отправка в Redis Stream (мягкое ограничение длины)
    fields = {"data": json.dumps(payload, separators=(",", ":"))}
    await redis.xadd(name=REPORT_READY_STREAM, fields=fields, maxlen=REPORT_READY_MAXLEN, approximate=True)

    # лог на результат
    log.debug(
        "[REPORT_READY] sid=%s tag=%s report_id=%s rows=%d tf_done=%s",
        strategy_id, time_frame, report_id, aggregate_rows, ",".join(tf_done) if tf_done else "-",
    )


# 🔸 Вспомогательные

def _parse_iso_utcnaive(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None


def _build_windows_from_ref(t_ref: datetime, win_start_7d_iso: Optional[str]) -> Dict[str, Tuple[datetime, datetime]]:
    windows: Dict[str, Tuple[datetime, datetime]] = {}
    # 7d
    if win_start_7d_iso:
        s7 = _parse_iso_utcnaive(win_start_7d_iso) or (t_ref - WINDOW_SIZES["7d"])
    else:
        s7 = t_ref - WINDOW_SIZES["7d"]
    windows["7d"] = (s7, t_ref)
    # 14d/28d
    windows["14d"] = (t_ref - WINDOW_SIZES["14d"], t_ref)
    windows["28d"] = (t_ref - WINDOW_SIZES["28d"], t_ref)
    return windows


async def _is_latest_or_equal_7d(conn, strategy_id: int, window_end: datetime) -> bool:
    last = await conn.fetchval(
        """
        SELECT MAX(window_end) FROM oracle_report_stat
        WHERE strategy_id = $1 AND time_frame = '7d' AND source = 'mw'
        """,
        int(strategy_id)
    )
    if last is None:
        return True
    # обрабатываем, если входящее окно не старше последнего
    return window_end >= last