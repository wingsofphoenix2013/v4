# bt_analysis_preproc.py — препроцессинг анализа: оптимальный порог winrate по биннам (run-aware) + выбор победителя и запись good-бинов в bt_analysis_bins_labels

import asyncio
import logging
import json
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Set

log = logging.getLogger("BT_ANALYSIS_PREPROC")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
PREPROC_STREAM_KEY = "bt:analysis:ready"
PREPROC_CONSUMER_GROUP = "bt_analysis_preproc"
PREPROC_CONSUMER_NAME = "bt_analysis_preproc_main"

# 🔸 Стрим готовности препроцессинга (внешний потребитель ждёт обновление bt_analysis_bins_labels)
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"

# 🔸 Настройки чтения стрима
PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

# 🔸 Таблицы результатов
PREPROC_TABLE = "bt_analysis_preproc_stat"
LABELS_TABLE = "bt_analysis_bins_labels"

# 🔸 Окна консенсуса (28 = базовое run-окно, проверки: 14 и 7)
CHECK_WINDOWS_DAYS = [14, 7]

# 🔸 Квантизация метрик
Q4 = Decimal("0.0001")


# 🔸 Квантизация Decimal до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Публичная точка входа: воркер препроцессинга анализа
async def run_bt_analysis_preproc_orchestrator(pg, redis) -> None:
    log.debug("BT_ANALYSIS_PREPROC: оркестратор препроцессинга запущен")

    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_groups = 0
            total_upserts = 0
            total_skipped = 0
            total_errors = 0

            for stream_key, entries in messages:
                if stream_key != PREPROC_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_analysis_ready(fields)
                    if not ctx:
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        total_skipped += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    total_pairs += 1

                    # грузим окно run (источник истины)
                    run_window = await _load_run_window(pg, run_id)
                    if not run_window:
                        log.error(
                            "BT_ANALYSIS_PREPROC: не найден run_id=%s в bt_signal_backfill_runs, scenario_id=%s, signal_id=%s",
                            run_id,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        total_errors += 1
                        continue

                    run_from = run_window["from_time"]
                    run_to = run_window["to_time"]

                    # грузим бин-статистику по паре run/scenario/signal
                    try:
                        bins_rows = await _load_bins_stat_rows(pg, run_id, scenario_id, signal_id)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC: ошибка загрузки bt_analysis_bins_stat для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        continue

                    # считаем preproc и выбираем победителя
                    try:
                        groups_processed, upserts_done, winner = await _process_and_store_groups(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_from=run_from,
                            run_to=run_to,
                            rows=bins_rows,
                        )
                        total_groups += groups_processed
                        total_upserts += upserts_done
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC: ошибка обработки групп для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        continue

                    # пишем labels победителя (или очищаем, если победителя нет)
                    try:
                        winner_analysis_id = int(winner["analysis_id"]) if winner else 0
                        await _rewrite_bins_labels_for_pair(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            winner=winner,
                        )
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC: ошибка записи bt_analysis_bins_labels для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
                        continue

                    log.debug(
                        "BT_ANALYSIS_PREPROC: pair done — scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s, groups=%s, upserts=%s, winner_analysis_id=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        groups_processed,
                        upserts_done,
                        winner_analysis_id,
                    )

                    # событие готовности: bt_analysis_bins_labels обновлена
                    finished_at_preproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            PREPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "run_id": str(run_id),
                                "winner_analysis_id": str(winner_analysis_id),
                                "finished_at": finished_at_preproc.isoformat(),
                            },
                        )
                    except Exception as e:
                        log.error(
                            "BT_ANALYSIS_PREPROC: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
                            PREPROC_READY_STREAM_KEY,
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )

                    await redis.xack(PREPROC_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)

            log.debug(
                "BT_ANALYSIS_PREPROC: batch summary — msgs=%s, pairs=%s, groups=%s, upserts=%s, skipped=%s, errors=%s",
                total_msgs,
                total_pairs,
                total_groups,
                total_upserts,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_PREPROC: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group (Render-safe: SETID '$' вместо DESTROY)
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=PREPROC_STREAM_KEY,
            groupname=PREPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC: создана consumer group '%s' для стрима '%s'",
            PREPROC_CONSUMER_GROUP,
            PREPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC: consumer group '%s' уже существует — сдвигаем курсор группы на '$' (SETID) для игнора истории до старта",
                PREPROC_CONSUMER_GROUP,
            )

            await redis.execute_command(
                "XGROUP",
                "SETID",
                PREPROC_STREAM_KEY,
                PREPROC_CONSUMER_GROUP,
                "$",
            )

            log.debug(
                "BT_ANALYSIS_PREPROC: consumer group '%s' SETID='$' для стрима '%s' выполнен",
                PREPROC_CONSUMER_GROUP,
                PREPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC: ошибка при создании consumer group '%s': %s",
                PREPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready (Render-safe: NOGROUP recovery)
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=PREPROC_CONSUMER_GROUP,
            consumername=PREPROC_CONSUMER_NAME,
            streams={PREPROC_STREAM_KEY: ">"},
            count=PREPROC_STREAM_BATCH_SIZE,
            block=PREPROC_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning(
                "BT_ANALYSIS_PREPROC: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем",
            )
            await _ensure_consumer_group(redis)
            return []
        raise

    if not entries:
        return []

    parsed: List[Any] = []
    for stream_key, messages in entries:
        if isinstance(stream_key, bytes):
            stream_key = stream_key.decode("utf-8")

        stream_entries: List[Any] = []
        for msg_id, fields in messages:
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode("utf-8")

            str_fields: Dict[str, str] = {}
            for k, v in fields.items():
                key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                str_fields[key_str] = val_str

            stream_entries.append((msg_id, str_fields))

        parsed.append((stream_key, stream_entries))

    return parsed


# 🔸 Разбор сообщения bt:analysis:ready (run-aware)
def _parse_analysis_ready(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and run_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "finished_at": finished_at,
        }
    except Exception:
        return None


# 🔸 Загрузка run-окна из bt_signal_backfill_runs
async def _load_run_window(pg, run_id: int) -> Optional[Dict[str, Any]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT from_time, to_time
            FROM bt_signal_backfill_runs
            WHERE id = $1
            """,
            int(run_id),
        )
    if not row:
        return None
    return {"from_time": row["from_time"], "to_time": row["to_time"]}


# 🔸 Загрузка бин-статистики по паре run/scenario/signal (как есть из bt_analysis_bins_stat)
async def _load_bins_stat_rows(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                trades,
                pnl_abs,
                winrate
            FROM bt_analysis_bins_stat
            WHERE run_id = $1
              AND scenario_id = $2
              AND signal_id = $3
            ORDER BY analysis_id, indicator_param NULLS FIRST, timeframe, direction, winrate, bin_name
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        ind = r["indicator_param"]
        indicator_param_norm = str(ind).strip() if ind is not None else ""

        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": indicator_param_norm,
                "timeframe": str(r["timeframe"]).strip().lower(),
                "direction": str(r["direction"]).strip().lower(),
                "bin_name": str(r["bin_name"]),
                "trades": int(r["trades"] or 0),
                "pnl_abs": _d(r["pnl_abs"]),
                "winrate": _d(r["winrate"]),
            }
        )
    return out


# 🔸 Сумма pnl_abs за окно по exit_time, только по kept_bins (фиксированный набор с 28 дней)
async def _calc_filt_pnl_for_exit_window(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
    w_from: datetime,
    w_to: datetime,
    kept_bins: Set[str],
) -> Decimal:
    if not kept_bins:
        return Decimal("0")

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                r.bin_name AS bin_name,
                r.pnl_abs  AS pnl_abs
            FROM bt_analysis_positions_raw r
            JOIN bt_scenario_positions p
              ON p.position_uid = r.position_uid
            WHERE r.run_id      = $1
              AND r.analysis_id = $2
              AND r.scenario_id = $3
              AND r.signal_id   = $4
              AND r.timeframe   = $5
              AND r.direction   = $6
              AND p.status      = 'closed'
              AND p.postproc    = true
              AND p.exit_time IS NOT NULL
              AND p.exit_time BETWEEN $7 AND $8
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(timeframe),
            str(direction),
            w_from,
            w_to,
        )

    total = Decimal("0")
    for r in rows:
        bn = str(r["bin_name"])
        if bn not in kept_bins:
            continue
        total += _d(r["pnl_abs"])

    return total


# 🔸 Обработка всех групп и запись результатов в bt_analysis_preproc_stat + выбор победителя
async def _process_and_store_groups(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    run_from: datetime,
    run_to: datetime,
    rows: List[Dict[str, Any]],
) -> Tuple[int, int, Optional[Dict[str, Any]]]:
    if not rows:
        return 0, 0, None

    grouped: Dict[Tuple[int, str, str, str], List[Dict[str, Any]]] = {}
    for r in rows:
        analysis_id = int(r["analysis_id"])
        indicator_param = str(r.get("indicator_param") or "").strip()
        timeframe = str(r.get("timeframe") or "").strip().lower()
        direction = str(r.get("direction") or "").strip().lower()

        key = (analysis_id, indicator_param, timeframe, direction)
        grouped.setdefault(key, []).append(r)

    groups_processed = 0
    upserts_done = 0

    winner: Optional[Dict[str, Any]] = None

    for (analysis_id, indicator_param, timeframe, direction), group_bins in grouped.items():
        groups_processed += 1

        best_28 = _compute_best_threshold(group_bins)
        if best_28 is None:
            continue

        (
            orig_trades,
            orig_pnl,
            orig_winrate,
            filt_trades,
            filt_pnl,
            filt_winrate,
            threshold,
            raw_stat,
            kept_bins,
        ) = best_28

        # правила active
        active = True
        active_reason = "ok"

        # 1) если на 28 днях filt_pnl_abs <= 0 — отключаем
        if filt_pnl <= Decimal("0"):
            active = False
            active_reason = "disabled_nonpositive_filt_pnl_28d"
        else:
            # 2) перепроверяем ЭТОТ же набор kept_bins на 14 и 7 днях (по exit_time)
            checks: Dict[str, Optional[Decimal]] = {}

            consensus_ok = True

            for days in CHECK_WINDOWS_DAYS:
                w_from = run_to - timedelta(days=int(days))
                if w_from < run_from:
                    w_from = run_from

                pnl_w = await _calc_filt_pnl_for_exit_window(
                    pg=pg,
                    run_id=run_id,
                    analysis_id=analysis_id,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    timeframe=timeframe,
                    direction=direction,
                    w_from=w_from,
                    w_to=run_to,
                    kept_bins=kept_bins,
                )

                checks[f"pnl_{days}d"] = pnl_w

                if pnl_w <= Decimal("0"):
                    consensus_ok = False

            if not consensus_ok:
                active = False
                active_reason = "disabled_no_14_7_consensus_fixed_bins"

            # дополняем raw_stat проверками
            raw_stat["active_checks"] = {
                "window_to": run_to.isoformat(),
                "pnl_28d": str(_q4(filt_pnl)),
                "pnl_14d": str(_q4(checks.get("pnl_14d") or Decimal("0"))),
                "pnl_7d": str(_q4(checks.get("pnl_7d") or Decimal("0"))),
                "check_basis": "exit_time",
                "consensus_rule": "fixed kept_bins from 28d must have pnl_abs > 0 on 14d and 7d",
            }

        raw_stat["active"] = bool(active)
        raw_stat["active_reason"] = str(active_reason)

        # upsert в bt_analysis_preproc_stat
        async with pg.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {PREPROC_TABLE} (
                    run_id,
                    analysis_id,
                    scenario_id,
                    signal_id,
                    indicator_param,
                    timeframe,
                    direction,
                    active,
                    orig_trades,
                    orig_pnl_abs,
                    orig_winrate,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    winrate_threshold,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8,
                    $9, $10, $11,
                    $12, $13, $14,
                    $15,
                    $16::jsonb,
                    now()
                )
                ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction)
                DO UPDATE SET
                    active             = EXCLUDED.active,
                    orig_trades        = EXCLUDED.orig_trades,
                    orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                    orig_winrate       = EXCLUDED.orig_winrate,
                    filt_trades        = EXCLUDED.filt_trades,
                    filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                    filt_winrate       = EXCLUDED.filt_winrate,
                    winrate_threshold  = EXCLUDED.winrate_threshold,
                    raw_stat           = EXCLUDED.raw_stat,
                    updated_at         = now()
                """,
                int(run_id),
                int(analysis_id),
                int(scenario_id),
                int(signal_id),
                str(indicator_param),
                str(timeframe),
                str(direction),
                bool(active),
                int(orig_trades),
                str(_q4(orig_pnl)),
                str(_q4(orig_winrate)),
                int(filt_trades),
                str(_q4(filt_pnl)),
                str(_q4(filt_winrate)),
                str(_q4(threshold)) if threshold is not None else None,
                json.dumps(raw_stat, ensure_ascii=False),
            )

        upserts_done += 1

        log.debug(
            "BT_ANALYSIS_PREPROC: group stored — run_id=%s scenario_id=%s signal_id=%s analysis_id=%s ind='%s' tf=%s dir=%s active=%s reason=%s filt_pnl_28=%s",
            run_id,
            scenario_id,
            signal_id,
            analysis_id,
            indicator_param,
            timeframe,
            direction,
            active,
            active_reason,
            str(_q4(filt_pnl)),
        )

        # выбираем победителя: max filt_pnl_abs (только active=true)
        if active:
            if winner is None or _d(winner.get("filt_pnl_abs")) < filt_pnl:
                # готовим карту bin_name -> (trades,pnl,winrate) для записи в labels
                stats_by_bin: Dict[str, Dict[str, Any]] = {}
                for b in group_bins:
                    stats_by_bin[str(b.get("bin_name"))] = {
                        "trades": int(b.get("trades") or 0),
                        "pnl_abs": _d(b.get("pnl_abs")),
                        "winrate": _d(b.get("winrate")),
                    }

                winner = {
                    "analysis_id": int(analysis_id),
                    "indicator_param": str(indicator_param),
                    "timeframe": str(timeframe),
                    "direction": str(direction),
                    "kept_bins": sorted(set(kept_bins)),
                    "threshold_used": _d(threshold) if threshold is not None else Decimal("0"),
                    "filt_pnl_abs": _q4(filt_pnl),
                    "stats_by_bin": stats_by_bin,
                }

    return groups_processed, upserts_done, winner


# 🔸 Перезапись bt_analysis_bins_labels по паре (scenario_id, signal_id): очистка + запись good-бинов победителя
async def _rewrite_bins_labels_for_pair(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    winner: Optional[Dict[str, Any]],
) -> None:
    async with pg.acquire() as conn:
        # очищаем старые данные по паре (чтобы не оставались устаревшие комплекты)
        await conn.execute(
            f"""
            DELETE FROM {LABELS_TABLE}
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            int(scenario_id),
            int(signal_id),
        )

        if not winner:
            return

        analysis_id = int(winner["analysis_id"])
        indicator_param = str(winner.get("indicator_param") or "")
        timeframe = str(winner.get("timeframe") or "")
        direction = str(winner.get("direction") or "")
        threshold_used = _d(winner.get("threshold_used") or Decimal("0"))
        kept_bins: List[str] = list(winner.get("kept_bins") or [])
        stats_by_bin: Dict[str, Dict[str, Any]] = winner.get("stats_by_bin") or {}

        to_insert: List[Tuple[Any, ...]] = []

        for bn in kept_bins:
            st = stats_by_bin.get(bn) or {}
            trades = int(st.get("trades") or 0)
            pnl_abs = _d(st.get("pnl_abs"))
            winrate = _d(st.get("winrate"))

            to_insert.append(
                (
                    int(run_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    int(analysis_id),
                    str(indicator_param),
                    str(timeframe),
                    str(bn),
                    "good",
                    str(_q4(threshold_used)),
                    int(trades),
                    str(_q4(pnl_abs)),
                    str(_q4(winrate)),
                )
            )

        if not to_insert:
            return

        await conn.executemany(
            f"""
            INSERT INTO {LABELS_TABLE} (
                run_id,
                scenario_id,
                signal_id,
                direction,
                analysis_id,
                indicator_param,
                timeframe,
                bin_name,
                state,
                threshold_used,
                trades,
                pnl_abs,
                winrate,
                created_at
            )
            VALUES (
                $1, $2, $3,
                $4,
                $5, $6, $7,
                $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                now()
            )
            ON CONFLICT (run_id, scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe, bin_name)
            DO UPDATE SET
                state          = EXCLUDED.state,
                threshold_used = EXCLUDED.threshold_used,
                trades         = EXCLUDED.trades,
                pnl_abs        = EXCLUDED.pnl_abs,
                winrate        = EXCLUDED.winrate,
                updated_at     = now()
            """,
            to_insert,
        )


# 🔸 Вычисление оптимального порога winrate для одной группы биннов (возвращает kept_bins)
def _compute_best_threshold(
    bins: List[Dict[str, Any]],
) -> Optional[
    Tuple[int, Decimal, Decimal, int, Decimal, Decimal, Optional[Decimal], Dict[str, Any], Set[str]]
]:
    if not bins:
        return None

    normalized: List[Dict[str, Any]] = []
    for b in bins:
        trades = int(b.get("trades") or 0)
        if trades <= 0:
            continue

        winrate = _d(b.get("winrate"))
        pnl = _d(b.get("pnl_abs"))

        normalized.append(
            {
                "bin_name": str(b.get("bin_name") or ""),
                "trades": trades,
                "winrate": winrate,
                "pnl_abs": pnl,
                "wins_est": (Decimal(trades) * winrate),
            }
        )

    if not normalized:
        return None

    # сортировка по winrate (хуже → лучше), затем по pnl (хуже → лучше), затем по имени (детерминизм)
    normalized.sort(key=lambda x: (x["winrate"], x["pnl_abs"], x["bin_name"]))

    n = len(normalized)

    pref_trades: List[int] = [0] * (n + 1)
    pref_pnl: List[Decimal] = [Decimal("0")] * (n + 1)
    pref_wins: List[Decimal] = [Decimal("0")] * (n + 1)

    for i in range(n):
        pref_trades[i + 1] = pref_trades[i] + int(normalized[i]["trades"])
        pref_pnl[i + 1] = pref_pnl[i] + _d(normalized[i]["pnl_abs"])
        pref_wins[i + 1] = pref_wins[i] + _d(normalized[i]["wins_est"])

    orig_trades = pref_trades[n]
    orig_pnl = pref_pnl[n]
    orig_winrate = (pref_wins[n] / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")

    # перебор k: выкинули первые k худших, оставили [k..n)
    best_k = 0
    best_pnl = orig_pnl
    best_trades = orig_trades
    best_wins = pref_wins[n]
    best_threshold = normalized[0]["winrate"] if n > 0 else None

    for k in range(0, n):
        kept_trades = orig_trades - pref_trades[k]
        if kept_trades <= 0:
            continue

        kept_pnl = orig_pnl - pref_pnl[k]
        kept_wins = pref_wins[n] - pref_wins[k]
        threshold = normalized[k]["winrate"]

        # критерий: максимум pnl по оставшимся
        if kept_pnl > best_pnl:
            best_pnl = kept_pnl
            best_k = k
            best_trades = kept_trades
            best_wins = kept_wins
            best_threshold = threshold

    filt_trades = int(best_trades)
    filt_pnl = best_pnl
    filt_winrate = (best_wins / Decimal(filt_trades)) if filt_trades > 0 else Decimal("0")

    removed_bins = [x["bin_name"] for x in normalized[:best_k]]
    kept_bins_list = [x["bin_name"] for x in normalized[best_k:]]
    kept_bins = set(kept_bins_list)

    raw_stat = {
        "version": "v1",
        "sort": "winrate_asc",
        "criterion": "max_filt_pnl_abs",
        "bins_total": n,
        "cut_k": int(best_k),
        "winrate_threshold": str(_q4(_d(best_threshold))) if best_threshold is not None else None,
        "kept_bins": kept_bins_list,
        "removed_bins": removed_bins,
        "orig": {
            "trades": int(orig_trades),
            "pnl_abs": str(_q4(orig_pnl)),
            "winrate": str(_q4(orig_winrate)),
        },
        "filt": {
            "trades": int(filt_trades),
            "pnl_abs": str(_q4(filt_pnl)),
            "winrate": str(_q4(filt_winrate)),
        },
    }

    return (
        int(orig_trades),
        _q4(orig_pnl),
        _q4(orig_winrate),
        int(filt_trades),
        _q4(filt_pnl),
        _q4(filt_winrate),
        _d(best_threshold) if best_threshold is not None else None,
        raw_stat,
        kept_bins,
    )