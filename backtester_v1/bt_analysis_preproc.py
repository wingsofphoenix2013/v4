# bt_analysis_preproc.py — препроцессинг анализа: оптимальный порог winrate по биннам + walk-forward стабильность (run-aware)

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

# 🔸 Стрим готовности препроцессинга (для следующего этапа)
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"

# 🔸 Настройки чтения стрима
PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

# 🔸 Таблицы результатов
PREPROC_TABLE = "bt_analysis_preproc_stat"
WALK_TABLE = "bt_analysis_preproc_walkforward"

# 🔸 Walk-forward параметры (по умолчанию)
WF_TRAIN_DAYS = 14
WF_TEST_DAYS = 2
WF_STEP_DAYS = 2
WF_MIN_STEPS_FOR_SCORE = 3

# 🔸 Квантизация метрик
Q4 = Decimal("0.0001")
DENOM_EPS = Decimal("1")


# 🔸 Длительность в Decimal с обрезкой до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Медиана для Decimal-списка
def _median_decimal(values: List[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    arr = sorted(values)
    n = len(arr)
    mid = n // 2
    if n % 2 == 1:
        return arr[mid]
    return (arr[mid - 1] + arr[mid]) / Decimal("2")


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
            total_steps_upserts = 0
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

                    window_from = run_window["from_time"]
                    window_to = run_window["to_time"]

                    # загружаем все бины по паре (scenario/signal/run)
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

                    # обрабатываем группы (analysis_id/indicator_param/timeframe/direction)
                    try:
                        groups_processed, upserts_done, steps_upserts_done = await _process_and_store_groups(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            window_from=window_from,
                            window_to=window_to,
                            bins_rows=bins_rows,
                        )
                        total_groups += groups_processed
                        total_upserts += upserts_done
                        total_steps_upserts += steps_upserts_done
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

                    log.info(
                        "BT_ANALYSIS_PREPROC: pair done — scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s, "
                        "groups=%s, preproc_upserts=%s, wf_steps_upserts=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        groups_processed,
                        upserts_done,
                        steps_upserts_done,
                    )

                    # публикуем событие готовности препроцессинга (run-aware)
                    finished_at_preproc = datetime.utcnow()
                    try:
                        await redis.xadd(
                            PREPROC_READY_STREAM_KEY,
                            {
                                "scenario_id": str(scenario_id),
                                "signal_id": str(signal_id),
                                "run_id": str(run_id),
                                "groups": str(groups_processed),
                                "upserts": str(upserts_done),
                                "wf_steps_upserts": str(steps_upserts_done),
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

            log.info(
                "BT_ANALYSIS_PREPROC: batch summary — msgs=%s, pairs=%s, groups=%s, preproc_upserts=%s, wf_steps_upserts=%s, skipped=%s, errors=%s",
                total_msgs,
                total_pairs,
                total_groups,
                total_upserts,
                total_steps_upserts,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_PREPROC: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group
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
            log.info(
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


# 🔸 Чтение сообщений из стрима bt:analysis:ready
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


# 🔸 Загрузка бин-статистики по паре run/scenario/signal
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
        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": r["indicator_param"],
                "timeframe": str(r["timeframe"]).strip().lower(),
                "direction": str(r["direction"]).strip().lower(),
                "bin_name": str(r["bin_name"]),
                "trades": int(r["trades"] or 0),
                "pnl_abs": _d(r["pnl_abs"]),
                "winrate": _d(r["winrate"]),
            }
        )
    return out


# 🔸 Выборка агрегированной статистики по биннам из позиций для окна (train)
async def _load_train_bin_stats(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
    train_from: datetime,
    train_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                r.bin_name                                      AS bin_name,
                COUNT(*)                                        AS trades,
                COUNT(*) FILTER (WHERE r.pnl_abs > 0)           AS wins,
                COALESCE(SUM(r.pnl_abs), 0)                     AS pnl_abs_total
            FROM bt_analysis_positions_raw r
            JOIN bt_scenario_positions p
              ON p.position_uid = r.position_uid
            WHERE r.run_id     = $1
              AND r.analysis_id = $2
              AND r.scenario_id = $3
              AND r.signal_id   = $4
              AND r.timeframe   = $5
              AND r.direction   = $6
              AND p.entry_time BETWEEN $7 AND $8
            GROUP BY r.bin_name
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(timeframe),
            str(direction),
            train_from,
            train_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        trades = int(r["trades"] or 0)
        wins = int(r["wins"] or 0)
        pnl = _d(r["pnl_abs_total"])
        winrate = (Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")

        out.append(
            {
                "bin_name": str(r["bin_name"]),
                "trades": trades,
                "pnl_abs": pnl,
                "winrate": winrate,
            }
        )

    return out


# 🔸 Выборка позиций для тестового окна (test): bin_name + pnl_abs
async def _load_test_positions(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
    test_from: datetime,
    test_to: datetime,
) -> List[Tuple[str, Decimal]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                r.bin_name AS bin_name,
                r.pnl_abs  AS pnl_abs
            FROM bt_analysis_positions_raw r
            JOIN bt_scenario_positions p
              ON p.position_uid = r.position_uid
            WHERE r.run_id     = $1
              AND r.analysis_id = $2
              AND r.scenario_id = $3
              AND r.signal_id   = $4
              AND r.timeframe   = $5
              AND r.direction   = $6
              AND p.entry_time BETWEEN $7 AND $8
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(timeframe),
            str(direction),
            test_from,
            test_to,
        )

    out: List[Tuple[str, Decimal]] = []
    for r in rows:
        out.append((str(r["bin_name"]), _d(r["pnl_abs"])))
    return out


# 🔸 Обработка всех групп и запись результатов в bt_analysis_preproc_stat + walk-forward таблицу
async def _process_and_store_groups(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    window_from: datetime,
    window_to: datetime,
    bins_rows: List[Dict[str, Any]],
) -> Tuple[int, int, int]:
    if not bins_rows:
        return 0, 0, 0

    # группируем по (analysis_id, indicator_param_norm, timeframe, direction)
    grouped: Dict[Tuple[int, str, str, str], List[Dict[str, Any]]] = {}
    for r in bins_rows:
        analysis_id = int(r["analysis_id"])
        ind = r.get("indicator_param")
        indicator_param_norm = str(ind).strip() if ind is not None else ""
        tf = str(r["timeframe"]).strip().lower()
        direction = str(r["direction"]).strip().lower()

        key = (analysis_id, indicator_param_norm, tf, direction)
        grouped.setdefault(key, []).append(r)

    groups_processed = 0
    upserts_done = 0
    steps_upserts_done = 0

    for (analysis_id, indicator_param_norm, tf, direction), group_bins in grouped.items():
        groups_processed += 1

        # 1) основной оптимум по полному окну run (из bt_analysis_bins_stat)
        best = _compute_best_threshold(group_bins)
        if best is None:
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
        ) = best

        # 2) walk-forward стабильность внутри окна run
        stability_score, steps_written = await _compute_walkforward_and_store(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            indicator_param_norm=indicator_param_norm,
            timeframe=tf,
            direction=direction,
            window_from=window_from,
            window_to=window_to,
        )
        steps_upserts_done += steps_written

        # 3) upsert в bt_analysis_preproc_stat (включая stability_score)
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
                    orig_trades,
                    orig_pnl_abs,
                    orig_winrate,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    winrate_threshold,
                    stability_score,
                    raw_stat,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10,
                    $11, $12, $13,
                    $14,
                    $15,
                    $16::jsonb,
                    now()
                )
                ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction)
                DO UPDATE SET
                    orig_trades        = EXCLUDED.orig_trades,
                    orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                    orig_winrate       = EXCLUDED.orig_winrate,
                    filt_trades        = EXCLUDED.filt_trades,
                    filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                    filt_winrate       = EXCLUDED.filt_winrate,
                    winrate_threshold  = EXCLUDED.winrate_threshold,
                    stability_score    = EXCLUDED.stability_score,
                    raw_stat           = EXCLUDED.raw_stat,
                    updated_at         = now()
                """,
                int(run_id),
                int(analysis_id),
                int(scenario_id),
                int(signal_id),
                str(indicator_param_norm),
                str(tf),
                str(direction),
                int(orig_trades),
                str(_q4(orig_pnl)),
                str(_q4(orig_winrate)),
                int(filt_trades),
                str(_q4(filt_pnl)),
                str(_q4(filt_winrate)),
                str(_q4(threshold)) if threshold is not None else None,
                str(_q4(stability_score)),
                json.dumps(raw_stat, ensure_ascii=False),
            )

        upserts_done += 1

        log.info(
            "BT_ANALYSIS_PREPROC: group stored — run_id=%s scenario_id=%s signal_id=%s analysis_id=%s tf=%s dir=%s "
            "orig(trades=%s,pnl=%s,wr=%s) filt(trades=%s,pnl=%s,wr=%s) thr=%s stability=%s steps=%s kept_bins=%s",
            run_id,
            scenario_id,
            signal_id,
            analysis_id,
            tf,
            direction,
            orig_trades,
            str(_q4(orig_pnl)),
            str(_q4(orig_winrate)),
            filt_trades,
            str(_q4(filt_pnl)),
            str(_q4(filt_winrate)),
            str(_q4(threshold)) if threshold is not None else None,
            str(_q4(stability_score)),
            steps_written,
            len(kept_bins),
        )

    return groups_processed, upserts_done, steps_upserts_done


# 🔸 Walk-forward: train/test rolling внутри одного run, запись шагов + расчёт stability_score
async def _compute_walkforward_and_store(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param_norm: str,
    timeframe: str,
    direction: str,
    window_from: datetime,
    window_to: datetime,
) -> Tuple[Decimal, int]:
    train_days = int(WF_TRAIN_DAYS)
    test_days = int(WF_TEST_DAYS)
    step_days = int(WF_STEP_DAYS)

    train_delta = timedelta(days=train_days)
    test_delta = timedelta(days=test_days)
    step_delta = timedelta(days=step_days)

    step_no = 0
    steps_written = 0

    lift_ratios: List[Decimal] = []
    shares: List[Decimal] = []
    hit_count = 0
    valid_steps = 0

    # первый test начинается сразу после первого train
    train_from = window_from
    train_to = train_from + train_delta

    while True:
        test_from = train_to
        test_to = test_from + test_delta

        # условия остановки
        if train_to > window_to:
            break
        if test_to > window_to:
            break

        step_no += 1

        # 1) train: бин-статистика по позициям в train окне
        train_bins = await _load_train_bin_stats(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            timeframe=timeframe,
            direction=direction,
            train_from=train_from,
            train_to=train_to,
        )

        # если на train нет данных — записываем шаг как пустой и двигаемся дальше
        if not train_bins:
            await _upsert_walk_step(
                pg=pg,
                run_id=run_id,
                analysis_id=analysis_id,
                scenario_id=scenario_id,
                signal_id=signal_id,
                indicator_param_norm=indicator_param_norm,
                timeframe=timeframe,
                direction=direction,
                step_no=step_no,
                train_from=train_from,
                train_to=train_to,
                test_from=test_from,
                test_to=test_to,
                threshold=None,
                orig_trades=0,
                orig_pnl=Decimal("0"),
                orig_winrate=Decimal("0"),
                filt_trades=0,
                filt_pnl=Decimal("0"),
                filt_winrate=Decimal("0"),
                lift_pnl=Decimal("0"),
                trades_share=Decimal("0"),
                raw_stat={"version": "v1", "skipped_reason": "no_train_data"},
            )
            steps_written += 1
            train_from = train_from + step_delta
            train_to = train_from + train_delta
            continue

        best_train = _compute_best_threshold(train_bins)
        if best_train is None:
            await _upsert_walk_step(
                pg=pg,
                run_id=run_id,
                analysis_id=analysis_id,
                scenario_id=scenario_id,
                signal_id=signal_id,
                indicator_param_norm=indicator_param_norm,
                timeframe=timeframe,
                direction=direction,
                step_no=step_no,
                train_from=train_from,
                train_to=train_to,
                test_from=test_from,
                test_to=test_to,
                threshold=None,
                orig_trades=0,
                orig_pnl=Decimal("0"),
                orig_winrate=Decimal("0"),
                filt_trades=0,
                filt_pnl=Decimal("0"),
                filt_winrate=Decimal("0"),
                lift_pnl=Decimal("0"),
                trades_share=Decimal("0"),
                raw_stat={"version": "v1", "skipped_reason": "train_threshold_failed"},
            )
            steps_written += 1
            train_from = train_from + step_delta
            train_to = train_from + train_delta
            continue

        (
            _orig_trades_train,
            _orig_pnl_train,
            _orig_winrate_train,
            _filt_trades_train,
            _filt_pnl_train,
            _filt_winrate_train,
            threshold_train,
            raw_stat_train,
            kept_bins_train,
        ) = best_train

        # 2) test: применяем kept_bins_train
        test_positions = await _load_test_positions(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            timeframe=timeframe,
            direction=direction,
            test_from=test_from,
            test_to=test_to,
        )

        orig_trades = len(test_positions)
        if orig_trades <= 0:
            await _upsert_walk_step(
                pg=pg,
                run_id=run_id,
                analysis_id=analysis_id,
                scenario_id=scenario_id,
                signal_id=signal_id,
                indicator_param_norm=indicator_param_norm,
                timeframe=timeframe,
                direction=direction,
                step_no=step_no,
                train_from=train_from,
                train_to=train_to,
                test_from=test_from,
                test_to=test_to,
                threshold=threshold_train,
                orig_trades=0,
                orig_pnl=Decimal("0"),
                orig_winrate=Decimal("0"),
                filt_trades=0,
                filt_pnl=Decimal("0"),
                filt_winrate=Decimal("0"),
                lift_pnl=Decimal("0"),
                trades_share=Decimal("0"),
                raw_stat={
                    "version": "v1",
                    "skipped_reason": "no_test_data",
                    "train": raw_stat_train,
                },
            )
            steps_written += 1
            train_from = train_from + step_delta
            train_to = train_from + train_delta
            continue

        orig_pnl = Decimal("0")
        orig_wins = Decimal("0")
        filt_trades = 0
        filt_pnl = Decimal("0")
        filt_wins = Decimal("0")

        for bn, pnl in test_positions:
            pnl_d = _d(pnl)
            orig_pnl += pnl_d
            if pnl_d > 0:
                orig_wins += Decimal("1")

            if bn in kept_bins_train:
                filt_trades += 1
                filt_pnl += pnl_d
                if pnl_d > 0:
                    filt_wins += Decimal("1")

        orig_winrate = (orig_wins / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")
        filt_winrate = (filt_wins / Decimal(filt_trades)) if filt_trades > 0 else Decimal("0")

        lift_pnl = filt_pnl - orig_pnl
        trades_share = (Decimal(filt_trades) / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")

        await _upsert_walk_step(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            indicator_param_norm=indicator_param_norm,
            timeframe=timeframe,
            direction=direction,
            step_no=step_no,
            train_from=train_from,
            train_to=train_to,
            test_from=test_from,
            test_to=test_to,
            threshold=threshold_train,
            orig_trades=orig_trades,
            orig_pnl=orig_pnl,
            orig_winrate=orig_winrate,
            filt_trades=filt_trades,
            filt_pnl=filt_pnl,
            filt_winrate=filt_winrate,
            lift_pnl=lift_pnl,
            trades_share=trades_share,
            raw_stat={
                "version": "v1",
                "train": raw_stat_train,
                "kept_bins_count": len(kept_bins_train),
            },
        )
        steps_written += 1

        # 3) вклад шага в scoring
        valid_steps += 1

        if lift_pnl > 0:
            hit_count += 1

        lift_ratio = lift_pnl / (abs(orig_pnl) + DENOM_EPS)
        lift_ratios.append(_d(lift_ratio))
        shares.append(_d(trades_share))

        # следующий шаг
        train_from = train_from + step_delta
        train_to = train_from + train_delta

    # scoring
    if valid_steps < WF_MIN_STEPS_FOR_SCORE:
        return Decimal("0"), steps_written

    hit_rate = Decimal(hit_count) / Decimal(valid_steps) if valid_steps > 0 else Decimal("0")
    median_share = _median_decimal(shares)
    median_lift_ratio = _median_decimal(lift_ratios)

    score = hit_rate * median_share * median_lift_ratio

    # clamp 0..1, floor at 0
    if score < 0:
        score = Decimal("0")
    if score > Decimal("1"):
        score = Decimal("1")

    return _q4(score), steps_written


# 🔸 Upsert одного шага walk-forward в bt_analysis_preproc_walkforward
async def _upsert_walk_step(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param_norm: str,
    timeframe: str,
    direction: str,
    step_no: int,
    train_from: datetime,
    train_to: datetime,
    test_from: datetime,
    test_to: datetime,
    threshold: Optional[Decimal],
    orig_trades: int,
    orig_pnl: Decimal,
    orig_winrate: Decimal,
    filt_trades: int,
    filt_pnl: Decimal,
    filt_winrate: Decimal,
    lift_pnl: Decimal,
    trades_share: Decimal,
    raw_stat: Dict[str, Any],
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {WALK_TABLE} (
                run_id,
                analysis_id,
                scenario_id,
                signal_id,
                indicator_param,
                timeframe,
                direction,
                step_no,
                train_from,
                train_to,
                test_from,
                test_to,
                winrate_threshold,
                orig_trades,
                orig_pnl_abs,
                orig_winrate,
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                lift_pnl_abs,
                trades_share,
                raw_stat,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10, $11, $12,
                $13,
                $14, $15, $16,
                $17, $18, $19,
                $20, $21,
                $22::jsonb,
                now()
            )
            ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction, step_no)
            DO UPDATE SET
                train_from         = EXCLUDED.train_from,
                train_to           = EXCLUDED.train_to,
                test_from          = EXCLUDED.test_from,
                test_to            = EXCLUDED.test_to,
                winrate_threshold  = EXCLUDED.winrate_threshold,
                orig_trades        = EXCLUDED.orig_trades,
                orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                orig_winrate       = EXCLUDED.orig_winrate,
                filt_trades        = EXCLUDED.filt_trades,
                filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                filt_winrate       = EXCLUDED.filt_winrate,
                lift_pnl_abs       = EXCLUDED.lift_pnl_abs,
                trades_share       = EXCLUDED.trades_share,
                raw_stat           = EXCLUDED.raw_stat,
                updated_at         = now()
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(indicator_param_norm),
            str(timeframe),
            str(direction),
            int(step_no),
            train_from,
            train_to,
            test_from,
            test_to,
            str(_q4(threshold)) if threshold is not None else None,
            int(orig_trades),
            str(_q4(orig_pnl)),
            str(_q4(orig_winrate)),
            int(filt_trades),
            str(_q4(filt_pnl)),
            str(_q4(filt_winrate)),
            str(_q4(lift_pnl)),
            str(_q4(trades_share)),
            json.dumps(raw_stat, ensure_ascii=False),
        )


# 🔸 Вычисление оптимального порога winrate для одной группы биннов
def _compute_best_threshold(
    bins: List[Dict[str, Any]],
) -> Optional[
    Tuple[int, Decimal, Decimal, int, Decimal, Decimal, Optional[Decimal], Dict[str, Any], Set[str]]
]:
    if not bins:
        return None

    # нормализуем и фильтруем мусор
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

    # префиксные суммы по "удаляемым" биннам
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
    kept_bins = [x["bin_name"] for x in normalized[best_k:]]
    kept_bins_set = set(kept_bins)

    raw_stat = {
        "version": "v1",
        "sort": "winrate_asc",
        "criterion": "max_filt_pnl_abs",
        "bins_total": n,
        "cut_k": int(best_k),
        "winrate_threshold": str(_q4(_d(best_threshold))) if best_threshold is not None else None,
        "kept_bins": kept_bins,
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
        kept_bins_set,
    )