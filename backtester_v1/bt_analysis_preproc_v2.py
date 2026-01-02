# bt_analysis_preproc_v2.py — препроцессинг v2: kept_bins (v1-логика) + дневная стабильность (run-aware, отдельная consumer group)

import asyncio
import logging
import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_PREPROC_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28

# 🔸 Константы стримов
PREPROC_V2_STREAM_KEY = "bt:analysis:ready"
PREPROC_V2_CONSUMER_GROUP = "bt_analysis_preproc_v2"
PREPROC_V2_CONSUMER_NAME = "bt_analysis_preproc_v2_main"

# 🔸 Стрим готовности препроцессинга v2
PREPROC_V2_READY_STREAM_KEY = "bt:analysis:preproc_ready_v2"

# 🔸 Настройки чтения стрима
PREPROC_V2_STREAM_BATCH_SIZE = 10
PREPROC_V2_STREAM_BLOCK_MS = 5000

# 🔸 Таблицы результатов v2
PREPROC_V2_TABLE = "bt_analysis_preproc_stat_v2"
DAILY_V2_TABLE = "bt_analysis_daily_stat_v2"

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


# 🔸 Публичная точка входа: воркер препроцессинга v2
async def run_bt_analysis_preproc_v2_orchestrator(pg, redis) -> None:
    log.debug("BT_ANALYSIS_PREPROC_V2: оркестратор v2 запущен")

    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_groups = 0
            total_preproc_upserts = 0
            total_daily_rows = 0
            total_skipped = 0
            total_errors = 0

            for stream_key, entries in messages:
                if stream_key != PREPROC_V2_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_analysis_ready(fields)
                    if not ctx:
                        await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
                        total_skipped += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    finished_at = ctx["finished_at"]

                    total_pairs += 1

                    # окно run (источник истины)
                    run_window = await _load_run_window(pg, run_id)
                    if not run_window:
                        log.error(
                            "BT_ANALYSIS_PREPROC_V2: не найден run_id=%s в bt_signal_backfill_runs, scenario_id=%s, signal_id=%s",
                            run_id,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
                        total_errors += 1
                        continue

                    run_from = run_window["from_time"]
                    run_to = run_window["to_time"]

                    # бин-статистика (как есть)
                    try:
                        bins_rows = await _load_bins_stat_rows(pg, run_id, scenario_id, signal_id)
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC_V2: ошибка загрузки bt_analysis_bins_stat для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    if not bins_rows:
                        log.info(
                            "BT_ANALYSIS_PREPROC_V2: нет данных bt_analysis_bins_stat — scenario_id=%s, signal_id=%s, run_id=%s",
                            scenario_id,
                            signal_id,
                            run_id,
                        )
                        await _publish_preproc_v2_ready(
                            redis=redis,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_id=run_id,
                            groups=0,
                            upserts=0,
                            daily_rows=0,
                        )
                        await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    # расчёт kept_bins (v1-логика) + дневной ряд + стабильность
                    try:
                        groups_processed, upserts_done, daily_rows_done = await _process_groups_v2(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            run_from=run_from,
                            run_to=run_to,
                            rows=bins_rows,
                        )
                        total_groups += groups_processed
                        total_preproc_upserts += upserts_done
                        total_daily_rows += daily_rows_done
                    except Exception as e:
                        total_errors += 1
                        log.error(
                            "BT_ANALYSIS_PREPROC_V2: ошибка обработки v2 для scenario_id=%s, signal_id=%s, run_id=%s: %s",
                            scenario_id,
                            signal_id,
                            run_id,
                            e,
                            exc_info=True,
                        )
                        await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)
                        continue

                    log.info(
                        "BT_ANALYSIS_PREPROC_V2: pair done — scenario_id=%s, signal_id=%s, run_id=%s, finished_at=%s, groups=%s, upserts=%s, daily_rows=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        finished_at,
                        groups_processed,
                        upserts_done,
                        daily_rows_done,
                    )

                    await _publish_preproc_v2_ready(
                        redis=redis,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        run_id=run_id,
                        groups=groups_processed,
                        upserts=upserts_done,
                        daily_rows=daily_rows_done,
                    )

                    await redis.xack(PREPROC_V2_STREAM_KEY, PREPROC_V2_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_ANALYSIS_PREPROC_V2: batch summary — msgs=%s, pairs=%s, groups=%s, upserts=%s, daily_rows=%s, skipped=%s, errors=%s",
                total_msgs,
                total_pairs,
                total_groups,
                total_preproc_upserts,
                total_daily_rows,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_PREPROC_V2: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group (Render-safe: SETID '$')
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=PREPROC_V2_STREAM_KEY,
            groupname=PREPROC_V2_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: создана consumer group '%s' для стрима '%s'",
            PREPROC_V2_CONSUMER_GROUP,
            PREPROC_V2_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_PREPROC_V2: consumer group '%s' уже существует — SETID '$' для игнора истории до старта",
                PREPROC_V2_CONSUMER_GROUP,
            )
            await redis.execute_command(
                "XGROUP",
                "SETID",
                PREPROC_V2_STREAM_KEY,
                PREPROC_V2_CONSUMER_GROUP,
                "$",
            )
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: consumer group '%s' SETID='$' выполнен для '%s'",
                PREPROC_V2_CONSUMER_GROUP,
                PREPROC_V2_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка при создании consumer group '%s': %s",
                PREPROC_V2_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready (Render-safe: NOGROUP recovery)
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=PREPROC_V2_CONSUMER_GROUP,
            consumername=PREPROC_V2_CONSUMER_NAME,
            streams={PREPROC_V2_STREAM_KEY: ">"},
            count=PREPROC_V2_STREAM_BATCH_SIZE,
            block=PREPROC_V2_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning(
                "BT_ANALYSIS_PREPROC_V2: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем",
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


# 🔸 Разбор сообщения bt:analysis:ready
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


# 🔸 Обработка групп v2: kept_bins (v1-логика) + дневной ряд + стабильность
async def _process_groups_v2(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    run_from: datetime,
    run_to: datetime,
    rows: List[Dict[str, Any]],
) -> Tuple[int, int, int]:
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
    daily_rows_total = 0

    for (analysis_id, indicator_param, timeframe, direction), group_bins in grouped.items():
        groups_processed += 1

        # kept_bins через v1-механику (макс filt_pnl_abs при отсечке худших по winrate)
        best = _compute_best_threshold_v1(group_bins)
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
            kept_bins,
            removed_bins,
            raw_stat,
        ) = best

        # дневной ряд по filtered сделкам (по exit_time::date), только дни с трейдами
        day_rows = await _load_daily_rows_for_kept_bins(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            timeframe=timeframe,
            direction=direction,
            run_from=run_from,
            run_to=run_to,
            kept_bins=kept_bins,
        )

        # стабильность по дневному ряду (days-with-trades)
        stability = _calc_stability_from_daily_rows(day_rows)

        # перезапись daily_stat_v2 по кандидату
        inserted_daily = await _rewrite_daily_rows_v2(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            indicator_param=indicator_param,
            timeframe=timeframe,
            direction=direction,
            day_rows=day_rows,
        )
        daily_rows_total += inserted_daily

        # upsert preproc_stat_v2 по кандидату
        await _upsert_preproc_v2_row(
            pg=pg,
            run_id=run_id,
            analysis_id=analysis_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            indicator_param=indicator_param,
            timeframe=timeframe,
            direction=direction,
            orig_trades=orig_trades,
            orig_pnl_abs=orig_pnl,
            orig_winrate=orig_winrate,
            filt_trades=filt_trades,
            filt_pnl_abs=filt_pnl,
            filt_winrate=filt_winrate,
            winrate_threshold=threshold,
            kept_bins=kept_bins,
            removed_bins=removed_bins,
            stability=stability,
            raw_stat=raw_stat,
        )

        upserts_done += 1

    return groups_processed, upserts_done, daily_rows_total


# 🔸 v1-логика подбора порога winrate: выкинуть k худших биннов (по winrate) так, чтобы filt_pnl_abs был максимален
def _compute_best_threshold_v1(
    bins: List[Dict[str, Any]],
) -> Optional[
    Tuple[int, Decimal, Decimal, int, Decimal, Decimal, Optional[Decimal], List[str], List[str], Dict[str, Any]]
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

    # сортировка: хуже → лучше (winrate asc, pnl asc, name asc) — детерминизм
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

    raw_stat = {
        "version": "v2_stage1",
        "algorithm": "v1_threshold_max_filt_pnl_abs",
        "sort": "winrate_asc",
        "criterion": "max_filt_pnl_abs",
        "bins_total": int(n),
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
        kept_bins,
        removed_bins,
        raw_stat,
    )


# 🔸 Дневной ряд по kept_bins: группировка по date(exit_time)
async def _load_daily_rows_for_kept_bins(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    timeframe: str,
    direction: str,
    run_from: datetime,
    run_to: datetime,
    kept_bins: List[str],
) -> List[Dict[str, Any]]:
    # условия достаточности
    if not kept_bins:
        return []

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                (p.exit_time::date)                     AS exit_day,
                COUNT(*)                                AS trades_day,
                COALESCE(SUM(r.pnl_abs), 0)             AS pnl_abs_day
            FROM bt_analysis_positions_raw r
            JOIN bt_scenario_positions p
              ON p.position_uid = r.position_uid
            WHERE r.run_id      = $1
              AND r.analysis_id = $2
              AND r.scenario_id = $3
              AND r.signal_id   = $4
              AND r.timeframe   = $5
              AND r.direction   = $6
              AND r.bin_name    = ANY($9::text[])
              AND p.status      = 'closed'
              AND p.postproc    = true
              AND p.exit_time IS NOT NULL
              AND p.exit_time BETWEEN $7 AND $8
            GROUP BY (p.exit_time::date)
            ORDER BY (p.exit_time::date)
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(timeframe),
            str(direction),
            run_from,
            run_to,
            list(kept_bins),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "exit_day": r["exit_day"],
                "trades_day": int(r["trades_day"] or 0),
                "pnl_abs_day": _d(r["pnl_abs_day"]),
            }
        )
    return out


# 🔸 Стабильность (MVP): neg_days, max_neg_streak, max_drawdown_abs, max_recovery_days (всё по days-with-trades)
def _calc_stability_from_daily_rows(day_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not day_rows:
        return {
            "days_with_trades": 0,
            "neg_days": 0,
            "max_neg_streak": 0,
            "max_drawdown_abs": _q4(Decimal("0")),
            "max_recovery_days": 0,
        }

    # сортировка на всякий случай (exit_day)
    day_rows_sorted = sorted(day_rows, key=lambda x: x.get("exit_day"))

    days_with_trades = len(day_rows_sorted)
    neg_days = 0

    # серия минус-дней (в рамках trade-days)
    cur_streak = 0
    max_streak = 0

    # equity / drawdown / recovery (в рамках trade-days)
    equity = Decimal("0")
    peak = Decimal("0")
    peak_idx = 0

    in_drawdown = False
    dd_start_peak_idx = 0
    dd_start_peak_value = Decimal("0")
    max_drawdown = Decimal("0")
    max_recovery_days = 0

    for idx, r in enumerate(day_rows_sorted):
        pnl_day = _d(r.get("pnl_abs_day"))
        equity += pnl_day

        if pnl_day < Decimal("0"):
            neg_days += 1
            cur_streak += 1
            if cur_streak > max_streak:
                max_streak = cur_streak
        else:
            cur_streak = 0

        # обновление peak / drawdown
        if equity >= peak:
            # если выходим из drawdown — фиксируем recovery
            if in_drawdown:
                recovery_len = idx - dd_start_peak_idx
                if recovery_len > max_recovery_days:
                    max_recovery_days = recovery_len
                in_drawdown = False

            peak = equity
            peak_idx = idx
        else:
            dd = peak - equity
            if dd > max_drawdown:
                max_drawdown = dd

            if not in_drawdown:
                in_drawdown = True
                dd_start_peak_idx = peak_idx
                dd_start_peak_value = peak  # для дебага (если понадобится)

    # если на конце остаёмся в drawdown — recovery не закрыт, не фиксируем (модель v2-stage1)
    return {
        "days_with_trades": int(days_with_trades),
        "neg_days": int(neg_days),
        "max_neg_streak": int(max_streak),
        "max_drawdown_abs": _q4(max_drawdown),
        "max_recovery_days": int(max_recovery_days),
    }


# 🔸 Перезапись дневного ряда в bt_analysis_daily_stat_v2 по кандидату (DELETE + INSERT/UPSERT)
async def _rewrite_daily_rows_v2(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param: str,
    timeframe: str,
    direction: str,
    day_rows: List[Dict[str, Any]],
) -> int:
    async with pg.acquire() as conn:
        # очистка старого дневного ряда по кандидату
        await conn.execute(
            f"""
            DELETE FROM {DAILY_V2_TABLE}
            WHERE run_id = $1
              AND analysis_id = $2
              AND scenario_id = $3
              AND signal_id = $4
              AND indicator_param = $5
              AND timeframe = $6
              AND direction = $7
            """,
            int(run_id),
            int(analysis_id),
            int(scenario_id),
            int(signal_id),
            str(indicator_param),
            str(timeframe),
            str(direction),
        )

        if not day_rows:
            return 0

        to_insert: List[Tuple[Any, ...]] = []
        for r in day_rows:
            to_insert.append(
                (
                    int(run_id),
                    int(analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(indicator_param),
                    str(timeframe),
                    str(direction),
                    r["exit_day"],
                    int(r.get("trades_day") or 0),
                    str(_q4(_d(r.get("pnl_abs_day")))),
                    json.dumps(r, ensure_ascii=False),
                )
            )

        await conn.executemany(
            f"""
            INSERT INTO {DAILY_V2_TABLE} (
                run_id,
                analysis_id,
                scenario_id,
                signal_id,
                indicator_param,
                timeframe,
                direction,
                exit_day,
                trades_day,
                pnl_abs_day,
                raw_stat,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10,
                $11::jsonb,
                now()
            )
            ON CONFLICT (run_id, analysis_id, scenario_id, signal_id, indicator_param, timeframe, direction, exit_day)
            DO UPDATE SET
                trades_day = EXCLUDED.trades_day,
                pnl_abs_day = EXCLUDED.pnl_abs_day,
                raw_stat = EXCLUDED.raw_stat,
                updated_at = now()
            """,
            to_insert,
        )

    return len(day_rows)


# 🔸 Upsert строки кандидата в bt_analysis_preproc_stat_v2
async def _upsert_preproc_v2_row(
    pg,
    run_id: int,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    indicator_param: str,
    timeframe: str,
    direction: str,
    orig_trades: int,
    orig_pnl_abs: Decimal,
    orig_winrate: Decimal,
    filt_trades: int,
    filt_pnl_abs: Decimal,
    filt_winrate: Decimal,
    winrate_threshold: Optional[Decimal],
    kept_bins: List[str],
    removed_bins: List[str],
    stability: Dict[str, Any],
    raw_stat: Dict[str, Any],
) -> None:
    # дополняем raw_stat стабильностью (для удобного дебага)
    raw_stat_out = dict(raw_stat or {})
    raw_stat_out["stability"] = {
        "days_with_trades": int(stability.get("days_with_trades") or 0),
        "neg_days": int(stability.get("neg_days") or 0),
        "max_neg_streak": int(stability.get("max_neg_streak") or 0),
        "max_drawdown_abs": str(_q4(_d(stability.get("max_drawdown_abs")))),
        "max_recovery_days": int(stability.get("max_recovery_days") or 0),
    }

    async with pg.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {PREPROC_V2_TABLE} (
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
                kept_bins,
                removed_bins,

                days_with_trades,
                neg_days,
                max_neg_streak,
                max_drawdown_abs,
                max_recovery_days,

                raw_stat,
                created_at
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,

                $8, $9, $10,
                $11, $12, $13,

                $14,
                $15::jsonb,
                $16::jsonb,

                $17, $18, $19, $20, $21,

                $22::jsonb,
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
                kept_bins          = EXCLUDED.kept_bins,
                removed_bins       = EXCLUDED.removed_bins,

                days_with_trades   = EXCLUDED.days_with_trades,
                neg_days           = EXCLUDED.neg_days,
                max_neg_streak     = EXCLUDED.max_neg_streak,
                max_drawdown_abs   = EXCLUDED.max_drawdown_abs,
                max_recovery_days  = EXCLUDED.max_recovery_days,

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

            int(orig_trades),
            str(_q4(_d(orig_pnl_abs))),
            str(_q4(_d(orig_winrate))),

            int(filt_trades),
            str(_q4(_d(filt_pnl_abs))),
            str(_q4(_d(filt_winrate))),

            (str(_q4(_d(winrate_threshold))) if winrate_threshold is not None else None),
            json.dumps(list(kept_bins), ensure_ascii=False),
            json.dumps(list(removed_bins), ensure_ascii=False),

            int(stability.get("days_with_trades") or 0),
            int(stability.get("neg_days") or 0),
            int(stability.get("max_neg_streak") or 0),
            str(_q4(_d(stability.get("max_drawdown_abs")))),
            int(stability.get("max_recovery_days") or 0),

            json.dumps(raw_stat_out, ensure_ascii=False),
        )


# 🔸 Публикация события готовности препроцессинга v2
async def _publish_preproc_v2_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    groups: int,
    upserts: int,
    daily_rows: int,
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            PREPROC_V2_READY_STREAM_KEY,
            {
                "scenario_id": str(int(scenario_id)),
                "signal_id": str(int(signal_id)),
                "run_id": str(int(run_id)),
                "groups": str(int(groups)),
                "upserts": str(int(upserts)),
                "daily_rows": str(int(daily_rows)),
                "finished_at": finished_at.isoformat(),
            },
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: published %s scenario_id=%s signal_id=%s run_id=%s groups=%s upserts=%s daily_rows=%s",
            PREPROC_V2_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            run_id,
            groups,
            upserts,
            daily_rows,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
            PREPROC_V2_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            run_id,
            e,
            exc_info=True,
        )