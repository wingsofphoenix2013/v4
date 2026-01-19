# bt_analysis_stabproc_v2.py — stabproc v2: проверка "good" биннов победителя на укороченных окнах (50% и 25%) и выставление consensus (run-aware)

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Логгер модуля
log = logging.getLogger("BT_ANALYSIS_STABPROC_V2")

# 🔸 Настройки Decimal
getcontext().prec = 28
Q4 = Decimal("0.0001")


# 🔸 Константы стримов
STABPROC_STREAM_KEY = "bt:analysis:postproc_ready_v2"
STABPROC_CONSUMER_GROUP = "bt_analysis_stabproc_v2"
STABPROC_CONSUMER_NAME = "bt_analysis_stabproc_v2_main"

# 🔸 Стрим готовности stabproc v2
STABPROC_READY_STREAM_KEY = "bt:analysis:stabproc_ready_v2"

# 🔸 Настройки чтения стрима
STABPROC_STREAM_BATCH_SIZE = 10
STABPROC_STREAM_BLOCK_MS = 5000

# 🔸 Таблицы
RUNS_TABLE = "bt_signal_backfill_runs"
LABELS_V2_TABLE = "bt_analysis_bins_labels_v2"
RAW_TABLE = "bt_analysis_positions_raw"
POSITIONS_TABLE = "bt_scenario_positions_v2"


# 🔸 Квантизация Decimal до 4 знаков
def _q4(value: Decimal) -> Decimal:
    return value.quantize(Q4, rounding=ROUND_DOWN)


# 🔸 Безопасный Decimal
def _d(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


# 🔸 Публичная точка входа: воркер stabproc v2
async def run_bt_analysis_stabproc_v2_orchestrator(pg, redis) -> None:
    log.debug("BT_ANALYSIS_STABPROC_V2: оркестратор v2 запущен")

    await _ensure_consumer_group(redis)

    while True:
        try:
            messages = await _read_from_stream(redis)
            if not messages:
                continue

            total_msgs = 0
            total_pairs = 0
            total_rows = 0
            total_true = 0
            total_false = 0
            total_skipped = 0
            total_errors = 0

            for stream_key, entries in messages:
                if stream_key != STABPROC_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1

                    ctx = _parse_postproc_ready(fields)
                    if not ctx:
                        await redis.xack(STABPROC_STREAM_KEY, STABPROC_CONSUMER_GROUP, entry_id)
                        total_skipped += 1
                        continue

                    scenario_id = ctx["scenario_id"]
                    signal_id = ctx["signal_id"]
                    run_id = ctx["run_id"]
                    score_version = ctx["score_version"]
                    finished_at_postproc = ctx["finished_at"]

                    total_pairs += 1

                    # грузим окно run (источник истины)
                    run_window = await _load_run_window(pg, run_id)
                    if not run_window:
                        log.error(
                            "BT_ANALYSIS_STABPROC_V2: не найден run_id=%s в %s (scenario_id=%s, signal_id=%s)",
                            run_id,
                            RUNS_TABLE,
                            scenario_id,
                            signal_id,
                        )
                        await redis.xack(STABPROC_STREAM_KEY, STABPROC_CONSUMER_GROUP, entry_id)
                        total_errors += 1
                        continue

                    run_from = run_window["from_time"]
                    run_to = run_window["to_time"]

                    # вычисляем укороченные окна "назад от run_to"
                    w50_from, w25_from = _calc_subwindows(run_from=run_from, run_to=run_to)

                    # грузим бинны победителя (как они записаны postproc)
                    winner_bins = await _load_winner_bins(
                        pg=pg,
                        run_id=run_id,
                        scenario_id=scenario_id,
                        signal_id=signal_id,
                        score_version=score_version,
                    )

                    if not winner_bins:
                        log.info(
                            "BT_ANALYSIS_STABPROC_V2: нет строк в %s — scenario_id=%s signal_id=%s run_id=%s score_version=%s",
                            LABELS_V2_TABLE,
                            scenario_id,
                            signal_id,
                            run_id,
                            score_version,
                        )
                        await _publish_stabproc_ready(redis=redis, ctx=ctx)
                        await redis.xack(STABPROC_STREAM_KEY, STABPROC_CONSUMER_GROUP, entry_id)
                        continue

                    # группируем по (analysis_id, indicator_param, timeframe, direction)
                    groups = _group_winner_bins(winner_bins)

                    rows_updated = 0
                    rows_true = 0
                    rows_false = 0

                    for gkey, g in groups.items():
                        analysis_id = g["analysis_id"]
                        indicator_param = g["indicator_param"]
                        timeframe = g["timeframe"]
                        direction = g["direction"]
                        bin_names = g["bin_names"]
                        threshold_by_bin = g["threshold_by_bin"]

                        # статистика по биннам на 50% и 25% окнах
                        stats_50 = await _load_bin_stats_in_window(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            analysis_id=analysis_id,
                            timeframe=timeframe,
                            direction=direction,
                            bin_names=bin_names,
                            w_from=w50_from,
                            w_to=run_to,
                        )

                        stats_25 = await _load_bin_stats_in_window(
                            pg=pg,
                            run_id=run_id,
                            scenario_id=scenario_id,
                            signal_id=signal_id,
                            analysis_id=analysis_id,
                            timeframe=timeframe,
                            direction=direction,
                            bin_names=bin_names,
                            w_from=w25_from,
                            w_to=run_to,
                        )

                        # выставляем consensus по каждому бину
                        updates: List[Tuple[Any, ...]] = []
                        for bn in bin_names:
                            threshold_used = threshold_by_bin.get(bn, Decimal("0"))

                            t50, wr50 = stats_50.get(bn, (0, Decimal("0")))
                            t25, wr25 = stats_25.get(bn, (0, Decimal("0")))

                            # trades=0 => consensus=false
                            if t50 <= 0 or t25 <= 0:
                                consensus = False
                            else:
                                consensus = (wr50 >= threshold_used) and (wr25 >= threshold_used)

                            updates.append(
                                (
                                    bool(consensus),
                                    int(run_id),
                                    int(scenario_id),
                                    int(signal_id),
                                    str(direction),
                                    str(score_version),
                                    int(analysis_id),
                                    indicator_param,          # nullable, используем IS NOT DISTINCT FROM
                                    str(timeframe),
                                    str(bn),
                                )
                            )

                        # апдейтим пачкой
                        ok, bad = await _update_consensus_batch(pg=pg, rows=updates)
                        rows_updated += (ok + bad)
                        rows_true += ok
                        rows_false += bad

                    total_rows += rows_updated
                    total_true += rows_true
                    total_false += rows_false

                    log.info(
                        "BT_ANALYSIS_STABPROC_V2: pair done — scenario_id=%s signal_id=%s run_id=%s score_version=%s "
                        "run=[%s..%s], w50_from=%s, w25_from=%s, rows=%s, consensus_true=%s, consensus_false=%s, source_finished_at=%s",
                        scenario_id,
                        signal_id,
                        run_id,
                        score_version,
                        run_from,
                        run_to,
                        w50_from,
                        w25_from,
                        rows_updated,
                        rows_true,
                        rows_false,
                        finished_at_postproc,
                    )

                    await _publish_stabproc_ready(redis=redis, ctx=ctx)

                    await redis.xack(STABPROC_STREAM_KEY, STABPROC_CONSUMER_GROUP, entry_id)

            log.info(
                "BT_ANALYSIS_STABPROC_V2: batch summary — msgs=%s pairs=%s rows=%s consensus_true=%s consensus_false=%s skipped=%s errors=%s",
                total_msgs,
                total_pairs,
                total_rows,
                total_true,
                total_false,
                total_skipped,
                total_errors,
            )

        except Exception as e:
            log.error("BT_ANALYSIS_STABPROC_V2: loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group (Render-safe: SETID '$')
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=STABPROC_STREAM_KEY,
            groupname=STABPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_STABPROC_V2: создана consumer group '%s' для стрима '%s'",
            STABPROC_CONSUMER_GROUP,
            STABPROC_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.info(
                "BT_ANALYSIS_STABPROC_V2: consumer group '%s' уже существует — SETID '$' для игнора истории до старта",
                STABPROC_CONSUMER_GROUP,
            )
            await redis.execute_command(
                "XGROUP",
                "SETID",
                STABPROC_STREAM_KEY,
                STABPROC_CONSUMER_GROUP,
                "$",
            )
            log.debug(
                "BT_ANALYSIS_STABPROC_V2: consumer group '%s' SETID='$' выполнен для '%s'",
                STABPROC_CONSUMER_GROUP,
                STABPROC_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_STABPROC_V2: ошибка при создании consumer group '%s': %s",
                STABPROC_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:postproc_ready_v2 (NOGROUP recovery)
async def _read_from_stream(redis) -> List[Any]:
    try:
        entries = await redis.xreadgroup(
            groupname=STABPROC_CONSUMER_GROUP,
            consumername=STABPROC_CONSUMER_NAME,
            streams={STABPROC_STREAM_KEY: ">"},
            count=STABPROC_STREAM_BATCH_SIZE,
            block=STABPROC_STREAM_BLOCK_MS,
        )
    except Exception as e:
        msg = str(e)
        if "NOGROUP" in msg:
            log.warning("BT_ANALYSIS_STABPROC_V2: NOGROUP при XREADGROUP — переинициализируем группу и продолжаем")
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


# 🔸 Разбор сообщения bt:analysis:postproc_ready_v2 (сохраняем состав полей как есть)
def _parse_postproc_ready(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id = int(fields.get("scenario_id") or 0)
        signal_id = int(fields.get("signal_id") or 0)
        run_id = int(fields.get("run_id") or 0)
        score_version = str(fields.get("score_version") or "v1").strip()
        finished_at_str = fields.get("finished_at")

        if scenario_id <= 0 or signal_id <= 0 or run_id <= 0 or not finished_at_str:
            return None

        # поля ниже оставляем как строки (как в исходном сообщении)
        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "candidates": str(fields.get("candidates") or "0"),
            "score_upserts": str(fields.get("score_upserts") or "0"),
            "labels_rows": str(fields.get("labels_rows") or "0"),
            "winner_analysis_id": str(fields.get("winner_analysis_id") or "0"),
            "winner_param": str(fields.get("winner_param") or ""),
            "score_version": score_version,
            "finished_at": datetime.fromisoformat(finished_at_str),
            "source_finished_at": str(fields.get("source_finished_at") or ""),
        }
    except Exception:
        return None


# 🔸 Расчёт двух sub-window (50% и 25%) "назад от run_to"
def _calc_subwindows(run_from: datetime, run_to: datetime) -> Tuple[datetime, datetime]:
    # длина окна в секундах
    total_sec = (run_to - run_from).total_seconds()
    if total_sec <= 0:
        return run_from, run_from

    # 50% и 25% назад от run_to (свежесть)
    w50_sec = total_sec * 0.50
    w25_sec = total_sec * 0.25

    w50_from = run_to - timedelta(seconds=w50_sec)
    w25_from = run_to - timedelta(seconds=w25_sec)

    return w50_from, w25_from


# 🔸 Загрузка run-окна из bt_signal_backfill_runs
async def _load_run_window(pg, run_id: int) -> Optional[Dict[str, Any]]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT from_time, to_time
            FROM {RUNS_TABLE}
            WHERE id = $1
            """,
            int(run_id),
        )
    if not row:
        return None
    return {"from_time": row["from_time"], "to_time": row["to_time"]}


# 🔸 Загрузка "good" биннов победителя из bt_analysis_bins_labels_v2
async def _load_winner_bins(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    score_version: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                analysis_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                threshold_used
            FROM {LABELS_V2_TABLE}
            WHERE run_id = $1
              AND scenario_id = $2
              AND signal_id = $3
              AND score_version = $4
              AND state = 'good'
            ORDER BY analysis_id, indicator_param NULLS FIRST, timeframe, direction, bin_name
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
            str(score_version),
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        ind = r["indicator_param"]
        indicator_param = str(ind).strip() if ind is not None else None

        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": indicator_param,
                "timeframe": str(r["timeframe"]).strip().lower(),
                "direction": str(r["direction"]).strip().lower(),
                "bin_name": str(r["bin_name"]),
                "threshold_used": _d(r["threshold_used"]),
            }
        )

    return out


# 🔸 Группировка winner-биннов по (analysis_id, indicator_param, timeframe, direction)
def _group_winner_bins(rows: List[Dict[str, Any]]) -> Dict[Tuple[int, Optional[str], str, str], Dict[str, Any]]:
    grouped: Dict[Tuple[int, Optional[str], str, str], Dict[str, Any]] = {}

    for r in rows:
        analysis_id = int(r["analysis_id"])
        indicator_param = r.get("indicator_param")
        timeframe = str(r.get("timeframe") or "").strip().lower()
        direction = str(r.get("direction") or "").strip().lower()
        bin_name = str(r.get("bin_name") or "")
        threshold_used = _d(r.get("threshold_used"))

        key = (analysis_id, indicator_param, timeframe, direction)
        g = grouped.setdefault(
            key,
            {
                "analysis_id": analysis_id,
                "indicator_param": indicator_param,
                "timeframe": timeframe,
                "direction": direction,
                "bin_names": [],
                "threshold_by_bin": {},
            },
        )

        g["bin_names"].append(bin_name)
        g["threshold_by_bin"][bin_name] = threshold_used

    return grouped


# 🔸 Загрузка trades/winrate по биннам в указанном окне времени
async def _load_bin_stats_in_window(
    pg,
    run_id: int,
    scenario_id: int,
    signal_id: int,
    analysis_id: int,
    timeframe: str,
    direction: str,
    bin_names: List[str],
    w_from: datetime,
    w_to: datetime,
) -> Dict[str, Tuple[int, Decimal]]:
    # условий достаточности
    if not bin_names:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                r.bin_name                                      AS bin_name,
                COUNT(*)                                        AS trades,
                COUNT(*) FILTER (WHERE r.pnl_abs > 0)           AS wins
            FROM {RAW_TABLE} r
            JOIN {POSITIONS_TABLE} p
              ON p.position_uid = r.position_uid
            WHERE r.run_id = $1
              AND r.scenario_id = $2
              AND r.signal_id = $3
              AND r.analysis_id = $4
              AND r.timeframe = $5
              AND r.direction = $6
              AND r.bin_name = ANY($7::text[])
              AND p.status = 'closed'
              AND p.postproc_v2 = true
              AND p.exit_time IS NOT NULL
              AND p.exit_time BETWEEN $8 AND $9
            GROUP BY r.bin_name
            """,
            int(run_id),
            int(scenario_id),
            int(signal_id),
            int(analysis_id),
            str(timeframe),
            str(direction),
            list(bin_names),
            w_from,
            w_to,
        )

    out: Dict[str, Tuple[int, Decimal]] = {}
    for r in rows:
        bn = str(r["bin_name"])
        trades = int(r["trades"] or 0)
        wins = int(r["wins"] or 0)

        if trades > 0:
            winrate = (Decimal(wins) / Decimal(trades))
        else:
            winrate = Decimal("0")

        out[bn] = (trades, winrate)

    return out


# 🔸 Обновление consensus пачкой (executemany)
async def _update_consensus_batch(pg, rows: List[Tuple[Any, ...]]) -> Tuple[int, int]:
    # условий достаточности
    if not rows:
        return 0, 0

    async with pg.acquire() as conn:
        await conn.executemany(
            f"""
            UPDATE {LABELS_V2_TABLE}
            SET consensus = $1,
                updated_at = now()
            WHERE run_id = $2
              AND scenario_id = $3
              AND signal_id = $4
              AND direction = $5
              AND score_version = $6
              AND analysis_id = $7
              AND indicator_param IS NOT DISTINCT FROM $8
              AND timeframe = $9
              AND bin_name = $10
            """,
            rows,
        )

    # rows уже содержит финальный consensus; посчитаем ok/bad для логов
    ok = 0
    bad = 0
    for r in rows:
        if bool(r[0]):
            ok += 1
        else:
            bad += 1
    return ok, bad


# 🔸 Публикация события готовности stabproc v2 (состав как у bt:analysis:postproc_ready_v2)
async def _publish_stabproc_ready(redis, ctx: Dict[str, Any]) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            STABPROC_READY_STREAM_KEY,
            {
                "scenario_id": str(int(ctx["scenario_id"])),
                "signal_id": str(int(ctx["signal_id"])),
                "run_id": str(int(ctx["run_id"])),

                "candidates": str(ctx.get("candidates") or "0"),
                "score_upserts": str(ctx.get("score_upserts") or "0"),
                "labels_rows": str(ctx.get("labels_rows") or "0"),

                "winner_analysis_id": str(ctx.get("winner_analysis_id") or "0"),
                "winner_param": str(ctx.get("winner_param") or ""),
                "score_version": str(ctx.get("score_version") or "v1"),

                # finished_at — время окончания stabproc
                "finished_at": finished_at.isoformat(),

                # source_finished_at — время окончания postproc (как "источник" для этого шага)
                "source_finished_at": ctx.get("finished_at").isoformat() if ctx.get("finished_at") else "",
            },
        )
        log.debug(
            "BT_ANALYSIS_STABPROC_V2: published %s scenario_id=%s signal_id=%s run_id=%s score_version=%s",
            STABPROC_READY_STREAM_KEY,
            ctx.get("scenario_id"),
            ctx.get("signal_id"),
            ctx.get("run_id"),
            ctx.get("score_version"),
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_STABPROC_V2: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s run_id=%s: %s",
            STABPROC_READY_STREAM_KEY,
            ctx.get("scenario_id"),
            ctx.get("signal_id"),
            ctx.get("run_id"),
            e,
            exc_info=True,
        )