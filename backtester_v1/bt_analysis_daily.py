# bt_analysis_daily.py — суточная агрегация статистики original/filtered по результатам финального postproc (run-aware, строго по окну run)

import asyncio
import logging
from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек воркера v1
POSTPROC_READY_STREAM_KEY = "bt:analysis:postproc_ready"

DAILY_CONSUMER_GROUP = "bt_analysis_daily"
DAILY_CONSUMER_NAME = "bt_analysis_daily_main"

DAILY_STREAM_BATCH_SIZE = 10
DAILY_STREAM_BLOCK_MS = 5000

DAILY_MAX_CONCURRENCY = 16

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id, run_id) для отсечки дублей
_last_daily_source_finished_at: Dict[Tuple[int, int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_DAILY")


# 🔸 Публичная точка входа: оркестратор суточной статистики v1
async def run_bt_analysis_daily_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_DAILY: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма по парам
    sema = asyncio.Semaphore(DAILY_MAX_CONCURRENCY)

    while True:
        try:
            entries = await _read_from_stream(redis)
            if not entries:
                continue

            tasks: List[asyncio.Task] = []
            total_msgs = 0

            for stream_key, messages in entries:
                if stream_key != POSTPROC_READY_STREAM_KEY:
                    continue

                for entry_id, fields in messages:
                    total_msgs += 1
                    task = asyncio.create_task(
                        _process_message(
                            entry_id=entry_id,
                            fields=fields,
                            pg=pg,
                            redis=redis,
                            sema=sema,
                        ),
                        name=f"BT_ANALYSIS_DAILY_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.debug(
                    "BT_ANALYSIS_DAILY: обработан пакет сообщений — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error("BT_ANALYSIS_DAILY: ошибка в основном цикле: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:postproc_ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_READY_STREAM_KEY,
            groupname=DAILY_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_DAILY: создана consumer group '%s' для стрима '%s'",
            DAILY_CONSUMER_GROUP,
            POSTPROC_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_DAILY: consumer group '%s' для стрима '%s' уже существует",
                DAILY_CONSUMER_GROUP,
                POSTPROC_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_DAILY: ошибка при создании consumer group '%s': %s",
                DAILY_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:postproc_ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=DAILY_CONSUMER_GROUP,
        consumername=DAILY_CONSUMER_NAME,
        streams={POSTPROC_READY_STREAM_KEY: ">"},
        count=DAILY_STREAM_BATCH_SIZE,
        block=DAILY_STREAM_BLOCK_MS,
    )

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


# 🔸 Разбор одного сообщения из стрима bt:analysis:postproc_ready (run-aware)
def _parse_postproc_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        run_id_str = fields.get("run_id")
        window_from_str = fields.get("window_from")
        window_to_str = fields.get("window_to")

        if not (scenario_id_str and signal_id_str and run_id_str and window_from_str and window_to_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        run_id = int(run_id_str)

        window_from = datetime.fromisoformat(window_from_str)
        window_to = datetime.fromisoformat(window_to_str)

        source_finished_at = None
        source_finished_at_str = fields.get("source_finished_at") or ""
        if source_finished_at_str:
            try:
                source_finished_at = datetime.fromisoformat(source_finished_at_str)
            except Exception:
                source_finished_at = None

        finished_at = None
        finished_at_str = fields.get("finished_at") or ""
        if finished_at_str:
            try:
                finished_at = datetime.fromisoformat(finished_at_str)
            except Exception:
                finished_at = None

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "run_id": run_id,
            "window_from": window_from,
            "window_to": window_to,
            "source_finished_at": source_finished_at,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error("BT_ANALYSIS_DAILY: ошибка разбора сообщения: %s, fields=%s", e, fields, exc_info=True)
        return None


# 🔸 Обработка одного сообщения из bt:analysis:postproc_ready
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        ctx = _parse_postproc_ready_message(fields)
        if not ctx:
            await redis.xack(POSTPROC_READY_STREAM_KEY, DAILY_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        run_id = ctx["run_id"]
        window_from = ctx["window_from"]
        window_to = ctx["window_to"]

        source_finished_at = ctx.get("source_finished_at")
        finished_at = ctx.get("finished_at")

        dedup_ts = source_finished_at or finished_at or datetime.utcnow()
        pair_key = (scenario_id, signal_id, run_id)

        last_finished = _last_daily_source_finished_at.get(pair_key)
        if last_finished is not None and last_finished == dedup_ts:
            log.debug(
                "BT_ANALYSIS_DAILY: дубликат scenario_id=%s signal_id=%s run_id=%s dedup_ts=%s — пропуск",
                scenario_id,
                signal_id,
                run_id,
                dedup_ts,
            )
            await redis.xack(POSTPROC_READY_STREAM_KEY, DAILY_CONSUMER_GROUP, entry_id)
            return

        _last_daily_source_finished_at[pair_key] = dedup_ts
        started_at = datetime.utcnow()

        try:
            deposit = await _load_scenario_deposit(pg, scenario_id)

            result = await _rebuild_daily_for_pair(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                run_id=run_id,
                window_from=window_from,
                window_to=window_to,
                deposit=deposit,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            log.info(
                "BT_ANALYSIS_DAILY: done — scenario_id=%s signal_id=%s run_id=%s window=[%s..%s] deposit=%s days=%s rows=%s "
                "orig_trades=%s filt_trades=%s removed_trades=%s elapsed_ms=%s",
                scenario_id,
                signal_id,
                run_id,
                window_from,
                window_to,
                str(deposit) if deposit is not None else None,
                result.get("days", 0),
                result.get("rows_inserted", 0),
                result.get("orig_trades_total", 0),
                result.get("filt_trades_total", 0),
                result.get("removed_trades_total", 0),
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_DAILY: ошибка расчёта scenario_id=%s signal_id=%s run_id=%s: %s",
                scenario_id,
                signal_id,
                run_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(POSTPROC_READY_STREAM_KEY, DAILY_CONSUMER_GROUP, entry_id)


# 🔸 Пересборка суточной статистики v1 для пары (scenario_id, signal_id) и run_id, по exit_time::date (UTC)
async def _rebuild_daily_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    window_from: datetime,
    window_to: datetime,
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    # orig по дням (строго окно run по entry_time; day = exit_time::date)
    orig_rows = await _load_orig_daily_rows(pg, scenario_id, signal_id, window_from, window_to)

    # filt по дням (run-aware контейнер)
    filt_rows = await _load_filt_daily_rows(pg, scenario_id, signal_id, run_id, window_from, window_to)

    filt_map: Dict[Tuple[date, str], Dict[str, Any]] = {}
    for r in filt_rows:
        filt_map[(r["day"], r["direction"])] = r

    rows_to_insert: List[Tuple[Any, ...]] = []

    orig_trades_total = 0
    filt_trades_total = 0
    removed_trades_total = 0

    for o in orig_rows:
        day = o["day"]
        direction = o["direction"]

        orig_trades = int(o["orig_trades"])
        orig_pnl_abs = _safe_decimal(o["orig_pnl_abs"])
        orig_wins = int(o["orig_wins"])

        orig_trades_total += orig_trades

        # winrate orig
        if orig_trades > 0:
            orig_winrate = Decimal(orig_wins) / Decimal(orig_trades)
        else:
            orig_winrate = Decimal("0")

        # roi orig
        if deposit and deposit > 0:
            try:
                orig_roi = orig_pnl_abs / deposit
            except (InvalidOperation, ZeroDivisionError):
                orig_roi = Decimal("0")
        else:
            orig_roi = Decimal("0")

        f = filt_map.get((day, direction)) or {}

        filt_trades = int(f.get("filt_trades", 0) or 0)
        filt_pnl_abs = _safe_decimal(f.get("filt_pnl_abs", 0))
        filt_wins = int(f.get("filt_wins", 0) or 0)

        removed_trades = int(f.get("removed_trades", 0) or 0)
        removed_losers = int(f.get("removed_losers", 0) or 0)

        filt_trades_total += filt_trades
        removed_trades_total += removed_trades

        # winrate filt
        if filt_trades > 0:
            filt_winrate = Decimal(filt_wins) / Decimal(filt_trades)
        else:
            filt_winrate = Decimal("0")

        # roi filt
        if deposit and deposit > 0:
            try:
                filt_roi = filt_pnl_abs / deposit
            except (InvalidOperation, ZeroDivisionError):
                filt_roi = Decimal("0")
        else:
            filt_roi = Decimal("0")

        # removed_accuracy
        if removed_trades > 0:
            removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
        else:
            removed_accuracy = Decimal("0")

        rows_to_insert.append(
            (
                run_id,
                scenario_id,
                signal_id,
                day,
                direction,
                orig_trades,
                _q_decimal(orig_pnl_abs),
                _q_decimal(orig_winrate),
                _q_decimal(orig_roi),
                filt_trades,
                _q_decimal(filt_pnl_abs),
                _q_decimal(filt_winrate),
                _q_decimal(filt_roi),
                _q_decimal(removed_accuracy),
            )
        )

    if rows_to_insert:
        async with pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bt_analysis_scenario_daily (
                    run_id,
                    scenario_id,
                    signal_id,
                    day,
                    direction,
                    orig_trades,
                    orig_pnl_abs,
                    orig_winrate,
                    orig_roi,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    filt_roi,
                    removed_accuracy,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13,
                    $14,
                    now()
                )
                ON CONFLICT (run_id, scenario_id, signal_id, day, direction)
                DO UPDATE SET
                    orig_trades      = EXCLUDED.orig_trades,
                    orig_pnl_abs     = EXCLUDED.orig_pnl_abs,
                    orig_winrate     = EXCLUDED.orig_winrate,
                    orig_roi         = EXCLUDED.orig_roi,
                    filt_trades      = EXCLUDED.filt_trades,
                    filt_pnl_abs     = EXCLUDED.filt_pnl_abs,
                    filt_winrate     = EXCLUDED.filt_winrate,
                    filt_roi         = EXCLUDED.filt_roi,
                    removed_accuracy = EXCLUDED.removed_accuracy,
                    updated_at       = now()
                """,
                rows_to_insert,
            )

    days_count = len({r[3] for r in rows_to_insert})
    return {
        "days": days_count,
        "rows_inserted": len(rows_to_insert),
        "orig_trades_total": orig_trades_total,
        "filt_trades_total": filt_trades_total,
        "removed_trades_total": removed_trades_total,
    }


# 🔸 Загрузка orig-агрегаций по дням из bt_scenario_positions (strict: closed+postproc=true + entry_time in window; day=exit_time::date)
async def _load_orig_daily_rows(
    pg,
    scenario_id: int,
    signal_id: int,
    window_from: datetime,
    window_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                (exit_time::date) AS day,
                direction,
                COUNT(*)                                          AS orig_trades,
                COALESCE(SUM(pnl_abs), 0)                         AS orig_pnl_abs,
                COUNT(*) FILTER (WHERE pnl_abs > 0)               AS orig_wins
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND status      = 'closed'
              AND postproc    = true
              AND entry_time BETWEEN $3 AND $4
            GROUP BY (exit_time::date), direction
            ORDER BY (exit_time::date), direction
            """,
            scenario_id,
            signal_id,
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "day": r["day"],
                "direction": str(r["direction"]).strip().lower(),
                "orig_trades": int(r["orig_trades"]),
                "orig_pnl_abs": _safe_decimal(r["orig_pnl_abs"]),
                "orig_wins": int(r["orig_wins"]),
            }
        )
    return out


# 🔸 Загрузка filt-агрегаций по дням (run-aware): bt_scenario_positions + bt_analysis_positions_postproc (run_id)
async def _load_filt_daily_rows(
    pg,
    scenario_id: int,
    signal_id: int,
    run_id: int,
    window_from: datetime,
    window_to: datetime,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                (p.exit_time::date) AS day,
                p.direction         AS direction,
                COUNT(*) FILTER (WHERE pp.good_state = true)                      AS filt_trades,
                COALESCE(SUM(p.pnl_abs) FILTER (WHERE pp.good_state = true), 0)   AS filt_pnl_abs,
                COUNT(*) FILTER (WHERE pp.good_state = true AND p.pnl_abs > 0)    AS filt_wins,
                COUNT(*) FILTER (WHERE pp.good_state = false)                     AS removed_trades,
                COUNT(*) FILTER (WHERE pp.good_state = false AND p.pnl_abs <= 0)  AS removed_losers
            FROM bt_scenario_positions p
            JOIN bt_analysis_positions_postproc pp
              ON pp.run_id       = $3
             AND pp.position_uid = p.position_uid
             AND pp.scenario_id  = p.scenario_id
             AND pp.signal_id    = p.signal_id
            WHERE p.scenario_id = $1
              AND p.signal_id   = $2
              AND p.status      = 'closed'
              AND p.postproc    = true
              AND p.entry_time BETWEEN $4 AND $5
            GROUP BY (p.exit_time::date), p.direction
            ORDER BY (p.exit_time::date), p.direction
            """,
            scenario_id,
            signal_id,
            run_id,
            window_from,
            window_to,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "day": r["day"],
                "direction": str(r["direction"]).strip().lower(),
                "filt_trades": int(r["filt_trades"]),
                "filt_pnl_abs": _safe_decimal(r["filt_pnl_abs"]),
                "filt_wins": int(r["filt_wins"]),
                "removed_trades": int(r["removed_trades"]),
                "removed_losers": int(r["removed_losers"]),
            }
        )
    return out


# 🔸 Загрузка депозита сценария
async def _load_scenario_deposit(pg, scenario_id: int) -> Optional[Decimal]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_scenario_parameters
            WHERE scenario_id = $1
              AND param_name  = 'deposit'
            LIMIT 1
            """,
            scenario_id,
        )

    if not row:
        return None

    dep = _safe_decimal(row["param_value"])
    if dep <= 0:
        return None
    return dep


# 🔸 Вспомогательная функция: безопасное Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков
def _q_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)