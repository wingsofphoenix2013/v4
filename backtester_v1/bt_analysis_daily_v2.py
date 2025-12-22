# bt_analysis_daily_v2.py — суточная агрегация статистики original/filtered по результатам финального postproc v2

import asyncio
import logging
from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек воркера v2
POSTPROC_READY_STREAM_KEY_V2 = "bt:analysis:postproc_ready_v2"

DAILY_CONSUMER_GROUP_V2 = "bt_analysis_daily_v2"
DAILY_CONSUMER_NAME_V2 = "bt_analysis_daily_v2_main"

DAILY_STREAM_BATCH_SIZE = 10
DAILY_STREAM_BLOCK_MS = 5000

DAILY_MAX_CONCURRENCY = 16

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_daily_source_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_DAILY_V2")


# 🔸 Публичная точка входа: оркестратор суточной статистики v2
async def run_bt_analysis_daily_v2_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_DAILY_V2: оркестратор запущен")

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
                if stream_key != POSTPROC_READY_STREAM_KEY_V2:
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
                        name=f"BT_ANALYSIS_DAILY_V2_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.info(
                    "BT_ANALYSIS_DAILY_V2: обработан пакет сообщений — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error("BT_ANALYSIS_DAILY_V2: ошибка в основном цикле: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:postproc_ready_v2
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=POSTPROC_READY_STREAM_KEY_V2,
            groupname=DAILY_CONSUMER_GROUP_V2,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_DAILY_V2: создана consumer group '%s' для стрима '%s'",
            DAILY_CONSUMER_GROUP_V2,
            POSTPROC_READY_STREAM_KEY_V2,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_DAILY_V2: consumer group '%s' для стрима '%s' уже существует",
                DAILY_CONSUMER_GROUP_V2,
                POSTPROC_READY_STREAM_KEY_V2,
            )
        else:
            log.error(
                "BT_ANALYSIS_DAILY_V2: ошибка при создании consumer group '%s': %s",
                DAILY_CONSUMER_GROUP_V2,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:postproc_ready_v2
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=DAILY_CONSUMER_GROUP_V2,
        consumername=DAILY_CONSUMER_NAME_V2,
        streams={POSTPROC_READY_STREAM_KEY_V2: ">"},
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:postproc_ready_v2
def _parse_postproc_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")

        if not (scenario_id_str and signal_id_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)

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
            "source_finished_at": source_finished_at,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error("BT_ANALYSIS_DAILY_V2: ошибка разбора сообщения: %s, fields=%s", e, fields, exc_info=True)
        return None


# 🔸 Обработка одного сообщения из bt:analysis:postproc_ready_v2
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
            await redis.xack(POSTPROC_READY_STREAM_KEY_V2, DAILY_CONSUMER_GROUP_V2, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        source_finished_at = ctx.get("source_finished_at")
        finished_at = ctx.get("finished_at")

        dedup_ts = source_finished_at or finished_at or datetime.utcnow()
        pair_key = (scenario_id, signal_id)

        last_finished = _last_daily_source_finished_at.get(pair_key)
        if last_finished is not None and last_finished == dedup_ts:
            log.debug(
                "BT_ANALYSIS_DAILY_V2: дубликат scenario_id=%s signal_id=%s dedup_ts=%s — пропуск",
                scenario_id,
                signal_id,
                dedup_ts,
            )
            await redis.xack(POSTPROC_READY_STREAM_KEY_V2, DAILY_CONSUMER_GROUP_V2, entry_id)
            return

        _last_daily_source_finished_at[pair_key] = dedup_ts
        started_at = datetime.utcnow()

        try:
            deposit = await _load_scenario_deposit(pg, scenario_id)

            result = await _rebuild_daily_for_pair_v2(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                deposit=deposit,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            log.info(
                "BT_ANALYSIS_DAILY_V2: пересчёт завершён — scenario_id=%s signal_id=%s deposit=%s days=%s rows=%s "
                "orig_trades=%s filt_trades=%s removed_trades=%s elapsed_ms=%s",
                scenario_id,
                signal_id,
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
                "BT_ANALYSIS_DAILY_V2: ошибка расчёта scenario_id=%s signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(POSTPROC_READY_STREAM_KEY_V2, DAILY_CONSUMER_GROUP_V2, entry_id)


# 🔸 Пересборка суточной статистики v2 для пары (scenario_id, signal_id) по exit_time::date (UTC)
async def _rebuild_daily_for_pair_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    # orig по дням
    orig_rows = await _load_orig_daily_rows(pg, scenario_id, signal_id)

    # filt по дням (по контейнеру v2)
    filt_rows = await _load_filt_daily_rows_v2(pg, scenario_id, signal_id)

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

        # квантизация
        rows_to_insert.append(
            (
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

    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                DELETE FROM bt_analysis_scenario_daily_v2
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )

            if rows_to_insert:
                await conn.executemany(
                    """
                    INSERT INTO bt_analysis_scenario_daily_v2 (
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
                        removed_accuracy
                    )
                    VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, $11, $12,
                        $13
                    )
                    """,
                    rows_to_insert,
                )

    days_count = len({r[2] for r in rows_to_insert})
    return {
        "days": days_count,
        "rows_inserted": len(rows_to_insert),
        "orig_trades_total": orig_trades_total,
        "filt_trades_total": filt_trades_total,
        "removed_trades_total": removed_trades_total,
    }


# 🔸 Загрузка orig-агрегаций по дням из bt_scenario_positions
async def _load_orig_daily_rows(pg, scenario_id: int, signal_id: int) -> List[Dict[str, Any]]:
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
              AND postproc    = true
            GROUP BY (exit_time::date), direction
            ORDER BY (exit_time::date), direction
            """,
            scenario_id,
            signal_id,
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


# 🔸 Загрузка filt-агрегаций по дням из bt_scenario_positions + bt_analysis_positions_postproc_v2
async def _load_filt_daily_rows_v2(pg, scenario_id: int, signal_id: int) -> List[Dict[str, Any]]:
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
            JOIN bt_analysis_positions_postproc_v2 pp
              ON pp.position_uid = p.position_uid
             AND pp.scenario_id  = p.scenario_id
             AND pp.signal_id    = p.signal_id
            WHERE p.scenario_id = $1
              AND p.signal_id   = $2
              AND p.postproc    = true
            GROUP BY (p.exit_time::date), p.direction
            ORDER BY (p.exit_time::date), p.direction
            """,
            scenario_id,
            signal_id,
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