# bt_complex_score.py — воркер оценки комплексных анализаторов (по бин-статистике)

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стрима и параметров
COMPLEX_READY_STREAM_KEY = "bt:complex:ready"
SCORE_CONSUMER_GROUP = "bt_complex_score"
SCORE_CONSUMER_NAME = "bt_complex_score_main"

SCORE_STREAM_BATCH_SIZE = 10
SCORE_STREAM_BLOCK_MS = 5000

SCORE_MAX_CONCURRENCY = 8

# минимальный uplift по winrate, чтобы считать бин "достаточно хорошим"
MIN_WINRATE_UPLIFT = Decimal("0.01")

# кеш последних finished_at по (scenario_id, signal_id) для отсечки дублей
_last_complex_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_COMPLEX_SCORE")


# 🔸 Публичная точка входа: оркестратор оценки комплексов
async def run_bt_complex_score_orchestrator(pg, redis):
    log.debug("BT_COMPLEX_SCORE: оркестратор оценки комплексов запущен")

    await _ensure_consumer_group(redis)

    score_sema = asyncio.Semaphore(SCORE_MAX_CONCURRENCY)

    while True:
        try:
            messages = await _read_from_stream(redis)

            if not messages:
                continue

            total_msgs = 0
            tasks: List[asyncio.Task] = []

            for stream_key, entries in messages:
                if stream_key != COMPLEX_READY_STREAM_KEY:
                    continue

                for entry_id, fields in entries:
                    total_msgs += 1
                    task = asyncio.create_task(
                        _process_message(
                            entry_id=entry_id,
                            fields=fields,
                            pg=pg,
                            redis=redis,
                            sema=score_sema,
                        ),
                        name=f"BT_COMPLEX_SCORE_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))

                log.info(
                    "BT_COMPLEX_SCORE: обработан пакет сообщений из bt:complex:ready — "
                    "сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_COMPLEX_SCORE: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:complex:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=COMPLEX_READY_STREAM_KEY,
            groupname=SCORE_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_COMPLEX_SCORE: создана consumer group '%s' для стрима '%s'",
            SCORE_CONSUMER_GROUP,
            COMPLEX_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_COMPLEX_SCORE: consumer group '%s' для стрима '%s' уже существует",
                SCORE_CONSUMER_GROUP,
                COMPLEX_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_COMPLEX_SCORE: ошибка при создании consumer group '%s': %s",
                SCORE_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:complex:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=SCORE_CONSUMER_GROUP,
        consumername=SCORE_CONSUMER_NAME,
        streams={COMPLEX_READY_STREAM_KEY: ">"},
        count=SCORE_STREAM_BATCH_SIZE,
        block=SCORE_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:complex:ready
def _parse_complex_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
        }
    except Exception as e:
        log.error(
            "BT_COMPLEX_SCORE: ошибка разбора сообщения стрима bt:complex:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения с ограничением по семафору
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        ctx = _parse_complex_ready_message(fields)
        if not ctx:
            await redis.xack(COMPLEX_READY_STREAM_KEY, SCORE_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        finished_at = ctx["finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_complex_finished_at.get(pair_key)

        if last_finished is not None and last_finished == finished_at:
            log.debug(
                "BT_COMPLEX_SCORE: дубликат сообщения для scenario_id=%s, signal_id=%s, "
                "finished_at=%s, stream_id=%s — оценка комплексов не выполняется",
                scenario_id,
                signal_id,
                finished_at,
                entry_id,
            )
            await redis.xack(COMPLEX_READY_STREAM_KEY, SCORE_CONSUMER_GROUP, entry_id)
            return

        _last_complex_finished_at[pair_key] = finished_at

        log.debug(
            "BT_COMPLEX_SCORE: получено сообщение готовности комплексного анализа "
            "scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s",
            scenario_id,
            signal_id,
            finished_at,
            entry_id,
        )

        try:
            result = await _process_pair_complex_score(pg, scenario_id, signal_id)

            log.info(
                "BT_COMPLEX_SCORE: оценка комплексов завершена для scenario_id=%s, signal_id=%s — "
                "строк_вставлено=%s, комплексов_оценено=%s",
                scenario_id,
                signal_id,
                result.get("rows_inserted", 0),
                result.get("instances_evaluated", 0),
            )

        except Exception as e:
            log.error(
                "BT_COMPLEX_SCORE: ошибка оценки комплексов для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(COMPLEX_READY_STREAM_KEY, SCORE_CONSUMER_GROUP, entry_id)


# 🔸 Основная логика оценки комплексов для одной пары (scenario_id, signal_id)
async def _process_pair_complex_score(
    pg,
    scenario_id: int,
    signal_id: int,
) -> Dict[str, Any]:
    # загружаем базовую статистику после обычного фильтра (analysis_postproc) по направлениям
    base_stats = await _load_base_stats_for_pair(pg, scenario_id, signal_id)
    if not base_stats:
        log.debug(
            "BT_COMPLEX_SCORE: нет базовой статистики в bt_analysis_scenario_stat "
            "для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        # очищаем старые записи на всякий случай
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_complex_instance_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
        return {
            "rows_inserted": 0,
            "instances_evaluated": 0,
        }

    # депозит сценария нужен для расчёта ROI
    deposit = await _load_scenario_deposit(pg, scenario_id)

    # загружаем бин-статистику комплексов
    bins = await _load_complex_bins_for_pair(pg, scenario_id, signal_id)
    if not bins:
        log.debug(
            "BT_COMPLEX_SCORE: нет строк в bt_complex_bins_stat для scenario_id=%s, signal_id=%s",
            scenario_id,
            signal_id,
        )
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_complex_instance_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
        return {
            "rows_inserted": 0,
            "instances_evaluated": 0,
        }

    # агрегатор по ключу (complex_id, indicator_param, direction, timeframe)
    agg: Dict[Tuple[int, str, str, str], Dict[str, Any]] = {}

    # перебираем все бинны комплексов
    for b in bins:
        complex_id = b["complex_id"]
        indicator_param = b["indicator_param"] or ""
        timeframe = b["timeframe"]
        direction = b["direction"]
        trades = int(b["trades"])
        pnl_abs = _safe_decimal(b["pnl_abs"])
        winrate = _safe_decimal(b["winrate"])

        # базовая статистика по направлению
        base = base_stats.get(direction)
        if not base:
            continue

        # uplift winrate на уровне бина относительно общего filt_winrate
        bin_winrate_uplift = winrate - base["filt_winrate"]

        if bin_winrate_uplift < MIN_WINRATE_UPLIFT:
            # бин не проходит порог
            continue

        key = (complex_id, indicator_param, direction, timeframe)
        cur = agg.get(key)
        if not cur:
            cur = {
                "complex_id": complex_id,
                "indicator_param": indicator_param,
                "direction": direction,
                "timeframe": timeframe,
                # накопительные значения
                "complex_trades": 0,
                "complex_pnl_abs": Decimal("0"),
                "winrate_weighted_sum": Decimal("0"),
            }
            agg[key] = cur

        cur["complex_trades"] += trades
        cur["complex_pnl_abs"] += pnl_abs
        # для общего winrate делаем взвешенное среднее
        cur["winrate_weighted_sum"] += winrate * trades

    if not agg:
        log.debug(
            "BT_COMPLEX_SCORE: ни один бин не прошёл порог winrate_uplift=%s "
            "для scenario_id=%s, signal_id=%s",
            MIN_WINRATE_UPLIFT,
            scenario_id,
            signal_id,
        )
        async with pg.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM bt_complex_instance_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                """,
                scenario_id,
                signal_id,
            )
        return {
            "rows_inserted": 0,
            "instances_evaluated": 0,
        }

    # готовим строки для вставки в bt_complex_instance_stat
    to_insert: List[Tuple[Any, ...]] = []
    instances_evaluated = 0

    for (complex_id, indicator_param, direction, timeframe), cur in agg.items():
        base = base_stats.get(direction)
        if not base:
            continue

        filt_trades = base["filt_trades"]
        filt_pnl_abs = base["filt_pnl_abs"]
        filt_winrate = base["filt_winrate"]
        filt_roi = base["filt_roi"]

        complex_trades = cur["complex_trades"]
        complex_pnl_abs = cur["complex_pnl_abs"]

        if complex_trades <= 0 or filt_trades <= 0:
            # бессмысленно оценивать, если нет сделок
            continue

        # coverage: доля сделок, которые выжили
        coverage = _safe_div(complex_trades, filt_trades)

        # общий winrate по комплексным сделкам (взвешенное среднее по биннам)
        winrate_weighted_sum = cur["winrate_weighted_sum"]
        complex_winrate = _safe_div(winrate_weighted_sum, complex_trades)

        # ROI
        if deposit and deposit > 0:
            try:
                complex_roi = complex_pnl_abs / deposit
            except (InvalidOperation, ZeroDivisionError):
                complex_roi = Decimal("0")
        else:
            complex_roi = Decimal("0")

        # uplift'ы
        pnl_uplift = complex_pnl_abs - filt_pnl_abs
        winrate_uplift = complex_winrate - filt_winrate
        roi_uplift = complex_roi - filt_roi

        # param_score пока не считаем, ставим 0
        param_score = Decimal("0")

        # is_winner пока не определяем
        is_winner = False

        to_insert.append(
            (
                complex_id,
                scenario_id,
                signal_id,
                indicator_param,
                direction,
                timeframe,
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                filt_roi,
                complex_trades,
                complex_pnl_abs,
                complex_winrate,
                complex_roi,
                coverage,
                pnl_uplift,
                winrate_uplift,
                roi_uplift,
                param_score,
                is_winner,
                None,  # raw_stat
            )
        )
        instances_evaluated += 1

    rows_inserted = 0

    async with pg.acquire() as conn:
        # очищаем старые записи для пары
        await conn.execute(
            """
            DELETE FROM bt_complex_instance_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

        if to_insert:
            await conn.executemany(
                """
                INSERT INTO bt_complex_instance_stat (
                    complex_id,
                    scenario_id,
                    signal_id,
                    indicator_param,
                    direction,
                    timeframe,
                    filt_trades,
                    filt_pnl_abs,
                    filt_winrate,
                    filt_roi,
                    complex_trades,
                    complex_pnl_abs,
                    complex_winrate,
                    complex_roi,
                    coverage,
                    pnl_uplift,
                    winrate_uplift,
                    roi_uplift,
                    param_score,
                    is_winner,
                    raw_stat
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16, $17, $18,
                    $19, $20, $21
                )
                """,
                to_insert,
            )
            rows_inserted = len(to_insert)

    log.debug(
        "BT_COMPLEX_SCORE: для scenario_id=%s, signal_id=%s записано строк в bt_complex_instance_stat=%s "
        "(комплексов_оценено=%s)",
        scenario_id,
        signal_id,
        rows_inserted,
        instances_evaluated,
    )

    return {
        "rows_inserted": rows_inserted,
        "instances_evaluated": instances_evaluated,
    }


# 🔸 Загрузка базовой статистики (после обычного анализа) из bt_analysis_scenario_stat
async def _load_base_stats_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> Dict[str, Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                direction,
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                filt_roi
            FROM bt_analysis_scenario_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    stats: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        direction = r["direction"]
        stats[direction] = {
            "filt_trades": int(r["filt_trades"]),
            "filt_pnl_abs": _safe_decimal(r["filt_pnl_abs"]),
            "filt_winrate": _safe_decimal(r["filt_winrate"]),
            "filt_roi": _safe_decimal(r["filt_roi"]),
        }

    return stats


# 🔸 Загрузка бин-статистики комплексов из bt_complex_bins_stat
async def _load_complex_bins_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                complex_id,
                indicator_param,
                timeframe,
                direction,
                bin_name,
                trades,
                pnl_abs,
                winrate
            FROM bt_complex_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            """,
            scenario_id,
            signal_id,
        )

    bins: List[Dict[str, Any]] = []
    for r in rows:
        bins.append(
            {
                "complex_id": r["complex_id"],
                "indicator_param": r["indicator_param"],
                "timeframe": r["timeframe"],
                "direction": r["direction"],
                "bin_name": r["bin_name"],
                "trades": int(r["trades"]),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "winrate": _safe_decimal(r["winrate"]),
            }
        )

    return bins


# 🔸 Загрузка депозита сценария из bt_scenario_parameters (param_name='deposit')
async def _load_scenario_deposit(
    pg,
    scenario_id: int,
) -> Optional[Decimal]:
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
        log.debug(
            "BT_COMPLEX_SCORE: депозит для scenario_id=%s не найден в bt_scenario_parameters",
            scenario_id,
        )
        return None

    value = row["param_value"]
    dep = _safe_decimal(value)
    if dep <= 0:
        log.debug(
            "BT_COMPLEX_SCORE: депозит для scenario_id=%s некорректен или неположителен: %s",
            scenario_id,
            dep,
        )
        return None

    return dep


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: безопасное деление
def _safe_div(a: Any, b: Any) -> Decimal:
    try:
        a_dec = _safe_decimal(a)
        b_dec = _safe_decimal(b)
        if b_dec == 0:
            return Decimal("0")
        return a_dec / b_dec
    except (InvalidOperation, ZeroDivisionError):
        return Decimal("0")