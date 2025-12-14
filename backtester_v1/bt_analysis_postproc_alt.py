# bt_analysis_postproc_alt.py — альтернативный постпроцессинг анализов (поиск оптимального MIN_WINRATE_THRESHOLD)

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек alt-постпроцессинга
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"

ALT_CONSUMER_GROUP = "bt_analysis_postproc_alt"
ALT_CONSUMER_NAME = "bt_analysis_postproc_alt_main"

ALT_STREAM_BATCH_SIZE = 10
ALT_STREAM_BLOCK_MS = 5000

ALT_MAX_CONCURRENCY = 6

# 🔸 Настройки расчёта оптимального порога
# eps нужен, потому что правило фильтрации строгое: winrate < threshold
EPS_THRESHOLD = Decimal("0.00000001")

# кеш последних finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_POSTPROC_ALT")


# 🔸 Публичная точка входа: оркестратор alt-постпроцессинга
async def run_bt_analysis_postproc_alt_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_POSTPROC_ALT: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма по парам (scenario_id, signal_id)
    sema = asyncio.Semaphore(ALT_MAX_CONCURRENCY)

    while True:
        try:
            entries = await _read_from_stream(redis)
            if not entries:
                continue

            tasks: List[asyncio.Task] = []
            total_msgs = 0

            for stream_key, messages in entries:
                if stream_key != ANALYSIS_READY_STREAM_KEY:
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
                        name=f"BT_ANALYSIS_POSTPROC_ALT_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.info(
                    "BT_ANALYSIS_POSTPROC_ALT: обработан пакет сообщений из bt:analysis:ready — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC_ALT: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=ALT_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_POSTPROC_ALT: создана consumer group '%s' для стрима '%s'",
            ALT_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_POSTPROC_ALT: consumer group '%s' для стрима '%s' уже существует",
                ALT_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_POSTPROC_ALT: ошибка при создании consumer group '%s': %s",
                ALT_CONSUMER_GROUP,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=ALT_CONSUMER_GROUP,
        consumername=ALT_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=ALT_STREAM_BATCH_SIZE,
        block=ALT_STREAM_BLOCK_MS,
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


# 🔸 Разбор одного сообщения из стрима bt:analysis:ready
def _parse_analysis_ready_message(fields: Dict[str, str]) -> Optional[Dict[str, Any]]:
    try:
        scenario_id_str = fields.get("scenario_id")
        signal_id_str = fields.get("signal_id")
        finished_at_str = fields.get("finished_at")

        if not (scenario_id_str and signal_id_str and finished_at_str):
            return None

        scenario_id = int(scenario_id_str)
        signal_id = int(signal_id_str)
        finished_at = datetime.fromisoformat(finished_at_str)

        analyses_total_str = fields.get("analyses_total") or "0"
        rows_bins_str = fields.get("rows_bins") or "0"

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "finished_at": finished_at,
            "analyses_total": int(analyses_total_str),
            "rows_bins": int(rows_bins_str),
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_POSTPROC_ALT: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения из bt:analysis:ready с ограничением семафором
async def _process_message(
    entry_id: str,
    fields: Dict[str, str],
    pg,
    redis,
    sema: asyncio.Semaphore,
) -> None:
    async with sema:
        ctx = _parse_analysis_ready_message(fields)
        if not ctx:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, ALT_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        finished_at = ctx["finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # отсечка дублей по равному finished_at
        if last_finished is not None and last_finished == finished_at:
            log.debug(
                "BT_ANALYSIS_POSTPROC_ALT: дубликат сообщения для scenario_id=%s, signal_id=%s, finished_at=%s, stream_id=%s — расчёт не выполняется",
                scenario_id,
                signal_id,
                finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, ALT_CONSUMER_GROUP, entry_id)
            return

        _last_analysis_finished_at[pair_key] = finished_at

        started_at = datetime.utcnow()

        try:
            # собираем “подпись” набора анализаторов (для объяснимости результатов)
            analysis_ids = await _load_analysis_ids_for_pair(pg, scenario_id, signal_id)

            # депозит сценария (для ROI)
            deposit = await _load_scenario_deposit(pg, scenario_id)

            # считаем оптимум отдельно для long/short
            results: Dict[str, Dict[str, Any]] = {}
            for direction in ("long", "short"):
                # расчёт оптимального порога и метрик
                res = await _compute_best_threshold_for_direction(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    deposit=deposit,
                )
                results[direction] = res

                # запись результата в отдельную таблицу (upsert)
                await _upsert_threshold_opt_result(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    result=res,
                    analysis_ids=analysis_ids,
                    deposit=deposit,
                    source_finished_at=finished_at,
                )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            log.info(
                "BT_ANALYSIS_POSTPROC_ALT: scenario_id=%s, signal_id=%s — best_threshold: long=%s (filt_roi=%s, trades=%s), short=%s (filt_roi=%s, trades=%s), elapsed_ms=%s",
                scenario_id,
                signal_id,
                results.get("long", {}).get("best_threshold"),
                results.get("long", {}).get("filt_roi"),
                results.get("long", {}).get("filt_trades"),
                results.get("short", {}).get("best_threshold"),
                results.get("short", {}).get("filt_roi"),
                results.get("short", {}).get("filt_trades"),
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_POSTPROC_ALT: ошибка расчёта для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            # помечаем сообщение как обработанное в любом случае
            await redis.xack(ANALYSIS_READY_STREAM_KEY, ALT_CONSUMER_GROUP, entry_id)


# 🔸 Загрузка analysis_id, присутствующих в bins_stat по паре (scenario_id, signal_id)
async def _load_analysis_ids_for_pair(
    pg,
    scenario_id: int,
    signal_id: int,
) -> List[int]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT analysis_id
            FROM bt_analysis_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
            ORDER BY analysis_id
            """,
            scenario_id,
            signal_id,
        )
    return [int(r["analysis_id"]) for r in rows]


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
        return None

    dep = _safe_decimal(row["param_value"])
    if dep <= 0:
        return None

    return dep


# 🔸 Расчёт оптимального порога для одного направления (worst_winrate sweep)
async def _compute_best_threshold_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    # загружаем позиции направления + worst_winrate по позиции
    positions = await _load_positions_with_worst_winrate(pg, scenario_id, signal_id, direction)

    if not positions:
        # если позиций нет — пишем нули
        return {
            "best_threshold": Decimal("0"),
            "orig_trades": 0,
            "orig_pnl_abs": Decimal("0"),
            "orig_winrate": Decimal("0"),
            "orig_roi": Decimal("0"),
            "filt_trades": 0,
            "filt_pnl_abs": Decimal("0"),
            "filt_winrate": Decimal("0"),
            "filt_roi": Decimal("0"),
            "removed_trades": 0,
            "removed_accuracy": Decimal("0"),
            "candidates": 0,
        }

    # исходные агрегаты (до фильтрации)
    orig_trades = len(positions)
    orig_pnl_abs = sum((p["pnl_abs"] for p in positions), Decimal("0"))
    orig_wins = sum(1 for p in positions if p["pnl_abs"] > 0)
    if orig_trades > 0:
        orig_winrate = Decimal(orig_wins) / Decimal(orig_trades)
    else:
        orig_winrate = Decimal("0")

    if deposit and deposit > 0:
        try:
            orig_roi = orig_pnl_abs / deposit
        except (InvalidOperation, ZeroDivisionError):
            orig_roi = Decimal("0")
    else:
        orig_roi = Decimal("0")

    # группируем позиции по worst_winrate (только те, которые потенциально удаляемы)
    groups: Dict[Decimal, Dict[str, Any]] = {}
    removable_count = 0
    for p in positions:
        w = p["worst_winrate"]
        if w is None:
            continue
        removable_count += 1
        g = groups.setdefault(
            w,
            {"trades": 0, "pnl": Decimal("0"), "wins": 0, "losers": 0},
        )
        g["trades"] += 1
        g["pnl"] += p["pnl_abs"]
        if p["pnl_abs"] > 0:
            g["wins"] += 1
        if p["pnl_abs"] <= 0:
            g["losers"] += 1

    unique_worst = sorted(groups.keys())
    candidates = 1 + len(unique_worst)  # baseline + по каждому worst_winrate

    # стартовое состояние: ничего не удалено (threshold=0)
    best_threshold = Decimal("0")
    best_filt_trades = orig_trades
    best_filt_pnl = orig_pnl_abs
    best_filt_winrate = orig_winrate
    best_filt_roi = orig_roi
    best_removed_trades = 0
    best_removed_accuracy = Decimal("0")

    # целевая функция
    if deposit and deposit > 0:
        best_objective = best_filt_roi
        objective_mode = "roi"
    else:
        best_objective = best_filt_pnl
        objective_mode = "pnl_abs"

    removed_trades = 0
    removed_pnl = Decimal("0")
    removed_wins = 0
    removed_losers = 0

    # sweep: постепенно повышаем threshold и удаляем группы worst_winrate <= v
    for v in unique_worst:
        g = groups[v]

        removed_trades += int(g["trades"])
        removed_pnl += g["pnl"]
        removed_wins += int(g["wins"])
        removed_losers += int(g["losers"])

        filt_trades = orig_trades - removed_trades
        filt_pnl = orig_pnl_abs - removed_pnl
        filt_wins = orig_wins - removed_wins

        if filt_trades > 0:
            filt_winrate = Decimal(filt_wins) / Decimal(filt_trades)
        else:
            filt_winrate = Decimal("0")

        if deposit and deposit > 0:
            try:
                filt_roi = filt_pnl / deposit
            except (InvalidOperation, ZeroDivisionError):
                filt_roi = Decimal("0")
        else:
            filt_roi = Decimal("0")

        if removed_trades > 0:
            removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
        else:
            removed_accuracy = Decimal("0")

        threshold = v + EPS_THRESHOLD

        # выбираем objective
        if objective_mode == "roi":
            objective = filt_roi
        else:
            objective = filt_pnl

        # правило выбора лучшего:
        # 1) максимальный objective
        # 2) при равенстве — больше filt_trades (меньше агрессия)
        # 3) при равенстве — меньший threshold
        if objective > best_objective:
            best_objective = objective
            best_threshold = threshold
            best_filt_trades = filt_trades
            best_filt_pnl = filt_pnl
            best_filt_winrate = filt_winrate
            best_filt_roi = filt_roi
            best_removed_trades = removed_trades
            best_removed_accuracy = removed_accuracy
        elif objective == best_objective:
            if filt_trades > best_filt_trades:
                best_threshold = threshold
                best_filt_trades = filt_trades
                best_filt_pnl = filt_pnl
                best_filt_winrate = filt_winrate
                best_filt_roi = filt_roi
                best_removed_trades = removed_trades
                best_removed_accuracy = removed_accuracy
            elif filt_trades == best_filt_trades and threshold < best_threshold:
                best_threshold = threshold
                best_filt_trades = filt_trades
                best_filt_pnl = filt_pnl
                best_filt_winrate = filt_winrate
                best_filt_roi = filt_roi
                best_removed_trades = removed_trades
                best_removed_accuracy = removed_accuracy

    # лёгкая квантизация метрик для предсказуемости (как в остальных модулях)
    return {
        "best_threshold": _q_decimal(best_threshold),
        "orig_trades": int(orig_trades),
        "orig_pnl_abs": _q_decimal(orig_pnl_abs),
        "orig_winrate": _q_decimal(orig_winrate),
        "orig_roi": _q_decimal(orig_roi),
        "filt_trades": int(best_filt_trades),
        "filt_pnl_abs": _q_decimal(best_filt_pnl),
        "filt_winrate": _q_decimal(best_filt_winrate),
        "filt_roi": _q_decimal(best_filt_roi),
        "removed_trades": int(best_removed_trades),
        "removed_accuracy": _q_decimal(best_removed_accuracy),
        "candidates": int(candidates),
        "removable_positions": int(removable_count),
    }


# 🔸 Загрузка позиций с worst_winrate (MIN winrate по всем попаданиям позиции)
async def _load_positions_with_worst_winrate(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH pos AS (
                SELECT position_uid, pnl_abs
                FROM bt_scenario_positions
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND postproc    = true
                  AND direction   = $3
            ),
            worst AS (
                SELECT
                    r.position_uid,
                    MIN(b.winrate) AS worst_winrate
                FROM bt_analysis_positions_raw r
                JOIN bt_analysis_bins_stat b
                  ON b.analysis_id = r.analysis_id
                 AND b.scenario_id = r.scenario_id
                 AND b.signal_id   = r.signal_id
                 AND b.timeframe   = r.timeframe
                 AND b.direction   = r.direction
                 AND b.bin_name    = r.bin_name
                WHERE r.scenario_id = $1
                  AND r.signal_id   = $2
                  AND r.direction   = $3
                GROUP BY r.position_uid
            )
            SELECT
                p.position_uid,
                p.pnl_abs,
                w.worst_winrate
            FROM pos p
            LEFT JOIN worst w
              ON w.position_uid = p.position_uid
            ORDER BY w.worst_winrate NULLS LAST
            """,
            scenario_id,
            signal_id,
            direction,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "position_uid": r["position_uid"],
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "worst_winrate": _safe_decimal_or_none(r["worst_winrate"]),
            }
        )
    return out


# 🔸 Upsert результата оптимизации в bt_analysis_threshold_opt
async def _upsert_threshold_opt_result(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    result: Dict[str, Any],
    analysis_ids: List[int],
    deposit: Optional[Decimal],
    source_finished_at: datetime,
) -> None:
    # собираем meta для explainability
    meta_obj = {
        "version": 1,
        "method": "worst_winrate_sweep",
        "eps": str(EPS_THRESHOLD),
        "direction": direction,
        "deposit": str(deposit) if deposit is not None else None,
        "analysis_ids": analysis_ids,
        "candidates": result.get("candidates", 0),
        "removable_positions": result.get("removable_positions", 0),
    }
    meta_json = json.dumps(meta_obj, ensure_ascii=False)

    async with pg.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bt_analysis_threshold_opt (
                scenario_id,
                signal_id,
                direction,
                best_threshold,
                orig_trades,
                orig_pnl_abs,
                orig_winrate,
                orig_roi,
                filt_trades,
                filt_pnl_abs,
                filt_winrate,
                filt_roi,
                removed_trades,
                removed_accuracy,
                meta,
                source_finished_at
            )
            VALUES (
                $1, $2, $3,
                $4,
                $5, $6, $7, $8,
                $9, $10, $11, $12,
                $13, $14,
                $15::jsonb,
                $16
            )
            ON CONFLICT (scenario_id, signal_id, direction)
            DO UPDATE SET
                best_threshold     = EXCLUDED.best_threshold,
                orig_trades        = EXCLUDED.orig_trades,
                orig_pnl_abs       = EXCLUDED.orig_pnl_abs,
                orig_winrate       = EXCLUDED.orig_winrate,
                orig_roi           = EXCLUDED.orig_roi,
                filt_trades        = EXCLUDED.filt_trades,
                filt_pnl_abs       = EXCLUDED.filt_pnl_abs,
                filt_winrate       = EXCLUDED.filt_winrate,
                filt_roi           = EXCLUDED.filt_roi,
                removed_trades     = EXCLUDED.removed_trades,
                removed_accuracy   = EXCLUDED.removed_accuracy,
                meta               = EXCLUDED.meta,
                source_finished_at = EXCLUDED.source_finished_at,
                updated_at         = now()
            """,
            scenario_id,
            signal_id,
            direction,
            result["best_threshold"],
            result["orig_trades"],
            result["orig_pnl_abs"],
            result["orig_winrate"],
            result["orig_roi"],
            result["filt_trades"],
            result["filt_pnl_abs"],
            result["filt_winrate"],
            result["filt_roi"],
            result["removed_trades"],
            result["removed_accuracy"],
            meta_json,
            source_finished_at,
        )

    log.info(
        "BT_ANALYSIS_POSTPROC_ALT: upsert bt_analysis_threshold_opt — scenario_id=%s, signal_id=%s, direction=%s, best_threshold=%s, filt_roi=%s, filt_trades=%s, removed_accuracy=%s",
        scenario_id,
        signal_id,
        direction,
        result.get("best_threshold"),
        result.get("filt_roi"),
        result.get("filt_trades"),
        result.get("removed_accuracy"),
    )


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: Decimal или None (если value is None)
def _safe_decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков
def _q_decimal(value: Decimal) -> Decimal:
    # 4 знака после запятой для предсказуемости (как в других частях системы)
    return value.quantize(Decimal("0.0001"))