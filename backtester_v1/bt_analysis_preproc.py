# bt_analysis_preproc.py — препроцессинг анализов (оптимизация порога + состава анализаторов, разметка биннов и публикация bt:analysis:preproc_ready)

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

# 🔸 Константы стримов и настроек препроцессинга
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
PREPROC_READY_STREAM_KEY = "bt:analysis:preproc_ready"

PREPROC_CONSUMER_GROUP = "bt_analysis_preproc"
PREPROC_CONSUMER_NAME = "bt_analysis_preproc_main"

PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

PREPROC_MAX_CONCURRENCY = 6

# 🔸 Настройки оптимизации
EPS_THRESHOLD = Decimal("0.00000001")          # технический eps для строгих сравнений (по факту winrate уже квантован)
MAX_MODEL_ITERS = 20                           # максимум итераций удаления "вредных" анализаторов
MIN_ANALYZERS_LEFT = 1                         # не даём удалиться до 0 (можно менять позже)

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_PREPROC")


# 🔸 Публичная точка входа: оркестратор препроцессинга
async def run_bt_analysis_preproc_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_PREPROC: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма по парам (scenario_id, signal_id)
    sema = asyncio.Semaphore(PREPROC_MAX_CONCURRENCY)

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
                        name=f"BT_ANALYSIS_PREPROC_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.info(
                    "BT_ANALYSIS_PREPROC: обработан пакет сообщений из bt:analysis:ready — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=PREPROC_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC: создана consumer group '%s' для стрима '%s'",
            PREPROC_CONSUMER_GROUP,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC: consumer group '%s' для стрима '%s' уже существует",
                PREPROC_CONSUMER_GROUP,
                ANALYSIS_READY_STREAM_KEY,
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
    entries = await redis.xreadgroup(
        groupname=PREPROC_CONSUMER_GROUP,
        consumername=PREPROC_CONSUMER_NAME,
        streams={ANALYSIS_READY_STREAM_KEY: ">"},
        count=PREPROC_STREAM_BATCH_SIZE,
        block=PREPROC_STREAM_BLOCK_MS,
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
        source_finished_at = datetime.fromisoformat(finished_at_str)

        return {
            "scenario_id": scenario_id,
            "signal_id": signal_id,
            "source_finished_at": source_finished_at,
        }
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
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
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        source_finished_at = ctx["source_finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # отсечка дублей по равному source_finished_at
        if last_finished is not None and last_finished == source_finished_at:
            log.debug(
                "BT_ANALYSIS_PREPROC: дубликат сообщения для scenario_id=%s, signal_id=%s, source_finished_at=%s, stream_id=%s — расчёт не выполняется",
                scenario_id,
                signal_id,
                source_finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)
            return

        _last_analysis_finished_at[pair_key] = source_finished_at

        started_at = datetime.utcnow()

        try:
            # определяем направления сигнала
            direction_mask = await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # депозит сценария (для ROI)
            deposit = await _load_scenario_deposit(pg, scenario_id)

            # считаем модель отдельно по каждому направлению
            results: Dict[str, Dict[str, Any]] = {}

            for direction in directions:
                # кандидаты анализаторов по направлению
                initial_analysis_ids = await _load_analysis_ids_for_pair_direction(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                )

                # оптимизация состава + порога
                model_result = await _optimize_model_for_direction(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    deposit=deposit,
                    direction_mask=direction_mask,
                    initial_analysis_ids=initial_analysis_ids,
                    source_finished_at=source_finished_at,
                )
                results[direction] = model_result

            # чистим лишние направления (если ранее были записаны)
            other_dirs = [d for d in ("long", "short") if d not in directions]
            for d in other_dirs:
                await _delete_model_and_threshold_for_direction(pg, scenario_id, signal_id, d)

            # публикуем событие готовности препроцессинга
            await _publish_preproc_ready(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                source_finished_at=source_finished_at,
                direction_mask=direction_mask,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            # суммарный лог
            parts: List[str] = []
            for d in directions:
                r = results.get(d) or {}
                parts.append(
                    f"{d} thr={r.get('best_threshold')} roi={r.get('filt_roi')} trades={r.get('filt_trades')} "
                    f"analyses={r.get('selected_cnt')}/{r.get('initial_cnt')} removed_harmful={r.get('harmful_removed_cnt')}"
                )

            log.info(
                "BT_ANALYSIS_PREPROC: scenario_id=%s, signal_id=%s — direction_mask=%s, directions=%s, %s, source_finished_at=%s, elapsed_ms=%s",
                scenario_id,
                signal_id,
                direction_mask,
                directions,
                " | ".join(parts) if parts else "no_results",
                source_finished_at,
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC: ошибка расчёта для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP, entry_id)


# 🔸 Оптимизация модели для одного направления: состав анализаторов + порог + запись model_opt + bins_labels
async def _optimize_model_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    direction_mask: Optional[str],
    initial_analysis_ids: List[int],
    source_finished_at: datetime,
) -> Dict[str, Any]:
    selected_ids: List[int] = list(initial_analysis_ids)
    harmful_removed: List[Dict[str, Any]] = []

    # основной итеративный цикл
    last_threshold = None
    last_filt_roi = None

    for it in range(MAX_MODEL_ITERS):
        # если совсем нечего выбирать — всё равно считаем порог/метрики (будут 0/исходные)
        threshold_result = await _compute_best_threshold_for_direction(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            deposit=deposit,
            analysis_ids=selected_ids,
        )

        best_threshold = threshold_result["best_threshold"]
        filt_roi = threshold_result["filt_roi"]

        # условия стабильности (порог и ROI не меняются)
        if last_threshold is not None and last_filt_roi is not None:
            if best_threshold == last_threshold and filt_roi == last_filt_roi:
                # стабилизировались
                break
        last_threshold = best_threshold
        last_filt_roi = filt_roi

        # если анализаторов уже почти нет — выходим
        if len(selected_ids) <= MIN_ANALYZERS_LEFT:
            break

        # считаем маржинальные уникальные удаления (по текущему threshold и текущему составу)
        marginal_map = await _load_marginal_unique_removed_map(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            analysis_ids=selected_ids,
            threshold=best_threshold,
        )

        # выбираем самый "вредный" (уникально удалил net winners => positive pnl)
        worst_id = None
        worst_pnl = Decimal("0")
        worst_trades = 0

        # комментарий: marginal_map содержит только тех, у кого есть unique_removed_trades
        for aid, m in marginal_map.items():
            pnl = m.get("unique_removed_pnl_abs", Decimal("0"))
            trades = int(m.get("unique_removed_trades", 0) or 0)
            if trades <= 0:
                continue
            if pnl > worst_pnl:
                worst_pnl = pnl
                worst_id = aid
                worst_trades = trades

        # если нет вредных — завершаем оптимизацию состава
        if worst_id is None or worst_pnl <= 0:
            # дополнительно: финальная метрика уже есть в threshold_result
            return await _finalize_and_store_model(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                direction=direction,
                deposit=deposit,
                direction_mask=direction_mask,
                initial_analysis_ids=initial_analysis_ids,
                selected_analysis_ids=selected_ids,
                harmful_removed=harmful_removed,
                threshold_result=threshold_result,
                source_finished_at=source_finished_at,
            )

        # удаляем вредный анализатор и продолжаем
        if worst_id in selected_ids:
            selected_ids = [x for x in selected_ids if x != worst_id]
            harmful_removed.append(
                {
                    "analysis_id": int(worst_id),
                    "unique_removed_trades": int(worst_trades),
                    "unique_removed_pnl_abs": str(_q_decimal(worst_pnl)),
                }
            )

        # если осталось слишком мало анализаторов — завершаем
        if len(selected_ids) <= MIN_ANALYZERS_LEFT:
            threshold_result = await _compute_best_threshold_for_direction(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                direction=direction,
                deposit=deposit,
                analysis_ids=selected_ids,
            )
            return await _finalize_and_store_model(
                pg=pg,
                scenario_id=scenario_id,
                signal_id=signal_id,
                direction=direction,
                deposit=deposit,
                direction_mask=direction_mask,
                initial_analysis_ids=initial_analysis_ids,
                selected_analysis_ids=selected_ids,
                harmful_removed=harmful_removed,
                threshold_result=threshold_result,
                source_finished_at=source_finished_at,
            )

    # если цикл вышел по лимиту итераций — сохраняем то, что получилось
    threshold_result = await _compute_best_threshold_for_direction(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        deposit=deposit,
        analysis_ids=selected_ids,
    )
    return await _finalize_and_store_model(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        deposit=deposit,
        direction_mask=direction_mask,
        initial_analysis_ids=initial_analysis_ids,
        selected_analysis_ids=selected_ids,
        harmful_removed=harmful_removed,
        threshold_result=threshold_result,
        source_finished_at=source_finished_at,
    )


# 🔸 Финализация: запись bt_analysis_model_opt + bt_analysis_bins_labels + (опционально) bt_analysis_threshold_opt
async def _finalize_and_store_model(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    direction_mask: Optional[str],
    initial_analysis_ids: List[int],
    selected_analysis_ids: List[int],
    harmful_removed: List[Dict[str, Any]],
    threshold_result: Dict[str, Any],
    source_finished_at: datetime,
) -> Dict[str, Any]:
    best_threshold = threshold_result["best_threshold"]

    meta_obj = {
        "version": 1,
        "method": "greedy_remove_harmful_unique",
        "threshold_method": "worst_winrate_sweep",
        "eps": str(EPS_THRESHOLD),
        "direction": direction,
        "direction_mask": direction_mask,
        "deposit": str(deposit) if deposit is not None else None,
        "analysis_ids_initial": initial_analysis_ids,
        "analysis_ids_selected": selected_analysis_ids,
        "harmful_removed": harmful_removed,
        "iterations_max": MAX_MODEL_ITERS,
        "harmful_removed_cnt": len(harmful_removed),
        "candidates": threshold_result.get("candidates", 0),
        "removable_positions": threshold_result.get("removable_positions", 0),
    }

    # upsert model_opt и получаем model_id
    model_id = await _upsert_model_opt_return_id(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=best_threshold,
        selected_analysis_ids=selected_analysis_ids,
        threshold_result=threshold_result,
        meta_obj=meta_obj,
        source_finished_at=source_finished_at,
    )

    # размечаем бинны для этой модели
    bins_rows = await _load_bins_stat_rows(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
    )
    labels_inserted = await _rebuild_bins_labels(
        pg=pg,
        model_id=model_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        threshold_used=best_threshold,
        selected_analysis_ids=set(selected_analysis_ids),
        bins_rows=bins_rows,
    )

    log.info(
        "BT_ANALYSIS_PREPROC: модель сохранена — scenario_id=%s, signal_id=%s, direction=%s, model_id=%s, thr=%s, analyses=%s/%s, harmful_removed=%s, bins_labels=%s, filt_roi=%s, filt_trades=%s",
        scenario_id,
        signal_id,
        direction,
        model_id,
        best_threshold,
        len(selected_analysis_ids),
        len(initial_analysis_ids),
        len(harmful_removed),
        labels_inserted,
        threshold_result.get("filt_roi"),
        threshold_result.get("filt_trades"),
    )

    # возвращаем данные для итогового суммарного лога
    return {
        "model_id": model_id,
        "best_threshold": best_threshold,
        "orig_trades": threshold_result.get("orig_trades"),
        "orig_pnl_abs": threshold_result.get("orig_pnl_abs"),
        "orig_winrate": threshold_result.get("orig_winrate"),
        "orig_roi": threshold_result.get("orig_roi"),
        "filt_trades": threshold_result.get("filt_trades"),
        "filt_pnl_abs": threshold_result.get("filt_pnl_abs"),
        "filt_winrate": threshold_result.get("filt_winrate"),
        "filt_roi": threshold_result.get("filt_roi"),
        "removed_trades": threshold_result.get("removed_trades"),
        "removed_accuracy": threshold_result.get("removed_accuracy"),
        "initial_cnt": len(initial_analysis_ids),
        "selected_cnt": len(selected_analysis_ids),
        "harmful_removed_cnt": len(harmful_removed),
        "bins_labels": labels_inserted,
    }


# 🔸 Расчёт оптимального порога для направления по заданному набору анализаторов (worst_winrate sweep)
async def _compute_best_threshold_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    analysis_ids: List[int],
) -> Dict[str, Any]:
    positions = await _load_positions_with_worst_winrate(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        analysis_ids=analysis_ids,
    )

    if not positions:
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
            "removable_positions": 0,
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
        g = groups.setdefault(w, {"trades": 0, "pnl": Decimal("0"), "wins": 0, "losers": 0})
        g["trades"] += 1
        g["pnl"] += p["pnl_abs"]
        if p["pnl_abs"] > 0:
            g["wins"] += 1
        if p["pnl_abs"] <= 0:
            g["losers"] += 1

    unique_worst = sorted(groups.keys())
    candidates = 1 + len(unique_worst)

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

    for v in unique_worst:
        # удаляем все позиции с worst_winrate <= v (эквивалент winrate <= threshold)
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

        if objective_mode == "roi":
            objective = filt_roi
        else:
            objective = filt_pnl

        # 1) максимальный objective
        # 2) при равенстве — больше filt_trades
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

    # комментарий: порог и метрики квантим до 4 знаков для предсказуемости (winrate в bins_stat уже квантован)
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


# 🔸 Загрузка позиций с worst_winrate (MIN winrate по всем попаданиям позиции) с фильтром по analysis_ids
async def _load_positions_with_worst_winrate(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_ids: List[int],
) -> List[Dict[str, Any]]:
    # если анализаторов нет — worst_winrate никогда не определится
    analysis_ids = analysis_ids or []

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
                  AND (
                        array_length($4::int[], 1) IS NULL
                        OR r.analysis_id = ANY($4::int[])
                      )
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
            analysis_ids if analysis_ids else None,
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


# 🔸 Маржинальные уникальные удаления по анализаторам (unique_removed_pnl_abs) при фиксированном threshold и составе
async def _load_marginal_unique_removed_map(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_ids: List[int],
    threshold: Decimal,
) -> Dict[int, Dict[str, Any]]:
    if not analysis_ids:
        return {}

    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH bad_bins AS (
                SELECT analysis_id, timeframe, direction, bin_name
                FROM bt_analysis_bins_stat
                WHERE scenario_id = $1
                  AND signal_id   = $2
                  AND direction   = $3
                  AND analysis_id = ANY($4::int[])
                  AND winrate     <= $5
            ),
            flags AS (
                SELECT DISTINCT
                    r.position_uid,
                    r.analysis_id
                FROM bt_analysis_positions_raw r
                JOIN bad_bins b
                  ON b.analysis_id = r.analysis_id
                 AND b.timeframe   = r.timeframe
                 AND b.direction   = r.direction
                 AND b.bin_name    = r.bin_name
                WHERE r.scenario_id = $1
                  AND r.signal_id   = $2
                  AND r.direction   = $3
                  AND r.analysis_id = ANY($4::int[])
            ),
            pos_agg AS (
                SELECT
                    position_uid,
                    COUNT(DISTINCT analysis_id) AS analyses_cnt,
                    MIN(analysis_id)            AS only_analysis_id
                FROM flags
                GROUP BY position_uid
            ),
            uniq_pos AS (
                SELECT only_analysis_id AS analysis_id, position_uid
                FROM pos_agg
                WHERE analyses_cnt = 1
            )
            SELECT
                u.analysis_id,
                COUNT(*)                                         AS unique_removed_trades,
                COALESCE(SUM(p.pnl_abs), 0)                      AS unique_removed_pnl_abs,
                COUNT(*) FILTER (WHERE p.pnl_abs > 0)            AS unique_removed_wins,
                COUNT(*) FILTER (WHERE p.pnl_abs <= 0)           AS unique_removed_losers
            FROM uniq_pos u
            JOIN bt_scenario_positions p
              ON p.position_uid = u.position_uid
            WHERE p.scenario_id = $1
              AND p.signal_id   = $2
              AND p.direction   = $3
              AND p.postproc    = true
            GROUP BY u.analysis_id
            """,
            scenario_id,
            signal_id,
            direction,
            analysis_ids,
            threshold,
        )

    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        aid = int(r["analysis_id"])
        out[aid] = {
            "unique_removed_trades": int(r["unique_removed_trades"]),
            "unique_removed_pnl_abs": _safe_decimal(r["unique_removed_pnl_abs"]),
            "unique_removed_wins": int(r["unique_removed_wins"]),
            "unique_removed_losers": int(r["unique_removed_losers"]),
        }
    return out


# 🔸 Upsert model_opt и возврат model_id
async def _upsert_model_opt_return_id(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    best_threshold: Decimal,
    selected_analysis_ids: List[int],
    threshold_result: Dict[str, Any],
    meta_obj: Dict[str, Any],
    source_finished_at: datetime,
) -> int:
    meta_json = json.dumps(meta_obj, ensure_ascii=False)
    selected_json = json.dumps(selected_analysis_ids, ensure_ascii=False)

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO bt_analysis_model_opt (
                scenario_id,
                signal_id,
                direction,
                best_threshold,
                selected_analysis_ids,
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
                $5::jsonb,
                $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15,
                $16::jsonb,
                $17
            )
            ON CONFLICT (scenario_id, signal_id, direction)
            DO UPDATE SET
                best_threshold        = EXCLUDED.best_threshold,
                selected_analysis_ids = EXCLUDED.selected_analysis_ids,
                orig_trades           = EXCLUDED.orig_trades,
                orig_pnl_abs          = EXCLUDED.orig_pnl_abs,
                orig_winrate          = EXCLUDED.orig_winrate,
                orig_roi              = EXCLUDED.orig_roi,
                filt_trades           = EXCLUDED.filt_trades,
                filt_pnl_abs          = EXCLUDED.filt_pnl_abs,
                filt_winrate          = EXCLUDED.filt_winrate,
                filt_roi              = EXCLUDED.filt_roi,
                removed_trades        = EXCLUDED.removed_trades,
                removed_accuracy      = EXCLUDED.removed_accuracy,
                meta                  = EXCLUDED.meta,
                source_finished_at    = EXCLUDED.source_finished_at,
                updated_at            = now()
            RETURNING id
            """,
            scenario_id,
            signal_id,
            direction,
            best_threshold,
            selected_json,
            threshold_result["orig_trades"],
            threshold_result["orig_pnl_abs"],
            threshold_result["orig_winrate"],
            threshold_result["orig_roi"],
            threshold_result["filt_trades"],
            threshold_result["filt_pnl_abs"],
            threshold_result["filt_winrate"],
            threshold_result["filt_roi"],
            threshold_result["removed_trades"],
            threshold_result["removed_accuracy"],
            meta_json,
            source_finished_at,
        )

    return int(row["id"])


# 🔸 Загрузка строк bt_analysis_bins_stat для пары и направления
async def _load_bins_stat_rows(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                indicator_param,
                timeframe,
                bin_name,
                trades,
                pnl_abs,
                winrate
            FROM bt_analysis_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            """,
            scenario_id,
            signal_id,
            direction,
        )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "analysis_id": int(r["analysis_id"]),
                "indicator_param": r["indicator_param"],
                "timeframe": str(r["timeframe"]),
                "bin_name": str(r["bin_name"]),
                "trades": int(r["trades"]),
                "pnl_abs": _safe_decimal(r["pnl_abs"]),
                "winrate": _safe_decimal(r["winrate"]),
            }
        )
    return out


# 🔸 Пересборка разметки биннов bt_analysis_bins_labels для model_id (good/bad/inactive)
async def _rebuild_bins_labels(
    pg,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    threshold_used: Decimal,
    selected_analysis_ids: set,
    bins_rows: List[Dict[str, Any]],
) -> int:
    async with pg.acquire() as conn:
        # сначала удаляем старую разметку по model_id
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_labels
            WHERE model_id = $1
            """,
            model_id,
        )

        if not bins_rows:
            return 0

        to_insert: List[Tuple[Any, ...]] = []
        for b in bins_rows:
            aid = int(b["analysis_id"])
            winrate = b["winrate"]

            # комментарий: inactive для отключенных анализаторов
            if aid not in selected_analysis_ids:
                state = "inactive"
            else:
                # bad если winrate <= threshold
                state = "bad" if winrate <= threshold_used else "good"

            to_insert.append(
                (
                    model_id,
                    scenario_id,
                    signal_id,
                    direction,
                    aid,
                    b["indicator_param"],
                    b["timeframe"],
                    b["bin_name"],
                    state,
                    threshold_used,
                    int(b["trades"]),
                    b["pnl_abs"],
                    b["winrate"],
                )
            )

        await conn.executemany(
            """
            INSERT INTO bt_analysis_bins_labels (
                model_id,
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
                winrate
            )
            VALUES (
                $1, $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10,
                $11, $12, $13
            )
            """,
            to_insert,
        )

    return len(to_insert)


# 🔸 Удаление старых строк модели и метрик для направления (каскадом удалит bins_labels)
async def _delete_model_and_threshold_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysis_model_opt
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            """,
            scenario_id,
            signal_id,
            direction,
        )


# 🔸 Публикация события готовности препроцессинга в bt:analysis:preproc_ready
async def _publish_preproc_ready(
    redis,
    scenario_id: int,
    signal_id: int,
    source_finished_at: datetime,
    direction_mask: Optional[str],
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            PREPROC_READY_STREAM_KEY,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "finished_at": finished_at.isoformat(),
                "source_finished_at": source_finished_at.isoformat(),
                "direction_mask": str(direction_mask) if direction_mask is not None else "",
            },
        )
        log.debug(
            "BT_ANALYSIS_PREPROC: опубликовано событие preproc_ready в стрим '%s' для scenario_id=%s, signal_id=%s, finished_at=%s",
            PREPROC_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC: не удалось опубликовать событие в стрим '%s' для scenario_id=%s, signal_id=%s: %s",
            PREPROC_READY_STREAM_KEY,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Загрузка direction_mask сигнала из bt_signals_parameters (param_name='direction_mask')
async def _load_signal_direction_mask(pg, signal_id: int) -> Optional[str]:
    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT param_value
            FROM bt_signals_parameters
            WHERE signal_id  = $1
              AND param_name = 'direction_mask'
            LIMIT 1
            """,
            signal_id,
        )

    if not row:
        return None

    value = row["param_value"]
    if value is None:
        return None

    return str(value).strip().lower() or None


# 🔸 Преобразование direction_mask -> список направлений
def _directions_from_mask(mask: Optional[str]) -> List[str]:
    if not mask:
        return ["long", "short"]

    m = mask.strip().lower()

    if m == "long":
        return ["long"]
    if m == "short":
        return ["short"]

    if m in ("both", "all", "any", "long_short", "short_long", "long+short", "short+long", "long|short", "short|long"):
        return ["long", "short"]

    return ["long", "short"]


# 🔸 Загрузка analysis_id кандидатов из bins_stat по паре и направлению
async def _load_analysis_ids_for_pair_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[int]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT analysis_id
            FROM bt_analysis_bins_stat
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            ORDER BY analysis_id
            """,
            scenario_id,
            signal_id,
            direction,
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


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков (вниз для предсказуемости)
def _q_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)