# bt_analysis_preproc_v2.py — препроцессинг анализов v2 (подбор bad-биннов через маржинальный ROI + holdout)

import asyncio
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Константы стримов и настроек препроцессинга v2
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
PREPROC_READY_STREAM_KEY_V2 = "bt:analysis:preproc_ready_v2"

PREPROC_CONSUMER_GROUP_V2 = "bt_analysis_preproc_v2"
PREPROC_CONSUMER_NAME_V2 = "bt_analysis_preproc_v2_main"

PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

PREPROC_MAX_CONCURRENCY = 6

# 🔸 Настройки v2-оптимизации (holdout и критерии принятия)
HOLDOUT_DAYS = 7

# ограничение на размер набора bad-биннов (чтобы не разрастаться)
V2_MAX_BAD_BINS = 200

# допускаем небольшой "вред" на holdout ради крупной выгоды на train
# (пример: если на train бин улучшает pnl на +100, то на holdout можно потерять до 5)
VAL_HARM_MAX_RATIO = Decimal("0.05")

# статистический фильтр "уверенности", заменяющий грубые MIN_TRADES
# используем Wilson upper bound для winrate на train: хотим, чтобы верхняя граница < 0.5
WILSON_Z = Decimal("1.96")

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_PREPROC_V2")


# 🔸 Публичная точка входа: оркестратор препроцессинга v2
async def run_bt_analysis_preproc_v2_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_PREPROC_V2: оркестратор запущен")

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
                        name=f"BT_ANALYSIS_PREPROC_V2_{entry_id}",
                    )
                    tasks.append(task)

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = sum(1 for r in results if isinstance(r, Exception))
                log.info(
                    "BT_ANALYSIS_PREPROC_V2: обработан пакет сообщений из bt:analysis:ready — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка в основном цикле оркестратора: %s",
                e,
                exc_info=True,
            )
            await asyncio.sleep(2)


# 🔸 Проверка/создание consumer group для стрима bt:analysis:ready
async def _ensure_consumer_group(redis) -> None:
    try:
        await redis.xgroup_create(
            name=ANALYSIS_READY_STREAM_KEY,
            groupname=PREPROC_CONSUMER_GROUP_V2,
            id="$",
            mkstream=True,
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: создана consumer group '%s' для стрима '%s'",
            PREPROC_CONSUMER_GROUP_V2,
            ANALYSIS_READY_STREAM_KEY,
        )
    except Exception as e:
        msg = str(e)
        if "BUSYGROUP" in msg:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: consumer group '%s' для стрима '%s' уже существует",
                PREPROC_CONSUMER_GROUP_V2,
                ANALYSIS_READY_STREAM_KEY,
            )
        else:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка при создании consumer group '%s': %s",
                PREPROC_CONSUMER_GROUP_V2,
                e,
                exc_info=True,
            )
            raise


# 🔸 Чтение сообщений из стрима bt:analysis:ready
async def _read_from_stream(redis) -> List[Any]:
    entries = await redis.xreadgroup(
        groupname=PREPROC_CONSUMER_GROUP_V2,
        consumername=PREPROC_CONSUMER_NAME_V2,
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
            "BT_ANALYSIS_PREPROC_V2: ошибка разбора сообщения стрима bt:analysis:ready: %s, fields=%s",
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
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP_V2, entry_id)
            return

        scenario_id = ctx["scenario_id"]
        signal_id = ctx["signal_id"]
        source_finished_at = ctx["source_finished_at"]

        pair_key = (scenario_id, signal_id)
        last_finished = _last_analysis_finished_at.get(pair_key)

        # отсечка дублей по равному source_finished_at
        if last_finished is not None and last_finished == source_finished_at:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: дубликат сообщения для scenario_id=%s, signal_id=%s, source_finished_at=%s, stream_id=%s — расчёт не выполняется",
                scenario_id,
                signal_id,
                source_finished_at,
                entry_id,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP_V2, entry_id)
            return

        _last_analysis_finished_at[pair_key] = source_finished_at

        started_at = datetime.utcnow()

        try:
            # определяем направления сигнала
            direction_mask = await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # депозит сценария (ROI)
            deposit = await _load_scenario_deposit(pg, scenario_id)

            # считаем модель отдельно по каждому направлению
            results: Dict[str, Dict[str, Any]] = {}
            for direction in directions:
                res = await _build_model_for_direction_v2(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    deposit=deposit,
                    source_finished_at=source_finished_at,
                    holdout_days=HOLDOUT_DAYS,
                )
                results[direction] = res

            # публикуем событие готовности препроцессинга v2
            await _publish_preproc_ready_v2(
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
                    f"{d} orig_roi={r.get('orig_roi')} filt_roi={r.get('filt_roi')} "
                    f"orig_trades={r.get('orig_trades')} filt_trades={r.get('filt_trades')} "
                    f"bins_selected={r.get('bad_bins_selected')} "
                    f"val_window_days={r.get('holdout_days')} val_used={r.get('val_used')}"
                )

            log.info(
                "BT_ANALYSIS_PREPROC_V2: scenario_id=%s, signal_id=%s — directions=%s, deposit=%s, %s, source_finished_at=%s, elapsed_ms=%s",
                scenario_id,
                signal_id,
                directions,
                str(deposit) if deposit is not None else None,
                " | ".join(parts) if parts else "no_results",
                source_finished_at,
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка расчёта для scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP_V2, entry_id)


# 🔸 Построение модели v2 для одного направления: выбор bad-биннов + запись model_opt_v2 + bins_labels_v2
async def _build_model_for_direction_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    source_finished_at: datetime,
    holdout_days: int,
) -> Dict[str, Any]:
    # загружаем позиции направления
    positions = await _load_positions_for_direction(pg, scenario_id, signal_id, direction)
    if not positions:
        # если позиций нет — очищаем модель/лейблы (на всякий случай) и выходим
        await _delete_model_for_direction_v2(pg, scenario_id, signal_id, direction)
        return {
            "direction": direction,
            "orig_trades": 0,
            "orig_pnl_abs": "0",
            "orig_winrate": "0",
            "orig_roi": "0",
            "filt_trades": 0,
            "filt_pnl_abs": "0",
            "filt_winrate": "0",
            "filt_roi": "0",
            "removed_trades": 0,
            "removed_accuracy": "0",
            "bad_bins_selected": 0,
            "holdout_days": holdout_days,
            "val_used": False,
        }

    # базовые индексы по позициям
    pos_pnl: Dict[Any, Decimal] = {}
    pos_is_win: Dict[Any, bool] = {}
    pos_exit_time: Dict[Any, datetime] = {}
    all_uids: Set[Any] = set()

    for p in positions:
        uid = p["position_uid"]
        all_uids.add(uid)
        pnl = p["pnl_abs"]
        pos_pnl[uid] = pnl
        pos_is_win[uid] = pnl > 0
        pos_exit_time[uid] = p["exit_time"]

    orig_trades = len(all_uids)
    orig_pnl = sum(pos_pnl.values(), Decimal("0"))
    orig_wins = sum(1 for uid in all_uids if pos_is_win.get(uid))
    orig_winrate = (Decimal(orig_wins) / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")
    orig_roi = (orig_pnl / deposit) if (deposit and deposit > 0) else Decimal("0")

    # делим на train/val по exit_time (UTC)
    train_uids, val_uids, val_used, val_window = _split_train_val_uids(
        uids=all_uids,
        pos_exit_time=pos_exit_time,
        holdout_days=holdout_days,
    )

    # загружаем кандидаты биннов из bins_stat
    candidates = await _load_candidate_bins_for_direction(pg, scenario_id, signal_id, direction)
    if not candidates:
        # модель без фильтрации
        model_id = await _upsert_model_opt_v2_return_id(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            best_threshold=Decimal("0"),
            selected_analysis_ids=[],
            orig_trades=orig_trades,
            orig_pnl_abs=orig_pnl,
            orig_winrate=orig_winrate,
            orig_roi=orig_roi,
            filt_trades=orig_trades,
            filt_pnl_abs=orig_pnl,
            filt_winrate=orig_winrate,
            filt_roi=orig_roi,
            removed_trades=0,
            removed_accuracy=Decimal("0"),
            meta_obj={
                "version": 1,
                "method": "greedy_bins_marginal_roi",
                "direction": direction,
                "deposit": str(deposit) if deposit is not None else None,
                "holdout": {"days": holdout_days, "used": bool(val_used), "window": val_window},
                "candidates": 0,
                "selected_bins": 0,
                "note": "no_candidates_in_bins_stat",
            },
            source_finished_at=source_finished_at,
        )
        await _rebuild_bins_labels_v2(
            pg=pg,
            model_id=model_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            selected_bins=[],
        )
        return {
            "direction": direction,
            "orig_trades": orig_trades,
            "orig_pnl_abs": str(orig_pnl),
            "orig_winrate": str(orig_winrate),
            "orig_roi": str(orig_roi),
            "filt_trades": orig_trades,
            "filt_pnl_abs": str(orig_pnl),
            "filt_winrate": str(orig_winrate),
            "filt_roi": str(orig_roi),
            "removed_trades": 0,
            "removed_accuracy": "0",
            "bad_bins_selected": 0,
            "holdout_days": holdout_days,
            "val_used": bool(val_used),
        }

    # строим index попаданий (analysis_id, timeframe, bin_name) -> set(position_uid)
    hits_index = await _load_hits_index_for_direction(pg, scenario_id, signal_id, direction)

    # фильтруем кандидатов только на те, у которых есть попадания в raw
    usable_candidates: List[Dict[str, Any]] = []
    missing_hits = 0
    for c in candidates:
        k = (int(c["analysis_id"]), str(c["timeframe"]), str(c["bin_name"]))
        if k not in hits_index:
            missing_hits += 1
            continue
        usable_candidates.append(c)

    # greedy отбор bad-биннов по маржинальному улучшению train и контролю holdout
    selected_bins, steps_meta = _select_bad_bins_greedy(
        usable_candidates=usable_candidates,
        hits_index=hits_index,
        pos_pnl=pos_pnl,
        pos_is_win=pos_is_win,
        train_uids=train_uids,
        val_uids=val_uids,
        val_used=val_used,
        max_bins=V2_MAX_BAD_BINS,
    )

    # финальные множества removed/kept по всему окну
    removed_all: Set[Any] = set()
    for b in selected_bins:
        k = (int(b["analysis_id"]), str(b["timeframe"]), str(b["bin_name"]))
        removed_all |= hits_index.get(k, set())

    kept_all = set(uid for uid in all_uids if uid not in removed_all)

    filt_trades = len(kept_all)
    filt_pnl = sum((pos_pnl[uid] for uid in kept_all), Decimal("0"))
    filt_wins = sum(1 for uid in kept_all if pos_is_win.get(uid))
    filt_winrate = (Decimal(filt_wins) / Decimal(filt_trades)) if filt_trades > 0 else Decimal("0")
    filt_roi = (filt_pnl / deposit) if (deposit and deposit > 0) else Decimal("0")

    removed_trades = orig_trades - filt_trades
    if removed_trades > 0:
        removed_losers = sum(1 for uid in removed_all if (uid in all_uids and not pos_is_win.get(uid, False)))
        removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
    else:
        removed_accuracy = Decimal("0")

    # список анализаторов, у которых реально выбраны bad-бины
    selected_analysis_ids = sorted({int(b["analysis_id"]) for b in selected_bins})

    # meta модели
    meta_obj = {
        "version": 1,
        "method": "greedy_bins_marginal_roi",
        "direction": direction,
        "deposit": str(deposit) if deposit is not None else None,
        "holdout": {"days": holdout_days, "used": bool(val_used), "window": val_window},
        "params": {
            "max_bad_bins": int(V2_MAX_BAD_BINS),
            "val_harm_max_ratio": str(VAL_HARM_MAX_RATIO),
            "wilson_z": str(WILSON_Z),
        },
        "candidates": int(len(candidates)),
        "candidates_usable": int(len(usable_candidates)),
        "candidates_missing_hits": int(missing_hits),
        "selected_bins": int(len(selected_bins)),
        "steps": steps_meta,
    }

    # upsert model_opt_v2 и получаем model_id
    model_id = await _upsert_model_opt_v2_return_id(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=Decimal("0"),
        selected_analysis_ids=selected_analysis_ids,
        orig_trades=orig_trades,
        orig_pnl_abs=orig_pnl,
        orig_winrate=orig_winrate,
        orig_roi=orig_roi,
        filt_trades=filt_trades,
        filt_pnl_abs=filt_pnl,
        filt_winrate=filt_winrate,
        filt_roi=filt_roi,
        removed_trades=removed_trades,
        removed_accuracy=removed_accuracy,
        meta_obj=meta_obj,
        source_finished_at=source_finished_at,
    )

    # пересобираем labels_v2 (state='bad' только для выбранных биннов)
    await _rebuild_bins_labels_v2(
        pg=pg,
        model_id=model_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        selected_bins=selected_bins,
    )

    return {
        "direction": direction,
        "orig_trades": orig_trades,
        "orig_pnl_abs": str(orig_pnl),
        "orig_winrate": str(orig_winrate),
        "orig_roi": str(orig_roi),
        "filt_trades": filt_trades,
        "filt_pnl_abs": str(filt_pnl),
        "filt_winrate": str(filt_winrate),
        "filt_roi": str(filt_roi),
        "removed_trades": removed_trades,
        "removed_accuracy": str(removed_accuracy),
        "bad_bins_selected": len(selected_bins),
        "holdout_days": holdout_days,
        "val_used": bool(val_used),
    }


# 🔸 Выбор bad-биннов greedy: максимизируем маржинальный gain на train, ограничиваем harm на holdout
def _select_bad_bins_greedy(
    usable_candidates: List[Dict[str, Any]],
    hits_index: Dict[Tuple[int, str, str], Set[Any]],
    pos_pnl: Dict[Any, Decimal],
    pos_is_win: Dict[Any, bool],
    train_uids: Set[Any],
    val_uids: Set[Any],
    val_used: bool,
    max_bins: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected: List[Dict[str, Any]] = []
    steps: List[Dict[str, Any]] = []

    # текущие удалённые множества
    removed_train: Set[Any] = set()
    removed_val: Set[Any] = set()

    # быстрый доступ: candidate_key -> candidate
    cand_by_key: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    cand_keys: List[Tuple[int, str, str]] = []

    for c in usable_candidates:
        k = (int(c["analysis_id"]), str(c["timeframe"]), str(c["bin_name"]))
        if k in cand_by_key:
            # на всякий случай: предпочитаем бин с большим trades
            if int(c.get("trades", 0) or 0) > int(cand_by_key[k].get("trades", 0) or 0):
                cand_by_key[k] = c
            continue
        cand_by_key[k] = c
        cand_keys.append(k)

    # основной greedy-цикл
    for it in range(int(max_bins or 0)):
        best_key = None
        best_gain = Decimal("0")
        best_train_pnl = Decimal("0")
        best_val_pnl = Decimal("0")
        best_train_trades = 0
        best_val_trades = 0
        best_train_wins = 0

        for k in cand_keys:
            c = cand_by_key.get(k)
            if c is None:
                continue

            # уже выбран
            if any(int(b["analysis_id"]) == k[0] and str(b["timeframe"]) == k[1] and str(b["bin_name"]) == k[2] for b in selected):
                continue

            hits = hits_index.get(k) or set()

            # маржинально новые удаления (train)
            new_train = hits.intersection(train_uids)
            if removed_train:
                new_train = set(uid for uid in new_train if uid not in removed_train)

            if not new_train:
                continue

            train_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in new_train), Decimal("0"))

            # нужен положительный gain => train_pnl должен быть отрицательным
            if train_pnl >= 0:
                continue

            train_trades = len(new_train)
            train_wins = sum(1 for uid in new_train if pos_is_win.get(uid))

            # критерий уверенности через Wilson upper bound
            if not _wilson_upper_ok(train_wins, train_trades, WILSON_Z):
                continue

            gain = -train_pnl  # положительная величина улучшения pnl (а значит и ROI)

            # holdout check (если используем)
            val_pnl = Decimal("0")
            val_trades = 0
            if val_used and val_uids:
                new_val = hits.intersection(val_uids)
                if removed_val:
                    new_val = set(uid for uid in new_val if uid not in removed_val)

                if new_val:
                    val_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in new_val), Decimal("0"))
                    val_trades = len(new_val)

                # если бин на holdout выбрасывает прибыль, ограничиваем допустимый вред
                if val_pnl > 0:
                    max_harm = gain * VAL_HARM_MAX_RATIO
                    if val_pnl > max_harm:
                        continue

            # выбираем лучший gain
            if gain > best_gain:
                best_gain = gain
                best_key = k
                best_train_pnl = train_pnl
                best_val_pnl = val_pnl
                best_train_trades = train_trades
                best_val_trades = val_trades
                best_train_wins = train_wins

        # если больше нет кандидатов, улучшающих цель — стоп
        if best_key is None or best_gain <= 0:
            break

        chosen = cand_by_key[best_key]
        selected.append(chosen)

        # обновляем removed множества
        hits = hits_index.get(best_key) or set()
        removed_train |= hits.intersection(train_uids)
        if val_used and val_uids:
            removed_val |= hits.intersection(val_uids)

        # мета-лог шага (сжато)
        steps.append(
            {
                "step": it + 1,
                "analysis_id": int(chosen["analysis_id"]),
                "indicator_param": chosen.get("indicator_param"),
                "timeframe": str(chosen["timeframe"]),
                "bin_name": str(chosen["bin_name"]),
                "train_trades": int(best_train_trades),
                "train_pnl_abs": str(best_train_pnl),
                "train_winrate": float(Decimal(best_train_wins) / Decimal(best_train_trades)) if best_train_trades > 0 else 0.0,
                "val_trades": int(best_val_trades),
                "val_pnl_abs": str(best_val_pnl),
                "gain_abs": str(best_gain),
            }
        )

    # ограничиваем размер meta (чтобы не раздувать jsonb)
    if len(steps) > 120:
        steps = steps[:120] + [{"note": f"steps_truncated total={len(steps)}"}]

    return selected, steps


# 🔸 Проверка: Wilson upper bound для winrate < 0.5
def _wilson_upper_ok(wins: int, n: int, z: Decimal) -> bool:
    # условия достаточности
    if n <= 0:
        return False

    # p̂ = wins/n
    p = Decimal(int(wins)) / Decimal(int(n))

    # Wilson upper bound:
    # center = (p + z^2/(2n)) / (1 + z^2/n)
    # margin = z * sqrt( (p(1-p) + z^2/(4n)) / n ) / (1 + z^2/n)
    # upper = center + margin
    try:
        z2 = z * z
        denom = Decimal("1") + (z2 / Decimal(n))
        center = (p + (z2 / (Decimal("2") * Decimal(n)))) / denom

        rad = (p * (Decimal("1") - p) + (z2 / (Decimal("4") * Decimal(n)))) / Decimal(n)
        if rad < 0:
            return False

        margin = (z * _sqrt_decimal(rad)) / denom
        upper = center + margin
    except Exception:
        return False

    return upper < Decimal("0.5")


# 🔸 Квадратный корень для Decimal (через float, здесь точность достаточна для критериев)
def _sqrt_decimal(x: Decimal) -> Decimal:
    try:
        return Decimal(str(float(x) ** 0.5))
    except Exception:
        return Decimal("0")


# 🔸 Split train/val по exit_time с holdout_days
def _split_train_val_uids(
    uids: Set[Any],
    pos_exit_time: Dict[Any, datetime],
    holdout_days: int,
) -> Tuple[Set[Any], Set[Any], bool, Dict[str, Any]]:
    # окно holdout
    max_ts = None
    for uid in uids:
        ts = pos_exit_time.get(uid)
        if ts is None:
            continue
        if max_ts is None or ts > max_ts:
            max_ts = ts

    if max_ts is None:
        # нет времени — всё в train
        return set(uids), set(), False, {"mode": "none", "reason": "no_exit_time"}

    cut = max_ts - timedelta(days=int(holdout_days or 0))

    train: Set[Any] = set()
    val: Set[Any] = set()

    for uid in uids:
        ts = pos_exit_time.get(uid)
        if ts is None:
            train.add(uid)
            continue
        if ts >= cut:
            val.add(uid)
        else:
            train.add(uid)

    # если val пустой или train пустой — отключаем валидацию
    if not train or not val:
        return set(uids), set(), False, {"mode": "none", "reason": "empty_split", "train": len(train), "val": len(val)}

    return train, val, True, {"mode": "exit_time_days", "days": int(holdout_days), "cut": cut.isoformat(), "train": len(train), "val": len(val)}


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


# 🔸 Загрузка депозита сценария из bt_scenario_parameters (param_name='deposit')
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


# 🔸 Загрузка позиций сценария/сигнала для направления (postproc=true)
async def _load_positions_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> List[Dict[str, Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                position_uid,
                pnl_abs,
                exit_time
            FROM bt_scenario_positions
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND postproc    = true
              AND direction   = $3
            ORDER BY exit_time
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
                "exit_time": r["exit_time"],
            }
        )
    return out


# 🔸 Загрузка кандидатов биннов из bt_analysis_bins_stat (по паре и направлению)
async def _load_candidate_bins_for_direction(
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
              AND trades      > 0
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


# 🔸 Индекс попаданий raw по ключу (analysis_id, timeframe, bin_name) -> set(position_uid) для направления
async def _load_hits_index_for_direction(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> Dict[Tuple[int, str, str], Set[Any]]:
    async with pg.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                analysis_id,
                timeframe,
                bin_name,
                position_uid
            FROM bt_analysis_positions_raw
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            """,
            scenario_id,
            signal_id,
            direction,
        )

    idx: Dict[Tuple[int, str, str], Set[Any]] = {}
    for r in rows:
        k = (int(r["analysis_id"]), str(r["timeframe"]), str(r["bin_name"]))
        idx.setdefault(k, set()).add(r["position_uid"])
    return idx


# 🔸 Upsert model_opt_v2 и возврат model_id
async def _upsert_model_opt_v2_return_id(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    best_threshold: Decimal,
    selected_analysis_ids: List[int],
    orig_trades: int,
    orig_pnl_abs: Decimal,
    orig_winrate: Decimal,
    orig_roi: Decimal,
    filt_trades: int,
    filt_pnl_abs: Decimal,
    filt_winrate: Decimal,
    filt_roi: Decimal,
    removed_trades: int,
    removed_accuracy: Decimal,
    meta_obj: Dict[str, Any],
    source_finished_at: datetime,
) -> int:
    meta_json = json.dumps(meta_obj, ensure_ascii=False)
    selected_json = json.dumps(selected_analysis_ids, ensure_ascii=False)

    async with pg.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO bt_analysis_model_opt_v2 (
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
            int(orig_trades),
            orig_pnl_abs,
            orig_winrate,
            orig_roi,
            int(filt_trades),
            filt_pnl_abs,
            filt_winrate,
            filt_roi,
            int(removed_trades),
            removed_accuracy,
            meta_json,
            source_finished_at,
        )

    return int(row["id"])


# 🔸 Пересборка разметки биннов bt_analysis_bins_labels_v2 для model_id (пишем только state='bad')
async def _rebuild_bins_labels_v2(
    pg,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    selected_bins: List[Dict[str, Any]],
) -> int:
    async with pg.acquire() as conn:
        # сначала удаляем старую разметку по model_id
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_labels_v2
            WHERE model_id = $1
            """,
            model_id,
        )

        if not selected_bins:
            return 0

        to_insert: List[Tuple[Any, ...]] = []
        for b in selected_bins:
            to_insert.append(
                (
                    model_id,
                    scenario_id,
                    signal_id,
                    direction,
                    int(b["analysis_id"]),
                    b.get("indicator_param"),
                    str(b["timeframe"]),
                    str(b["bin_name"]),
                    "bad",
                    Decimal("0"),  # threshold_used не применяется в v2
                    int(b.get("trades", 0) or 0),
                    _safe_decimal(b.get("pnl_abs", 0)),
                    _safe_decimal(b.get("winrate", 0)),
                )
            )

        await conn.executemany(
            """
            INSERT INTO bt_analysis_bins_labels_v2 (
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


# 🔸 Удаление модели по направлению (каскадом удалит bins_labels_v2)
async def _delete_model_for_direction_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
) -> None:
    async with pg.acquire() as conn:
        await conn.execute(
            """
            DELETE FROM bt_analysis_model_opt_v2
            WHERE scenario_id = $1
              AND signal_id   = $2
              AND direction   = $3
            """,
            scenario_id,
            signal_id,
            direction,
        )


# 🔸 Публикация события готовности препроцессинга v2 в bt:analysis:preproc_ready_v2
async def _publish_preproc_ready_v2(
    redis,
    scenario_id: int,
    signal_id: int,
    source_finished_at: datetime,
    direction_mask: Optional[str],
) -> None:
    finished_at = datetime.utcnow()

    try:
        await redis.xadd(
            PREPROC_READY_STREAM_KEY_V2,
            {
                "scenario_id": str(scenario_id),
                "signal_id": str(signal_id),
                "finished_at": finished_at.isoformat(),
                "source_finished_at": source_finished_at.isoformat(),
                "direction_mask": str(direction_mask) if direction_mask is not None else "",
            },
        )
        log.debug(
            "BT_ANALYSIS_PREPROC_V2: опубликовано событие preproc_ready_v2 в стрим '%s' для scenario_id=%s, signal_id=%s, finished_at=%s",
            PREPROC_READY_STREAM_KEY_V2,
            scenario_id,
            signal_id,
            finished_at,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: не удалось опубликовать событие в стрим '%s' для scenario_id=%s, signal_id=%s: %s",
            PREPROC_READY_STREAM_KEY_V2,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Вспомогательная функция: безопасное приведение к Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")