# bt_analysis_preproc_v2.py — препроцессинг анализов v2 (копия логики v3 + prune "only BAD" -> state=inactive)

import asyncio
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple, Set

# 🔸 Константы стримов и настроек препроцессинга v2
ANALYSIS_READY_STREAM_KEY = "bt:analysis:ready"
PREPROC_READY_STREAM_KEY_V2 = "bt:analysis:preproc_ready_v2"

PREPROC_CONSUMER_GROUP_V2 = "bt_analysis_preproc_v2"
PREPROC_CONSUMER_NAME_V2 = "bt_analysis_preproc_v2_main"

PREPROC_STREAM_BATCH_SIZE = 10
PREPROC_STREAM_BLOCK_MS = 5000

PREPROC_MAX_CONCURRENCY = 6

# 🔸 Настройки (повторяем v3)
HOLDOUT_DAYS = 7
V2_LAMBDA = Decimal("0.5")
NEAR_THRESHOLD_MARGIN = Decimal("0.0500")

GOOD_WINRATE_MIN = Decimal("0.50")
GOOD_WINRATE_MAX = Decimal("1.00")
GOOD_WINRATE_STEP = Decimal("0.01")

MAX_TOGGLE_ITERS = 220
MAX_BAD_BINS_LIMIT = 350

EPS_THRESHOLD = Decimal("0.00000001")
EPS_SCORE = Decimal("0.00000001")
EPS_ROI = Decimal("0.00000001")

# 🔸 Настройки prune (only BAD -> inactive)
PRUNE_ENABLE = True
PRUNE_MAX_ITERS = 64

# 🔸 Кеш последних source_finished_at по (scenario_id, signal_id) для отсечки дублей
_last_analysis_finished_at: Dict[Tuple[int, int], datetime] = {}

log = logging.getLogger("BT_ANALYSIS_PREPROC_V2")


# 🔸 Публичная точка входа: оркестратор препроцессинга v2
async def run_bt_analysis_preproc_v2_orchestrator(pg, redis):
    log.debug("BT_ANALYSIS_PREPROC_V2: оркестратор запущен")

    await _ensure_consumer_group(redis)

    # общий семафор для ограничения параллелизма
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
                    "BT_ANALYSIS_PREPROC_V2: обработан пакет сообщений — сообщений=%s, ошибок=%s",
                    total_msgs,
                    errors,
                )

        except Exception as e:
            log.error("BT_ANALYSIS_PREPROC_V2: ошибка в основном цикле: %s", e, exc_info=True)
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
            "BT_ANALYSIS_PREPROC_V2: ошибка разбора сообщения bt:analysis:ready: %s, fields=%s",
            e,
            fields,
            exc_info=True,
        )
        return None


# 🔸 Обработка одного сообщения
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

        # отсечка дублей
        if last_finished is not None and last_finished == source_finished_at:
            log.debug(
                "BT_ANALYSIS_PREPROC_V2: дубликат scenario_id=%s, signal_id=%s, source_finished_at=%s — пропуск",
                scenario_id,
                signal_id,
                source_finished_at,
            )
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP_V2, entry_id)
            return

        _last_analysis_finished_at[pair_key] = source_finished_at
        started_at = datetime.utcnow()

        try:
            # направления сигнала
            direction_mask = await _load_signal_direction_mask(pg, signal_id)
            directions = _directions_from_mask(direction_mask)

            # депозит сценария (ROI)
            deposit = await _load_scenario_deposit(pg, scenario_id)

            results: Dict[str, Dict[str, Any]] = {}
            for direction in directions:
                res = await _build_model_for_direction_v2(
                    pg=pg,
                    scenario_id=scenario_id,
                    signal_id=signal_id,
                    direction=direction,
                    deposit=deposit,
                    source_finished_at=source_finished_at,
                )
                results[direction] = res

            # чистим направления вне mask (если вдруг есть старые модели)
            other_dirs = [d for d in ("long", "short") if d not in directions]
            for d in other_dirs:
                await _delete_model_for_direction_v2(pg, scenario_id, signal_id, d)

            # публикуем событие готовности
            await _publish_preproc_ready_v2(
                redis=redis,
                scenario_id=scenario_id,
                signal_id=signal_id,
                source_finished_at=source_finished_at,
                direction_mask=direction_mask,
            )

            elapsed_ms = int((datetime.utcnow() - started_at).total_seconds() * 1000)

            parts: List[str] = []
            for d in directions:
                r = results.get(d) or {}
                parts.append(
                    f"{d} thr={r.get('best_threshold')} "
                    f"orig_roi={r.get('orig_roi')} filt_roi={r.get('filt_roi')} "
                    f"train_roi={r.get('train_roi')} val_roi={r.get('val_roi')} score={r.get('score')} "
                    f"labels_total={r.get('labels_total')} bad={r.get('bad_bins')} good={r.get('good_bins')} neutral={r.get('neutral_bins')} inactive={r.get('inactive_bins')} "
                    f"pruned_aids={r.get('pruned_analysis_ids')}"
                )

            log.info(
                "BT_ANALYSIS_PREPROC_V2: scenario_id=%s, signal_id=%s — directions=%s, good_thr_sweep=%s..%s step=%s, deposit=%s, %s, elapsed_ms=%s",
                scenario_id,
                signal_id,
                directions,
                str(GOOD_WINRATE_MIN),
                str(GOOD_WINRATE_MAX),
                str(GOOD_WINRATE_STEP),
                str(deposit) if deposit is not None else None,
                " | ".join(parts) if parts else "no_results",
                elapsed_ms,
            )

        except Exception as e:
            log.error(
                "BT_ANALYSIS_PREPROC_V2: ошибка расчёта scenario_id=%s, signal_id=%s: %s",
                scenario_id,
                signal_id,
                e,
                exc_info=True,
            )
        finally:
            await redis.xack(ANALYSIS_READY_STREAM_KEY, PREPROC_CONSUMER_GROUP_V2, entry_id)


# 🔸 Построение модели v2 для одного направления:
#   - bad: по логике v3 (threshold+toggle)
#   - good: winrate > good_threshold_selected
#   - neutral: остальное
#   - prune: greedy disable BAD by analysis_id => state='inactive'
async def _build_model_for_direction_v2(
    pg,
    scenario_id: int,
    signal_id: int,
    direction: str,
    deposit: Optional[Decimal],
    source_finished_at: datetime,
) -> Dict[str, Any]:
    # загружаем позиции направления
    positions = await _load_positions_for_direction(pg, scenario_id, signal_id, direction)
    if not positions:
        await _delete_model_for_direction_v2(pg, scenario_id, signal_id, direction)
        return {
            "direction": direction,
            "best_threshold": "0",
            "orig_roi": "0",
            "filt_roi": "0",
            "train_roi": "0",
            "val_roi": "0",
            "score": "0",
            "labels_total": 0,
            "bad_bins": 0,
            "good_bins": 0,
            "neutral_bins": 0,
            "inactive_bins": 0,
            "pruned_analysis_ids": [],
        }

    # индексы по позициям
    all_uids: Set[Any] = set()
    pos_pnl: Dict[Any, Decimal] = {}
    pos_win: Dict[Any, bool] = {}
    pos_exit_time: Dict[Any, datetime] = {}

    for p in positions:
        uid = p["position_uid"]
        all_uids.add(uid)
        pnl = p["pnl_abs"]
        pos_pnl[uid] = pnl
        pos_win[uid] = pnl > 0
        pos_exit_time[uid] = p["exit_time"]

    # split train/val
    train_uids, val_uids, val_used, val_window = _split_train_val_uids(
        uids=all_uids,
        pos_exit_time=pos_exit_time,
        holdout_days=HOLDOUT_DAYS,
    )

    # orig метрики
    orig_trades = len(all_uids)
    orig_pnl_abs = sum(pos_pnl.values(), Decimal("0"))
    orig_wins = sum(1 for uid in all_uids if pos_win.get(uid))
    orig_winrate = (Decimal(orig_wins) / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")
    orig_roi = (orig_pnl_abs / deposit) if (deposit and deposit > 0) else Decimal("0")

    # порог по train (worst_winrate sweep)
    worst_rows = await _load_positions_with_worst_winrate(pg, scenario_id, signal_id, direction)
    best_threshold = _compute_best_threshold_train(
        rows=worst_rows,
        train_uids=train_uids,
        deposit=deposit,
    )

    # bins_stat (все бины, которые попадались в статистике)
    bins_rows = await _load_bins_stat_rows(pg, scenario_id, signal_id, direction)
    if not bins_rows:
        # пишем пустую модель+labels
        model_id = await _upsert_model_opt_v2_return_id(
            pg=pg,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            best_threshold=best_threshold,
            selected_analysis_ids=[],
            orig_trades=orig_trades,
            orig_pnl_abs=orig_pnl_abs,
            orig_winrate=orig_winrate,
            orig_roi=orig_roi,
            filt_trades=orig_trades,
            filt_pnl_abs=orig_pnl_abs,
            filt_winrate=orig_winrate,
            filt_roi=orig_roi,
            removed_trades=0,
            removed_accuracy=Decimal("0"),
            meta_obj={
                "version": 2,
                "method": "bad=v3_toggle; good=winrate_gt_X; labels=all_bins; prune=off (no_bins_stat_rows)",
                "direction": direction,
                "deposit": str(deposit) if deposit is not None else None,
                "lambda": str(V2_LAMBDA),
                "holdout": {"days": HOLDOUT_DAYS, "used": bool(val_used), "window": val_window},
                "threshold": str(best_threshold),
                "good_threshold_sweep": {
                    "from": str(GOOD_WINRATE_MIN),
                    "to": str(GOOD_WINRATE_MAX),
                    "step": str(GOOD_WINRATE_STEP),
                },
                "good_threshold_selected": str(GOOD_WINRATE_MIN),
                "prune": {"enabled": False, "note": "no_bins_stat_rows"},
            },
            source_finished_at=source_finished_at,
        )

        labels_total, bad_cnt, good_cnt, neutral_cnt, inactive_cnt = await _rebuild_labels_v2_all_bins(
            pg=pg,
            model_id=model_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            threshold_used=best_threshold,
            good_threshold_selected=GOOD_WINRATE_MIN,
            bins_rows=[],
            bad_bins_active=set(),
            bad_bins_inactive=set(),
        )

        return {
            "direction": direction,
            "best_threshold": str(best_threshold),
            "orig_roi": str(_q_decimal(orig_roi)),
            "filt_roi": str(_q_decimal(orig_roi)),
            "train_roi": str(_q_decimal(orig_roi)),
            "val_roi": str(_q_decimal(orig_roi)),
            "score": str(_q_decimal(orig_roi)),
            "labels_total": labels_total,
            "bad_bins": bad_cnt,
            "good_bins": good_cnt,
            "neutral_bins": neutral_cnt,
            "inactive_bins": inactive_cnt,
            "pruned_analysis_ids": [],
        }

    # raw hits index
    hits_index = await _load_hits_index_for_direction(pg, scenario_id, signal_id, direction)

    # кандидаты — только те, у которых есть hits (для оптимизации bad-набора)
    bin_by_key: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    for b in bins_rows:
        k = (int(b["analysis_id"]), str(b["timeframe"]), str(b["bin_name"]))
        if hits_index.get(k):
            bin_by_key[k] = b

    # стартовый bad-набор: winrate <= threshold
    active_bad_bins: Set[Tuple[int, str, str]] = set()
    for k, b in bin_by_key.items():
        if _safe_decimal(b["winrate"]) <= best_threshold:
            active_bad_bins.add(k)

    # пул на включение (рядом с порогом)
    enable_pool: Set[Tuple[int, str, str]] = set()
    thr_hi = best_threshold + NEAR_THRESHOLD_MARGIN
    for k, b in bin_by_key.items():
        if _safe_decimal(b["winrate"]) <= thr_hi:
            enable_pool.add(k)

    # hits train/val
    hits_train: Dict[Tuple[int, str, str], Set[Any]] = {}
    hits_val: Dict[Tuple[int, str, str], Set[Any]] = {}

    for k, hits in hits_index.items():
        if k not in bin_by_key:
            continue

        ht = hits.intersection(train_uids)
        if ht:
            hits_train[k] = ht

        if val_used:
            hv = hits.intersection(val_uids)
            if hv:
                hits_val[k] = hv

    # init state + optimize bad bins (v3-подобно)
    state = _init_state_counts(
        train_uids=train_uids,
        val_uids=val_uids,
        val_used=val_used,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
        hits_train=hits_train,
        hits_val=hits_val,
        active_bad_bins=active_bad_bins,
        deposit=deposit,
    )

    bad_bins_active, iters_used, steps = _optimize_bad_bins_by_score(
        state=state,
        enable_pool=enable_pool,
        max_iters=MAX_TOGGLE_ITERS,
        max_bad_bins=MAX_BAD_BINS_LIMIT,
    )

    # финальные kept/rem по bad (до good)
    removed_all: Set[Any] = set()
    for k in bad_bins_active:
        removed_all |= hits_index.get(k, set())
    kept_after_bad = set(uid for uid in all_uids if uid not in removed_all)

    # подбор good_threshold по max filt_roi (на всём окне) для whitelist
    good_sel = _select_best_good_threshold(
        all_uids=all_uids,
        kept_after_bad=kept_after_bad,
        bins_rows=bins_rows,
        hits_index=hits_index,
        bad_bins_set=bad_bins_active,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
        deposit=deposit,
    )

    good_threshold_selected = good_sel["good_threshold_selected"]
    kept_final = set(good_sel["kept_uids"])

    # базовые filt метрики (до prune)
    base_filt_trades, base_filt_pnl_abs, base_filt_winrate, base_filt_roi = _calc_filt_metrics(
        kept_uids=kept_final,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
        deposit=deposit,
    )
    base_removed_trades, base_removed_accuracy, base_removed_winners = _calc_removed_metrics(
        all_uids=all_uids,
        kept_uids=kept_final,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
    )
    base_filt_roi_val = _calc_filt_roi_on_subset(
        kept_uids=kept_final,
        subset_uids=val_uids if val_used else set(),
        pos_pnl=pos_pnl,
        deposit=deposit,
    ) if val_used else base_filt_roi

    # prune: только BAD -> inactive (не меняем good)
    pruned_analysis_ids: List[int] = []
    bad_bins_inactive: Set[Tuple[int, str, str]] = set()

    if PRUNE_ENABLE and bad_bins_active:
        # условия достаточности
        pr = _prune_only_bad_by_analysis_id(
            all_uids=all_uids,
            val_uids=val_uids,
            val_used=val_used,
            pos_pnl=pos_pnl,
            pos_win=pos_win,
            deposit=deposit,
            hits_index=hits_index,
            bins_rows=bins_rows,
            bad_bins_active=set(bad_bins_active),
            good_threshold_selected=good_threshold_selected,
            base_filt_roi=base_filt_roi,
            base_filt_roi_val=base_filt_roi_val,
            base_removed_winners=base_removed_winners,
            max_iters=PRUNE_MAX_ITERS,
        )
        pruned_analysis_ids = pr["pruned_analysis_ids"]
        bad_bins_active = pr["bad_bins_active"]
        bad_bins_inactive = pr["bad_bins_inactive"]

    # пересчёт финальных filt метрик (после prune)
    # условия достаточности
    has_good_map = _build_has_good_map(
        all_uids=all_uids,
        hits_index=hits_index,
        bins_rows=bins_rows,
        bad_bins_all=(set(bad_bins_active) | set(bad_bins_inactive)),
        good_threshold_selected=good_threshold_selected,
    )
    kept_final_after_prune = _calc_kept_uids_from_sets(
        all_uids=all_uids,
        hits_index=hits_index,
        bad_bins_active=bad_bins_active,
        has_good_map=has_good_map,
    )

    filt_trades, filt_pnl_abs, filt_winrate, filt_roi = _calc_filt_metrics(
        kept_uids=kept_final_after_prune,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
        deposit=deposit,
    )

    removed_trades, removed_accuracy, removed_winners = _calc_removed_metrics(
        all_uids=all_uids,
        kept_uids=kept_final_after_prune,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
    )

    selected_analysis_ids = sorted({int(k[0]) for k in bad_bins_active})

    meta_obj = {
        "version": 2,
        "method": "bad=v3_toggle; good=winrate_gt_X; labels=all_bins; prune=only_bad_by_analysis_id",
        "direction": direction,
        "deposit": str(deposit) if deposit is not None else None,
        "lambda": str(V2_LAMBDA),
        "holdout": {"days": HOLDOUT_DAYS, "used": bool(val_used), "window": val_window},
        "threshold": str(best_threshold),
        "near_threshold_margin": str(NEAR_THRESHOLD_MARGIN),
        "good_threshold_sweep": {
            "from": str(GOOD_WINRATE_MIN),
            "to": str(GOOD_WINRATE_MAX),
            "step": str(GOOD_WINRATE_STEP),
        },
        "good_threshold_selected": str(good_threshold_selected),
        "bad_bins": {
            "final_active": int(len(bad_bins_active)),
            "final_inactive": int(len(bad_bins_inactive)),
            "iters_used": int(iters_used),
        },
        "score": {
            "train_roi": str(_q_decimal(state.get("roi_train", Decimal("0")))),
            "val_roi": str(_q_decimal(state.get("roi_val", Decimal("0")))),
            "score": str(_q_decimal(state.get("score", Decimal("0")))),
        },
        "steps": steps,
        "prune": {
            "enabled": bool(PRUNE_ENABLE),
            "only_bad": True,
            "pruned_analysis_ids": pruned_analysis_ids,
            "baseline": {
                "filt_roi": str(_q_decimal(base_filt_roi)),
                "filt_roi_val": str(_q_decimal(base_filt_roi_val)),
                "removed_winners": int(base_removed_winners),
                "removed_accuracy": str(_q_decimal(base_removed_accuracy)),
            },
            "final": {
                "filt_roi": str(_q_decimal(filt_roi)),
                "removed_winners": int(removed_winners),
                "removed_accuracy": str(_q_decimal(removed_accuracy)),
            },
        },
    }

    # upsert model_opt_v2
    model_id = await _upsert_model_opt_v2_return_id(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=best_threshold,
        selected_analysis_ids=selected_analysis_ids,
        orig_trades=orig_trades,
        orig_pnl_abs=orig_pnl_abs,
        orig_winrate=orig_winrate,
        orig_roi=orig_roi,
        filt_trades=filt_trades,
        filt_pnl_abs=filt_pnl_abs,
        filt_winrate=filt_winrate,
        filt_roi=filt_roi,
        removed_trades=removed_trades,
        removed_accuracy=removed_accuracy,
        meta_obj=meta_obj,
        source_finished_at=source_finished_at,
    )

    # rebuild labels_v2: все строки из bins_stat, state=bad/good/neutral/inactive
    labels_total, bad_cnt, good_cnt, neutral_cnt, inactive_cnt = await _rebuild_labels_v2_all_bins(
        pg=pg,
        model_id=model_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        threshold_used=best_threshold,
        good_threshold_selected=good_threshold_selected,
        bins_rows=bins_rows,
        bad_bins_active=bad_bins_active,
        bad_bins_inactive=bad_bins_inactive,
    )

    return {
        "direction": direction,
        "best_threshold": str(best_threshold),
        "orig_roi": str(_q_decimal(orig_roi)),
        "filt_roi": str(_q_decimal(filt_roi)),
        "train_roi": str(_q_decimal(state.get("roi_train", Decimal("0")))),
        "val_roi": str(_q_decimal(state.get("roi_val", Decimal("0")))),
        "score": str(_q_decimal(state.get("score", Decimal("0")))),
        "labels_total": labels_total,
        "bad_bins": bad_cnt,
        "good_bins": good_cnt,
        "neutral_bins": neutral_cnt,
        "inactive_bins": inactive_cnt,
        "pruned_analysis_ids": pruned_analysis_ids,
    }


# 🔸 Инициализация состояния (counts + kept-агрегаты) для bad-набора (как v3)
def _init_state_counts(
    train_uids: Set[Any],
    val_uids: Set[Any],
    val_used: bool,
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
    hits_train: Dict[Tuple[int, str, str], Set[Any]],
    hits_val: Dict[Tuple[int, str, str], Set[Any]],
    active_bad_bins: Set[Tuple[int, str, str]],
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    # базовые kept = все
    train_kept_trades = len(train_uids)
    train_kept_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in train_uids), Decimal("0"))
    train_kept_wins = sum(1 for uid in train_uids if pos_win.get(uid))

    val_kept_trades = len(val_uids) if val_used else 0
    val_kept_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in val_uids), Decimal("0")) if val_used else Decimal("0")
    val_kept_wins = sum(1 for uid in val_uids if pos_win.get(uid)) if val_used else 0

    # counts
    bad_count_train: Dict[Any, int] = {}
    bad_count_val: Dict[Any, int] = {}

    # применяем активные bad бинны
    for k in active_bad_bins:
        ht = hits_train.get(k) or set()
        for uid in ht:
            c = bad_count_train.get(uid, 0)
            if c == 0:
                train_kept_trades -= 1
                train_kept_pnl -= pos_pnl.get(uid, Decimal("0"))
                if pos_win.get(uid):
                    train_kept_wins -= 1
            bad_count_train[uid] = c + 1

        if val_used:
            hv = hits_val.get(k) or set()
            for uid in hv:
                c = bad_count_val.get(uid, 0)
                if c == 0:
                    val_kept_trades -= 1
                    val_kept_pnl -= pos_pnl.get(uid, Decimal("0"))
                    if pos_win.get(uid):
                        val_kept_wins -= 1
                bad_count_val[uid] = c + 1

    state: Dict[str, Any] = {
        "deposit": deposit,
        "val_used": val_used,
        "hits_train": hits_train,
        "hits_val": hits_val,
        "pos_pnl": pos_pnl,
        "pos_win": pos_win,
        "active": set(active_bad_bins),
        "bad_count_train": bad_count_train,
        "bad_count_val": bad_count_val,
        "train_kept_trades": train_kept_trades,
        "train_kept_pnl": train_kept_pnl,
        "train_kept_wins": train_kept_wins,
        "val_kept_trades": val_kept_trades,
        "val_kept_pnl": val_kept_pnl,
        "val_kept_wins": val_kept_wins,
        "roi_train": Decimal("0"),
        "roi_val": Decimal("0"),
        "score": Decimal("0"),
    }

    _recalc_score(state)
    return state


# 🔸 Пересчёт score: score = train_roi - λ * max(0, train_roi - val_roi)
def _recalc_score(state: Dict[str, Any]) -> None:
    deposit = state.get("deposit")
    val_used = bool(state.get("val_used"))

    if deposit and deposit > 0:
        try:
            roi_train = state["train_kept_pnl"] / deposit
        except (InvalidOperation, ZeroDivisionError):
            roi_train = Decimal("0")
    else:
        roi_train = Decimal("0")

    if val_used:
        if deposit and deposit > 0:
            try:
                roi_val = state["val_kept_pnl"] / deposit
            except (InvalidOperation, ZeroDivisionError):
                roi_val = Decimal("0")
        else:
            roi_val = Decimal("0")
    else:
        roi_val = roi_train

    drop = roi_train - roi_val
    if drop > 0:
        score = roi_train - (V2_LAMBDA * drop)
    else:
        score = roi_train

    state["roi_train"] = roi_train
    state["roi_val"] = roi_val
    state["score"] = score


# 🔸 Оптимизация bad-биннов (enable/disable по одному) по score (как v3)
def _optimize_bad_bins_by_score(
    state: Dict[str, Any],
    enable_pool: Set[Tuple[int, str, str]],
    max_iters: int,
    max_bad_bins: int,
) -> Tuple[Set[Tuple[int, str, str]], int, List[Dict[str, Any]]]:
    steps: List[Dict[str, Any]] = []

    active: Set[Tuple[int, str, str]] = state["active"]
    hits_train: Dict[Tuple[int, str, str], Set[Any]] = state["hits_train"]
    hits_val: Dict[Tuple[int, str, str], Set[Any]] = state["hits_val"]

    pos_pnl: Dict[Any, Decimal] = state["pos_pnl"]
    pos_win: Dict[Any, bool] = state["pos_win"]

    bad_count_train: Dict[Any, int] = state["bad_count_train"]
    bad_count_val: Dict[Any, int] = state["bad_count_val"]

    val_used = bool(state.get("val_used"))

    iters_used = 0

    for it in range(int(max_iters or 0)):
        iters_used = it + 1

        best_move = None
        best_new_score = state["score"]

        # disable
        for k in list(active):
            ht = hits_train.get(k) or set()

            delta_train_pnl = Decimal("0")
            delta_val_pnl = Decimal("0")

            for uid in ht:
                if bad_count_train.get(uid, 0) == 1:
                    delta_train_pnl += pos_pnl.get(uid, Decimal("0"))

            if val_used:
                hv = hits_val.get(k) or set()
                for uid in hv:
                    if bad_count_val.get(uid, 0) == 1:
                        delta_val_pnl += pos_pnl.get(uid, Decimal("0"))

            snap = {
                "deposit": state.get("deposit"),
                "val_used": val_used,
                "train_kept_pnl": state["train_kept_pnl"] + delta_train_pnl,
                "val_kept_pnl": state["val_kept_pnl"] + delta_val_pnl,
            }
            new_score, new_rt, new_rv = _calc_score_from_pnl(snap)

            if new_score > best_new_score + EPS_SCORE:
                best_new_score = new_score
                best_move = ("disable", k, delta_train_pnl, delta_val_pnl, new_rt, new_rv, new_score)

        # enable
        if len(active) < int(max_bad_bins or 0):
            for k in enable_pool:
                if k in active:
                    continue

                ht = hits_train.get(k) or set()
                if not ht:
                    continue

                delta_train_pnl = Decimal("0")
                for uid in ht:
                    if bad_count_train.get(uid, 0) == 0:
                        delta_train_pnl += pos_pnl.get(uid, Decimal("0"))

                if delta_train_pnl == 0:
                    continue

                delta_val_pnl = Decimal("0")
                if val_used:
                    hv = hits_val.get(k) or set()
                    for uid in hv:
                        if bad_count_val.get(uid, 0) == 0:
                            delta_val_pnl += pos_pnl.get(uid, Decimal("0"))

                snap = {
                    "deposit": state.get("deposit"),
                    "val_used": val_used,
                    "train_kept_pnl": state["train_kept_pnl"] - delta_train_pnl,
                    "val_kept_pnl": state["val_kept_pnl"] - delta_val_pnl,
                }
                new_score, new_rt, new_rv = _calc_score_from_pnl(snap)

                if new_score > best_new_score + EPS_SCORE:
                    best_new_score = new_score
                    best_move = ("enable", k, delta_train_pnl, delta_val_pnl, new_rt, new_rv, new_score)

        if best_move is None:
            break

        action, k, dp_tr, dp_v, new_rt, new_rv, new_sc = best_move

        if action == "disable":
            ht = hits_train.get(k) or set()
            for uid in ht:
                c = bad_count_train.get(uid, 0)
                if c <= 0:
                    continue
                bad_count_train[uid] = c - 1
                if c == 1:
                    state["train_kept_trades"] += 1
                    state["train_kept_pnl"] += pos_pnl.get(uid, Decimal("0"))
                    if pos_win.get(uid):
                        state["train_kept_wins"] += 1
                    if bad_count_train[uid] == 0:
                        bad_count_train.pop(uid, None)

            if val_used:
                hv = hits_val.get(k) or set()
                for uid in hv:
                    c = bad_count_val.get(uid, 0)
                    if c <= 0:
                        continue
                    bad_count_val[uid] = c - 1
                    if c == 1:
                        state["val_kept_trades"] += 1
                        state["val_kept_pnl"] += pos_pnl.get(uid, Decimal("0"))
                        if pos_win.get(uid):
                            state["val_kept_wins"] += 1
                        if bad_count_val[uid] == 0:
                            bad_count_val.pop(uid, None)

            active.discard(k)

        else:
            ht = hits_train.get(k) or set()
            for uid in ht:
                c = bad_count_train.get(uid, 0)
                if c == 0:
                    state["train_kept_trades"] -= 1
                    state["train_kept_pnl"] -= pos_pnl.get(uid, Decimal("0"))
                    if pos_win.get(uid):
                        state["train_kept_wins"] -= 1
                bad_count_train[uid] = c + 1

            if val_used:
                hv = hits_val.get(k) or set()
                for uid in hv:
                    c = bad_count_val.get(uid, 0)
                    if c == 0:
                        state["val_kept_trades"] -= 1
                        state["val_kept_pnl"] -= pos_pnl.get(uid, Decimal("0"))
                        if pos_win.get(uid):
                            state["val_kept_wins"] -= 1
                    bad_count_val[uid] = c + 1

            active.add(k)

        _recalc_score(state)

        steps.append(
            {
                "step": it + 1,
                "action": action,
                "analysis_id": int(k[0]),
                "timeframe": str(k[1]),
                "bin_name": str(k[2]),
                "roi_train": str(_q_decimal(state["roi_train"])),
                "roi_val": str(_q_decimal(state["roi_val"])),
                "score": str(_q_decimal(state["score"])),
                "bad_bins": int(len(active)),
            }
        )
        if len(steps) >= 80:
            steps.append({"note": "steps_truncated"})
            break

    return set(active), iters_used, steps


# 🔸 Быстрый расчёт score по pnl (без полного пересчёта состояния)
def _calc_score_from_pnl(snap: Dict[str, Any]) -> Tuple[Decimal, Decimal, Decimal]:
    deposit = snap.get("deposit")
    val_used = bool(snap.get("val_used"))

    train_pnl = snap.get("train_kept_pnl", Decimal("0"))
    val_pnl = snap.get("val_kept_pnl", Decimal("0"))

    if deposit and deposit > 0:
        try:
            roi_train = train_pnl / deposit
        except (InvalidOperation, ZeroDivisionError):
            roi_train = Decimal("0")
    else:
        roi_train = Decimal("0")

    if val_used:
        if deposit and deposit > 0:
            try:
                roi_val = val_pnl / deposit
            except (InvalidOperation, ZeroDivisionError):
                roi_val = Decimal("0")
        else:
            roi_val = Decimal("0")
    else:
        roi_val = roi_train

    drop = roi_train - roi_val
    if drop > 0:
        score = roi_train - (V2_LAMBDA * drop)
    else:
        score = roi_train

    return score, roi_train, roi_val


# 🔸 Split train/val по exit_time (последние HOLDOUT_DAYS — val)
def _split_train_val_uids(
    uids: Set[Any],
    pos_exit_time: Dict[Any, datetime],
    holdout_days: int,
) -> Tuple[Set[Any], Set[Any], bool, Dict[str, Any]]:
    max_ts = None
    for uid in uids:
        ts = pos_exit_time.get(uid)
        if ts is None:
            continue
        if max_ts is None or ts > max_ts:
            max_ts = ts

    if max_ts is None:
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

    if not train or not val:
        return set(uids), set(), False, {"mode": "none", "reason": "empty_split", "train": len(train), "val": len(val)}

    return train, val, True, {"mode": "exit_time_days", "days": int(holdout_days), "cut": cut.isoformat(), "train": len(train), "val": len(val)}


# 🔸 Sweep: расчёт оптимального порога по train (worst_winrate)
def _compute_best_threshold_train(
    rows: List[Dict[str, Any]],
    train_uids: Set[Any],
    deposit: Optional[Decimal],
) -> Decimal:
    train_rows = [r for r in rows if r["position_uid"] in train_uids and r.get("worst_winrate") is not None]
    if not train_rows:
        return Decimal("0")

    orig_trades = len(train_rows)
    orig_pnl = sum((r["pnl_abs"] for r in train_rows), Decimal("0"))

    if deposit and deposit > 0:
        try:
            orig_roi = orig_pnl / deposit
        except (InvalidOperation, ZeroDivisionError):
            orig_roi = Decimal("0")
    else:
        orig_roi = Decimal("0")

    groups: Dict[Decimal, Dict[str, Any]] = {}
    for r in train_rows:
        w = r["worst_winrate"]
        if w is None:
            continue
        g = groups.setdefault(w, {"trades": 0, "pnl": Decimal("0")})
        g["trades"] += 1
        g["pnl"] += r["pnl_abs"]

    unique_w = sorted(groups.keys())
    if not unique_w:
        return Decimal("0")

    best_threshold = Decimal("0")
    best_filt_trades = orig_trades
    best_objective = orig_roi

    removed_trades = 0
    removed_pnl = Decimal("0")

    for v in unique_w:
        g = groups[v]
        removed_trades += int(g["trades"])
        removed_pnl += g["pnl"]

        filt_trades = orig_trades - removed_trades
        filt_pnl = orig_pnl - removed_pnl

        if deposit and deposit > 0:
            try:
                filt_roi = filt_pnl / deposit
            except (InvalidOperation, ZeroDivisionError):
                filt_roi = Decimal("0")
            objective = filt_roi
        else:
            objective = filt_pnl

        threshold = v + EPS_THRESHOLD

        if objective > best_objective:
            best_objective = objective
            best_threshold = threshold
            best_filt_trades = filt_trades
        elif objective == best_objective:
            if filt_trades > best_filt_trades:
                best_threshold = threshold
                best_filt_trades = filt_trades
            elif filt_trades == best_filt_trades and threshold < best_threshold:
                best_threshold = threshold

    return _q_decimal(best_threshold)


# 🔸 Подбор best GOOD_WINRATE_THRESHOLD по max filt_roi (на всём окне), при фиксированном bad_set
def _select_best_good_threshold(
    all_uids: Set[Any],
    kept_after_bad: Set[Any],
    bins_rows: List[Dict[str, Any]],
    hits_index: Dict[Tuple[int, str, str], Set[Any]],
    bad_bins_set: Set[Tuple[int, str, str]],
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
    deposit: Optional[Decimal],
) -> Dict[str, Any]:
    # best_good_winrate для каждой позиции (только среди биннов НЕ bad)
    best_win: Dict[Any, Optional[Decimal]] = {uid: None for uid in kept_after_bad}

    # строим map winrate по key
    winrate_by_key: Dict[Tuple[int, str, str], Decimal] = {}
    for b in bins_rows:
        k = (int(b["analysis_id"]), str(b["timeframe"]), str(b["bin_name"]))
        winrate_by_key[k] = _safe_decimal(b.get("winrate", 0))

    for k, hits in hits_index.items():
        if k in bad_bins_set:
            continue
        w = winrate_by_key.get(k)
        if w is None:
            continue
        for uid in hits:
            if uid not in best_win:
                continue
            cur = best_win.get(uid)
            if cur is None or w > cur:
                best_win[uid] = w

    # перебор порогов 0.50..1.00 step 0.01
    best_thr = GOOD_WINRATE_MIN
    best_roi = Decimal("-999999")
    best_trades = 0
    best_pnl = Decimal("0")
    best_winrate = Decimal("0")
    best_kept_uids: Set[Any] = set()

    thr = GOOD_WINRATE_MIN
    while thr <= GOOD_WINRATE_MAX + Decimal("0.000000001"):
        kept = []
        for uid in kept_after_bad:
            w = best_win.get(uid)
            if w is not None and w > thr:
                kept.append(uid)

        trades = len(kept)
        pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in kept), Decimal("0"))
        wins = sum(1 for uid in kept if pos_win.get(uid))

        if trades > 0:
            wr = Decimal(wins) / Decimal(trades)
        else:
            wr = Decimal("0")

        if deposit and deposit > 0:
            try:
                roi = pnl / deposit
            except (InvalidOperation, ZeroDivisionError):
                roi = Decimal("0")
        else:
            roi = Decimal("0")

        # выбираем max filt_roi; при равенстве — больше trades; потом меньший thr
        if roi > best_roi:
            best_roi = roi
            best_thr = thr
            best_trades = trades
            best_pnl = pnl
            best_winrate = wr
            best_kept_uids = set(kept)
        elif roi == best_roi:
            if trades > best_trades:
                best_thr = thr
                best_trades = trades
                best_pnl = pnl
                best_winrate = wr
                best_kept_uids = set(kept)
            elif trades == best_trades and thr < best_thr:
                best_thr = thr
                best_kept_uids = set(kept)

        thr += GOOD_WINRATE_STEP

    return {
        "good_threshold_selected": best_thr,
        "filt_trades": int(best_trades),
        "filt_pnl_abs": best_pnl,
        "filt_winrate": best_winrate,
        "filt_roi": best_roi,
        "kept_uids": best_kept_uids,
    }


# 🔸 Prune: greedy disable BAD по analysis_id (перевод bad->inactive для всех его bad биннов)
def _prune_only_bad_by_analysis_id(
    all_uids: Set[Any],
    val_uids: Set[Any],
    val_used: bool,
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
    deposit: Optional[Decimal],
    hits_index: Dict[Tuple[int, str, str], Set[Any]],
    bins_rows: List[Dict[str, Any]],
    bad_bins_active: Set[Tuple[int, str, str]],
    good_threshold_selected: Decimal,
    base_filt_roi: Decimal,
    base_filt_roi_val: Decimal,
    base_removed_winners: int,
    max_iters: int,
) -> Dict[str, Any]:
    # условия достаточности: нужны позиции + bad
    if not all_uids or not bad_bins_active:
        return {
            "pruned_analysis_ids": [],
            "bad_bins_active": set(bad_bins_active),
            "bad_bins_inactive": set(),
        }

    # has_good фиксирован (good не трогаем)
    has_good_map = _build_has_good_map(
        all_uids=all_uids,
        hits_index=hits_index,
        bins_rows=bins_rows,
        bad_bins_all=set(bad_bins_active),
        good_threshold_selected=good_threshold_selected,
    )

    # bad_count по uid + mapping analysis_id -> uid->count
    bad_count_by_uid: Dict[Any, int] = {}
    bad_hits_by_analysis: Dict[int, Dict[Any, int]] = {}

    for k in bad_bins_active:
        aid = int(k[0])
        hits = hits_index.get(k) or set()
        if not hits:
            continue
        a_map = bad_hits_by_analysis.setdefault(aid, {})
        for uid in hits:
            bad_count_by_uid[uid] = bad_count_by_uid.get(uid, 0) + 1
            a_map[uid] = a_map.get(uid, 0) + 1

    # текущие kept и агрегаты
    kept_uids = set()
    kept_trades = 0
    kept_pnl = Decimal("0")
    kept_wins = 0

    kept_pnl_val = Decimal("0")
    kept_trades_val = 0

    removed_trades = 0
    removed_losers = 0
    removed_winners = 0

    for uid in all_uids:
        has_bad = bad_count_by_uid.get(uid, 0) > 0
        has_good = bool(has_good_map.get(uid))
        pnl = pos_pnl.get(uid, Decimal("0"))
        win = bool(pos_win.get(uid))

        if (not has_bad) and has_good:
            kept_uids.add(uid)
            kept_trades += 1
            kept_pnl += pnl
            kept_wins += 1 if win else 0
            if val_used and uid in val_uids:
                kept_trades_val += 1
                kept_pnl_val += pnl
        else:
            removed_trades += 1
            if win:
                removed_winners += 1
            else:
                removed_losers += 1

    cur_filt_roi = (kept_pnl / deposit) if (deposit and deposit > 0) else Decimal("0")
    cur_filt_roi_val = (kept_pnl_val / deposit) if (val_used and deposit and deposit > 0) else cur_filt_roi
    cur_removed_accuracy = (Decimal(removed_losers) / Decimal(removed_trades)) if removed_trades > 0 else Decimal("0")
    cur_removed_winners = removed_winners

    disabled: Set[int] = set()
    bad_bins_inactive: Set[Tuple[int, str, str]] = set()

    # greedy итерации
    for it in range(int(max_iters or 0)):
        best_candidate = None
        best_new_roi = cur_filt_roi
        best_new_roi_val = cur_filt_roi_val
        best_new_removed_winners = cur_removed_winners
        best_new_removed_accuracy = cur_removed_accuracy

        # перебор analysis_id-кандидатов
        for aid, uid_cnt in bad_hits_by_analysis.items():
            if aid in disabled:
                continue

            delta_kept_trades = 0
            delta_kept_pnl = Decimal("0")
            delta_kept_wins = 0

            delta_kept_pnl_val = Decimal("0")
            delta_kept_trades_val = 0

            delta_removed_trades = 0
            delta_removed_losers = 0
            delta_removed_winners = 0

            for uid, dec in uid_cnt.items():
                cur_cnt = bad_count_by_uid.get(uid, 0)
                if cur_cnt <= 0:
                    continue
                new_cnt = cur_cnt - int(dec)
                if new_cnt > 0:
                    continue

                # bad снят полностью
                if uid in kept_uids:
                    # уже kept — ничего не меняется
                    continue

                # мог стать kept, только если есть good
                if not has_good_map.get(uid):
                    continue

                pnl = pos_pnl.get(uid, Decimal("0"))
                win = bool(pos_win.get(uid))

                delta_kept_trades += 1
                delta_kept_pnl += pnl
                delta_kept_wins += 1 if win else 0

                if val_used and uid in val_uids:
                    delta_kept_trades_val += 1
                    delta_kept_pnl_val += pnl

                delta_removed_trades += 1
                if win:
                    delta_removed_winners += 1
                else:
                    delta_removed_losers += 1

            if delta_kept_trades <= 0:
                continue

            new_kept_pnl = kept_pnl + delta_kept_pnl
            new_kept_pnl_val = kept_pnl_val + delta_kept_pnl_val

            new_roi = (new_kept_pnl / deposit) if (deposit and deposit > 0) else Decimal("0")
            new_roi_val = (new_kept_pnl_val / deposit) if (val_used and deposit and deposit > 0) else new_roi

            new_removed_trades = removed_trades - delta_removed_trades
            new_removed_losers = removed_losers - delta_removed_losers
            new_removed_winners = removed_winners - delta_removed_winners

            new_removed_accuracy = (Decimal(new_removed_losers) / Decimal(new_removed_trades)) if new_removed_trades > 0 else Decimal("0")

            # условия достаточности:
            # 1) общий ROI должен расти
            # 2) holdout ROI не должен падать
            # 3) winners_removed не должен ухудшаться
            if new_roi <= cur_filt_roi + EPS_ROI:
                continue
            if val_used and new_roi_val + EPS_ROI < cur_filt_roi_val:
                continue
            if new_removed_winners > cur_removed_winners:
                continue

            # выбрать лучший
            if new_roi > best_new_roi + EPS_ROI:
                best_candidate = aid
                best_new_roi = new_roi
                best_new_roi_val = new_roi_val
                best_new_removed_winners = new_removed_winners
                best_new_removed_accuracy = new_removed_accuracy
            elif new_roi == best_new_roi:
                if new_removed_winners < best_new_removed_winners:
                    best_candidate = aid
                    best_new_roi = new_roi
                    best_new_roi_val = new_roi_val
                    best_new_removed_winners = new_removed_winners
                    best_new_removed_accuracy = new_removed_accuracy
                elif new_removed_winners == best_new_removed_winners and new_removed_accuracy > best_new_removed_accuracy:
                    best_candidate = aid
                    best_new_roi = new_roi
                    best_new_roi_val = new_roi_val
                    best_new_removed_winners = new_removed_winners
                    best_new_removed_accuracy = new_removed_accuracy

        if best_candidate is None:
            break

        # применяем best_candidate
        aid = int(best_candidate)
        disabled.add(aid)

        uid_cnt = bad_hits_by_analysis.get(aid) or {}
        for uid, dec in uid_cnt.items():
            cur_cnt = bad_count_by_uid.get(uid, 0)
            if cur_cnt <= 0:
                continue
            bad_count_by_uid[uid] = cur_cnt - int(dec)
            if bad_count_by_uid[uid] <= 0:
                bad_count_by_uid.pop(uid, None)

        # обновляем kept/removed агрегаты "точно" (пересчёт по delta проще сделать в лоб по affected uids)
        # условия достаточности: для читаемости и надёжности в маленьком prune-проходе
        for uid, dec in uid_cnt.items():
            # если у позиции больше нет bad и есть good — она может стать kept
            if uid in kept_uids:
                continue
            if bad_count_by_uid.get(uid, 0) > 0:
                continue
            if not has_good_map.get(uid):
                continue

            pnl = pos_pnl.get(uid, Decimal("0"))
            win = bool(pos_win.get(uid))

            kept_uids.add(uid)
            kept_trades += 1
            kept_pnl += pnl
            kept_wins += 1 if win else 0

            if val_used and uid in val_uids:
                kept_trades_val += 1
                kept_pnl_val += pnl

            removed_trades -= 1
            if win:
                removed_winners -= 1
            else:
                removed_losers -= 1

        # обновляем текущие метрики
        cur_filt_roi = (kept_pnl / deposit) if (deposit and deposit > 0) else Decimal("0")
        cur_filt_roi_val = (kept_pnl_val / deposit) if (val_used and deposit and deposit > 0) else cur_filt_roi
        cur_removed_accuracy = (Decimal(removed_losers) / Decimal(removed_trades)) if removed_trades > 0 else Decimal("0")
        cur_removed_winners = removed_winners

        log.debug(
            "BT_ANALYSIS_PREPROC_V2: prune step=%s direction=%s disabled_aid=%s roi=%s roi_val=%s removed_winners=%s removed_accuracy=%s",
            it + 1,
            "n/a",
            aid,
            str(_q_decimal(cur_filt_roi)),
            str(_q_decimal(cur_filt_roi_val)),
            int(cur_removed_winners),
            str(_q_decimal(cur_removed_accuracy)),
        )

    # формируем inactive бинны: все bad бинны для отключённых analysis_id
    for k in list(bad_bins_active):
        if int(k[0]) in disabled:
            bad_bins_active.discard(k)
            bad_bins_inactive.add(k)

    # итоговый sanity: должно быть лучше базового по ROI (если что-то отключили)
    if disabled:
        if cur_filt_roi <= base_filt_roi + EPS_ROI:
            # условия достаточности: если улучшения не получилось — откат (без усложнений)
            return {
                "pruned_analysis_ids": [],
                "bad_bins_active": set(bad_bins_active | bad_bins_inactive),
                "bad_bins_inactive": set(),
            }

    return {
        "pruned_analysis_ids": sorted(disabled),
        "bad_bins_active": set(bad_bins_active),
        "bad_bins_inactive": set(bad_bins_inactive),
    }


# 🔸 Построение has_good_map по текущему good_threshold (good = (winrate > thr) AND (бин не bad))
def _build_has_good_map(
    all_uids: Set[Any],
    hits_index: Dict[Tuple[int, str, str], Set[Any]],
    bins_rows: List[Dict[str, Any]],
    bad_bins_all: Set[Tuple[int, str, str]],
    good_threshold_selected: Decimal,
) -> Dict[Any, bool]:
    has_good: Dict[Any, bool] = {uid: False for uid in all_uids}

    for b in bins_rows:
        k = (int(b["analysis_id"]), str(b["timeframe"]), str(b["bin_name"]))
        if k in bad_bins_all:
            continue
        if _safe_decimal(b.get("winrate", 0)) <= good_threshold_selected:
            continue

        hits = hits_index.get(k) or set()
        for uid in hits:
            if uid in has_good:
                has_good[uid] = True

    return has_good


# 🔸 Расчёт kept_uids по bad_active + has_good
def _calc_kept_uids_from_sets(
    all_uids: Set[Any],
    hits_index: Dict[Tuple[int, str, str], Set[Any]],
    bad_bins_active: Set[Tuple[int, str, str]],
    has_good_map: Dict[Any, bool],
) -> Set[Any]:
    bad_hit: Set[Any] = set()
    for k in bad_bins_active:
        bad_hit |= (hits_index.get(k) or set())

    kept: Set[Any] = set()
    for uid in all_uids:
        if uid in bad_hit:
            continue
        if has_good_map.get(uid):
            kept.add(uid)
    return kept


# 🔸 Фильтр-метрики по kept_uids
def _calc_filt_metrics(
    kept_uids: Set[Any],
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
    deposit: Optional[Decimal],
) -> Tuple[int, Decimal, Decimal, Decimal]:
    trades = len(kept_uids)
    pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in kept_uids), Decimal("0"))
    wins = sum(1 for uid in kept_uids if pos_win.get(uid))
    winrate = (Decimal(wins) / Decimal(trades)) if trades > 0 else Decimal("0")
    roi = (pnl / deposit) if (deposit and deposit > 0) else Decimal("0")
    return int(trades), pnl, winrate, roi


# 🔸 Removed-метрики (включая winners_removed)
def _calc_removed_metrics(
    all_uids: Set[Any],
    kept_uids: Set[Any],
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
) -> Tuple[int, Decimal, int]:
    removed = [uid for uid in all_uids if uid not in kept_uids]
    removed_trades = len(removed)
    removed_losers = sum(1 for uid in removed if not pos_win.get(uid, False))
    removed_winners = sum(1 for uid in removed if pos_win.get(uid, False))
    removed_accuracy = (Decimal(removed_losers) / Decimal(removed_trades)) if removed_trades > 0 else Decimal("0")
    return int(removed_trades), removed_accuracy, int(removed_winners)


# 🔸 ROI на подмножестве (holdout)
def _calc_filt_roi_on_subset(
    kept_uids: Set[Any],
    subset_uids: Set[Any],
    pos_pnl: Dict[Any, Decimal],
    deposit: Optional[Decimal],
) -> Decimal:
    if not subset_uids:
        return Decimal("0")
    kept = [uid for uid in kept_uids if uid in subset_uids]
    pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in kept), Decimal("0"))
    if deposit and deposit > 0:
        try:
            return pnl / deposit
        except (InvalidOperation, ZeroDivisionError):
            return Decimal("0")
    return Decimal("0")


# 🔸 Загрузка позиций с worst_winrate
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


# 🔸 Загрузка direction_mask сигнала
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


# 🔸 Преобразование direction_mask -> направления
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


# 🔸 Загрузка позиций направления (postproc=true)
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


# 🔸 Загрузка bins_stat строк (все бины, которые попались в статистике)
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


# 🔸 Индекс попаданий raw (analysis_id, timeframe, bin_name) -> set(position_uid)
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


# 🔸 Пересборка labels_v2: вставляем ВСЕ бины из bins_stat со state bad/good/neutral/inactive
async def _rebuild_labels_v2_all_bins(
    pg,
    model_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    threshold_used: Decimal,
    good_threshold_selected: Decimal,
    bins_rows: List[Dict[str, Any]],
    bad_bins_active: Set[Tuple[int, str, str]],
    bad_bins_inactive: Set[Tuple[int, str, str]],
) -> Tuple[int, int, int, int, int]:
    async with pg.acquire() as conn:
        # чистим старые по model_id
        await conn.execute(
            """
            DELETE FROM bt_analysis_bins_labels_v2
            WHERE model_id = $1
            """,
            model_id,
        )

        if not bins_rows:
            return 0, 0, 0, 0, 0

        to_insert: List[Tuple[Any, ...]] = []
        bad_cnt = 0
        good_cnt = 0
        neutral_cnt = 0
        inactive_cnt = 0

        for b in bins_rows:
            aid = int(b["analysis_id"])
            tf = str(b["timeframe"])
            bn = str(b["bin_name"])
            winrate = _safe_decimal(b["winrate"])

            key = (aid, tf, bn)

            # state (повторяем логику v3 + prune inactive)
            if key in bad_bins_active:
                state = "bad"
                bad_cnt += 1
            elif key in bad_bins_inactive:
                state = "inactive"
                inactive_cnt += 1
            else:
                if winrate > good_threshold_selected:
                    state = "good"
                    good_cnt += 1
                else:
                    state = "neutral"
                    neutral_cnt += 1

            to_insert.append(
                (
                    model_id,
                    scenario_id,
                    signal_id,
                    direction,
                    aid,
                    b.get("indicator_param"),
                    tf,
                    bn,
                    state,
                    threshold_used,
                    int(b.get("trades", 0) or 0),
                    _safe_decimal(b.get("pnl_abs", 0)),
                    winrate,
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

    return len(to_insert), bad_cnt, good_cnt, neutral_cnt, inactive_cnt


# 🔸 Удаление модели по направлению (и каскад labels_v2)
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


# 🔸 Публикация события готовности препроцессинга v2
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
            "BT_ANALYSIS_PREPROC_V2: опубликовано событие preproc_ready_v2 в '%s' scenario_id=%s signal_id=%s",
            PREPROC_READY_STREAM_KEY_V2,
            scenario_id,
            signal_id,
        )
    except Exception as e:
        log.error(
            "BT_ANALYSIS_PREPROC_V2: не удалось опубликовать событие в '%s' scenario_id=%s signal_id=%s: %s",
            PREPROC_READY_STREAM_KEY_V2,
            scenario_id,
            signal_id,
            e,
            exc_info=True,
        )


# 🔸 Вспомогательная функция: безопасное Decimal
def _safe_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


# 🔸 Вспомогательная функция: Decimal или None
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
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)