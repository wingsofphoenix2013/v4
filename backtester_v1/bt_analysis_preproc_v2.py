# bt_analysis_preproc_v2.py — препроцессинг анализов v2.1 (v1-порог + оптимизация набора bad-биннов через score с holdout)

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

# 🔸 Настройки окна holdout (при окне истории 28 дней)
HOLDOUT_DAYS = 7

# 🔸 Настройки оптимизации v2.1
EPS_THRESHOLD = Decimal("0.00000001")
EPS_SCORE = Decimal("0.00000001")

V2_LAMBDA = Decimal("0.5")          # штраф за просадку holdout относительно train
NEAR_THRESHOLD_MARGIN = Decimal("0.0500")  # зона "рядом с порогом" для кандидатов на включение
MAX_TOGGLE_ITERS = 200              # максимум итераций улучшения набора bad-биннов
MAX_BAD_BINS_LIMIT = 300            # safety-лимит на число bad-биннов (не min_trades, а ограничение сложности)

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

            results: Dict[str, Dict[str, Any]] = {}
            for direction in directions:
                res = await _build_model_for_direction_v21(
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

            parts: List[str] = []
            for d in directions:
                r = results.get(d) or {}
                parts.append(
                    f"{d} thr={r.get('best_threshold')} "
                    f"orig_roi={r.get('orig_roi')} filt_roi={r.get('filt_roi')} "
                    f"train_roi={r.get('train_roi')} val_roi={r.get('val_roi')} score={r.get('score')} "
                    f"orig_trades={r.get('orig_trades')} filt_trades={r.get('filt_trades')} "
                    f"bad_bins_init={r.get('bad_bins_initial')} bad_bins_final={r.get('bad_bins_final')} iters={r.get('iters_used')}"
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


# 🔸 v2.1: построение модели для одного направления (v1-порог + оптимизация bad-биннов по score)
async def _build_model_for_direction_v21(
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
        await _delete_model_for_direction_v2(pg, scenario_id, signal_id, direction)
        return {
            "direction": direction,
            "best_threshold": "0",
            "orig_trades": 0,
            "orig_roi": "0",
            "filt_trades": 0,
            "filt_roi": "0",
            "train_roi": "0",
            "val_roi": "0",
            "score": "0",
            "bad_bins_initial": 0,
            "bad_bins_final": 0,
            "iters_used": 0,
        }

    # базовые индексы по позициям
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

    # split train/val по exit_time
    train_uids, val_uids, val_used, val_window = _split_train_val_uids(
        uids=all_uids,
        pos_exit_time=pos_exit_time,
        holdout_days=holdout_days,
    )

    # базовые метрики (orig) на полном окне
    orig_trades = len(all_uids)
    orig_pnl_abs = sum(pos_pnl.values(), Decimal("0"))
    orig_wins = sum(1 for uid in all_uids if pos_win.get(uid))
    orig_winrate = (Decimal(orig_wins) / Decimal(orig_trades)) if orig_trades > 0 else Decimal("0")
    orig_roi = (orig_pnl_abs / deposit) if (deposit and deposit > 0) else Decimal("0")

    # получаем порог как в v1, но оптимизируем по train (и только по direction)
    worst_rows = await _load_positions_with_worst_winrate(pg, scenario_id, signal_id, direction)
    best_threshold = _compute_best_threshold_train(
        rows=worst_rows,
        train_uids=train_uids,
        deposit=deposit,
    )

    # грузим bins_stat по направлению
    bins_rows = await _load_bins_stat_rows(pg, scenario_id, signal_id, direction)
    if not bins_rows:
        # модель без фильтрации
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
                "version": 21,
                "method": "v1_threshold + toggle_bad_bins_by_score",
                "direction": direction,
                "deposit": str(deposit) if deposit is not None else None,
                "lambda": str(V2_LAMBDA),
                "holdout": {"days": holdout_days, "used": bool(val_used), "window": val_window},
                "threshold": str(best_threshold),
                "note": "no_bins_stat_rows",
            },
            source_finished_at=source_finished_at,
        )
        await _rebuild_bins_labels_v2(
            pg=pg,
            model_id=model_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            best_threshold=best_threshold,
            selected_bins=[],
        )
        return {
            "direction": direction,
            "best_threshold": str(best_threshold),
            "orig_trades": orig_trades,
            "orig_roi": str(_q_decimal(orig_roi)),
            "filt_trades": orig_trades,
            "filt_roi": str(_q_decimal(orig_roi)),
            "train_roi": str(_q_decimal(orig_roi)),
            "val_roi": str(_q_decimal(orig_roi)),
            "score": str(_q_decimal(orig_roi)),
            "bad_bins_initial": 0,
            "bad_bins_final": 0,
            "iters_used": 0,
        }

    # строим индекс попаданий raw
    hits_index = await _load_hits_index_for_direction(pg, scenario_id, signal_id, direction)

    # формируем кандидаты-бинны: только те, что реально имеют попадания в positions_raw
    bin_by_key: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    for b in bins_rows:
        k = (int(b["analysis_id"]), str(b["timeframe"]), str(b["bin_name"]))
        if k not in hits_index:
            continue
        if not hits_index.get(k):
            continue
        bin_by_key[k] = b

    if not bin_by_key:
        # нечего оптимизировать
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
                "version": 21,
                "method": "v1_threshold + toggle_bad_bins_by_score",
                "direction": direction,
                "deposit": str(deposit) if deposit is not None else None,
                "lambda": str(V2_LAMBDA),
                "holdout": {"days": holdout_days, "used": bool(val_used), "window": val_window},
                "threshold": str(best_threshold),
                "note": "no_usable_bins_with_hits",
            },
            source_finished_at=source_finished_at,
        )
        await _rebuild_bins_labels_v2(
            pg=pg,
            model_id=model_id,
            scenario_id=scenario_id,
            signal_id=signal_id,
            direction=direction,
            best_threshold=best_threshold,
            selected_bins=[],
        )
        return {
            "direction": direction,
            "best_threshold": str(best_threshold),
            "orig_trades": orig_trades,
            "orig_roi": str(_q_decimal(orig_roi)),
            "filt_trades": orig_trades,
            "filt_roi": str(_q_decimal(orig_roi)),
            "train_roi": str(_q_decimal(orig_roi)),
            "val_roi": str(_q_decimal(orig_roi)),
            "score": str(_q_decimal(orig_roi)),
            "bad_bins_initial": 0,
            "bad_bins_final": 0,
            "iters_used": 0,
        }

    # стартовый активный набор bad-биннов как в v1: winrate <= threshold
    active_bad_bins: Set[Tuple[int, str, str]] = set()
    for k, b in bin_by_key.items():
        if _safe_decimal(b["winrate"]) <= best_threshold:
            # условия достаточности: бин должен реально задевать хоть одну позицию в окне
            if hits_index.get(k):
                active_bad_bins.add(k)

    bad_bins_initial = len(active_bad_bins)

    # кандидаты на включение: все бины "рядом с порогом" (<= threshold + margin)
    enable_pool: Set[Tuple[int, str, str]] = set()
    thr_hi = best_threshold + NEAR_THRESHOLD_MARGIN
    for k, b in bin_by_key.items():
        if _safe_decimal(b["winrate"]) <= thr_hi:
            enable_pool.add(k)

    # подготавливаем hits для train/val
    hits_train: Dict[Tuple[int, str, str], Set[Any]] = {}
    hits_val: Dict[Tuple[int, str, str], Set[Any]] = {}

    for k, hits in hits_index.items():
        if k not in bin_by_key:
            continue

        # hits на train/val
        ht = hits.intersection(train_uids)
        hv = hits.intersection(val_uids) if val_used else set()

        if ht:
            hits_train[k] = ht
        if hv:
            hits_val[k] = hv

    # инициализация состояния (counts + kept-агрегаты)
    state = _init_state_counts(
        all_uids=all_uids,
        train_uids=train_uids,
        val_uids=val_uids,
        val_used=val_used,
        pos_pnl=pos_pnl,
        pos_win=pos_win,
        hits_train=hits_train,
        hits_val=hits_val,
        active_bad_bins=active_bad_bins,
    )

    # оптимизация набора bad-биннов (disable/enable) по score
    selected_bins_set, iters_used, steps = _optimize_bad_bins_by_score(
        state=state,
        deposit=deposit,
        val_used=val_used,
        enable_pool=enable_pool,
        max_iters=MAX_TOGGLE_ITERS,
        max_bad_bins=MAX_BAD_BINS_LIMIT,
    )

    bad_bins_final = len(selected_bins_set)

    # финальные метрики на полном окне (не только train)
    # считаем removed как union hits по выбранным bad-бинам
    removed_all: Set[Any] = set()
    for k in selected_bins_set:
        removed_all |= hits_index.get(k, set())

    kept_all = set(uid for uid in all_uids if uid not in removed_all)
    filt_trades = len(kept_all)
    filt_pnl_abs = sum((pos_pnl[uid] for uid in kept_all), Decimal("0"))
    filt_wins = sum(1 for uid in kept_all if pos_win.get(uid))
    filt_winrate = (Decimal(filt_wins) / Decimal(filt_trades)) if filt_trades > 0 else Decimal("0")
    filt_roi = (filt_pnl_abs / deposit) if (deposit and deposit > 0) else Decimal("0")

    removed_trades = orig_trades - filt_trades
    if removed_trades > 0:
        removed_losers = sum(1 for uid in removed_all if (uid in all_uids and not pos_win.get(uid, False)))
        removed_accuracy = Decimal(removed_losers) / Decimal(removed_trades)
    else:
        removed_accuracy = Decimal("0")

    selected_bins: List[Dict[str, Any]] = []
    selected_analysis_ids: Set[int] = set()

    for k in sorted(list(selected_bins_set), key=lambda x: (x[0], x[1], x[2])):
        b = bin_by_key.get(k)
        if not b:
            continue
        selected_analysis_ids.add(int(b["analysis_id"]))
        selected_bins.append(b)

    selected_analysis_ids_list = sorted(list(selected_analysis_ids))

    # train/val/score из состояния
    train_roi = state.get("roi_train") or Decimal("0")
    val_roi = state.get("roi_val") or Decimal("0")
    score = state.get("score") or Decimal("0")

    meta_obj = {
        "version": 21,
        "method": "v1_threshold + toggle_bad_bins_by_score",
        "direction": direction,
        "deposit": str(deposit) if deposit is not None else None,
        "lambda": str(V2_LAMBDA),
        "holdout": {"days": holdout_days, "used": bool(val_used), "window": val_window},
        "threshold": str(best_threshold),
        "near_threshold_margin": str(NEAR_THRESHOLD_MARGIN),
        "bins": {
            "initial_bad": int(bad_bins_initial),
            "final_bad": int(bad_bins_final),
            "enable_pool": int(len(enable_pool)),
        },
        "iters": int(iters_used),
        "score": {
            "train_roi": str(_q_decimal(train_roi)),
            "val_roi": str(_q_decimal(val_roi)),
            "score": str(_q_decimal(score)),
        },
        "steps": steps,
    }

    # upsert model_opt_v2
    model_id = await _upsert_model_opt_v2_return_id(
        pg=pg,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=best_threshold,
        selected_analysis_ids=selected_analysis_ids_list,
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

    # пересобираем labels_v2: state='bad' только для выбранных биннов
    await _rebuild_bins_labels_v2(
        pg=pg,
        model_id=model_id,
        scenario_id=scenario_id,
        signal_id=signal_id,
        direction=direction,
        best_threshold=best_threshold,
        selected_bins=selected_bins,
    )

    return {
        "direction": direction,
        "best_threshold": str(best_threshold),
        "orig_trades": orig_trades,
        "orig_roi": str(_q_decimal(orig_roi)),
        "filt_trades": filt_trades,
        "filt_roi": str(_q_decimal(filt_roi)),
        "train_roi": str(_q_decimal(train_roi)),
        "val_roi": str(_q_decimal(val_roi)),
        "score": str(_q_decimal(score)),
        "bad_bins_initial": bad_bins_initial,
        "bad_bins_final": bad_bins_final,
        "iters_used": iters_used,
    }


# 🔸 Инициализация состояния (counts + kept-метрики) для активного набора bad-биннов
def _init_state_counts(
    all_uids: Set[Any],
    train_uids: Set[Any],
    val_uids: Set[Any],
    val_used: bool,
    pos_pnl: Dict[Any, Decimal],
    pos_win: Dict[Any, bool],
    hits_train: Dict[Tuple[int, str, str], Set[Any]],
    hits_val: Dict[Tuple[int, str, str], Set[Any]],
    active_bad_bins: Set[Tuple[int, str, str]],
) -> Dict[str, Any]:
    # стартовые агрегаты kept = все
    train_kept_trades = len(train_uids)
    train_kept_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in train_uids), Decimal("0"))
    train_kept_wins = sum(1 for uid in train_uids if pos_win.get(uid))

    val_kept_trades = len(val_uids) if val_used else 0
    val_kept_pnl = sum((pos_pnl.get(uid, Decimal("0")) for uid in val_uids), Decimal("0")) if val_used else Decimal("0")
    val_kept_wins = sum(1 for uid in val_uids if pos_win.get(uid)) if val_used else 0

    # counts: uid -> сколько активных bad-биннов по нему сработало
    bad_count_train: Dict[Any, int] = {}
    bad_count_val: Dict[Any, int] = {}

    # применяем все активные бины (переводим часть позиций в removed)
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
        "all_uids": all_uids,
        "train_uids": train_uids,
        "val_uids": val_uids,
        "val_used": val_used,
        "pos_pnl": pos_pnl,
        "pos_win": pos_win,
        "hits_train": hits_train,
        "hits_val": hits_val,
        "active": set(active_bad_bins),
        "bad_count_train": bad_count_train,
        "bad_count_val": bad_count_val,
        "train_kept_trades": train_kept_trades,
        "train_kept_pnl": train_kept_pnl,
        "train_kept_wins": train_kept_wins,
        "val_kept_trades": val_kept_trades,
        "val_kept_pnl": val_kept_pnl,
        "val_kept_wins": val_kept_wins,
    }
    return state


# 🔸 Оптимизация набора bad-биннов (disable/enable по одному) по score с holdout
def _optimize_bad_bins_by_score(
    state: Dict[str, Any],
    deposit: Optional[Decimal],
    val_used: bool,
    enable_pool: Set[Tuple[int, str, str]],
    max_iters: int,
    max_bad_bins: int,
) -> Tuple[Set[Tuple[int, str, str]], int, List[Dict[str, Any]]]:
    steps: List[Dict[str, Any]] = []

    # вспомогательные вычисления score
    def calc_roi(pnl: Decimal) -> Decimal:
        if deposit and deposit > 0:
            try:
                return pnl / deposit
            except (InvalidOperation, ZeroDivisionError):
                return Decimal("0")
        return Decimal("0")

    def calc_score(roi_train: Decimal, roi_val: Decimal) -> Decimal:
        # если holdout хуже train — штрафуем разницу
        drop = roi_train - roi_val
        if drop > 0:
            return roi_train - (V2_LAMBDA * drop)
        return roi_train

    # текущие значения
    roi_train = calc_roi(state["train_kept_pnl"])
    roi_val = calc_roi(state["val_kept_pnl"]) if val_used else roi_train
    score = calc_score(roi_train, roi_val)

    state["roi_train"] = roi_train
    state["roi_val"] = roi_val
    state["score"] = score

    active: Set[Tuple[int, str, str]] = state["active"]
    hits_train: Dict[Tuple[int, str, str], Set[Any]] = state["hits_train"]
    hits_val: Dict[Tuple[int, str, str], Set[Any]] = state["hits_val"]
    pos_pnl: Dict[Any, Decimal] = state["pos_pnl"]
    pos_win: Dict[Any, bool] = state["pos_win"]
    bad_count_train: Dict[Any, int] = state["bad_count_train"]
    bad_count_val: Dict[Any, int] = state["bad_count_val"]

    iters_used = 0

    for it in range(int(max_iters or 0)):
        iters_used = it + 1

        best_move = None
        best_new_score = score
        best_new_roi_train = roi_train
        best_new_roi_val = roi_val

        # 1) пробуем выключать активные bad-бинны
        for k in list(active):
            # условия достаточности: нужно иметь hits в train/val, иначе выключение бессмысленно
            ht = hits_train.get(k) or set()
            hv = hits_val.get(k) or set()

            # считаем дельты для disable:
            # позиция вернётся в kept, если её текущий count == 1
            delta_train_trades = 0
            delta_train_pnl = Decimal("0")
            delta_train_wins = 0

            for uid in ht:
                if bad_count_train.get(uid, 0) == 1:
                    delta_train_trades += 1
                    delta_train_pnl += pos_pnl.get(uid, Decimal("0"))
                    if pos_win.get(uid):
                        delta_train_wins += 1

            delta_val_trades = 0
            delta_val_pnl = Decimal("0")
            delta_val_wins = 0

            if val_used:
                for uid in hv:
                    if bad_count_val.get(uid, 0) == 1:
                        delta_val_trades += 1
                        delta_val_pnl += pos_pnl.get(uid, Decimal("0"))
                        if pos_win.get(uid):
                            delta_val_wins += 1

            new_train_pnl = state["train_kept_pnl"] + delta_train_pnl
            new_val_pnl = (state["val_kept_pnl"] + delta_val_pnl) if val_used else new_train_pnl

            new_roi_train = calc_roi(new_train_pnl)
            new_roi_val = calc_roi(new_val_pnl) if val_used else new_roi_train
            new_score = calc_score(new_roi_train, new_roi_val)

            # выбираем улучшение score; при равенстве предпочитаем больше train_kept_trades
            if new_score > best_new_score + EPS_SCORE:
                best_new_score = new_score
                best_new_roi_train = new_roi_train
                best_new_roi_val = new_roi_val
                best_move = ("disable", k, delta_train_trades, delta_train_pnl, delta_val_trades, delta_val_pnl)

        # 2) пробуем включать кандидаты из enable_pool (но не те, что уже активны)
        if len(active) < int(max_bad_bins or 0):
            for k in enable_pool:
                if k in active:
                    continue

                ht = hits_train.get(k) or set()
                if not ht:
                    continue

                # dельты для enable:
                # позиция уйдёт из kept, если её текущий count == 0
                delta_train_trades = 0
                delta_train_pnl = Decimal("0")
                delta_train_wins = 0

                for uid in ht:
                    if bad_count_train.get(uid, 0) == 0:
                        delta_train_trades += 1
                        delta_train_pnl += pos_pnl.get(uid, Decimal("0"))
                        if pos_win.get(uid):
                            delta_train_wins += 1

                if delta_train_trades <= 0:
                    continue

                hv = hits_val.get(k) or set()

                delta_val_trades = 0
                delta_val_pnl = Decimal("0")
                delta_val_wins = 0

                if val_used and hv:
                    for uid in hv:
                        if bad_count_val.get(uid, 0) == 0:
                            delta_val_trades += 1
                            delta_val_pnl += pos_pnl.get(uid, Decimal("0"))
                            if pos_win.get(uid):
                                delta_val_wins += 1

                new_train_pnl = state["train_kept_pnl"] - delta_train_pnl
                new_val_pnl = (state["val_kept_pnl"] - delta_val_pnl) if val_used else new_train_pnl

                new_roi_train = calc_roi(new_train_pnl)
                new_roi_val = calc_roi(new_val_pnl) if val_used else new_roi_train
                new_score = calc_score(new_roi_train, new_roi_val)

                if new_score > best_new_score + EPS_SCORE:
                    best_new_score = new_score
                    best_new_roi_train = new_roi_train
                    best_new_roi_val = new_roi_val
                    best_move = ("enable", k, delta_train_trades, delta_train_pnl, delta_val_trades, delta_val_pnl)

        # если шагов улучшения нет — стоп
        if best_move is None:
            break

        # применяем лучший шаг
        action, k, dt_tr, dt_pnl_tr, dv_tr, dv_pnl_tr = best_move

        if action == "disable":
            # обновляем counts и агрегаты (позиции могут вернуться в kept)
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

        elif action == "enable":
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

        # обновляем score
        roi_train = calc_roi(state["train_kept_pnl"])
        roi_val = calc_roi(state["val_kept_pnl"]) if val_used else roi_train
        score = calc_score(roi_train, roi_val)

        state["roi_train"] = roi_train
        state["roi_val"] = roi_val
        state["score"] = score

        # шаги meta (сжато)
        steps.append(
            {
                "step": it + 1,
                "action": action,
                "bin": {"analysis_id": int(k[0]), "timeframe": str(k[1]), "bin_name": str(k[2])},
                "roi_train": str(_q_decimal(roi_train)),
                "roi_val": str(_q_decimal(roi_val)),
                "score": str(_q_decimal(score)),
                "active_bad_bins": int(len(active)),
            }
        )

        # ограничение размера meta
        if len(steps) >= 120:
            steps.append({"note": "steps_truncated"})
            break

    return set(active), iters_used, steps


# 🔸 v1-подобный sweep: расчёт оптимального порога по train (через worst_winrate позиции)
def _compute_best_threshold_train(
    rows: List[Dict[str, Any]],
    train_uids: Set[Any],
    deposit: Optional[Decimal],
) -> Decimal:
    # фильтруем только train и только позиции с worst_winrate
    train_rows = [r for r in rows if r["position_uid"] in train_uids and r.get("worst_winrate") is not None]
    if not train_rows:
        return Decimal("0")

    orig_trades = len(train_rows)
    orig_pnl = sum((r["pnl_abs"] for r in train_rows), Decimal("0"))
    orig_wins = sum(1 for r in train_rows if r["pnl_abs"] > 0)

    if orig_trades > 0:
        orig_winrate = Decimal(orig_wins) / Decimal(orig_trades)
    else:
        orig_winrate = Decimal("0")

    if deposit and deposit > 0:
        try:
            orig_roi = orig_pnl / deposit
        except (InvalidOperation, ZeroDivisionError):
            orig_roi = Decimal("0")
    else:
        orig_roi = Decimal("0")

    # группируем по worst_winrate
    groups: Dict[Decimal, Dict[str, Any]] = {}
    for r in train_rows:
        w = r["worst_winrate"]
        if w is None:
            continue
        g = groups.setdefault(w, {"trades": 0, "pnl": Decimal("0"), "wins": 0, "losers": 0})
        g["trades"] += 1
        g["pnl"] += r["pnl_abs"]
        if r["pnl_abs"] > 0:
            g["wins"] += 1
        else:
            g["losers"] += 1

    unique_w = sorted(groups.keys())
    if not unique_w:
        return Decimal("0")

    best_threshold = Decimal("0")
    best_filt_trades = orig_trades
    best_filt_pnl = orig_pnl
    best_filt_roi = orig_roi

    best_objective = best_filt_roi if (deposit and deposit > 0) else best_filt_pnl

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
            filt_roi = Decimal("0")
            objective = filt_pnl

        threshold = v + EPS_THRESHOLD

        # 1) max objective, 2) при равенстве — больше trades, 3) при равенстве — меньший threshold
        if objective > best_objective:
            best_objective = objective
            best_threshold = threshold
            best_filt_trades = filt_trades
            best_filt_pnl = filt_pnl
            best_filt_roi = filt_roi
        elif objective == best_objective:
            if filt_trades > best_filt_trades:
                best_threshold = threshold
                best_filt_trades = filt_trades
                best_filt_pnl = filt_pnl
                best_filt_roi = filt_roi
            elif filt_trades == best_filt_trades and threshold < best_threshold:
                best_threshold = threshold
                best_filt_trades = filt_trades
                best_filt_pnl = filt_pnl
                best_filt_roi = filt_roi

    return _q_decimal(best_threshold)


# 🔸 Загрузка позиций с worst_winrate (MIN winrate по попаданиям позиции в бинны)
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
    best_threshold: Decimal,
    selected_bins: List[Dict[str, Any]],
) -> int:
    async with pg.acquire() as conn:
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
                    best_threshold,
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


# 🔸 Вспомогательная функция: квантизация Decimal до 4 знаков (вниз)
def _q_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.0001"), rounding=ROUND_DOWN)