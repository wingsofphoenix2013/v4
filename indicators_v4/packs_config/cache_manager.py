# packs_config/cache_manager.py — кеши/реестр ind_pack + init winners-cache (labels_v2) + reload rules on postproc_ready_v2 (winner-driven) + signal direction masks

from __future__ import annotations

# 🔸 Imports
import asyncio
import logging
from typing import Any

from packs_config.db_loaders import (
    load_adaptive_bins_for_pair,
    load_analysis_instances,
    load_analysis_parameters,
    load_enabled_packs,
    load_static_bins_dict,
    load_winners_from_labels_v2,
    load_signal_direction_masks,
)
from packs_config.models import PackRuntime, BinRule
from packs_config.registry import build_pack_registry


# 🔸 Константы Redis (постпроцессинг бектеста → сигнал обновления winner/rules)
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready_v2"
POSTPROC_GROUP = "ind_pack_postproc_group_v4_v2"
POSTPROC_CONSUMER = "ind_pack_postproc_1"


# 🔸 Глобальный реестр pack-инстансов, готовых к работе
pack_registry: dict[tuple[str, str], list[PackRuntime]] = {}
# key: (timeframe_from_stream, indicator_from_stream) -> list[PackRuntime]


# 🔸 Кеши правил (winner-driven, из bt_analysis_bin_dict_adaptive)
adaptive_bins_cache: dict[tuple[int, int, int, str, str], list[BinRule]] = {}
adaptive_quantiles_cache: dict[tuple[int, int, int, str, str], list[BinRule]] = {}
# key: (analysis_id, scenario_id, signal_id, timeframe, direction)


# 🔸 Кеш победителей (winner-driven, из bt_analysis_bins_labels_v2)
winners_by_pair: dict[tuple[int, int], dict[str, Any]] = {}
# key: (scenario_id, signal_id) -> {"run_id": int, "analysis_id": int, "winner_param": str|None, "timeframe": str|None}


# 🔸 Кеш направлений сигналов (mono-direction): signal_id -> 'long'|'short'|...
signal_direction_mask: dict[int, str] = {}
signal_dir_lock = asyncio.Lock()


# 🔸 Набор “интересующих” пар (берётся из bins_policy.pairs включённых pack-инстансов)
configured_pairs_set: set[tuple[int, int]] = set()

# 🔸 Активные m5-триггеры (indicator_stream.indicator), которые нужны текущим winner’ам
active_trigger_keys_m5: set[str] = set()
triggers_lock = asyncio.Lock()

# 🔸 Locks и флаги готовности кешей
adaptive_lock = asyncio.Lock()
winners_lock = asyncio.Lock()

caches_ready = {
    "registry": False,
    "winners": False,
    "adaptive_bins": False,
    "quantiles": False,
}


# 🔸 Статусы перезагрузки пар (для внешних модулей/диагностики)
reloading_pairs_bins: set[tuple[int, int]] = set()
reloading_pairs_quantiles: set[tuple[int, int]] = set()
reloading_pairs_labels: set[tuple[int, int]] = set()  # legacy name: используем как “winner reload”


# 🔸 Consumer-group helper
async def ensure_stream_group(redis: Any, stream: str, group: str):
    log = logging.getLogger("PACK_STREAM")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create error for %s/%s: %s", stream, group, e)


# 🔸 Helpers: winner getters
def get_winner_meta(scenario_id: int, signal_id: int) -> dict[str, Any] | None:
    try:
        return winners_by_pair.get((int(scenario_id), int(signal_id)))
    except Exception:
        return None


def get_winner_analysis_id(scenario_id: int, signal_id: int) -> int | None:
    m = get_winner_meta(scenario_id, signal_id)
    if not m:
        return None
    try:
        return int(m.get("analysis_id"))
    except Exception:
        return None


def get_winner_run_id(scenario_id: int, signal_id: int) -> int | None:
    m = get_winner_meta(scenario_id, signal_id)
    if not m:
        return None
    try:
        return int(m.get("run_id"))
    except Exception:
        return None


# 🔸 Helpers: signal directions
def get_allowed_directions(signal_id: int) -> list[str]:
    """
    Возвращает список разрешённых направлений для signal_id на основе bt_signals_parameters.direction_mask.
    По умолчанию (если неизвестно) — ['long','short'].
    """
    try:
        dm = str(signal_direction_mask.get(int(signal_id), "") or "").strip().lower()
    except Exception:
        dm = ""

    if dm == "long":
        return ["long"]
    if dm == "short":
        return ["short"]

    # на будущее: если когда-то появится мульти-маска
    if dm in ("both", "all", "long,short", "short,long"):
        return ["long", "short"]

    return ["long", "short"]


# 🔸 Helpers: active m5 trigger keys (winner-driven)
async def _rebuild_active_triggers_m5():
    log = logging.getLogger("PACK_INIT")

    # соберём множество analysis_id текущих победителей
    winner_aids: set[int] = set()
    for meta in winners_by_pair.values():
        try:
            winner_aids.add(int(meta.get("analysis_id")))
        except Exception:
            continue

    # пройдём по registry и найдём ключи (m5, indicator), где есть runtime победителя
    new_set: set[str] = set()
    for (tf, ind_key), rts in pack_registry.items():
        if str(tf) != "m5":
            continue
        for rt in rts:
            try:
                if int(rt.analysis_id) in winner_aids:
                    new_set.add(str(ind_key))
                    break
            except Exception:
                continue

    async with triggers_lock:
        active_trigger_keys_m5.clear()
        active_trigger_keys_m5.update(new_set)

    log.info("PACK_INIT: active m5 triggers rebuilt — winners=%s, triggers=%s", len(winner_aids), len(new_set))


def get_active_trigger_keys_m5() -> set[str]:
    # возвращаем копию, чтобы снаружи не модифицировали внутренний set
    return set(active_trigger_keys_m5)


# 🔸 LEGACY helper: current run by signal (оставлено для совместимости старых вызовов)
def get_current_run_id(signal_id: int) -> int | None:
    try:
        sid = int(signal_id)
    except Exception:
        return None

    # выбираем любой run_id из winners по этому signal_id (в текущей модели сигнал обычно имеет один актуальный run)
    best: int | None = None
    for (sc, sg), meta in winners_by_pair.items():
        if int(sg) != sid:
            continue
        try:
            rid = int(meta.get("run_id"))
        except Exception:
            continue
        if best is None or rid > best:
            best = rid
    return best


# 🔸 Get adaptive rules (winner-driven)
def get_adaptive_rules(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    return adaptive_bins_cache.get((int(analysis_id), int(scenario_id), int(signal_id), str(timeframe), str(direction)), [])


def get_adaptive_quantiles(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    return adaptive_quantiles_cache.get((int(analysis_id), int(scenario_id), int(signal_id), str(timeframe), str(direction)), [])


# 🔸 Cache init: registry + configured_pairs + signal directions + winners + rules (winner-driven)
async def init_pack_runtime(pg: Any):
    global pack_registry, configured_pairs_set

    log = logging.getLogger("PACK_INIT")

    caches_ready["registry"] = False
    caches_ready["winners"] = False
    caches_ready["adaptive_bins"] = False
    caches_ready["quantiles"] = False

    # очистка кешей и флагов reload на старте
    adaptive_bins_cache.clear()
    adaptive_quantiles_cache.clear()

    winners_by_pair.clear()
    configured_pairs_set.clear()

    reloading_pairs_bins.clear()
    reloading_pairs_quantiles.clear()
    reloading_pairs_labels.clear()

    async with signal_dir_lock:
        signal_direction_mask.clear()

    # 1) загрузка pack-конфига и построение registry
    packs = await load_enabled_packs(pg)
    analysis_ids = sorted({int(p["analysis_id"]) for p in packs})

    analysis_meta = await load_analysis_instances(pg, analysis_ids)
    analysis_params = await load_analysis_parameters(pg, analysis_ids)
    static_bins_dict = await load_static_bins_dict(pg, analysis_ids)

    new_registry = build_pack_registry(packs, analysis_meta, analysis_params, static_bins_dict)

    pack_registry.clear()
    pack_registry.update(new_registry)
    caches_ready["registry"] = True

    # 2) собираем интересующие пары из включённых runtime (в твоём случае это MTF-паки)
    all_runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        all_runtimes.extend(lst)

    mtf_runtimes = 0

    for rt in all_runtimes:
        if rt.is_mtf and rt.mtf_pairs:
            mtf_runtimes += 1
            for pair in rt.mtf_pairs:
                configured_pairs_set.add((int(pair[0]), int(pair[1])))

    pairs_total = len(configured_pairs_set)
    log.info("PACK_INIT: mtf runtimes=%s, configured_pairs=%s", mtf_runtimes, pairs_total)

    # 2.1) направления сигналов (direction_mask)
    signal_ids = sorted({int(sig) for (_, sig) in configured_pairs_set})
    dm = await load_signal_direction_masks(pg, signal_ids)
    async with signal_dir_lock:
        signal_direction_mask.update(dm)

    # 3) загрузка winners из labels_v2 (актуальный снимок)
    winners = await load_winners_from_labels_v2(pg, sorted(list(configured_pairs_set)))

    async with winners_lock:
        for (sc, sg), meta in winners.items():
            winners_by_pair[(int(sc), int(sg))] = {
                "run_id": int(meta.get("run_id")),
                "analysis_id": int(meta.get("analysis_id")),
                "winner_param": meta.get("indicator_param"),
                "timeframe": meta.get("timeframe"),
            }

    caches_ready["winners"] = True

    # 4) загрузка правил из bt_analysis_bin_dict_adaptive по winner (bins + quantiles)
    bins_rules_total = 0
    quant_rules_total = 0
    winners_found = len(winners_by_pair)
    winners_missing = max(0, pairs_total - winners_found)

    async with adaptive_lock:
        for (scenario_id, signal_id), meta in winners_by_pair.items():
            try:
                run_id = int(meta["run_id"])
                winner_aid = int(meta["analysis_id"])
            except Exception:
                continue

            # bins
            loaded_bins = await load_adaptive_bins_for_pair(pg, int(run_id), [int(winner_aid)], int(scenario_id), int(signal_id), "bins")
            for (aid, tf, direction), rules in loaded_bins.items():
                adaptive_bins_cache[(int(aid), int(scenario_id), int(signal_id), str(tf), str(direction))] = rules
                bins_rules_total += len(rules)

            # quantiles
            loaded_q = await load_adaptive_bins_for_pair(pg, int(run_id), [int(winner_aid)], int(scenario_id), int(signal_id), "quantiles")
            for (aid, tf, direction), rules in loaded_q.items():
                adaptive_quantiles_cache[(int(aid), int(scenario_id), int(signal_id), str(tf), str(direction))] = rules
                quant_rules_total += len(rules)

    caches_ready["adaptive_bins"] = True
    caches_ready["quantiles"] = True

    # итоговый лог старта
    log.info(
        "PACK_INIT: winners cache ready — pairs=%s, found=%s, missing=%s; rules loaded — bins=%s, quantiles=%s; signal_dirs=%s",
        pairs_total,
        winners_found,
        winners_missing,
        bins_rules_total,
        quant_rules_total,
        len(signal_direction_mask),
    )

    # пересобираем активные m5-триггеры победителей
    await _rebuild_active_triggers_m5()


# 🔸 Reload on postproc_ready_v2: обновляем winner по паре + перезагружаем rules из bt_analysis_bin_dict_adaptive
async def watch_postproc_ready(pg: Any, redis: Any):
    log = logging.getLogger("PACK_POSTPROC")
    sem = asyncio.Semaphore(50)

    async def _reload_pair(
        run_id: int,
        scenario_id: int,
        signal_id: int,
        winner_analysis_id: int,
        winner_param: str | None,
    ):
        async with sem:
            pair = (int(scenario_id), int(signal_id))

            reloading_pairs_bins.add(pair)
            reloading_pairs_quantiles.add(pair)
            reloading_pairs_labels.add(pair)

            try:
                # обновим winner cache
                async with winners_lock:
                    winners_by_pair[pair] = {
                        "run_id": int(run_id),
                        "analysis_id": int(winner_analysis_id),
                        "winner_param": (str(winner_param) if winner_param is not None else None),
                        "timeframe": "mtf",
                    }

                # перезагрузим rules (bins + quantiles) только для winner
                bins_loaded = 0
                quant_loaded = 0

                async with adaptive_lock:
                    # удаляем старые rules по паре (любых analysis_id)
                    to_del_bins = [k for k in list(adaptive_bins_cache.keys()) if k[1] == int(scenario_id) and k[2] == int(signal_id)]
                    for k in to_del_bins:
                        adaptive_bins_cache.pop(k, None)

                    to_del_q = [k for k in list(adaptive_quantiles_cache.keys()) if k[1] == int(scenario_id) and k[2] == int(signal_id)]
                    for k in to_del_q:
                        adaptive_quantiles_cache.pop(k, None)

                    loaded_bins = await load_adaptive_bins_for_pair(pg, int(run_id), [int(winner_analysis_id)], int(scenario_id), int(signal_id), "bins")
                    for (aid, tf, direction), rules in loaded_bins.items():
                        adaptive_bins_cache[(int(aid), int(scenario_id), int(signal_id), str(tf), str(direction))] = rules
                        bins_loaded += len(rules)

                    loaded_q = await load_adaptive_bins_for_pair(pg, int(run_id), [int(winner_analysis_id)], int(scenario_id), int(signal_id), "quantiles")
                    for (aid, tf, direction), rules in loaded_q.items():
                        adaptive_quantiles_cache[(int(aid), int(scenario_id), int(signal_id), str(tf), str(direction))] = rules
                        quant_loaded += len(rules)

                log.info(
                    "PACK_WINNER: updated (scenario_id=%s, signal_id=%s, run_id=%s, winner_analysis_id=%s, winner_param=%s, bins_rules=%s, quantiles_rules=%s)",
                    scenario_id,
                    signal_id,
                    run_id,
                    winner_analysis_id,
                    winner_param,
                    bins_loaded,
                    quant_loaded,
                )

                # победитель мог поменяться → пересобираем активные m5-триггеры
                await _rebuild_active_triggers_m5()

            finally:
                reloading_pairs_bins.discard(pair)
                reloading_pairs_quantiles.discard(pair)
                reloading_pairs_labels.discard(pair)

    while True:
        try:
            resp = await redis.xreadgroup(
                POSTPROC_GROUP,
                POSTPROC_CONSUMER,
                streams={POSTPROC_STREAM_KEY: ">"},
                count=200,
                block=2000,
            )
            if not resp:
                continue

            to_ack = []
            scheduled = 0
            ignored = 0
            bad = 0

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)

                    # парсим обязательные поля
                    try:
                        run_id = int(data.get("run_id"))
                        scenario_id = int(data.get("scenario_id"))
                        signal_id = int(data.get("signal_id"))
                        winner_analysis_id = int(data.get("winner_analysis_id"))
                        winner_param = data.get("winner_param")
                    except Exception:
                        bad += 1
                        continue

                    pair = (int(scenario_id), int(signal_id))
                    # условия достаточности: работаем только по интересующим парам
                    if pair not in configured_pairs_set:
                        ignored += 1
                        continue

                    # планируем reload (не блокируем основной loop)
                    asyncio.create_task(_reload_pair(int(run_id), int(scenario_id), int(signal_id), int(winner_analysis_id), winner_param))
                    scheduled += 1

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

            # суммирующий лог по батчу
            if scheduled or ignored or bad:
                log.info(
                    "PACK_POSTPROC: batch handled (scheduled=%s, ignored=%s, bad=%s, ack=%s)",
                    scheduled,
                    ignored,
                    bad,
                    len(to_ack),
                )

        except Exception as e:
            log.error("PACK_POSTPROC loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)