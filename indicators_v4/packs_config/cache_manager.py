# packs_config/cache_manager.py — кеши/реестр ind_pack + init/runtime reload (postproc_ready) для adaptive/quantiles/labels

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
    load_labels_bins_for_pair,
    load_static_bins_dict,
)
from packs_config.models import LabelsContext, PackRuntime, BinRule
from packs_config.registry import build_pack_registry


# 🔸 Константы Redis (постпроцессинг бектеста → сигнал обновления словарей)
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"
POSTPROC_GROUP = "ind_pack_postproc_group_v4"
POSTPROC_CONSUMER = "ind_pack_postproc_1"


# 🔸 Глобальный реестр pack-инстансов, готовых к работе
pack_registry: dict[tuple[str, str], list[PackRuntime]] = {}
# key: (timeframe_from_stream, indicator_from_stream) -> list[PackRuntime]

# 🔸 Кеши правил
adaptive_bins_cache: dict[tuple[int, int, int, str, str], list[BinRule]] = {}
adaptive_quantiles_cache: dict[tuple[int, int, int, str, str], list[BinRule]] = {}
labels_bins_cache: dict[tuple[int, int, str, int, str, str], set[str]] = {}

# 🔸 Индексы и быстрые множества для reload по парам
adaptive_pairs_index: dict[tuple[int, int], set[int]] = {}
adaptive_pairs_set: set[tuple[int, int]] = set()

adaptive_quantiles_pairs_index: dict[tuple[int, int], set[int]] = {}
adaptive_quantiles_pairs_set: set[tuple[int, int]] = set()

labels_pairs_index: dict[tuple[int, int], set[LabelsContext]] = {}
labels_pairs_set: set[tuple[int, int]] = set()

# 🔸 Locks и флаги готовности кешей
adaptive_lock = asyncio.Lock()
labels_lock = asyncio.Lock()

caches_ready = {
    "registry": False,
    "adaptive_bins": False,
    "quantiles": False,
    "labels": False,
}

# 🔸 Статусы перезагрузки пар
reloading_pairs_bins: set[tuple[int, int]] = set()
reloading_pairs_quantiles: set[tuple[int, int]] = set()
reloading_pairs_labels: set[tuple[int, int]] = set()


# 🔸 Helpers: labels cache key + contains
def labels_cache_key(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
) -> tuple[int, int, str, int, str, str]:
    return (
        int(scenario_id),
        int(signal_id),
        str(direction),
        int(analysis_id),
        str(indicator_param),
        str(timeframe),
    )


def labels_has_bin(
    scenario_id: int,
    signal_id: int,
    direction: str,
    analysis_id: int,
    indicator_param: str,
    timeframe: str,
    bin_name: str,
) -> bool:
    s = labels_bins_cache.get(labels_cache_key(scenario_id, signal_id, direction, analysis_id, indicator_param, timeframe))
    if not s:
        return False
    return str(bin_name) in s


# 🔸 Get adaptive rules
def get_adaptive_rules(analysis_id: int, scenario_id: int, signal_id: int, timeframe: str, direction: str) -> list[BinRule]:
    return adaptive_bins_cache.get((analysis_id, scenario_id, signal_id, timeframe, direction), [])


# 🔸 Consumer-group helper
async def ensure_stream_group(redis: Any, stream: str, group: str):
    log = logging.getLogger("PACK_STREAM")
    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning("xgroup_create error for %s/%s: %s", stream, group, e)


# 🔸 Cache init + indexes build
async def init_pack_runtime(pg: Any):
    global pack_registry, adaptive_pairs_index, adaptive_pairs_set
    global adaptive_quantiles_pairs_index, adaptive_quantiles_pairs_set
    global labels_pairs_index, labels_pairs_set

    log = logging.getLogger("PACK_INIT")

    caches_ready["registry"] = False
    caches_ready["adaptive_bins"] = False
    caches_ready["quantiles"] = False
    caches_ready["labels"] = False

    # очистка кешей и флагов reload на старте
    adaptive_bins_cache.clear()
    adaptive_quantiles_cache.clear()
    labels_bins_cache.clear()

    reloading_pairs_bins.clear()
    reloading_pairs_quantiles.clear()
    reloading_pairs_labels.clear()

    packs = await load_enabled_packs(pg)
    analysis_ids = sorted({int(p["analysis_id"]) for p in packs})

    analysis_meta = await load_analysis_instances(pg, analysis_ids)
    analysis_params = await load_analysis_parameters(pg, analysis_ids)
    static_bins_dict = await load_static_bins_dict(pg, analysis_ids)

    new_registry = build_pack_registry(packs, analysis_meta, analysis_params, static_bins_dict)

    pack_registry.clear()
    pack_registry.update(new_registry)

    caches_ready["registry"] = True

    # reset indices
    adaptive_pairs_index = {}
    adaptive_pairs_set = set()

    adaptive_quantiles_pairs_index = {}
    adaptive_quantiles_pairs_set = set()

    labels_pairs_index = {}
    labels_pairs_set = set()

    all_runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        all_runtimes.extend(lst)

    # build indices
    adaptive_runtimes = 0
    mtf_runtimes = 0

    for rt in all_runtimes:
        if rt.bins_source == "adaptive":
            adaptive_runtimes += 1
            for pair in rt.adaptive_pairs:
                adaptive_pairs_set.add(pair)
                adaptive_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

        if rt.is_mtf and rt.mtf_pairs:
            mtf_runtimes += 1
            for pair in rt.mtf_pairs:
                labels_pairs_set.add(pair)
                ctx = LabelsContext(
                    analysis_id=int(rt.analysis_id),
                    indicator_param=str(rt.source_param_name),
                    timeframe="mtf",
                )
                labels_pairs_index.setdefault(pair, set()).add(ctx)

        if rt.is_mtf and rt.mtf_pairs and rt.mtf_quantiles_key:
            for pair in rt.mtf_pairs:
                adaptive_quantiles_pairs_set.add(pair)
                adaptive_quantiles_pairs_index.setdefault(pair, set()).add(int(rt.analysis_id))

    log.info("PACK_INIT: adaptive pairs configured: %s (adaptive_runtimes=%s)", len(adaptive_pairs_set), adaptive_runtimes)
    log.info("PACK_INIT: labels pairs configured: %s (mtf_runtimes=%s)", len(labels_pairs_set), mtf_runtimes)

    # первичная загрузка adaptive bins cache
    loaded_pairs = 0
    loaded_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_pairs_set)):
        analysis_list = sorted(list(adaptive_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id, "bins")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_pairs += 1
        loaded_rules_total += rules_loaded

    caches_ready["adaptive_bins"] = True
    log.info("PACK_INIT: adaptive cache ready — pairs_loaded=%s, rules_total=%s", loaded_pairs, loaded_rules_total)

    # первичная загрузка quantiles cache
    loaded_q_pairs = 0
    loaded_q_rules_total = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_quantiles_pairs_set)):
        analysis_list = sorted(list(adaptive_quantiles_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded_q = await load_adaptive_bins_for_pair(pg, analysis_list, scenario_id, signal_id, "quantiles")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded_q.items():
                adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_q_pairs += 1
        loaded_q_rules_total += rules_loaded

    caches_ready["quantiles"] = True
    log.info("PACK_INIT: adaptive quantiles cache ready — pairs_loaded=%s, rules_total=%s", loaded_q_pairs, loaded_q_rules_total)

    # первичная загрузка labels cache
    loaded_lbl_pairs = 0
    loaded_lbl_keys = 0

    for (scenario_id, signal_id) in sorted(list(labels_pairs_set)):
        contexts = sorted(
            list(labels_pairs_index.get((scenario_id, signal_id), set())),
            key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe),
        )
        if not contexts:
            continue

        loaded = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

        async with labels_lock:
            for k, s in loaded.items():
                labels_bins_cache[k] = s

        loaded_lbl_pairs += 1
        loaded_lbl_keys += len(loaded)

    caches_ready["labels"] = True
    log.info("PACK_INIT: labels cache ready — pairs_loaded=%s, keys=%s", loaded_lbl_pairs, loaded_lbl_keys)


# 🔸 Reload on postproc_ready
async def watch_postproc_ready(pg: Any, redis: Any):
    log = logging.getLogger("PACK_POSTPROC")
    sem = asyncio.Semaphore(50)

    async def _reload_pair(scenario_id: int, signal_id: int):
        async with sem:
            pair = (int(scenario_id), int(signal_id))

            reloading_pairs_bins.add(pair)
            reloading_pairs_quantiles.add(pair)
            reloading_pairs_labels.add(pair)

            try:
                # bins reload
                analysis_ids = sorted(list(adaptive_pairs_index.get(pair, set())))
                if analysis_ids:
                    loaded = await load_adaptive_bins_for_pair(pg, analysis_ids, scenario_id, signal_id, "bins")

                    async with adaptive_lock:
                        # условия: удаляем старые ключи этой пары для наших analysis_ids
                        to_del = [k for k in list(adaptive_bins_cache.keys()) if k[1] == scenario_id and k[2] == signal_id and k[0] in analysis_ids]
                        for k in to_del:
                            adaptive_bins_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded.items():
                            adaptive_bins_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info(
                        "PACK_ADAPTIVE: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                        scenario_id,
                        signal_id,
                        analysis_ids,
                        loaded_rules,
                    )

                # quantiles reload
                q_analysis_ids = sorted(list(adaptive_quantiles_pairs_index.get(pair, set())))
                if q_analysis_ids:
                    loaded_q = await load_adaptive_bins_for_pair(pg, q_analysis_ids, scenario_id, signal_id, "quantiles")

                    async with adaptive_lock:
                        # условия: удаляем старые ключи этой пары для наших analysis_ids
                        to_del = [k for k in list(adaptive_quantiles_cache.keys()) if k[1] == scenario_id and k[2] == signal_id and k[0] in q_analysis_ids]
                        for k in to_del:
                            adaptive_quantiles_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded_q.items():
                            adaptive_quantiles_cache[(aid, scenario_id, signal_id, tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info(
                        "PACK_ADAPTIVE_QUANTILES: updated (scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                        scenario_id,
                        signal_id,
                        q_analysis_ids,
                        loaded_rules,
                    )

                # labels reload
                contexts = sorted(
                    list(labels_pairs_index.get(pair, set())),
                    key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe),
                )
                if contexts:
                    loaded_bins = await load_labels_bins_for_pair(pg, scenario_id, signal_id, contexts)

                    async with labels_lock:
                        # условия: удаляем старые ключи этой пары по ctx_set
                        ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}
                        to_del = [k for k in list(labels_bins_cache.keys()) if k[0] == scenario_id and k[1] == signal_id and (k[3], k[4], k[5]) in ctx_set]
                        for k in to_del:
                            labels_bins_cache.pop(k, None)

                        bins_loaded = 0
                        for k, s in loaded_bins.items():
                            labels_bins_cache[k] = s
                            bins_loaded += len(s)

                    log.info(
                        "PACK_LABELS: updated (scenario_id=%s, signal_id=%s, ctx=%s, bins_loaded=%s)",
                        scenario_id,
                        signal_id,
                        len(contexts),
                        bins_loaded,
                    )

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

            for _, messages in resp:
                for msg_id, data in messages:
                    to_ack.append(msg_id)
                    try:
                        scenario_id = int(data.get("scenario_id"))
                        signal_id = int(data.get("signal_id"))
                    except Exception:
                        ignored += 1
                        continue

                    pair = (scenario_id, signal_id)
                    if pair not in adaptive_pairs_set and pair not in labels_pairs_set and pair not in adaptive_quantiles_pairs_set:
                        ignored += 1
                        continue

                    asyncio.create_task(_reload_pair(scenario_id, signal_id))
                    scheduled += 1

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

            if scheduled or ignored:
                log.info("PACK_POSTPROC: batch handled (scheduled=%s, ignored=%s, ack=%s)", scheduled, ignored, len(to_ack))

        except Exception as e:
            log.error("PACK_POSTPROC loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)