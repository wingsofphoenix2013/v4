# packs_config/cache_manager.py — кеши/реестр ind_pack + init/runtime reload (postproc_ready) + run-aware загрузка + refresh m15/h1 adaptive + current run_id map

from __future__ import annotations

# 🔸 Imports
import asyncio
import logging
import math
from typing import Any

from packs_config.db_loaders import (
    load_adaptive_bins_for_pair,
    load_analysis_instances,
    load_analysis_parameters,
    load_enabled_packs,
    load_labels_bins_for_pair,
    load_latest_finished_run_ids,
    load_static_bins_dict,
)
from packs_config.models import LabelsContext, PackRuntime, BinRule
from packs_config.registry import build_pack_registry

from packs_config.contract import (
    build_fail_details_base,
    pack_fail,
    pack_ok,
    short_error_str,
)
from packs_config.publish import publish_pair
from packs_config.redis_ts import IND_TS_PREFIX, ts_get, ts_get_value_at


# 🔸 Константы Redis (постпроцессинг бектеста → сигнал обновления словарей)
POSTPROC_STREAM_KEY = "bt:analysis:postproc_ready"
POSTPROC_GROUP = "ind_pack_postproc_group_v4"
POSTPROC_CONSUMER = "ind_pack_postproc_1"

# 🔸 Константы БД
BB_TICKERS_TABLE = "tickers_bb"

# 🔸 Параметры refresh m15/h1 (после postproc_ready)
REFRESH_MAX_PARALLEL = 250

# 🔸 Константы Redis TS (feed_bb)
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}


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

# 🔸 Текущий run_id по signal_id (для мета-записи в PG)
current_run_by_signal: dict[int, int] = {}
current_run_lock = asyncio.Lock()

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


# 🔸 Helpers: current run getters
def get_current_run_id(signal_id: int) -> int | None:
    try:
        return current_run_by_signal.get(int(signal_id))
    except Exception:
        return None


async def _set_current_run_id(signal_id: int, run_id: int):
    async with current_run_lock:
        current_run_by_signal[int(signal_id)] = int(run_id)


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


# 🔸 DB helper: active symbols
async def _load_active_symbols(pg: Any) -> list[str]:
    log = logging.getLogger("PACK_REFRESH")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    symbols = [str(r["symbol"]) for r in rows if r.get("symbol")]
    log.info("PACK_REFRESH: активных тикеров загружено: %s", len(symbols))
    return symbols


# 🔸 Refresh: adaptive single-TF m15/h1 после postproc_ready (по обновлённым кешам)
async def _refresh_pair_adaptive_m15_h1(pg: Any, redis: Any, run_id: int, scenario_id: int, signal_id: int):
    log = logging.getLogger("PACK_REFRESH")

    # собираем релевантные runtime: adaptive + single-TF + m15/h1 + содержит пару
    runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        for rt in lst:
            if rt.is_mtf:
                continue
            if rt.bins_source != "adaptive":
                continue
            if str(rt.timeframe) not in ("m15", "h1"):
                continue
            if (int(scenario_id), int(signal_id)) not in (rt.adaptive_pairs or []):
                continue
            runtimes.append(rt)

    # условия достаточности
    if not runtimes:
        return

    symbols = await _load_active_symbols(pg)
    if not symbols:
        return

    sem = asyncio.Semaphore(REFRESH_MAX_PARALLEL)
    ok_sum = 0
    fail_sum = 0
    errors_sum = 0

    async def _process_one(symbol: str, rt: PackRuntime):
        nonlocal ok_sum, fail_sum, errors_sum
        async with sem:
            trigger = {
                "indicator": "postproc_ready",
                "timeframe": str(rt.timeframe),
                "open_time": None,
                "status": "refresh",
                "run_id": str(run_id),
            }
            open_ts_ms: int | None = None

            # извлекаем value по логике bootstrap (не ждём indicator_stream)
            value: Any = None
            missing: list[Any] = []
            invalid_info: dict[str, Any] | None = None

            try:
                if rt.analysis_key == "bb_band_bin":
                    prefix = rt.source_param_name
                    upper = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_upper")
                    lower = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_lower")

                    if not upper:
                        missing.append(f"{prefix}_upper")
                    if not lower:
                        missing.append(f"{prefix}_lower")

                    if upper and lower:
                        open_ts_ms = int(upper[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", int(open_ts_ms))
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"price": str(close_val), "upper": str(upper[1]), "lower": str(lower[1])}

                elif rt.analysis_key == "lr_band_bin":
                    prefix = rt.source_param_name
                    upper = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_upper")
                    lower = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{prefix}_lower")

                    if not upper:
                        missing.append(f"{prefix}_upper")
                    if not lower:
                        missing.append(f"{prefix}_lower")

                    if upper and lower:
                        open_ts_ms = int(upper[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", int(open_ts_ms))
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"price": str(close_val), "upper": str(upper[1]), "lower": str(lower[1])}

                elif rt.analysis_key == "atr_bin":
                    atr = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{rt.source_param_name}")
                    if not atr:
                        missing.append(str(rt.source_param_name))
                    else:
                        open_ts_ms = int(atr[0])
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", int(open_ts_ms))
                        if close_val is None:
                            missing.append({"tf": rt.timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})
                        else:
                            value = {"atr": str(atr[1]), "price": str(close_val)}

                elif rt.analysis_key == "dmigap_bin":
                    base = rt.source_param_name
                    plus = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_plus_di")
                    minus = await ts_get(redis, f"{IND_TS_PREFIX}:{symbol}:{rt.timeframe}:{base}_minus_di")

                    if not plus:
                        missing.append(f"{base}_plus_di")
                    if not minus:
                        missing.append(f"{base}_minus_di")

                    if plus and minus:
                        value = {"plus": str(plus[1]), "minus": str(minus[1])}

                else:
                    raw = await redis.get(f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}")
                    if raw is None:
                        missing.append(str(rt.source_param_name))
                    else:
                        try:
                            f = float(raw)
                            if not math.isfinite(f):
                                raise ValueError("NaN/inf")
                            value = f
                        except Exception:
                            invalid_info = {"tf": str(rt.timeframe), "param": str(rt.source_param_name), "raw": str(raw)}

            except Exception as e:
                invalid_info = {"tf": str(rt.timeframe), "param": str(rt.source_param_name), "raw": short_error_str(e)}

            # публикуем только adaptive-pair ключи
            for direction in ("long", "short"):
                base_details = build_fail_details_base(
                    int(rt.analysis_id),
                    str(symbol),
                    str(direction),
                    str(rt.timeframe),
                    {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger,
                    open_ts_ms,
                )

                # invalid input
                if invalid_info is not None:
                    base_details["kind"] = "single_value"
                    base_details["input"] = invalid_info
                    base_details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("invalid_input_value", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    fail_sum += 1
                    continue

                # missing inputs
                if value is None:
                    base_details["missing"] = missing
                    base_details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("missing_inputs", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    fail_sum += 1
                    continue

                # rules readiness (на всякий случай)
                if not caches_ready.get("adaptive_bins", False):
                    base_details["need"] = ["adaptive_bins"]
                    base_details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("rules_not_loaded_yet", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    fail_sum += 1
                    continue

                rules = get_adaptive_rules(int(rt.analysis_id), int(scenario_id), int(signal_id), str(rt.timeframe), str(direction))
                if not rules:
                    base_details["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "adaptive", "direction": str(direction)}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("no_rules_adaptive", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    fail_sum += 1
                    continue

                try:
                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                except Exception as e:
                    base_details["where"] = "postproc_refresh/bin_value"
                    base_details["error"] = short_error_str(e)
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("internal_error", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    errors_sum += 1
                    continue

                if not bin_name:
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        str(rt.timeframe),
                        pack_fail("no_candidates", base_details),
                        int(rt.ttl_sec),
                        meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                    )
                    fail_sum += 1
                    continue

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    str(rt.timeframe),
                    pack_ok(str(bin_name)),
                    int(rt.ttl_sec),
                    meta={"run_id": str(run_id), "open_ts_ms": str(open_ts_ms) if open_ts_ms is not None else "", "open_time": ""},
                )
                ok_sum += 1

    tasks = [asyncio.create_task(_process_one(sym, rt)) for sym in symbols for rt in runtimes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # суммирующий лог
    hard_errors = sum(1 for r in results if isinstance(r, Exception))
    log.info(
        "PACK_REFRESH: done (run_id=%s, scenario_id=%s, signal_id=%s, runtimes=%s, symbols=%s, ok=%s, fail=%s, errors=%s, hard_errors=%s)",
        run_id,
        scenario_id,
        signal_id,
        len(runtimes),
        len(symbols),
        ok_sum,
        fail_sum,
        errors_sum,
        hard_errors,
    )


# 🔸 Cache init + indexes build (run-aware)
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

    # run registry: свежий run_id на старте (по signal_id)
    all_pairs = set(adaptive_pairs_set) | set(labels_pairs_set) | set(adaptive_quantiles_pairs_set)
    signal_ids = sorted({int(sig) for (_, sig) in all_pairs})
    latest_runs_by_signal = await load_latest_finished_run_ids(pg, signal_ids)

    # обновим current_run_by_signal
    async with current_run_lock:
        current_run_by_signal.clear()
        current_run_by_signal.update({int(k): int(v) for k, v in latest_runs_by_signal.items()})

    runs_ok = 0
    runs_missing = 0
    for (_, sid) in all_pairs:
        if int(sid) in latest_runs_by_signal:
            runs_ok += 1
        else:
            runs_missing += 1
    log.info("PACK_INIT: start runs resolved — pairs=%s, ok=%s, missing=%s", len(all_pairs), runs_ok, runs_missing)

    # первичная загрузка adaptive bins cache (run-aware)
    loaded_pairs = 0
    loaded_rules_total = 0
    skipped_pairs = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_pairs_set)):
        run_id = latest_runs_by_signal.get(int(signal_id))
        # условия достаточности
        if not run_id:
            skipped_pairs += 1
            continue

        analysis_list = sorted(list(adaptive_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded = await load_adaptive_bins_for_pair(pg, int(run_id), analysis_list, int(scenario_id), int(signal_id), "bins")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded.items():
                adaptive_bins_cache[(aid, int(scenario_id), int(signal_id), tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_pairs += 1
        loaded_rules_total += rules_loaded

    caches_ready["adaptive_bins"] = True
    log.info(
        "PACK_INIT: adaptive cache ready — pairs_loaded=%s, pairs_skipped_no_run=%s, rules_total=%s",
        loaded_pairs,
        skipped_pairs,
        loaded_rules_total,
    )

    # первичная загрузка quantiles cache (run-aware)
    loaded_q_pairs = 0
    loaded_q_rules_total = 0
    skipped_q_pairs = 0

    for (scenario_id, signal_id) in sorted(list(adaptive_quantiles_pairs_set)):
        run_id = latest_runs_by_signal.get(int(signal_id))
        # условия достаточности
        if not run_id:
            skipped_q_pairs += 1
            continue

        analysis_list = sorted(list(adaptive_quantiles_pairs_index.get((scenario_id, signal_id), set())))
        if not analysis_list:
            continue

        loaded_q = await load_adaptive_bins_for_pair(pg, int(run_id), analysis_list, int(scenario_id), int(signal_id), "quantiles")

        async with adaptive_lock:
            rules_loaded = 0
            for (aid, tf, direction), rules in loaded_q.items():
                adaptive_quantiles_cache[(aid, int(scenario_id), int(signal_id), tf, direction)] = rules
                rules_loaded += len(rules)

        loaded_q_pairs += 1
        loaded_q_rules_total += rules_loaded

    caches_ready["quantiles"] = True
    log.info(
        "PACK_INIT: adaptive quantiles cache ready — pairs_loaded=%s, pairs_skipped_no_run=%s, rules_total=%s",
        loaded_q_pairs,
        skipped_q_pairs,
        loaded_q_rules_total,
    )

    # первичная загрузка labels cache (run-aware, без фильтра по state)
    loaded_lbl_pairs = 0
    loaded_lbl_keys = 0
    skipped_lbl_pairs = 0

    for (scenario_id, signal_id) in sorted(list(labels_pairs_set)):
        run_id = latest_runs_by_signal.get(int(signal_id))
        # условия достаточности
        if not run_id:
            skipped_lbl_pairs += 1
            continue

        contexts = sorted(
            list(labels_pairs_index.get((scenario_id, signal_id), set())),
            key=lambda x: (x.analysis_id, x.indicator_param, x.timeframe),
        )
        if not contexts:
            continue

        loaded = await load_labels_bins_for_pair(pg, int(run_id), int(scenario_id), int(signal_id), contexts)

        async with labels_lock:
            for k, s in loaded.items():
                labels_bins_cache[k] = s

        loaded_lbl_pairs += 1
        loaded_lbl_keys += len(loaded)

    caches_ready["labels"] = True
    log.info(
        "PACK_INIT: labels cache ready — pairs_loaded=%s, pairs_skipped_no_run=%s, keys=%s",
        loaded_lbl_pairs,
        skipped_lbl_pairs,
        loaded_lbl_keys,
    )


# 🔸 Reload on postproc_ready (run-aware + refresh m15/h1 adaptive)
async def watch_postproc_ready(pg: Any, redis: Any):
    log = logging.getLogger("PACK_POSTPROC")
    sem = asyncio.Semaphore(50)

    async def _reload_pair(run_id: int, scenario_id: int, signal_id: int):
        async with sem:
            pair = (int(scenario_id), int(signal_id))

            reloading_pairs_bins.add(pair)
            reloading_pairs_quantiles.add(pair)
            reloading_pairs_labels.add(pair)

            try:
                # bins reload
                analysis_ids = sorted(list(adaptive_pairs_index.get(pair, set())))
                if analysis_ids:
                    loaded = await load_adaptive_bins_for_pair(pg, int(run_id), analysis_ids, int(scenario_id), int(signal_id), "bins")

                    async with adaptive_lock:
                        # условия: удаляем старые ключи этой пары для наших analysis_ids
                        to_del = [k for k in list(adaptive_bins_cache.keys()) if k[1] == int(scenario_id) and k[2] == int(signal_id) and k[0] in analysis_ids]
                        for k in to_del:
                            adaptive_bins_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded.items():
                            adaptive_bins_cache[(aid, int(scenario_id), int(signal_id), tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info(
                        "PACK_ADAPTIVE: updated (run_id=%s, scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                        run_id,
                        scenario_id,
                        signal_id,
                        analysis_ids,
                        loaded_rules,
                    )

                # quantiles reload
                q_analysis_ids = sorted(list(adaptive_quantiles_pairs_index.get(pair, set())))
                if q_analysis_ids:
                    loaded_q = await load_adaptive_bins_for_pair(pg, int(run_id), q_analysis_ids, int(scenario_id), int(signal_id), "quantiles")

                    async with adaptive_lock:
                        # условия: удаляем старые ключи этой пары для наших analysis_ids
                        to_del = [k for k in list(adaptive_quantiles_cache.keys()) if k[1] == int(scenario_id) and k[2] == int(signal_id) and k[0] in q_analysis_ids]
                        for k in to_del:
                            adaptive_quantiles_cache.pop(k, None)

                        loaded_rules = 0
                        for (aid, tf, direction), rules in loaded_q.items():
                            adaptive_quantiles_cache[(aid, int(scenario_id), int(signal_id), tf, direction)] = rules
                            loaded_rules += len(rules)

                    log.info(
                        "PACK_ADAPTIVE_QUANTILES: updated (run_id=%s, scenario_id=%s, signal_id=%s, analysis_ids=%s, rules_loaded=%s)",
                        run_id,
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
                    loaded_bins = await load_labels_bins_for_pair(pg, int(run_id), int(scenario_id), int(signal_id), contexts)

                    async with labels_lock:
                        # условия: удаляем старые ключи этой пары по ctx_set
                        ctx_set = {(c.analysis_id, c.indicator_param, c.timeframe) for c in contexts}
                        to_del = [k for k in list(labels_bins_cache.keys()) if k[0] == int(scenario_id) and k[1] == int(signal_id) and (k[3], k[4], k[5]) in ctx_set]
                        for k in to_del:
                            labels_bins_cache.pop(k, None)

                        bins_loaded = 0
                        for k, s in loaded_bins.items():
                            labels_bins_cache[k] = s
                            bins_loaded += len(s)

                    log.info(
                        "PACK_LABELS: updated (run_id=%s, scenario_id=%s, signal_id=%s, ctx=%s, bins_loaded=%s)",
                        run_id,
                        scenario_id,
                        signal_id,
                        len(contexts),
                        bins_loaded,
                    )

            finally:
                reloading_pairs_bins.discard(pair)
                reloading_pairs_quantiles.discard(pair)
                reloading_pairs_labels.discard(pair)

            # refresh: сразу публикуем adaptive single-TF m15/h1 по обновлённым кешам
            try:
                await _refresh_pair_adaptive_m15_h1(pg, redis, int(run_id), int(scenario_id), int(signal_id))
            except Exception as e:
                log.error(
                    "PACK_REFRESH error (run_id=%s, scenario_id=%s, signal_id=%s): %s",
                    run_id,
                    scenario_id,
                    signal_id,
                    e,
                    exc_info=True,
                )

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
                        run_id = int(data.get("run_id"))
                        scenario_id = int(data.get("scenario_id"))
                        signal_id = int(data.get("signal_id"))
                    except Exception:
                        ignored += 1
                        continue

                    pair = (scenario_id, signal_id)
                    if pair not in adaptive_pairs_set and pair not in labels_pairs_set and pair not in adaptive_quantiles_pairs_set:
                        ignored += 1
                        continue

                    # обновим current_run_by_signal по факту postproc_ready
                    await _set_current_run_id(int(signal_id), int(run_id))

                    asyncio.create_task(_reload_pair(int(run_id), int(scenario_id), int(signal_id)))
                    scheduled += 1

            if to_ack:
                await redis.xack(POSTPROC_STREAM_KEY, POSTPROC_GROUP, *to_ack)

            if scheduled or ignored:
                log.info(
                    "PACK_POSTPROC: batch handled (scheduled=%s, ignored=%s, ack=%s)",
                    scheduled,
                    ignored,
                    len(to_ack),
                )

        except Exception as e:
            log.error("PACK_POSTPROC loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)