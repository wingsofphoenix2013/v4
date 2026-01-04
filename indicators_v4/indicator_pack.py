# indicator_pack.py — ++ оркестратор расчёта и публикации ind_pack (winner-driven): MTF only + кеш rules (adaptive) + static bins in runtime + PG meta

from __future__ import annotations

# 🔸 Базовые импорты
import asyncio
import logging
from decimal import Decimal
from typing import Any

# 🔸 Imports: packs_config (cache manager)
from packs_config.cache_manager import (
    POSTPROC_GROUP,
    POSTPROC_STREAM_KEY,
    caches_ready,
    pack_registry,
    reloading_pairs_bins,
    reloading_pairs_labels,
    reloading_pairs_quantiles,
    ensure_stream_group,
    get_active_trigger_keys_m5,
    get_adaptive_rules,
    get_adaptive_quantiles,
    get_winner_analysis_id,
    get_winner_run_id,
    init_pack_runtime,
    watch_postproc_ready,
)

# 🔸 Imports: packs_config (models + contract)
from packs_config.models import PackRuntime
from packs_config.contract import (
    pack_ok,
    pack_fail,
    short_error_str,
    build_fail_details_base,
    parse_open_time_to_open_ts_ms,
)

# 🔸 Imports: packs_config (redis ts helpers)
from packs_config.redis_ts import (
    MTF_RETRY_STEP_SEC,
    clip_0_100,
    get_mtf_value_decimal,
    get_ts_decimal_with_retry,
    safe_decimal,
    ts_get_value_at,
)

# 🔸 Imports: packs_config (publish helpers)
from packs_config.publish import publish_pair

# 🔸 Imports: packs_config (bootstrap)
from packs_config.bootstrap import bootstrap_current_state

# 🔸 Константы Redis (индикаторы)
INDICATOR_STREAM = "indicator_stream"          # входной стрим готовности индикаторов
IND_PACK_GROUP = "ind_pack_group_v4"           # consumer-group для indicator_stream
IND_PACK_CONSUMER = "ind_pack_consumer_1"      # consumer name

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500          # сколько сообщений читать за раз
STREAM_BLOCK_MS = 2000           # блокировка XREADGROUP (мс)
MAX_PARALLEL_MESSAGES = 200      # сколько сообщений обрабатывать параллельно

# 🔸 Ограничения диагностики (не заливаем Redis)
MAX_CANDIDATES_IN_DETAILS = 5

# 🔸 Константы Redis TS (feed_bb)
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}


# 🔸 Publish meta helper (для записи в PG через ind_pack_stream_core)
def build_publish_meta(open_ts_ms: int | None, open_time: str | None, run_id: int | None) -> dict[str, Any] | None:
    meta: dict[str, Any] = {}
    if run_id is not None:
        meta["run_id"] = str(int(run_id))
    if open_ts_ms is not None:
        meta["open_ts_ms"] = str(int(open_ts_ms))
    if open_time is not None:
        meta["open_time"] = str(open_time)
    return meta if meta else None


# 🔸 Handle MTF runtime: publish only for winner pairs (scenario_id, signal_id) where winner_analysis_id == rt.analysis_id
async def handle_mtf_runtime(
    redis: Any,
    rt: PackRuntime,
    symbol: str,
    trigger: dict[str, Any],
    open_ts_ms: int | None,
) -> tuple[int, int]:
    log = logging.getLogger("PACK_MTF")

    # условия достаточности runtime
    if not rt.mtf_pairs or not rt.mtf_component_tfs or not rt.mtf_component_params or not rt.mtf_bins_static:
        return 0, 0

    published_ok = 0
    published_fail = 0

    # helper: publish fail for winner pairs only
    async def _publish_fail_for_winner_pairs(reason: str, details_extra: dict[str, Any], open_ts_for_details: int | None):
        nonlocal published_fail

        for (scenario_id, signal_id) in rt.mtf_pairs:
            # winner gating
            if get_winner_analysis_id(int(scenario_id), int(signal_id)) != int(rt.analysis_id):
                continue

            run_id = get_winner_run_id(int(scenario_id), int(signal_id))

            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_for_details) if open_ts_for_details is not None else None,
                )
                # доп. данные
                for k, v in (details_extra or {}).items():
                    details[k] = v

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    pack_fail(str(reason), details),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(open_ts_ms, trigger.get("open_time"), run_id),
                )
                published_fail += 1

    # invalid_trigger_event: open_time не парсится
    if open_ts_ms is None:
        await _publish_fail_for_winner_pairs(
            "invalid_trigger_event",
            {
                "missing_fields": ["open_time"],
                "parse": {"open_time": trigger.get("open_time"), "open_ts_ms": None},
                "retry": {"recommended": False, "after_sec": 0},
            },
            None,
        )
        return published_ok, published_fail

    # rules_not_loaded_yet: winners / rules cache not ready
    if not caches_ready.get("winners", False) or not caches_ready.get("adaptive_bins", False) or (rt.mtf_quantiles_key and not caches_ready.get("quantiles", False)):
        need: list[str] = []
        if not caches_ready.get("winners", False):
            need.append("winners")
        if not caches_ready.get("adaptive_bins", False):
            need.append("adaptive_bins")
        if rt.mtf_quantiles_key and not caches_ready.get("quantiles", False):
            need.append("quantiles")

        await _publish_fail_for_winner_pairs(
            "rules_not_loaded_yet",
            {"need": need, "retry": {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}},
            int(open_ts_ms),
        )
        return published_ok, published_fail

    # собираем значения по TF
    tasks: list[asyncio.Task] = []
    meta: list[tuple[str, str | None, str]] = []  # (tf, sub_name|None, param_name)

    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)

        # один параметр на TF
        if isinstance(spec, str):
            pname = str(spec or "").strip()
            tasks.append(
                asyncio.create_task(
                    get_mtf_value_decimal(redis, str(symbol), int(open_ts_ms), str(tf), pname)
                    if pname else asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))
                )
            )
            meta.append((str(tf), None, pname))

        # несколько параметров на TF (dict)
        elif isinstance(spec, dict):
            for name, param_name in spec.items():
                pname = str(param_name or "").strip()
                if not pname:
                    tasks.append(asyncio.create_task(asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
                    meta.append((str(tf), str(name), ""))
                    continue

                # для m5 в multi-param режиме читаем TS по open_time m5
                if str(tf) == "m5":
                    tasks.append(asyncio.create_task(get_ts_decimal_with_retry(redis, str(symbol), "m5", pname, int(open_ts_ms))))
                    meta.append((str(tf), str(name), pname))
                else:
                    tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, str(symbol), int(open_ts_ms), str(tf), pname)))
                    meta.append((str(tf), str(name), pname))

        else:
            tasks.append(asyncio.create_task(asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
            meta.append((str(tf), None, ""))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    values_by_tf: dict[str, Any] = {}
    missing_items: list[Any] = []
    invalid_items: list[Any] = []
    styk_m15 = False
    styk_h1 = False
    waited_total = 0

    for (tf, name, param), r in zip(meta, results):
        if isinstance(r, Exception):
            missing_items.append({"tf": tf, "param": param or None, "error": short_error_str(r)})
            continue

        # нормализуем формат return
        d_val: Decimal | None = None
        raw_val: str | None = None
        meta_info: dict[str, Any] = {"styk": False, "waited_sec": 0}

        # get_mtf_value_decimal -> (Decimal|None, raw|None, meta)
        if isinstance(r, tuple) and len(r) == 3 and isinstance(r[2], dict):
            d_val = r[0]
            raw_val = r[1]
            meta_info = r[2] or meta_info

        # get_ts_decimal_with_retry -> (Decimal|None, raw|None, waited)
        elif isinstance(r, tuple) and len(r) == 3 and isinstance(r[2], int):
            d_val = r[0]
            raw_val = r[1]
            meta_info = {"styk": True, "waited_sec": int(r[2])}

        if meta_info.get("styk"):
            if str(tf) == "m15":
                styk_m15 = True
            if str(tf) == "h1":
                styk_h1 = True
        waited_total += int(meta_info.get("waited_sec") or 0)

        # значение отсутствует
        if d_val is None:
            missing_items.append({"tf": tf, "param": param or None, "name": name})
            continue

        # invalid_input_value: raw exists but not parseable
        if raw_val is not None and safe_decimal(raw_val) is None:
            invalid_items.append({"tf": tf, "param": param or None, "raw": str(raw_val), "kind": "mtf_component_value"})
            continue

        v = clip_0_100(d_val) if rt.mtf_clip_0_100 else d_val
        if name is None:
            values_by_tf[str(tf)] = v
        else:
            block = values_by_tf.get(str(tf))
            if not isinstance(block, dict):
                block = {}
                values_by_tf[str(tf)] = block
            block[str(name)] = v

    # проверка полноты по конфигу
    missing = False
    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)
        if isinstance(spec, str):
            if str(tf) not in values_by_tf:
                missing = True
                break
        elif isinstance(spec, dict):
            block = values_by_tf.get(str(tf))
            if not isinstance(block, dict):
                missing = True
                break
            for nm in spec.keys():
                if str(nm) not in block:
                    missing = True
                    break
            if missing:
                break
        else:
            missing = True
            break

    if missing or missing_items or invalid_items:
        # invalid_input_value имеет приоритет над “missing component values”
        if invalid_items:
            reason = "invalid_input_value"
        else:
            reason = "mtf_boundary_wait" if (styk_m15 or styk_h1) else "mtf_missing_component_values"

        await _publish_fail_for_winner_pairs(
            reason,
            {
                "missing": missing_items if not invalid_items else None,
                "kind": "mtf_component_value" if invalid_items else None,
                "input": invalid_items if invalid_items else None,
                "styk": {"m15": bool(styk_m15), "h1": bool(styk_h1), "waited_sec": int(waited_total)},
                "retry": {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)},
            },
            int(open_ts_ms),
        )

        if styk_m15 or styk_h1:
            log.info(
                "PACK_MTF: boundary wait → fail=%s (symbol=%s, analysis_id=%s, open_ts_ms=%s, styk_m15=%s, styk_h1=%s)",
                published_fail,
                symbol,
                rt.analysis_id,
                open_ts_ms,
                styk_m15,
                styk_h1,
            )
        return published_ok, published_fail

    # price for *_mtf where needed (например lr_mtf)
    if rt.mtf_needs_price:
        price_key = f"{BB_TS_PREFIX}:{symbol}:{rt.mtf_price_tf}:{rt.mtf_price_field}"
        raw_price = await ts_get_value_at(redis, price_key, int(open_ts_ms))
        price_d = safe_decimal(raw_price)

        if price_d is None:
            # missing/invalid price
            reason = "missing_inputs" if raw_price is None else "invalid_input_value"
            extra = {}
            if raw_price is None:
                extra["missing"] = [{"tf": str(rt.mtf_price_tf), "field": str(rt.mtf_price_field), "source": "bb:ts", "open_ts_ms": int(open_ts_ms)}]
                extra["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
            else:
                extra["kind"] = "price_value"
                extra["input"] = {"tf": str(rt.mtf_price_tf), "param": f"bb:{rt.mtf_price_field}", "raw": str(raw_price)}
                extra["retry"] = {"recommended": False, "after_sec": int(MTF_RETRY_STEP_SEC)}

            await _publish_fail_for_winner_pairs(reason, extra, int(open_ts_ms))
            return published_ok, published_fail

        values_by_tf["price"] = price_d

    # for each winner pair and direction compute and publish
    for (scenario_id, signal_id) in rt.mtf_pairs:
        # winner gating
        if get_winner_analysis_id(int(scenario_id), int(signal_id)) != int(rt.analysis_id):
            continue

        run_id = get_winner_run_id(int(scenario_id), int(signal_id))
        pair_key = (int(scenario_id), int(signal_id))

        # during reload — rules_not_loaded_yet (priority)
        pair_reloading = (pair_key in reloading_pairs_labels) or (pair_key in reloading_pairs_bins) or (pair_key in reloading_pairs_quantiles)
        if pair_reloading:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )
                details["need"] = ["winner", "rules"]
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    pack_fail("rules_not_loaded_yet", details),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                )
                published_fail += 1
            continue

        for direction in ("long", "short"):
            rules_by_tf: dict[str, list[Any]] = {}

            # static bins rules
            if str(rt.mtf_bins_tf) == "mtf":
                rules_by_tf["mtf"] = (rt.mtf_bins_static.get("mtf", {}) or {}).get(direction, []) or []
                if not rules_by_tf["mtf"]:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "bins", "tf": "mtf", "source": "static"}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        pack_fail("no_rules_static", details),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                    )
                    published_fail += 1
                    continue
            else:
                required_tfs = rt.mtf_required_bins_tfs or rt.mtf_component_tfs or []
                missing_rules_tfs: list[str] = []
                for tf in required_tfs:
                    rules = (rt.mtf_bins_static.get(str(tf), {}) or {}).get(direction, []) or []
                    rules_by_tf[str(tf)] = rules
                    if not rules:
                        missing_rules_tfs.append(str(tf))
                if missing_rules_tfs:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "bins", "tf": missing_rules_tfs, "source": "static"}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        pack_fail("no_rules_static", details),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                    )
                    published_fail += 1
                    continue

            # quantiles rules (adaptive; winner-driven cache)
            if rt.mtf_quantiles_key:
                q_rules = get_adaptive_quantiles(int(rt.analysis_id), int(scenario_id), int(signal_id), "mtf", str(direction))
                if not q_rules:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "quantiles", "tf": "mtf", "source": "adaptive"}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        pack_fail("no_quantiles_rules", details),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                    )
                    published_fail += 1
                    continue
                rules_by_tf[str(rt.mtf_quantiles_key)] = q_rules

            # candidates
            try:
                try:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf, direction=str(direction))
                except TypeError:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf)
            except Exception as e:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                details["where"] = "handle_mtf_runtime/bin_candidates"
                details["error"] = short_error_str(e)
                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    pack_fail("internal_error", details),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                )
                published_fail += 1
                continue

            # choose deterministically (labels больше не используются)
            cand_list = [str(x) for x in list(candidates or [])]
            cand_list.sort()
            chosen = cand_list[0] if cand_list else None

            if not chosen:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                details["candidates"] = [str(x) for x in list(candidates or [])[:MAX_CANDIDATES_IN_DETAILS]]
                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    pack_fail("no_candidates", details),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                )
                published_fail += 1
                continue

            await publish_pair(
                redis,
                int(rt.analysis_id),
                int(scenario_id),
                int(signal_id),
                str(direction),
                str(symbol),
                "mtf",
                pack_ok(chosen),
                int(rt.ttl_sec),
                meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
            )
            published_ok += 1

    if published_ok or published_fail:
        log.debug(
            "PACK_MTF: done (symbol=%s, analysis_id=%s, open_ts_ms=%s, ok=%s, fail=%s)",
            symbol,
            rt.analysis_id,
            open_ts_ms,
            published_ok,
            published_fail,
        )

    return published_ok, published_fail


# 🔸 Handle indicator_stream message (m5 only, filtered by active triggers): publish only for winner pairs
async def handle_indicator_event(redis: Any, msg: dict[str, Any]) -> dict[str, int]:
    log = logging.getLogger("PACK_SET")

    symbol = msg.get("symbol")
    timeframe = msg.get("timeframe")
    indicator_key = msg.get("indicator")
    status = msg.get("status")
    open_time = msg.get("open_time")
    source_param_name = msg.get("source_param_name")

    # invalid_trigger_event: если нельзя построить ключи (нет symbol/timeframe/indicator) — в Redis не пишем
    if not symbol or not timeframe or not indicator_key:
        log.debug(
            "PACK_SET: invalid trigger (missing fields) %s",
            {k: msg.get(k) for k in ("symbol", "timeframe", "indicator", "status", "open_time")},
        )
        return {"ok": 0, "fail": 0, "runtimes": 0}

    runtimes = pack_registry.get((str(timeframe), str(indicator_key)))
    if not runtimes:
        return {"ok": 0, "fail": 0, "runtimes": 0}

    # supertrend: если есть source_param_name из stream — фильтруем runtimes до точной серии (length+mult)
    if str(indicator_key).startswith("supertrend") and source_param_name:
        # нормализация серии: *_trend -> base
        def _st_series(p: Any) -> str | None:
            s = str(p or "").strip()
            if not s:
                return None
            if s.endswith("_trend"):
                return s[:-6]
            return s

        stream_series = _st_series(source_param_name)

        # условия достаточности
        if stream_series:
            before_n = len(runtimes)
            filtered: list[PackRuntime] = []

            for rt in runtimes:
                # MTF: проверяем, что в component_params встречается нужная серия
                if rt.is_mtf:
                    spec = rt.mtf_component_params or {}
                    found = False

                    for _, tf_spec in spec.items():
                        if isinstance(tf_spec, str):
                            if _st_series(tf_spec) == stream_series:
                                found = True
                                break
                        elif isinstance(tf_spec, dict):
                            for _, p in tf_spec.items():
                                if _st_series(p) == stream_series:
                                    found = True
                                    break
                            if found:
                                break

                    if found:
                        filtered.append(rt)
                else:
                    # non-mtf runtimes are not used in this module version
                    continue

            runtimes = filtered

            if before_n != len(runtimes):
                log.info(
                    "PACK_SET: supertrend filter (symbol=%s, tf=%s, indicator=%s, series=%s) runtimes %s→%s",
                    symbol,
                    timeframe,
                    indicator_key,
                    stream_series,
                    before_n,
                    len(runtimes),
                )

            if not runtimes:
                return {"ok": 0, "fail": 0, "runtimes": 0}

    open_ts_ms = parse_open_time_to_open_ts_ms(str(open_time) if open_time is not None else None)

    trigger = {
        "indicator": str(indicator_key),
        "timeframe": str(timeframe),
        "open_time": (str(open_time) if open_time is not None else None),
        "status": (str(status) if status is not None else None),
    }

    published_ok = 0
    published_fail = 0

    # status != ready → not_ready_retrying (winner pairs only)
    if status != "ready":
        for rt in runtimes:
            if not rt.is_mtf or not rt.mtf_pairs:
                continue

            for (scenario_id, signal_id) in rt.mtf_pairs:
                if get_winner_analysis_id(int(scenario_id), int(signal_id)) != int(rt.analysis_id):
                    continue

                run_id = get_winner_run_id(int(scenario_id), int(signal_id))

                for direction in ("long", "short"):
                    details = build_fail_details_base(
                        int(rt.analysis_id),
                        str(symbol),
                        str(direction),
                        "mtf",
                        {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                        trigger,
                        open_ts_ms,
                    )
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        pack_fail("not_ready_retrying", details),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(open_ts_ms, trigger.get("open_time"), run_id),
                    )
                    published_fail += 1

        return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}

    # ready: process runtimes
    for rt in runtimes:
        if not rt.is_mtf:
            continue
        ok_n, fail_n = await handle_mtf_runtime(redis, rt, str(symbol), trigger, open_ts_ms)
        published_ok += ok_n
        published_fail += fail_n

    return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}


# 🔸 Watch indicator_stream (parallel + фильтры m5/active triggers)
async def watch_indicator_stream(redis: Any):
    log = logging.getLogger("PACK_STREAM")
    sem = asyncio.Semaphore(MAX_PARALLEL_MESSAGES)

    async def _process_one(data: dict, active_keys_m5: set[str]) -> dict[str, int]:
        async with sem:
            tf = data.get("timeframe")
            ind = data.get("indicator")

            # фильтр: нас интересует только m5
            if tf != "m5":
                return {"ok": 0, "fail": 0, "runtimes": 0, "skipped_tf": 1, "skipped_trigger": 0}

            # фильтр: только активные триггеры победителей
            if not ind or str(ind) not in active_keys_m5:
                return {"ok": 0, "fail": 0, "runtimes": 0, "skipped_tf": 0, "skipped_trigger": 1}

            msg = {
                "symbol": data.get("symbol"),
                "timeframe": str(tf),
                "indicator": str(ind),
                "open_time": data.get("open_time"),
                "status": data.get("status"),
                "source_param_name": data.get("source_param_name"),
            }

            r = await handle_indicator_event(redis, msg)
            r.setdefault("skipped_tf", 0)
            r.setdefault("skipped_trigger", 0)
            return r

    while True:
        try:
            resp = await redis.xreadgroup(
                IND_PACK_GROUP,
                IND_PACK_CONSUMER,
                streams={INDICATOR_STREAM: ">"},
                count=STREAM_READ_COUNT,
                block=STREAM_BLOCK_MS,
            )
            if not resp:
                continue

            flat: list[tuple[str, dict[str, Any]]] = []
            for _, messages in resp:
                for msg_id, data in messages:
                    flat.append((msg_id, data))
            if not flat:
                continue

            to_ack = [msg_id for msg_id, _ in flat]

            # активные m5-триггеры победителей (снимок на батч)
            active_keys_m5 = get_active_trigger_keys_m5()

            tasks = [asyncio.create_task(_process_one(data, active_keys_m5)) for _, data in flat]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_ok = 0
            batch_fail = 0
            batch_runtimes = 0
            batch_skipped_tf = 0
            batch_skipped_trigger = 0
            errors = 0

            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    continue
                batch_ok += int(r.get("ok", 0))
                batch_fail += int(r.get("fail", 0))
                batch_runtimes += int(r.get("runtimes", 0))
                batch_skipped_tf += int(r.get("skipped_tf", 0))
                batch_skipped_trigger += int(r.get("skipped_trigger", 0))

            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

            # суммирующий лог по батчу
            log.info(
                "PACK_STREAM: batch done (msgs=%s, runtimes_total=%s, ok=%s, fail=%s, skipped_tf=%s, skipped_trigger=%s, errors=%s)",
                len(flat),
                batch_runtimes,
                batch_ok,
                batch_fail,
                batch_skipped_tf,
                batch_skipped_trigger,
                errors,
            )

        except Exception as e:
            log.error("PACK_STREAM loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 run worker
async def run_indicator_pack(pg: Any, redis: Any):
    # первичная загрузка (registry + winners + rules)
    await init_pack_runtime(pg)

    # создать группы заранее
    await ensure_stream_group(redis, INDICATOR_STREAM, IND_PACK_GROUP)
    await ensure_stream_group(redis, POSTPROC_STREAM_KEY, POSTPROC_GROUP)

    # холодный старт (в текущей схеме отключён внутри bootstrap.py)
    await bootstrap_current_state(pg, redis)

    # основная работа
    await asyncio.gather(
        watch_indicator_stream(redis),
        watch_postproc_ready(pg, redis),
    )