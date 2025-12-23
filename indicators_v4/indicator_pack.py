# indicator_pack.py — оркестратор расчёта и публикации ind_pack (JSON Contract v1): static/adaptive/MTF + кеши rules/labels

# 🔸 Базовые импорты
import asyncio
import logging
import math
from datetime import datetime
from decimal import Decimal
from typing import Any

# 🔸 Imports: packs_config (cache manager)
from packs_config.cache_manager import (
    POSTPROC_GROUP,
    POSTPROC_STREAM_KEY,
    caches_ready,
    adaptive_quantiles_cache,
    pack_registry,
    reloading_pairs_bins,
    reloading_pairs_labels,
    reloading_pairs_quantiles,
    ensure_stream_group,
    get_adaptive_rules,
    init_pack_runtime,
    labels_has_bin,
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
    IND_TS_PREFIX,
    MTF_RETRY_STEP_SEC,
    clip_0_100,
    get_mtf_value_decimal,
    get_ts_decimal_with_retry,
    safe_decimal,
    ts_get,
    ts_get_value_at,
)

# 🔸 Imports: packs_config (publish helpers)
from packs_config.publish import publish_pair, publish_static

# 🔸 Imports: packs_config (value builders)
from packs_config.value_builders import (
    build_atr_pct_value,
    build_bb_band_value,
    build_dmigap_value,
    build_lr_band_value,
)

# 🔸 Константы Redis (индикаторы)
INDICATOR_STREAM = "indicator_stream"          # входной стрим готовности индикаторов
IND_PACK_GROUP = "ind_pack_group_v4"           # consumer-group для indicator_stream
IND_PACK_CONSUMER = "ind_pack_consumer_1"      # consumer name

# 🔸 Константы Redis TS (feed_bb + indicators_v4)
BB_TS_PREFIX = "bb:ts"                         # bb:ts:{symbol}:{tf}:{field}

# 🔸 Константы БД
BB_TICKERS_TABLE = "tickers_bb"

# 🔸 Параметры чтения и параллельной обработки stream
STREAM_READ_COUNT = 500          # сколько сообщений читать за раз
STREAM_BLOCK_MS = 2000           # блокировка XREADGROUP (мс)
MAX_PARALLEL_MESSAGES = 200      # сколько сообщений обрабатывать параллельно

# 🔸 Параметры холодного старта (bootstrap)
BOOTSTRAP_MAX_PARALLEL = 300     # сколько тикеров/паков обрабатывать параллельно при старте

# 🔸 Ограничения диагностики (не заливаем Redis)
MAX_CANDIDATES_IN_DETAILS = 5

# 🔸 Handle MTF runtime: always publish for all pairs + dirs
async def handle_mtf_pack_publish_all(redis, rt: PackRuntime, symbol: str, trigger: dict[str, Any], open_ts_ms: int | None) -> tuple[int, int]:
    log = logging.getLogger("PACK_MTF")

    # условия достаточности runtime
    if not rt.mtf_pairs or not rt.mtf_component_tfs or not rt.mtf_component_params or not rt.mtf_bins_static:
        return 0, 0

    published_ok = 0
    published_fail = 0

    # invalid_trigger_event (open_time unparsable) — можем публиковать, т.к. key однозначен
    if open_ts_ms is None:
        for (scenario_id, signal_id) in rt.mtf_pairs:
            for direction in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=None,
                )
                details["missing_fields"] = ["open_time"]
                details["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                payload = pack_fail("invalid_trigger_event", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
        return published_ok, published_fail

    # rules_not_loaded_yet при старте/перезагрузке кешей
    if not caches_ready.get("labels", False) or (rt.mtf_quantiles_key and not caches_ready.get("quantiles", False)):
        need = []
        if not caches_ready.get("labels", False):
            need.append("labels")
        if rt.mtf_quantiles_key and not caches_ready.get("quantiles", False):
            need.append("quantiles")
        for (scenario_id, signal_id) in rt.mtf_pairs:
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
                details["need"] = need
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                payload = pack_fail("rules_not_loaded_yet", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
        return published_ok, published_fail

    # собираем значения по TF
    tasks = []
    meta: list[tuple[str, str | None, str]] = []  # (tf, sub_name|None, param_name)

    for tf in rt.mtf_component_tfs:
        spec = rt.mtf_component_params.get(tf)

        # один параметр на TF
        if isinstance(spec, str):
            pname = str(spec or "").strip()
            tasks.append(asyncio.create_task(get_mtf_value_decimal(redis, str(symbol), int(open_ts_ms), str(tf), pname) if pname else asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))))
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

        for (scenario_id, signal_id) in rt.mtf_pairs:
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

                if invalid_items:
                    details["kind"] = "mtf_component_value"
                    details["input"] = invalid_items
                else:
                    details["missing"] = missing_items

                details["styk"] = {"m15": bool(styk_m15), "h1": bool(styk_h1), "waited_sec": int(waited_total)}
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}

                payload = pack_fail(reason, details)
                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    payload,
                    int(rt.ttl_sec),
                )
                published_fail += 1

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

    # price for lr_mtf
    if rt.mtf_needs_price:
        price_key = f"{BB_TS_PREFIX}:{symbol}:{rt.mtf_price_tf}:{rt.mtf_price_field}"
        raw_price = await ts_get_value_at(redis, price_key, int(open_ts_ms))
        price_d = safe_decimal(raw_price)
        if price_d is None:
            for (scenario_id, signal_id) in rt.mtf_pairs:
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
                    if raw_price is None:
                        details["missing"] = [{"tf": str(rt.mtf_price_tf), "field": str(rt.mtf_price_field), "source": "bb:ts", "open_ts_ms": int(open_ts_ms)}]
                        details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                        payload = pack_fail("missing_inputs", details)
                    else:
                        details["kind"] = "price_value"
                        details["input"] = {"tf": str(rt.mtf_price_tf), "param": f"bb:{rt.mtf_price_field}", "raw": str(raw_price)}
                        details["retry"] = {"recommended": False, "after_sec": int(MTF_RETRY_STEP_SEC)}
                        payload = pack_fail("invalid_input_value", details)

                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
            return published_ok, published_fail

        values_by_tf["price"] = price_d

    # for each pair and direction compute and publish
    for (scenario_id, signal_id) in rt.mtf_pairs:
        pair_key = (int(scenario_id), int(signal_id))

        # during reload — rules_not_loaded_yet (priority)
        pair_reloading = (pair_key in reloading_pairs_labels) or (pair_key in reloading_pairs_quantiles)
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
                details["need"] = ["labels", "quantiles"] if rt.mtf_quantiles_key else ["labels"]
                details["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
                payload = pack_fail("rules_not_loaded_yet", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
            continue

        for direction in ("long", "short"):
            if direction not in ("long", "short"):
                details = build_fail_details_base(
                    analysis_id=int(rt.analysis_id),
                    symbol=str(symbol),
                    direction=str(direction),
                    timeframe="mtf",
                    pair={"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger=trigger,
                    open_ts_ms=int(open_ts_ms),
                )
                payload = pack_fail("invalid_direction", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            rules_by_tf: dict[str, list[Any]] = {}

            # static bins rules
            if str(rt.mtf_bins_tf) == "mtf":
                rules_by_tf["mtf"] = (rt.mtf_bins_static.get("mtf", {}) or {}).get(direction, []) or []
                if not rules_by_tf["mtf"]:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "bins", "tf": "mtf", "source": "static"}
                    payload = pack_fail("no_rules_static", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
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
                    payload = pack_fail("no_rules_static", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                    published_fail += 1
                    continue

            # quantiles rules
            if rt.mtf_quantiles_key:
                q_rules = adaptive_quantiles_cache.get((int(rt.analysis_id), int(scenario_id), int(signal_id), "mtf", str(direction)), [])
                if not q_rules:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                    details["expected"] = {"bin_type": "quantiles", "tf": "mtf", "source": "adaptive"}
                    payload = pack_fail("no_quantiles_rules", details)
                    await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
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
                details["where"] = "handle_mtf_pack/bin_candidates"
                details["error"] = short_error_str(e)
                payload = pack_fail("internal_error", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            if not candidates:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                payload = pack_fail("no_candidates", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            # choose by labels cache
            chosen = None
            cand_list = [str(x) for x in list(candidates)[:MAX_CANDIDATES_IN_DETAILS]]
            for cand in cand_list:
                if labels_has_bin(int(scenario_id), int(signal_id), str(direction), int(rt.analysis_id), str(rt.source_param_name), "mtf", cand):
                    chosen = cand
                    break

            if not chosen:
                details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, int(open_ts_ms))
                details["candidates"] = cand_list
                details["labels_ctx"] = {
                    "analysis_id": int(rt.analysis_id),
                    "indicator_param": str(rt.source_param_name),
                    "timeframe": "mtf",
                    "scenario_id": int(scenario_id),
                    "signal_id": int(signal_id),
                }
                payload = pack_fail("no_labels_match", details)
                await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                published_fail += 1
                continue

            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", pack_ok(chosen), int(rt.ttl_sec))
            published_ok += 1

    if published_ok or published_fail:
        log.debug("PACK_MTF: done (symbol=%s, analysis_id=%s, open_ts_ms=%s, ok=%s, fail=%s)", symbol, rt.analysis_id, open_ts_ms, published_ok, published_fail)

    return published_ok, published_fail


# 🔸 Handle indicator_stream message: publish always for expected keys of matched runtimes
async def handle_indicator_event(redis, msg: dict[str, Any]) -> dict[str, int]:
    log = logging.getLogger("PACK_SET")

    symbol = msg.get("symbol")
    timeframe = msg.get("timeframe")
    indicator_key = msg.get("indicator")
    status = msg.get("status")
    open_time = msg.get("open_time")

    # invalid_trigger_event: если нельзя построить ключи (нет symbol/timeframe/indicator) — в Redis не пишем
    if not symbol or not timeframe or not indicator_key:
        log.debug("PACK_SET: invalid trigger (missing fields) %s", {k: msg.get(k) for k in ("symbol", "timeframe", "indicator", "status", "open_time")})
        return {"ok": 0, "fail": 0, "runtimes": 0}

    runtimes = pack_registry.get((str(timeframe), str(indicator_key)))
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

    # status != ready → not_ready_retrying для всех ожидаемых ключей
    if status != "ready":
        for rt in runtimes:
            if rt.is_mtf and rt.mtf_pairs:
                for (scenario_id, signal_id) in rt.mtf_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("not_ready_retrying", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                        published_fail += 1

            elif rt.bins_source == "adaptive" and rt.adaptive_pairs:
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("not_ready_retrying", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1

            else:
                for direction in ("long", "short"):
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    payload = pack_fail("not_ready_retrying", details)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                    published_fail += 1

        return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}

    # ready: process runtimes
    for rt in runtimes:
        try:
            # MTF
            if rt.is_mtf:
                ok_n, fail_n = await handle_mtf_pack_publish_all(redis, rt, str(symbol), trigger, open_ts_ms)
                published_ok += ok_n
                published_fail += fail_n
                continue

            # single TF: если open_time не парсится, то для TS-зависимых паков это invalid_trigger_event
            if open_ts_ms is None and rt.analysis_key in ("bb_band_bin", "lr_band_bin", "atr_bin"):
                if rt.bins_source == "adaptive" and rt.adaptive_pairs:
                    for (scenario_id, signal_id) in rt.adaptive_pairs:
                        for direction in ("long", "short"):
                            base = build_fail_details_base(
                                int(rt.analysis_id),
                                str(symbol),
                                str(direction),
                                str(rt.timeframe),
                                {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                                trigger,
                                None,
                            )
                            base["missing_fields"] = ["open_time"]
                            base["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                            base["retry"] = {"recommended": False, "after_sec": 0}
                            payload = pack_fail("invalid_trigger_event", base)

                            await publish_pair(
                                redis,
                                int(rt.analysis_id),
                                int(scenario_id),
                                int(signal_id),
                                str(direction),
                                str(symbol),
                                str(rt.timeframe),
                                payload,
                                int(rt.ttl_sec),
                            )
                            published_fail += 1
                else:
                    for direction in ("long", "short"):
                        base = build_fail_details_base(
                            int(rt.analysis_id),
                            str(symbol),
                            str(direction),
                            str(rt.timeframe),
                            None,
                            trigger,
                            None,
                        )
                        base["missing_fields"] = ["open_time"]
                        base["parse"] = {"open_time": trigger.get("open_time"), "open_ts_ms": None}
                        base["retry"] = {"recommended": False, "after_sec": 0}
                        payload = pack_fail("invalid_trigger_event", base)

                        await publish_static(
                            redis,
                            int(rt.analysis_id),
                            str(direction),
                            str(symbol),
                            str(rt.timeframe),
                            payload,
                            int(rt.ttl_sec),
                        )
                        published_fail += 1

                continue

            # single TF: read value
            value: Any = None
            missing: list[Any] = []
            invalid_info: dict[str, Any] | None = None

            if rt.analysis_key == "bb_band_bin":
                value, missing = await build_bb_band_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "lr_band_bin":
                value, missing = await build_lr_band_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "atr_bin":
                value, missing = await build_atr_pct_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                    open_ts_ms,
                )
            elif rt.analysis_key == "dmigap_bin":
                value, missing = await build_dmigap_value(
                    redis,
                    str(symbol),
                    str(rt.timeframe),
                    str(rt.source_param_name),
                )
            else:
                raw_key = f"ind:{symbol}:{rt.timeframe}:{rt.source_param_name}"
                raw_value = await redis.get(raw_key)
                if raw_value is None:
                    missing = [str(rt.source_param_name)]
                else:
                    try:
                        f = float(raw_value)
                        if not math.isfinite(f):
                            raise ValueError("NaN/inf")
                        value = f
                    except Exception:
                        invalid_info = {
                            "tf": str(rt.timeframe),
                            "param": str(rt.source_param_name),
                            "raw": str(raw_value),
                        }

            # publish for adaptive / static
            if rt.bins_source == "adaptive":
                # pairs guaranteed by registry (otherwise runtime skipped)
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    pair_key = (int(scenario_id), int(signal_id))
                    pair_reloading = pair_key in reloading_pairs_bins

                    for direction in ("long", "short"):
                        base = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)

                        # invalid input
                        if invalid_info is not None:
                            base["kind"] = "single_value"
                            base["input"] = invalid_info
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("invalid_input_value", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        # missing inputs
                        if value is None:
                            base["missing"] = missing
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("missing_inputs", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        # rules not loaded yet / reload
                        if not caches_ready.get("adaptive_bins", False) or pair_reloading:
                            base["need"] = ["adaptive_bins"]
                            base["retry"] = {"recommended": True, "after_sec": 5}
                            payload = pack_fail("rules_not_loaded_yet", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        rules = get_adaptive_rules(int(rt.analysis_id), int(scenario_id), int(signal_id), str(rt.timeframe), str(direction))
                        if not rules:
                            base["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "adaptive", "direction": str(direction)}
                            payload = pack_fail("no_rules_adaptive", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        try:
                            bin_name = rt.worker.bin_value(value=value, rules=rules)
                        except Exception as e:
                            base["where"] = "handle_indicator_event/adaptive/bin_value"
                            base["error"] = short_error_str(e)
                            payload = pack_fail("internal_error", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        if not bin_name:
                            payload = pack_fail("no_candidates", base)
                            await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                            published_fail += 1
                            continue

                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                        published_ok += 1

            else:
                for direction in ("long", "short"):
                    base = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)

                    if invalid_info is not None:
                        base["kind"] = "single_value"
                        base["input"] = invalid_info
                        base["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("invalid_input_value", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    if value is None:
                        base["missing"] = missing
                        base["retry"] = {"recommended": True, "after_sec": 5}
                        payload = pack_fail("missing_inputs", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    rules = rt.bins_by_direction.get(str(direction)) or []
                    if not rules:
                        base["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "static", "direction": str(direction)}
                        payload = pack_fail("no_rules_static", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    try:
                        bin_name = rt.worker.bin_value(value=value, rules=rules)
                    except Exception as e:
                        base["where"] = "handle_indicator_event/static/bin_value"
                        base["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    if not bin_name:
                        payload = pack_fail("no_candidates", base)
                        await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
                        continue

                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                    published_ok += 1

        except Exception as e:
            # internal_error: если runtime сматчился — publish fail на ожидаемые ключи runtime
            if rt.is_mtf and rt.mtf_pairs:
                for (scenario_id, signal_id) in rt.mtf_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), "mtf", {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["where"] = "handle_indicator_event/mtf/runtime"
                        details["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), "mtf", payload, int(rt.ttl_sec))
                        published_fail += 1
            elif rt.bins_source == "adaptive" and rt.adaptive_pairs:
                for (scenario_id, signal_id) in rt.adaptive_pairs:
                    for direction in ("long", "short"):
                        details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}, trigger, open_ts_ms)
                        details["where"] = "handle_indicator_event/adaptive/runtime"
                        details["error"] = short_error_str(e)
                        payload = pack_fail("internal_error", details)
                        await publish_pair(redis, int(rt.analysis_id), int(scenario_id), int(signal_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                        published_fail += 1
            else:
                for direction in ("long", "short"):
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["where"] = "handle_indicator_event/static/runtime"
                    details["error"] = short_error_str(e)
                    payload = pack_fail("internal_error", details)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), payload, int(rt.ttl_sec))
                    published_fail += 1

            log.warning("PACK_SET: runtime error (analysis_id=%s): %s", rt.analysis_id, e, exc_info=True)

    return {"ok": published_ok, "fail": published_fail, "runtimes": len(runtimes)}


# 🔸 Watch indicator_stream (parallel)
async def watch_indicator_stream(redis):
    log = logging.getLogger("PACK_STREAM")
    sem = asyncio.Semaphore(MAX_PARALLEL_MESSAGES)

    async def _process_one(data: dict) -> dict[str, int]:
        async with sem:
            msg = {
                "symbol": data.get("symbol"),
                "timeframe": data.get("timeframe"),
                "indicator": data.get("indicator"),
                "open_time": data.get("open_time"),
                "status": data.get("status"),
            }
            return await handle_indicator_event(redis, msg)

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

            flat: list[tuple[str, dict]] = []
            for _, messages in resp:
                for msg_id, data in messages:
                    flat.append((msg_id, data))
            if not flat:
                continue

            to_ack = [msg_id for msg_id, _ in flat]
            tasks = [asyncio.create_task(_process_one(data)) for _, data in flat]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_ok = 0
            batch_fail = 0
            batch_runtimes = 0
            errors = 0

            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    continue
                batch_ok += int(r.get("ok", 0))
                batch_fail += int(r.get("fail", 0))
                batch_runtimes += int(r.get("runtimes", 0))

            await redis.xack(INDICATOR_STREAM, IND_PACK_GROUP, *to_ack)

            # суммирующий лог по батчу
            log.info(
                "PACK_STREAM: batch done (msgs=%s, runtimes_total=%s, ok=%s, fail=%s, errors=%s)",
                len(flat),
                batch_runtimes,
                batch_ok,
                batch_fail,
                errors,
            )

        except Exception as e:
            log.error("PACK_STREAM loop error: %s", e, exc_info=True)
            await asyncio.sleep(2)


# 🔸 Bootstrap helpers
async def load_active_symbols(pg) -> list[str]:
    log = logging.getLogger("PACK_BOOT")
    async with pg.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT symbol
            FROM {BB_TICKERS_TABLE}
            WHERE status = 'enabled' AND tradepermission = 'enabled'
        """)
    symbols = [str(r["symbol"]) for r in rows if r.get("symbol")]
    log.info("PACK_BOOT: активных тикеров загружено: %s", len(symbols))
    return symbols


async def bootstrap_current_state(pg, redis):
    log = logging.getLogger("PACK_BOOT")

    runtimes: list[PackRuntime] = []
    for lst in pack_registry.values():
        runtimes.extend(lst)

    if not runtimes:
        log.info("PACK_BOOT: нет активных pack-инстансов, bootstrap пропущен")
        return

    symbols = await load_active_symbols(pg)
    if not symbols:
        log.info("PACK_BOOT: нет активных тикеров, bootstrap пропущен")
        return

    sem = asyncio.Semaphore(BOOTSTRAP_MAX_PARALLEL)
    ok_sum = 0
    fail_sum = 0

    async def _process_one(symbol: str, rt: PackRuntime):
        nonlocal ok_sum, fail_sum
        async with sem:
            # MTF bootstrap осознанно пропускаем
            if rt.is_mtf:
                return

            trigger = {"indicator": "bootstrap", "timeframe": str(rt.timeframe), "open_time": None, "status": "bootstrap"}
            open_ts_ms = None

            # извлекаем value по логике исходного bootstrap (упрощённо)
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
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
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
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
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
                        close_val = await ts_get_value_at(redis, f"{BB_TS_PREFIX}:{symbol}:{rt.timeframe}:c", open_ts_ms)
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

            # публикуем только static в bootstrap (как было)
            if rt.bins_source != "static":
                return

            for direction in ("long", "short"):
                if invalid_info is not None:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["kind"] = "single_value"
                    details["input"] = invalid_info
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("invalid_input_value", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                if value is None:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["missing"] = missing
                    details["retry"] = {"recommended": True, "after_sec": 5}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("missing_inputs", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                rules = rt.bins_by_direction.get(str(direction)) or []
                if not rules:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["expected"] = {"bin_type": "bins", "tf": str(rt.timeframe), "source": "static", "direction": str(direction)}
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("no_rules_static", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                try:
                    bin_name = rt.worker.bin_value(value=value, rules=rules)
                except Exception as e:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    details["where"] = "bootstrap/bin_value"
                    details["error"] = short_error_str(e)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("internal_error", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                if not bin_name:
                    details = build_fail_details_base(int(rt.analysis_id), str(symbol), str(direction), str(rt.timeframe), None, trigger, open_ts_ms)
                    await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_fail("no_candidates", details), int(rt.ttl_sec))
                    fail_sum += 1
                    continue

                await publish_static(redis, int(rt.analysis_id), str(direction), str(symbol), str(rt.timeframe), pack_ok(str(bin_name)), int(rt.ttl_sec))
                ok_sum += 1

    tasks = [asyncio.create_task(_process_one(sym, rt)) for sym in symbols for rt in runtimes]
    await asyncio.gather(*tasks, return_exceptions=True)

    log.info("PACK_BOOT: bootstrap done — packs=%s, symbols=%s, ok=%s, fail=%s", len(runtimes), len(symbols), ok_sum, fail_sum)


# 🔸 run worker
async def run_indicator_pack(pg, redis):
    # первичная загрузка
    await init_pack_runtime(pg)

    # создать группы заранее
    await ensure_stream_group(redis, INDICATOR_STREAM, IND_PACK_GROUP)
    await ensure_stream_group(redis, POSTPROC_STREAM_KEY, POSTPROC_GROUP)

    # холодный старт
    await bootstrap_current_state(pg, redis)

    # основная работа
    await asyncio.gather(
        watch_indicator_stream(redis),
        watch_postproc_ready(pg, redis),
    )
