# indicator_pack.py — оркестратор расчёта и публикации ind_pack (winner-driven): MTF only + кеш rules (adaptive) + static bins in runtime + PG meta + optional full trace

from __future__ import annotations

# 🔸 Базовые импорты
import asyncio
import json
import logging
import re
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

# 🔸 Константы / флаги трассировки
PACK_TRACE_ENABLED = True  # включить/выключить запись полного трейса в payload_json (ok/fail)

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


# 🔸 JSON helper (compact)
def _json_dumps(obj: dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


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


# 🔸 Candidate chooser (preserve pack priority; avoid lexicographic sort)
def choose_candidate(candidates: Any) -> str | None:
    """
    Pack workers возвращают кандидатов в порядке приоритета.
    Нельзя делать sort/min по строке: это ломает приоритеты (пример: 'A_0' < 'A_bin_0').
    """
    if not candidates:
        return None

    # нормализуем в список строк
    items: list[str] = []
    try:
        for x in candidates:
            s = str(x).strip()
            if s:
                items.append(s)
    except Exception:
        return None

    if not items:
        return None

    # dedupe, сохраняя порядок
    seen: set[str] = set()
    uniq: list[str] = []
    for s in items:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)

    return uniq[0] if uniq else None


# 🔸 Trace helpers
def _to_trace_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, dict):
        out: dict[str, Any] = {}
        for k, vv in v.items():
            out[str(k)] = _to_trace_value(vv)
        return out
    if isinstance(v, (list, tuple)):
        return [_to_trace_value(x) for x in v]
    return v


def _parse_bin_idx(token: str, prefix: str) -> int | None:
    try:
        m = re.match(rf"^{re.escape(prefix)}(\d+)$", str(token).strip())
        if not m:
            return None
        return int(m.group(1))
    except Exception:
        return None


def _extract_bin_parts(bin_name: str) -> dict[str, Any]:
    parts = [p.strip() for p in str(bin_name or "").split("|")]
    out: dict[str, Any] = {
        "bin_name": str(bin_name),
        "h1": parts[0] if len(parts) > 0 else None,
        "m15": parts[1] if len(parts) > 1 else None,
        "m5": parts[2] if len(parts) > 2 else None,
    }
    out["h1_idx"] = _parse_bin_idx(out["h1"] or "", "H1_bin_")
    out["m15_idx"] = _parse_bin_idx(out["m15"] or "", "M15_bin_")
    # M5_Qk или M5_bin_k
    if isinstance(out["m5"], str) and out["m5"].startswith("M5_Q"):
        out["m5_q_idx"] = _parse_bin_idx(out["m5"].replace("M5_Q", "M5_bin_"), "M5_bin_")
    else:
        out["m5_q_idx"] = None
    return out


def _build_quantiles_group_debug(q_rules: list[Any], h1_idx: int | None, m15_idx: int | None) -> dict[str, Any] | None:
    # условия достаточности
    if not q_rules or h1_idx is None or m15_idx is None:
        return None

    group: list[dict[str, Any]] = []
    for r in q_rules:
        try:
            bo = int(getattr(r, "bin_order"))
        except Exception:
            continue

        if (bo // 100) != int(h1_idx):
            continue
        if ((bo % 100) // 10) != int(m15_idx):
            continue

        q_idx = int(bo % 10)
        group.append(
            {
                "bin_order": bo,
                "q_idx": q_idx,
                "bin_name": getattr(r, "bin_name", None),
                "val_from": str(getattr(r, "val_from", None)) if getattr(r, "val_from", None) is not None else None,
                "val_to": str(getattr(r, "val_to", None)) if getattr(r, "val_to", None) is not None else None,
                "to_inclusive": bool(getattr(r, "to_inclusive", False)),
            }
        )

    if not group:
        return {"h1_idx": int(h1_idx), "m15_idx": int(m15_idx), "rules": [], "rules_count": 0}

    group.sort(key=lambda x: int(x.get("bin_order") or 0))
    return {"h1_idx": int(h1_idx), "m15_idx": int(m15_idx), "rules": group, "rules_count": len(group)}


def _build_trace_base(
    rt: PackRuntime,
    symbol: str,
    trigger: dict[str, Any],
    open_ts_ms: int | None,
) -> dict[str, Any]:
    return {
        "analysis_id": int(rt.analysis_id) if getattr(rt, "analysis_id", None) is not None else None,
        "analysis_key": str(getattr(rt, "analysis_key", "")),
        "family_key": str(getattr(rt, "family_key", "")),
        "symbol": str(symbol),
        "timeframe": "mtf",
        "open_time": trigger.get("open_time"),
        "open_ts_ms": int(open_ts_ms) if open_ts_ms is not None else None,
        "trigger": trigger,
        "runtime": {
            "mtf_component_tfs": list(getattr(rt, "mtf_component_tfs", []) or []),
            "mtf_needs_price": bool(getattr(rt, "mtf_needs_price", False)),
            "mtf_price_tf": str(getattr(rt, "mtf_price_tf", "m5")),
            "mtf_price_field": str(getattr(rt, "mtf_price_field", "c")),
            "mtf_bins_tf": str(getattr(rt, "mtf_bins_tf", "")),
            "mtf_quantiles_key": str(getattr(rt, "mtf_quantiles_key", "")) if getattr(rt, "mtf_quantiles_key", None) else None,
            "ttl_sec": int(getattr(rt, "ttl_sec", 0)) if getattr(rt, "ttl_sec", None) is not None else None,
        },
    }


def _pack_ok_json(bin_name: str, trace: dict[str, Any] | None) -> str:
    if not PACK_TRACE_ENABLED:
        return pack_ok(bin_name)
    obj = {"ok": True, "bin_name": str(bin_name), "debug": (trace or {})}
    return _json_dumps(obj)


def _pack_fail_json(reason: str, details: dict[str, Any], trace: dict[str, Any] | None) -> str:
    if PACK_TRACE_ENABLED and isinstance(details, dict):
        details["debug"] = trace or {}
    return pack_fail(reason, details)


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

        # базовая трасса на fail (даже если чего-то не хватает)
        trace_base = _build_trace_base(rt, symbol, trigger, open_ts_ms)
        trace_base["phase"] = "fail"
        trace_base["reason"] = str(reason)

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

                trace = dict(trace_base)
                trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                trace["direction"] = str(direction)
                trace["run_id"] = int(run_id) if run_id is not None else None

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    _pack_fail_json(str(reason), details, trace),
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
    if (
        not caches_ready.get("winners", False)
        or not caches_ready.get("adaptive_bins", False)
        or (rt.mtf_quantiles_key and not caches_ready.get("quantiles", False))
    ):
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
                    if pname
                    else asyncio.sleep(0, result=(None, None, {"styk": False, "waited_sec": 0}))
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
    components_trace: list[dict[str, Any]] = []
    missing_items: list[Any] = []
    invalid_items: list[Any] = []
    styk_m15 = False
    styk_h1 = False
    waited_total = 0

    for (tf, name, param), r in zip(meta, results):
        if isinstance(r, Exception):
            missing_items.append({"tf": tf, "param": param or None, "error": short_error_str(r)})
            components_trace.append({"tf": tf, "name": name, "param": param or None, "error": short_error_str(r)})
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

        components_trace.append(
            {
                "tf": str(tf),
                "name": (str(name) if name is not None else None),
                "param": (str(param) if param else None),
                "raw": (str(raw_val) if raw_val is not None else None),
                "value": (str(d_val) if isinstance(d_val, Decimal) else None),
                "meta": meta_info,
            }
        )

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
                "trace_components": components_trace if PACK_TRACE_ENABLED else None,
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

    # price for *_mtf where needed (например lr_mtf / bb_mtf)
    price_raw: str | None = None
    if rt.mtf_needs_price:
        price_key = f"{BB_TS_PREFIX}:{symbol}:{rt.mtf_price_tf}:{rt.mtf_price_field}"
        price_raw = await ts_get_value_at(redis, price_key, int(open_ts_ms))
        price_d = safe_decimal(price_raw)

        if price_d is None:
            # missing/invalid price
            reason = "missing_inputs" if price_raw is None else "invalid_input_value"
            extra = {}
            if price_raw is None:
                extra["missing"] = [{"tf": str(rt.mtf_price_tf), "field": str(rt.mtf_price_field), "source": "bb:ts", "open_ts_ms": int(open_ts_ms)}]
                extra["retry"] = {"recommended": True, "after_sec": int(MTF_RETRY_STEP_SEC)}
            else:
                extra["kind"] = "price_value"
                extra["input"] = {"tf": str(rt.mtf_price_tf), "param": f"bb:{rt.mtf_price_field}", "raw": str(price_raw)}
                extra["retry"] = {"recommended": False, "after_sec": int(MTF_RETRY_STEP_SEC)}

            extra["trace_components"] = components_trace if PACK_TRACE_ENABLED else None
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
                details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                trace["phase"] = "fail"
                trace["reason"] = "rules_not_loaded_yet"
                trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                trace["direction"] = str(direction)
                trace["run_id"] = int(run_id) if run_id is not None else None
                trace["styk"] = {"m15": bool(styk_m15), "h1": bool(styk_h1), "waited_sec": int(waited_total)}
                trace["values_by_tf"] = _to_trace_value(values_by_tf)
                trace["components"] = components_trace
                trace["price_raw"] = str(price_raw) if price_raw is not None else None

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    _pack_fail_json("rules_not_loaded_yet", details, trace),
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
                    details = build_fail_details_base(
                        int(rt.analysis_id),
                        str(symbol),
                        str(direction),
                        "mtf",
                        {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                        trigger,
                        int(open_ts_ms),
                    )
                    details["expected"] = {"bin_type": "bins", "tf": "mtf", "source": "static"}
                    details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                    trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                    trace["phase"] = "fail"
                    trace["reason"] = "no_rules_static"
                    trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                    trace["direction"] = str(direction)
                    trace["run_id"] = int(run_id) if run_id is not None else None
                    trace["values_by_tf"] = _to_trace_value(values_by_tf)
                    trace["components"] = components_trace
                    trace["rules_counts"] = {"mtf": len(rules_by_tf["mtf"])}

                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        _pack_fail_json("no_rules_static", details, trace),
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
                    details = build_fail_details_base(
                        int(rt.analysis_id),
                        str(symbol),
                        str(direction),
                        "mtf",
                        {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                        trigger,
                        int(open_ts_ms),
                    )
                    details["expected"] = {"bin_type": "bins", "tf": missing_rules_tfs, "source": "static"}
                    details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                    trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                    trace["phase"] = "fail"
                    trace["reason"] = "no_rules_static"
                    trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                    trace["direction"] = str(direction)
                    trace["run_id"] = int(run_id) if run_id is not None else None
                    trace["values_by_tf"] = _to_trace_value(values_by_tf)
                    trace["components"] = components_trace
                    trace["rules_counts"] = {k: len(v or []) for k, v in rules_by_tf.items()}

                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        _pack_fail_json("no_rules_static", details, trace),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                    )
                    published_fail += 1
                    continue

            # quantiles rules (adaptive; winner-driven cache)
            q_rules: list[Any] = []
            if rt.mtf_quantiles_key:
                q_rules = get_adaptive_quantiles(int(rt.analysis_id), int(scenario_id), int(signal_id), "mtf", str(direction))
                if not q_rules:
                    details = build_fail_details_base(
                        int(rt.analysis_id),
                        str(symbol),
                        str(direction),
                        "mtf",
                        {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                        trigger,
                        int(open_ts_ms),
                    )
                    details["expected"] = {"bin_type": "quantiles", "tf": "mtf", "source": "adaptive"}
                    details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                    trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                    trace["phase"] = "fail"
                    trace["reason"] = "no_quantiles_rules"
                    trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                    trace["direction"] = str(direction)
                    trace["run_id"] = int(run_id) if run_id is not None else None
                    trace["values_by_tf"] = _to_trace_value(values_by_tf)
                    trace["components"] = components_trace
                    trace["rules_counts"] = {k: len(v or []) for k, v in rules_by_tf.items()}
                    trace["quantiles_rules_count"] = 0

                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        _pack_fail_json("no_quantiles_rules", details, trace),
                        int(rt.ttl_sec),
                        meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                    )
                    published_fail += 1
                    continue
                rules_by_tf[str(rt.mtf_quantiles_key)] = q_rules

            # candidates
            candidates: Any = None
            try:
                try:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf, direction=str(direction))
                except TypeError:
                    candidates = rt.worker.bin_candidates(values_by_tf=values_by_tf, rules_by_tf=rules_by_tf)
            except Exception as e:
                details = build_fail_details_base(
                    int(rt.analysis_id),
                    str(symbol),
                    str(direction),
                    "mtf",
                    {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger,
                    int(open_ts_ms),
                )
                details["where"] = "handle_mtf_runtime/bin_candidates"
                details["error"] = short_error_str(e)
                details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                trace["phase"] = "fail"
                trace["reason"] = "internal_error"
                trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                trace["direction"] = str(direction)
                trace["run_id"] = int(run_id) if run_id is not None else None
                trace["values_by_tf"] = _to_trace_value(values_by_tf)
                trace["components"] = components_trace
                trace["error"] = short_error_str(e)

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    _pack_fail_json("internal_error", details, trace),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                )
                published_fail += 1
                continue

            # choose candidate in pack-defined priority order (do not lexicographically sort)
            chosen = choose_candidate(candidates)

            if not chosen:
                details = build_fail_details_base(
                    int(rt.analysis_id),
                    str(symbol),
                    str(direction),
                    "mtf",
                    {"scenario_id": int(scenario_id), "signal_id": int(signal_id)},
                    trigger,
                    int(open_ts_ms),
                )
                details["candidates"] = [str(x) for x in list(candidates or [])[:MAX_CANDIDATES_IN_DETAILS]]
                details["trace_components"] = components_trace if PACK_TRACE_ENABLED else None

                trace = _build_trace_base(rt, symbol, trigger, open_ts_ms)
                trace["phase"] = "fail"
                trace["reason"] = "no_candidates"
                trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                trace["direction"] = str(direction)
                trace["run_id"] = int(run_id) if run_id is not None else None
                trace["values_by_tf"] = _to_trace_value(values_by_tf)
                trace["components"] = components_trace
                trace["candidates"] = [str(x) for x in list(candidates or [])]

                await publish_pair(
                    redis,
                    int(rt.analysis_id),
                    int(scenario_id),
                    int(signal_id),
                    str(direction),
                    str(symbol),
                    "mtf",
                    _pack_fail_json("no_candidates", details, trace),
                    int(rt.ttl_sec),
                    meta=build_publish_meta(int(open_ts_ms), trigger.get("open_time"), run_id),
                )
                published_fail += 1
                continue

            # trace ok
            trace_ok = _build_trace_base(rt, symbol, trigger, open_ts_ms)
            trace_ok["phase"] = "ok"
            trace_ok["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
            trace_ok["direction"] = str(direction)
            trace_ok["run_id"] = int(run_id) if run_id is not None else None
            trace_ok["values_by_tf"] = _to_trace_value(values_by_tf)
            trace_ok["components"] = components_trace
            trace_ok["styk"] = {"m15": bool(styk_m15), "h1": bool(styk_h1), "waited_sec": int(waited_total)}
            trace_ok["price_raw"] = str(price_raw) if price_raw is not None else None
            trace_ok["rules_counts"] = {k: len(v or []) for k, v in rules_by_tf.items()}
            trace_ok["quantiles_rules_count"] = len(q_rules or [])
            trace_ok["candidates"] = [str(x) for x in list(candidates or [])]
            trace_ok["chosen"] = str(chosen)

            parts = _extract_bin_parts(str(chosen))
            trace_ok["chosen_parts"] = parts
            if q_rules and parts.get("h1_idx") is not None and parts.get("m15_idx") is not None:
                trace_ok["quantiles_group"] = _build_quantiles_group_debug(q_rules, int(parts["h1_idx"]), int(parts["m15_idx"]))

            await publish_pair(
                redis,
                int(rt.analysis_id),
                int(scenario_id),
                int(signal_id),
                str(direction),
                str(symbol),
                "mtf",
                _pack_ok_json(chosen, trace_ok),
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

                    trace = _build_trace_base(rt, str(symbol), trigger, open_ts_ms)
                    trace["phase"] = "fail"
                    trace["reason"] = "not_ready_retrying"
                    trace["pair"] = {"scenario_id": int(scenario_id), "signal_id": int(signal_id)}
                    trace["direction"] = str(direction)
                    trace["run_id"] = int(run_id) if run_id is not None else None

                    await publish_pair(
                        redis,
                        int(rt.analysis_id),
                        int(scenario_id),
                        int(signal_id),
                        str(direction),
                        str(symbol),
                        "mtf",
                        _pack_fail_json("not_ready_retrying", details, trace),
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
            log.debug(
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