# packs_config/bootstrap.py — bootstrap (холодный старт) ind_pack: публикация текущего static-состояния по всем тикерам

from __future__ import annotations

# 🔸 Imports
import asyncio
import logging
import math
from typing import Any

from packs_config.cache_manager import pack_registry
from packs_config.contract import (
    build_fail_details_base,
    pack_fail,
    pack_ok,
    short_error_str,
)
from packs_config.models import PackRuntime
from packs_config.publish import publish_static
from packs_config.redis_ts import IND_TS_PREFIX, ts_get, ts_get_value_at


# 🔸 Константы БД
BB_TICKERS_TABLE = "tickers_bb"

# 🔸 Константы Redis TS (feed_bb)
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}

# 🔸 Параметры холодного старта (bootstrap)
BOOTSTRAP_MAX_PARALLEL = 300  # сколько тикеров/паков обрабатывать параллельно при старте


# 🔸 Bootstrap helpers
async def load_active_symbols(pg: Any) -> list[str]:
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


async def bootstrap_current_state(pg: Any, redis: Any):
    log = logging.getLogger("PACK_BOOT")

    # bootstrap отключён: по новой логике публикуем только synced (labels_v2) pair-ключи
    log.info("PACK_BOOT: bootstrap disabled (static publish skipped)")
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