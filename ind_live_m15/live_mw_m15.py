# live_mw_m15.py — live-расчёт MW m15 (trend/volatility/momentum/extremes) с использованием L1-кэша; публикация в ind_mw_live:*

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Set

from indicators.compute_and_store import compute_snapshot_values_async, get_expected_param_names
from packs.trend_pack import build_trend_pack
from packs.volatility_pack import build_volatility_pack
from packs.momentum_pack import build_momentum_pack
from packs.extremes_pack import build_extremes_pack
from packs.pack_utils import floor_to_bar


# 🔸 Логгер
log = logging.getLogger("MW_M15")

# 🔸 Константы
TF = "m15"
TTL_SEC = 90
PAUSE_SEC = 3  # рекомендуется такая же пауза между LIVE и MW
MW_KINDS = ("trend", "volatility", "momentum", "extremes")


# 🔸 Обёртка compute_fn: сначала L1, потом фолбэк к compute_snapshot_values_async
def make_compute_with_l1(live_cache, bar_open_ms: int):
    async def _compute(inst: Dict[str, Any], symbol: str, df, precision: int) -> Dict[str, str]:
        tf = inst.get("timeframe", TF)
        needed: Set[str] = set(get_expected_param_names(inst["indicator"], inst["params"]))
        # сначала пробуем L1
        if live_cache is not None:
            try:
                hit = await live_cache.get(symbol, tf, needed, expect_bar_open_ms=bar_open_ms)
                if hit:
                    return hit
            except Exception:
                pass
        # фолбэк к расчёту
        return await compute_snapshot_values_async(inst, symbol, df, precision)
    return _compute


# 🔸 Публикация состояния MW в ind_mw_live:{symbol}:{tf}:{kind}
async def _publish_mw(redis, symbol: str, tf: str, kind: str, pack_obj: Dict[str, Any]) -> bool:
    key = f"ind_mw_live:{symbol}:{tf}:{kind}"
    try:
        # храним сам JSON пакета; потребителям удобно
        js = json.dumps(pack_obj, ensure_ascii=False)
        await redis.set(key, js, ex=TTL_SEC)
        return True
    except Exception as e:
        log.debug(f"MW_M15 publish error {symbol}/{tf} {kind}: {e}")
        return False


# 🔸 Один проход MW m15 (использовать после LIVE m15, чтобы L1 был тёплый)
async def mw_m15_pass(redis,
                     get_active_symbols,
                     get_precision,
                     live_cache) -> None:
    t0 = time.monotonic()

    symbols = list(get_active_symbols()) or []
    if not symbols:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        log.info(f"MW_M15 PASS done: symbols=0 written=0 errors=0 elapsed_ms={elapsed_ms}")
        return

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    bar_open_ms = floor_to_bar(now_ms, TF)
    compute_with_l1 = make_compute_with_l1(live_cache, bar_open_ms)

    written = 0
    errors = 0

    # ограничим одновременную работу
    sem = asyncio.Semaphore(20)

    async def _wrap(sym: str):
        nonlocal written, errors
        async with sem:
            precision = 8
            try:
                precision = int(get_precision(sym) or 8)
            except Exception:
                pass

            # по каждому виду MW строим pack (использует compute_with_l1)
            try:
                trend = await build_trend_pack(sym, TF, now_ms, precision, redis, compute_with_l1)
                if trend:
                    trend["pack"]["ref"] = "live"
                    if await _publish_mw(redis, sym, TF, "trend", trend):
                        written += 1
                vol = await build_volatility_pack(sym, TF, now_ms, precision, redis, compute_with_l1)
                if vol:
                    vol["pack"]["ref"] = "live"
                    if await _publish_mw(redis, sym, TF, "volatility", vol):
                        written += 1
                mom = await build_momentum_pack(sym, TF, now_ms, precision, redis, compute_with_l1)
                if mom:
                    mom["pack"]["ref"] = "live"
                    if await _publish_mw(redis, sym, TF, "momentum", mom):
                        written += 1
                ext = await build_extremes_pack(sym, TF, now_ms, precision, redis, compute_with_l1)
                if ext:
                    ext["pack"]["ref"] = "live"
                    if await _publish_mw(redis, sym, TF, "extremes", ext):
                        written += 1
            except Exception as e:
                errors += 1
                log.debug(f"MW_M15 compute error {sym}: {e}", exc_info=False)

    await asyncio.gather(*[asyncio.create_task(_wrap(s)) for s in symbols])

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    log.info(
        f"MW_M15 PASS done: symbols={len(symbols)} written={written} errors={errors} elapsed_ms={elapsed_ms}"
    )