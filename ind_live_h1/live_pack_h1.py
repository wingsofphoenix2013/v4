# live_pack_h1.py — live-PACK h1 (rsi/mfi/ema/atr/lr/adx_dmi/macd/bb) с использованием L1; публикация «минимального» JSON в pack_live:{indicator}:{symbol}:{tf}:{base}

# 🔸 Импорты
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Set, List, Tuple

from indicators.compute_and_store import compute_snapshot_values_async, get_expected_param_names
from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.ema_pack import build_ema_pack
from packs.atr_pack import build_atr_pack
from packs.lr_pack import build_lr_pack
from packs.adx_dmi_pack import build_adx_dmi_pack
from packs.macd_pack import build_macd_pack
from packs.bb_pack import build_bb_pack
from packs.pack_utils import floor_to_bar


# 🔸 Логгер
log = logging.getLogger("PACK_H1")


# 🔸 Константы
TF = "h1"
TTL_SEC = 90


# 🔸 Обёртка compute_fn: сначала L1, потом фолбэк к compute_snapshot_values_async
def make_compute_with_l1(live_cache, bar_open_ms: int, hits_misses: Dict[str, int]):
    async def _compute(inst: Dict[str, Any], symbol: str, df, precision: int) -> Dict[str, str]:
        tf = inst.get("timeframe", TF)
        needed: Set[str] = set(get_expected_param_names(inst["indicator"], inst["params"]))
        # сначала пробуем L1
        if live_cache is not None:
            try:
                hit = await live_cache.get(symbol, tf, needed, expect_bar_open_ms=bar_open_ms)
                if hit:
                    hits_misses["hits"] += 1
                    return hit
            except Exception:
                pass
        # фолбэк к расчёту
        hits_misses["misses"] += 1
        return await compute_snapshot_values_async(inst, symbol, df, precision)
    return _compute


# 🔸 Минимальный набор полей для публикации (совместим с consumers)
PACK_WHITELIST: Dict[str, List[str]] = {
    "rsi":     ["bucket_low", "trend"],
    "mfi":     ["bucket_low", "trend"],
    "ema":     ["side", "dynamic", "dynamic_strict", "dynamic_smooth"],
    "atr":     ["bucket", "bucket_delta"],
    "lr":      ["bucket", "bucket_delta", "angle_trend"],
    "adx_dmi": ["adx_bucket_low", "adx_dynamic_strict", "adx_dynamic_smooth",
                "gap_bucket_low", "gap_dynamic_strict", "gap_dynamic_smooth"],
    "macd":    ["mode", "cross", "zero_side", "hist_bucket_low_pct",
                "hist_trend_strict", "hist_trend_smooth"],
    "bb":      ["bucket", "bucket_delta", "bw_trend_strict", "bw_trend_smooth"],
}


# 🔸 Сбор активных баз по инстансам TF=h1
def _collect_active_bases(instances_h1: List[Dict[str, Any]]) -> Dict[str, Any]:
    r: Dict[str, Any] = {
        "rsi": set(), "mfi": set(), "ema": set(), "atr": set(), "kama": set(),
        "lr": set(), "adx_dmi": set(), "macd": set(), "bb": set(),
    }
    for inst in instances_h1:
        ind = inst.get("indicator")
        params = inst.get("params") or {}
        try:
            if ind in ("rsi", "mfi", "ema", "atr", "kama", "lr", "adx_dmi"):
                L = int(params.get("length"))
                r[ind].add(L)
            elif ind == "macd":
                F = int(params.get("fast"))
                r["macd"].add(F)
            elif ind == "bb":
                L = int(params.get("length"))
                S = round(float(params.get("std")), 2)
                r["bb"].add((L, S))
        except Exception:
            continue
    return r


# 🔸 Сделать «минимальный» pack (с whitelisted полями + open_time)
def _slim_pack(indicator: str, full_pack: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(full_pack, dict):
        return None
    base = full_pack.get("base")
    p = full_pack.get("pack") or {}
    if not isinstance(p, dict):
        return None

    fields = PACK_WHITELIST.get(indicator, [])
    out_pack: Dict[str, Any] = {"open_time": p.get("open_time")}
    for f in fields:
        if f in p:
            out_pack[f] = p[f]

    # если в whitelisted поле ничего не попало — публикация не нужна
    if len(out_pack) <= 1:  # только open_time
        return None

    return {"base": base, "pack": out_pack}


# 🔸 Публикация «минимального» PACK JSON в Redis
async def _publish_pack_min(redis, indicator: str, symbol: str, tf: str, base: str, slim: Dict[str, Any]) -> bool:
    key = f"pack_live:{indicator}:{symbol}:{tf}:{base}"
    try:
        js = json.dumps(slim, ensure_ascii=False, separators=(",", ":"))
        await redis.set(key, js, ex=TTL_SEC)
        return True
    except Exception as e:
        log.debug(f"PACK_H1 publish error {key}: {e}")
        return False


# 🔸 Один проход PACK h1 (использовать после MW h1, чтобы L1 был тёплый)
async def pack_h1_pass(redis,
                       get_instances_by_tf,
                       get_active_symbols,
                       get_precision,
                       live_cache) -> None:
    t0 = time.monotonic()

    symbols = list(get_active_symbols()) or []
    instances_h1 = [i for i in get_instances_by_tf(TF)]

    if not symbols or not instances_h1:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        log.info(f"PACK_H1 PASS done: symbols={len(symbols)} bases=0 written=0 errors=0 hits=0 misses=0 elapsed_ms={elapsed_ms}")
        return

    # текущий бар и счётчики hit/miss L1
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    bar_open_ms = floor_to_bar(now_ms, TF)
    hits_misses = {"hits": 0, "misses": 0}
    compute_with_l1 = make_compute_with_l1(live_cache, bar_open_ms, hits_misses)

    # активные базы по типам
    bases = _collect_active_bases(instances_h1)

    written = 0
    errors = 0

    # ограничение параллелизма по символам
    sem = asyncio.Semaphore(20)

    async def _wrap(sym: str):
        nonlocal written, errors
        async with sem:
            try:
                precision = int(get_precision(sym) or 8)
            except Exception:
                precision = 8

            # RSI
            for L in sorted(bases["rsi"]):
                try:
                    full = await build_rsi_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("rsi", full) if full else None
                    if slim and await _publish_pack_min(redis, "rsi", sym, TF, f"rsi{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # MFI
            for L in sorted(bases["mfi"]):
                try:
                    full = await build_mfi_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("mfi", full) if full else None
                    if slim and await _publish_pack_min(redis, "mfi", sym, TF, f"mfi{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # EMA
            for L in sorted(bases["ema"]):
                try:
                    full = await build_ema_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("ema", full) if full else None
                    if slim and await _publish_pack_min(redis, "ema", sym, TF, f"ema{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # ATR
            for L in sorted(bases["atr"]):
                try:
                    full = await build_atr_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("atr", full) if full else None
                    if slim and await _publish_pack_min(redis, "atr", sym, TF, f"atr{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # LR
            for L in sorted(bases["lr"]):
                try:
                    full = await build_lr_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("lr", full) if full else None
                    if slim and await _publish_pack_min(redis, "lr", sym, TF, f"lr{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # ADX_DMI
            for L in sorted(bases["adx_dmi"]):
                try:
                    full = await build_adx_dmi_pack(sym, TF, L, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("adx_dmi", full) if full else None
                    if slim and await _publish_pack_min(redis, "adx_dmi", sym, TF, f"adx_dmi{L}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # MACD
            for F in sorted(bases["macd"]):
                try:
                    full = await build_macd_pack(sym, TF, F, now_ms, precision, redis, compute_with_l1)
                    slim = _slim_pack("macd", full) if full else None
                    if slim and await _publish_pack_min(redis, "macd", sym, TF, f"macd{F}", slim):
                        written += 1
                except Exception:
                    errors += 1

            # BB
            for (L, S) in sorted(bases["bb"]):
                try:
                    full = await build_bb_pack(sym, TF, L, S, now_ms, precision, redis, compute_with_l1)
                    # base уже вида bb{L}_{std}
                    slim = _slim_pack("bb", full) if full else None
                    if slim:
                        base_key = str(slim.get("base") or f"bb{L}_{str(S).replace('.', '_')}")
                        if await _publish_pack_min(redis, "bb", sym, TF, base_key, slim):
                            written += 1
                except Exception:
                    errors += 1

    await asyncio.gather(*[asyncio.create_task(_wrap(s)) for s in symbols])

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    total_bases = sum(len(v) if isinstance(v, set) else 0 for v in bases.values())
    log.info(
        f"PACK_H1 PASS done: symbols={len(symbols)} bases={total_bases} written={written} errors={errors} "
        f"hits={hits_misses['hits']} misses={hits_misses['misses']} elapsed_ms={elapsed_ms}"
    )