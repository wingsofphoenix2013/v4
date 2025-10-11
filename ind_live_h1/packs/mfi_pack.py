# packs/mfi_pack.py — on-demand построитель пакета MFI (значение, корзина, дельта, тренд)

import logging
from .pack_utils import (
    floor_to_bar,
    load_ohlcv_df,
    rsi_bucket_low,        # корзина 0..95 с шагом 5 — подойдёт и для MFI
    classify_abs_delta,    # up/flat/down по абсолютному ε (m5=0.3, m15=0.4, h1=0.6)
    bar_open_iso,
)

log = logging.getLogger("MFI_PACK")

# 🔸 Прочитать закрытое значение MFI из KV ind:{symbol}:{tf}:mfi{length}
async def get_closed_mfi(redis, symbol: str, tf: str, length: int) -> float | None:
    key = f"ind:{symbol}:{tf}:mfi{length}"
    try:
        s = await redis.get(key)
        return float(s) if s is not None else None
    except Exception:
        return None

# 🔸 Построить пакет MFI для конкретного length
async def build_mfi_pack(symbol: str, tf: str, length: int, now_ms: int,
                         precision: int, redis, compute_fn) -> dict | None:
    """
    symbol      — тикер (например SOLUSDT)
    tf          — таймфрейм (m5/m15/h1)
    length      — длина MFI (например 14)
    now_ms      — текущее время UTC в мс
    precision   — точность цены по символу (для округлений)
    redis       — клиент Redis
    compute_fn  — ссылка на compute_snapshot_values_async
    """

    # нормализуем время к открытию бара
    bar_open_ms = floor_to_bar(now_ms, tf)

    # грузим OHLCV
    df = await load_ohlcv_df(redis, symbol, tf, bar_open_ms, 800)
    if df is None or df.empty:
        log.warning(f"[MFI_PACK] {symbol}/{tf} mfi{length}: no ohlcv")
        return None

    # рассчитываем live значение через compute_snapshot_values_async
    inst = {
        "indicator": "mfi",
        "params": {"length": str(length)},
        "timeframe": tf,
    }
    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[MFI_PACK] {symbol}/{tf} mfi{length}: compute failed")
        return None

    param_name = f"mfi{length}"
    sval = values.get(param_name)
    if sval is None:
        log.warning(f"[MFI_PACK] {symbol}/{tf} mfi{length}: no value")
        return None

    try:
        v_live = float(sval)
    except Exception:
        log.warning(f"[MFI_PACK] {symbol}/{tf} mfi{length}: bad value {sval}")
        return None

    # корзина (нижняя граница кратная 5)
    bucket = rsi_bucket_low(v_live)

    # закрытое значение
    v_closed = await get_closed_mfi(redis, symbol, tf, length)
    if v_closed is None:
        delta = 0.0
        trend = "flat"
    else:
        delta = v_live - v_closed
        trend = classify_abs_delta(delta, tf)

    # сборка пакета
    pack = {
        "base": f"mfi{length}",
        "pack": {
            "value": f"{v_live:.2f}",
            "bucket_low": bucket,
            "delta": f"{delta:.2f}",
            "trend": trend,
            "ref": "closed",
            "open_time": bar_open_iso(bar_open_ms),
        },
    }

    return pack