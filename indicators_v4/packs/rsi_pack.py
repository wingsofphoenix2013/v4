# packs/rsi_pack.py — on-demand построитель пакета RSI (значение, корзина, дельта, тренд)

import logging
from .pack_utils import (
    floor_to_bar,
    load_ohlcv_df,
    rsi_bucket_low,
    classify_abs_delta,
    get_closed_rsi,
    bar_open_iso,
)

log = logging.getLogger("RSI_PACK")

# 🔸 Построить пакет RSI для конкретного length
async def build_rsi_pack(symbol: str, tf: str, length: int, now_ms: int,
                         precision: int, redis, compute_fn) -> dict | None:
    """
    symbol      — тикер (например ADAUSDT)
    tf          — таймфрейм (m5/m15/h1)
    length      — длина RSI (например 14)
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
        log.warning(f"[RSI_PACK] {symbol}/{tf} rsi{length}: no ohlcv")
        return None

    # рассчитываем live значение через compute_snapshot_values_async
    inst = {
        "indicator": "rsi",
        "params": {"length": str(length)},
        "timeframe": tf,
    }
    values = await compute_fn(inst, symbol, df, precision)
    if not values:
        log.warning(f"[RSI_PACK] {symbol}/{tf} rsi{length}: compute failed")
        return None

    param_name = f"rsi{length}"
    sval = values.get(param_name)
    if sval is None:
        log.warning(f"[RSI_PACK] {symbol}/{tf} rsi{length}: no value")
        return None

    try:
        v_live = float(sval)
    except Exception:
        log.warning(f"[RSI_PACK] {symbol}/{tf} rsi{length}: bad value {sval}")
        return None

    # корзина
    bucket = rsi_bucket_low(v_live)

    # закрытое значение
    v_closed = await get_closed_rsi(redis, symbol, tf, length)
    if v_closed is None:
        delta = 0.0
        trend = "flat"
    else:
        delta = v_live - v_closed
        trend = classify_abs_delta(delta, tf)

    # сборка пакета
    pack = {
        "base": f"rsi{length}",
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