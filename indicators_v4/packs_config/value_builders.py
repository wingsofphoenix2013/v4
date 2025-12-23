# packs_config/value_builders.py — builders входных “обогащённых” значений для single-TF pack’ов (bb/lr/atr/dmigap)

from __future__ import annotations

# 🔸 Imports
from typing import Any

from packs_config.redis_ts import ts_get_value_at


# 🔸 Константы Redis TS (feed_bb)
BB_TS_PREFIX = "bb:ts"  # bb:ts:{symbol}:{tf}:{field}


# 🔸 Value builders (single-TF)
async def build_bb_band_value(
    redis: Any,
    symbol: str,
    timeframe: str,
    bb_prefix: str,
    open_ts_ms: int | None,
) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    upper_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{bb_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)

    if upper_val is None:
        missing.append(f"{bb_prefix}_upper")
    if lower_val is None:
        missing.append(f"{bb_prefix}_lower")

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"price": str(close_val), "upper": str(upper_val), "lower": str(lower_val)}, []


async def build_lr_band_value(
    redis: Any,
    symbol: str,
    timeframe: str,
    lr_prefix: str,
    open_ts_ms: int | None,
) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    upper_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_upper"
    lower_key = f"ind:{symbol}:{timeframe}:{lr_prefix}_lower"

    upper_val = await redis.get(upper_key)
    lower_val = await redis.get(lower_key)

    if upper_val is None:
        missing.append(f"{lr_prefix}_upper")
    if lower_val is None:
        missing.append(f"{lr_prefix}_lower")

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"price": str(close_val), "upper": str(upper_val), "lower": str(lower_val)}, []


async def build_atr_pct_value(
    redis: Any,
    symbol: str,
    timeframe: str,
    atr_param_name: str,
    open_ts_ms: int | None,
) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    atr_key = f"ind:{symbol}:{timeframe}:{atr_param_name}"
    atr_val = await redis.get(atr_key)
    if atr_val is None:
        missing.append(str(atr_param_name))

    if open_ts_ms is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": None})
        return None, missing

    close_key = f"{BB_TS_PREFIX}:{symbol}:{timeframe}:c"
    close_val = await ts_get_value_at(redis, close_key, int(open_ts_ms))
    if close_val is None:
        missing.append({"tf": timeframe, "field": "c", "source": "bb:ts", "open_ts_ms": int(open_ts_ms)})

    if missing:
        return None, missing

    return {"atr": str(atr_val), "price": str(close_val)}, []


async def build_dmigap_value(
    redis: Any,
    symbol: str,
    timeframe: str,
    base_param_name: str,
) -> tuple[dict[str, str] | None, list[Any]]:
    missing: list[Any] = []

    plus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_plus_di"
    minus_key = f"ind:{symbol}:{timeframe}:{base_param_name}_minus_di"

    plus_val = await redis.get(plus_key)
    minus_val = await redis.get(minus_key)

    if plus_val is None:
        missing.append(f"{base_param_name}_plus_di")
    if minus_val is None:
        missing.append(f"{base_param_name}_minus_di")

    if missing:
        return None, missing

    return {"plus": str(plus_val), "minus": str(minus_val)}, []