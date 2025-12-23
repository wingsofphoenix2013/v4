# packs_config/redis_ts.py — Redis TS/KV helpers + Decimal/MTF boundary logic для ind_pack

from __future__ import annotations

# 🔸 Imports
import asyncio
from decimal import Decimal, InvalidOperation
from typing import Any


# 🔸 Таймшаги TF (ms) — в системе везде open_time (начало бара)
TF_STEP_MS = {
    "m5": 300_000,
    "m15": 900_000,
    "h1": 3_600_000,
}

# 🔸 Retry для «свежих» значений MTF (по TS на стыках TF)
MTF_RETRY_TOTAL_SEC = 60  # максимум ожидания «свежего» TF
MTF_RETRY_STEP_SEC = 5    # период опроса TS

# 🔸 Константы Redis TS (indicators_v4)
IND_TS_PREFIX = "ts_ind"  # ts_ind:{symbol}:{tf}:{param_name}


# 🔸 Redis TS helpers
async def ts_get(redis: Any, key: str) -> tuple[int, str] | None:
    try:
        res = await redis.execute_command("TS.GET", key)
        if not res:
            return None
        ts_ms, value = res
        return int(ts_ms), str(value)
    except Exception:
        return None


async def ts_get_value_at(redis: Any, key: str, ts_ms: int) -> str | None:
    try:
        res = await redis.execute_command("TS.RANGE", key, int(ts_ms), int(ts_ms))
        if not res:
            return None
        _, value = res[-1]
        return str(value)
    except Exception:
        return None


# 🔸 Decimal helpers
def safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def clip_0_100(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("100"):
        return Decimal("100")
    return value


# 🔸 MTF boundary helpers
def is_tf_boundary(ts_ms: int, tf: str) -> bool:
    step = TF_STEP_MS.get(tf)
    if not step:
        return False
    return (int(ts_ms) % int(step)) == 0


def calc_close_boundary_ts_ms(open_ts_ms: int, tf: str) -> int:
    step = TF_STEP_MS.get(tf)
    if not step:
        return int(open_ts_ms)
    return int(open_ts_ms) + int(step)


def just_closed_open_time(boundary_ts_ms: int, tf: str) -> int:
    return int(boundary_ts_ms) - int(TF_STEP_MS[tf])


# 🔸 KV/TS indicator getters for MTF
async def get_kv_decimal(redis: Any, symbol: str, tf: str, param_name: str) -> tuple[Decimal | None, str | None]:
    key = f"ind:{symbol}:{tf}:{param_name}"
    raw = await redis.get(key)
    if raw is None:
        return None, None
    return safe_decimal(raw), str(raw)


async def get_ts_decimal_with_retry(
    redis: Any,
    symbol: str,
    tf: str,
    param_name: str,
    open_ts_ms: int,
) -> tuple[Decimal | None, str | None, int]:
    key = f"{IND_TS_PREFIX}:{symbol}:{tf}:{param_name}"
    waited = 0
    raw: str | None = None

    while waited <= MTF_RETRY_TOTAL_SEC:
        raw = await ts_get_value_at(redis, key, int(open_ts_ms))
        d = safe_decimal(raw)
        if d is not None:
            return d, (str(raw) if raw is not None else None), waited

        # таймаут достигнут
        if waited >= MTF_RETRY_TOTAL_SEC:
            break

        await asyncio.sleep(MTF_RETRY_STEP_SEC)
        waited += MTF_RETRY_STEP_SEC

    return None, (str(raw) if raw is not None else None), waited


async def get_mtf_value_decimal(
    redis: Any,
    symbol: str,
    trigger_open_ts_ms: int,
    target_tf: str,
    param_name: str,
) -> tuple[Decimal | None, str | None, dict[str, Any]]:
    meta: dict[str, Any] = {"styk": False, "waited_sec": 0, "target_tf": str(target_tf)}

    # m5 — событие ready уже гарантирует актуальность значения для этого open_time
    if target_tf == "m5":
        d, raw = await get_kv_decimal(redis, symbol, "m5", param_name)
        return d, raw, meta

    # граница закрытия m5-бара
    boundary = calc_close_boundary_ts_ms(int(trigger_open_ts_ms), "m5")

    # если boundary не является границей target_tf — target_tf не пересчитывается сейчас, KV безопасен
    if not is_tf_boundary(boundary, str(target_tf)):
        d, raw = await get_kv_decimal(redis, symbol, str(target_tf), param_name)
        return d, raw, meta

    # styk TF: нужен «свежий» бар target_tf, который только что закрылся на boundary
    meta["styk"] = True
    meta["boundary_open_ts_ms"] = int(boundary)

    target_open = just_closed_open_time(boundary, str(target_tf))
    d, raw, waited = await get_ts_decimal_with_retry(redis, symbol, str(target_tf), param_name, int(target_open))

    meta["target_open_ts_ms"] = int(target_open)
    meta["waited_sec"] = int(waited)
    return d, raw, meta