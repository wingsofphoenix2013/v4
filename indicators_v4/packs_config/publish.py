# packs_config/publish.py — публикация результатов ind_pack в Redis KV (static/pair keys)

from __future__ import annotations

# 🔸 Imports
from typing import Any


# 🔸 Константы Redis (результаты pack)
IND_PACK_PREFIX = "ind_pack"  # префикс ключей результата


# 🔸 Publish helpers (JSON)
async def publish_static(
    redis: Any,
    analysis_id: int,
    direction: str,
    symbol: str,
    timeframe: str,
    payload_json: str,
    ttl_sec: int,
):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))


async def publish_pair(
    redis: Any,
    analysis_id: int,
    scenario_id: int,
    signal_id: int,
    direction: str,
    symbol: str,
    timeframe: str,
    payload_json: str,
    ttl_sec: int,
):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))