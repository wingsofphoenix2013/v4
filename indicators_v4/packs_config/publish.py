# packs_config/publish.py — публикация результатов ind_pack в Redis KV + дублирование в Redis Stream (core) для записи в PG

from __future__ import annotations

# 🔸 Imports
from typing import Any


# 🔸 Константы Redis (результаты pack)
IND_PACK_PREFIX = "ind_pack"  # префикс ключей результата

# 🔸 Константы Redis (core stream для PG)
IND_PACK_STREAM_CORE = "ind_pack_stream_core"


# 🔸 Publish helpers (JSON -> Redis KV + Stream)
async def publish_static(
    redis: Any,
    analysis_id: int,
    direction: str,
    symbol: str,
    timeframe: str,
    payload_json: str,
    ttl_sec: int,
    meta: dict[str, Any] | None = None,
):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))

    # stream mirror (best-effort)
    try:
        fields = {
            "kind": "static",
            "analysis_id": str(int(analysis_id)),
            "scenario_id": "",
            "signal_id": "",
            "direction": str(direction),
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "ttl_sec": str(int(ttl_sec)),
            "payload_json": str(payload_json),
        }

        # опциональная мета (если вызывающая сторона передаст)
        if isinstance(meta, dict) and meta:
            if meta.get("run_id") is not None:
                fields["run_id"] = str(meta.get("run_id"))
            if meta.get("open_ts_ms") is not None:
                fields["open_ts_ms"] = str(meta.get("open_ts_ms"))
            if meta.get("open_time") is not None:
                fields["open_time"] = str(meta.get("open_time"))

        await redis.xadd(IND_PACK_STREAM_CORE, fields)
    except Exception:
        # не ломаем hot-path публикации в KV
        pass


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
    meta: dict[str, Any] | None = None,
):
    key = f"{IND_PACK_PREFIX}:{analysis_id}:{scenario_id}:{signal_id}:{direction}:{symbol}:{timeframe}"
    await redis.set(key, payload_json, ex=int(ttl_sec))

    # stream mirror (best-effort)
    try:
        fields = {
            "kind": "pair",
            "analysis_id": str(int(analysis_id)),
            "scenario_id": str(int(scenario_id)),
            "signal_id": str(int(signal_id)),
            "direction": str(direction),
            "symbol": str(symbol),
            "timeframe": str(timeframe),
            "ttl_sec": str(int(ttl_sec)),
            "payload_json": str(payload_json),
        }

        # опциональная мета (если вызывающая сторона передаст)
        if isinstance(meta, dict) and meta:
            if meta.get("run_id") is not None:
                fields["run_id"] = str(meta.get("run_id"))
            if meta.get("open_ts_ms") is not None:
                fields["open_ts_ms"] = str(meta.get("open_ts_ms"))
            if meta.get("open_time") is not None:
                fields["open_time"] = str(meta.get("open_time"))

        await redis.xadd(IND_PACK_STREAM_CORE, fields)
    except Exception:
        # не ломаем hot-path публикации в KV
        pass