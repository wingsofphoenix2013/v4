# position_snapshot_sharedmemory.py — общий буфер в памяти для снапшотов TF от position_snapshot_worker к пост-обработчику

# 🔸 Импорты
import os
import asyncio
import time
import logging
from typing import Any

# 🔸 Логгер
log = logging.getLogger("POS_SNAP_SHM")

# 🔸 Константы
SNAP_QUEUE_MAXSIZE = int(os.getenv("SNAP_QUEUE_MAXSIZE", "0"))          # 0 = безлимит
SNAP_CACHE_TTL_SEC = int(os.getenv("SNAP_CACHE_TTL_SEC", "300"))        # 5 минут

# 🔸 Глобальные структуры
SNAP_QUEUE: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=SNAP_QUEUE_MAXSIZE)
SNAP_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

# 🔸 Ключи и утилиты
def make_cache_key(position_uid: str, timeframe: str) -> str:
    return f"{position_uid}:{timeframe}"

def _normalize_payload(payload: Any) -> dict[str, str]:
    if isinstance(payload, dict):
        return {str(k): str(v) for k, v in payload.items()}
    if isinstance(payload, list):
        out: dict[str, str] = {}
        for row in payload:
            try:
                pname = str(row.get("param_name"))
                vstr = row.get("value_str")
                if pname and vstr is not None:
                    out[pname] = str(vstr)
            except Exception:
                continue
        return out
    return {}

# 🔸 Публичный API
async def put_snapshot_tf(
    *,
    position_uid: str,
    log_uid: str | None,
    strategy_id: int,
    symbol: str,
    direction: str,
    timeframe: str,                # "m5" | "m15" | "h1"
    bar_open_time: str,            # ISO UTC
    payload: Any,                  # dict[param_name -> value_str] ИЛИ list[{param_name,value_str,...}]
    entry_price: float | None = None
) -> None:
    item = {
        "position_uid": position_uid,
        "log_uid": (str(log_uid) if log_uid is not None else ""),
        "strategy_id": int(strategy_id),
        "symbol": symbol,
        "direction": direction,
        "timeframe": timeframe,
        "bar_open_time": bar_open_time,
        "payload": _normalize_payload(payload),
        "entry_price": (float(entry_price) if entry_price is not None else None),
        "ts_put": time.time(),
    }
    await SNAP_QUEUE.put(item)
    SNAP_CACHE[make_cache_key(position_uid, timeframe)] = (item["ts_put"], item)

def get_snapshot_tf(position_uid: str, timeframe: str) -> dict[str, Any] | None:
    key = make_cache_key(position_uid, timeframe)
    rec = SNAP_CACHE.get(key)
    if not rec:
        return None
    ts, item = rec
    if time.time() - ts > SNAP_CACHE_TTL_SEC:
        SNAP_CACHE.pop(key, None)
        return None
    return item

async def run_sharedmemory_gc() -> None:
    while True:
        now = time.time()
        for key, (ts, _) in list(SNAP_CACHE.items()):
            if now - ts > SNAP_CACHE_TTL_SEC:
                SNAP_CACHE.pop(key, None)
        await asyncio.sleep(30)