# position_snapshot_postproc.py — пост-обработчик снапшотов TF: корзины RSI14 и композитные ключи в Redis KV

# 🔸 Импорты
import os
import asyncio
import logging
import time

from typing import Dict, Tuple

from position_snapshot_sharedmemory import SNAP_QUEUE, run_sharedmemory_gc

# 🔸 Логгер
log = logging.getLogger("POS_SNAP_POSTPROC")

# 🔸 Конфиг
POSTPROC_WORKERS = int(os.getenv("POSTPROC_WORKERS", "16"))
REDIS_KV_TTL_SEC = int(os.getenv("REDIS_KV_TTL_SEC", "60"))  # TTL ключей в KV
STATE_GC_SEC = int(os.getenv("STATE_GC_SEC", "120"))         # TTL агрегатора композита

# 🔸 Внутреннее состояние для композита (ждём все 3 TF)
_state: Dict[Tuple[int, str], Dict[str, int]] = {}     # (strategy_id, log_uid) -> {"m5":b, "m15":b, "h1":b}
_state_ts: Dict[Tuple[int, str], float] = {}           # последний апдейт по ключу
_locks: Dict[Tuple[int, str], asyncio.Lock] = {}       # per-key lock

# 🔸 Биты маски по TF
_TF_BIT = {"m5": 1, "m15": 2, "h1": 4}
_ALL_MASK = 1 | 2 | 4


# 🔸 Корзина RSI14: нижняя граница диапазона кратная 5 (0..95)
def _bucket_rsi14(val: float) -> int:
    try:
        x = float(val)
    except Exception:
        return None  # сигнал пропуска
    if x < 0.0:
        x = 0.0
    if x > 100.0:
        x = 100.0
    b = int(x // 5) * 5
    return 95 if b > 95 else b


# 🔸 Ключи Redis KV
def _kv_key_tf(strategy_id: int, log_uid: str, tf: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:{tf}:rsi14_bucket"

def _kv_key_combo(strategy_id: int, log_uid: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:rsi14_bucket_combo"


# 🔸 Один потребитель очереди (worker)
async def _postproc_worker(redis):
    while True:
        item = await SNAP_QUEUE.get()

        try:
            strategy_id = int(item["strategy_id"])
            log_uid     = str(item.get("log_uid") or "")
            tf          = item["timeframe"]          # "m5" | "m15" | "h1"
            payload     = item["payload"]            # dict[param_name -> value_str]
            if not isinstance(payload, dict):
                continue

            # найти rsi14
            rsi_str = payload.get("rsi14")
            if rsi_str is None:
                log.debug(f"[RSI14] skip: no rsi14 for sid={strategy_id} log={log_uid} tf={tf}")
                continue

            # корзина TF
            bucket = _bucket_rsi14(rsi_str)
            if bucket is None:
                log.debug(f"[RSI14] skip: bad rsi14 for sid={strategy_id} log={log_uid} tf={tf} val={rsi_str}")
                continue

            # записать пер-TF ключ с TTL
            try:
                await redis.set(_kv_key_tf(strategy_id, log_uid, tf), str(bucket), ex=REDIS_KV_TTL_SEC)
            except Exception as e:
                log.warning(f"[KV_TF_SET] err: {e}")

            # обновить агрегатор композита (ждать все 3 TF)
            key = (strategy_id, log_uid)
            lock = _locks.setdefault(key, asyncio.Lock())

            async with lock:
                cur = _state.get(key)
                if cur is None:
                    cur = {}
                    _state[key] = cur
                cur[tf] = bucket
                _state_ts[key] = time.time()

                # маска готовности
                mask = 0
                for tfn, bit in _TF_BIT.items():
                    if tfn in cur:
                        mask |= bit

                # когда есть все 3 TF — публикуем композит и чистим
                if mask == _ALL_MASK:
                    vals = [cur["m5"], cur["m15"], cur["h1"]]
                    vals.sort()
                    combo_str = f"{vals[0]}-{vals[1]}-{vals[2]}"

                    try:
                        await redis.set(_kv_key_combo(strategy_id, log_uid), combo_str, ex=REDIS_KV_TTL_SEC)
                        log.info(f"[RSI14_COMBO] sid={strategy_id} log={log_uid} combo={combo_str}")
                    except Exception as e:
                        log.warning(f"[KV_COMBO_SET] err: {e}")

                    _state.pop(key, None)
                    _state_ts.pop(key, None)
                    _locks.pop(key, None)

            log.info(f"[RSI14] sid={strategy_id} log={log_uid} tf={tf} bucket={bucket}")

        except Exception:
            log.exception("[POSTPROC] unexpected error")


# 🔸 GC зависших агрегатов (если один из TF не пришёл)
async def _garbage_collector():
    while True:
        now = time.time()
        for key, ts in list(_state_ts.items()):
            if now - ts > STATE_GC_SEC:
                _state.pop(key, None)
                _state_ts.pop(key, None)
                _locks.pop(key, None)
        await asyncio.sleep(30)


# 🔸 Точка входа пост-обработчика
async def run_snapshot_postproc(redis):
    # GC для sharedmemory (кэш) и локальный GC агрегатора
    asyncio.create_task(run_sharedmemory_gc())
    asyncio.create_task(_garbage_collector())

    # запуск N параллельных воркеров
    for _ in range(max(1, POSTPROC_WORKERS)):
        asyncio.create_task(_postproc_worker(redis))

    # держим корутину живой
    while True:
        await asyncio.sleep(3600)