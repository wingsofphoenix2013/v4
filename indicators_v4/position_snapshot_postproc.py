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

# 🔸 Внутреннее состояние для ADX-композита
_state_adx: Dict[Tuple[int, str], Dict[str, int]] = {}   # (strategy_id, log_uid) -> {"m5":b, "m15":b, "h1":b}
_state_adx_ts: Dict[Tuple[int, str], float] = {}
_locks_adx: Dict[Tuple[int, str], asyncio.Lock] = {}

# 🔸 Внутреннее состояние для DMI-GAP композита
_state_gap: Dict[Tuple[int, str], Dict[str, int]] = {}
_state_gap_ts: Dict[Tuple[int, str], float] = {}
_locks_gap: Dict[Tuple[int, str], asyncio.Lock] = {}

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

# 🔸 Биннинг ADX: нижняя граница шага 5 (0..95)
def _bucket_adx(val: float) -> int | None:
    try:
        x = float(val)
    except Exception:
        return None
    if x < 0.0:
        x = 0.0
    if x > 100.0:
        x = 100.0
    b = int(x // 5) * 5
    return 95 if b > 95 else b

# 🔸 Биннинг DMI-GAP: clip [-100..100], шаг 5; 100 → 95, −100 остаётся −100
def _bucket_dmigap(val: float) -> int | None:
    try:
        x = float(val)
    except Exception:
        return None
    if x < -100.0:
        x = -100.0
    elif x > 100.0:
        x = 100.0
    b = int(x // 5) * 5
    return 95 if b > 95 else b
    
# 🔸 Ключи Redis KV
def _kv_key_tf(strategy_id: int, log_uid: str, tf: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:{tf}:rsi14_bucket"

def _kv_key_combo(strategy_id: int, log_uid: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:rsi14_bucket_combo"

# 🔸 Ключи Redis KV (ADX)
def _kv_key_tf_adx(strategy_id: int, log_uid: str, tf: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:{tf}:adxbin"

def _kv_key_combo_adx(strategy_id: int, log_uid: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:adxbin_combo"

# 🔸 Ключи Redis KV (DMI-GAP)
def _kv_key_tf_dmigap(strategy_id: int, log_uid: str, tf: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:{tf}:dmigapbin"

def _kv_key_combo_dmigap(strategy_id: int, log_uid: str) -> str:
    return f"posfeat:{strategy_id}:{log_uid}:dmigapbin_combo"

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

            # =========================
            # RSI14: per-TF + композит
            # =========================
            rsi_str = payload.get("rsi14")
            if rsi_str is not None:
                rsi_bin = _bucket_rsi14(rsi_str)
                if rsi_bin is not None:
                    # per-TF KV с TTL
                    try:
                        await redis.set(_kv_key_tf(strategy_id, log_uid, tf), str(rsi_bin), ex=REDIS_KV_TTL_SEC)
                    except Exception as e:
                        log.warning(f"[KV_TF_SET] err: {e}")

                    # обновить агрегатор и, если собраны три TF, опубликовать композит (m5-m15-h1)
                    key = (strategy_id, log_uid)
                    lock = _locks.setdefault(key, asyncio.Lock())
                    async with lock:
                        cur = _state.get(key) or {}
                        cur[tf] = rsi_bin
                        _state[key] = cur
                        _state_ts[key] = time.time()

                        mask = 0
                        for tfn, bit in _TF_BIT.items():
                            if tfn in cur:
                                mask |= bit

                        if mask == _ALL_MASK:
                            combo_str = f"{cur['m5']}-{cur['m15']}-{cur['h1']}"
                            try:
                                await redis.set(_kv_key_combo(strategy_id, log_uid), combo_str, ex=REDIS_KV_TTL_SEC)
                                log.info(f"[RSI14_COMBO] sid={strategy_id} log={log_uid} combo={combo_str}")
                            except Exception as e:
                                log.warning(f"[KV_COMBO_SET] err: {e}")

                            _state.pop(key, None)
                            _state_ts.pop(key, None)
                            _locks.pop(key, None)

                    log.debug(f"[RSI14] sid={strategy_id} log={log_uid} tf={tf} bucket={rsi_bin}")
                else:
                    log.debug(f"[RSI14] skip: bad rsi14 for sid={strategy_id} log={log_uid} tf={tf} val={rsi_str}")
            else:
                log.debug(f"[RSI14] skip: no rsi14 for sid={strategy_id} log={log_uid} tf={tf}")

            # =========================
            # ADXbin: per-TF + композит
            # =========================
            adx_key = "adx_dmi14_adx" if tf in ("m5", "m15") else ("adx_dmi28_adx" if tf == "h1" else None)
            if adx_key:
                adx_str = payload.get(adx_key)
                if adx_str is not None:
                    adx_bin = _bucket_adx(adx_str)
                    if adx_bin is not None:
                        # per-TF KV с TTL
                        try:
                            await redis.set(_kv_key_tf_adx(strategy_id, log_uid, tf), str(adx_bin), ex=REDIS_KV_TTL_SEC)
                        except Exception as e:
                            log.warning(f"[KV_TF_SET_ADX] err: {e}")

                        # обновить агрегатор и, если собраны три TF, опубликовать композит (m5-m15-h1)
                        key_adx = (strategy_id, log_uid)
                        lock_adx = _locks_adx.setdefault(key_adx, asyncio.Lock())
                        async with lock_adx:
                            cur = _state_adx.get(key_adx) or {}
                            cur[tf] = adx_bin
                            _state_adx[key_adx] = cur
                            _state_adx_ts[key_adx] = time.time()

                            mask = 0
                            for tfn, bit in _TF_BIT.items():
                                if tfn in cur:
                                    mask |= bit

                            if mask == _ALL_MASK:
                                combo_adx = f"{cur['m5']}-{cur['m15']}-{cur['h1']}"
                                try:
                                    await redis.set(_kv_key_combo_adx(strategy_id, log_uid), combo_adx, ex=REDIS_KV_TTL_SEC)
                                    log.info(f"[ADXBIN_COMBO] sid={strategy_id} log={log_uid} combo={combo_adx}")
                                except Exception as e:
                                    log.warning(f"[KV_COMBO_SET_ADX] err: {e}")

                                _state_adx.pop(key_adx, None)
                                _state_adx_ts.pop(key_adx, None)
                                _locks_adx.pop(key_adx, None)

                        log.debug(f"[ADXBIN] sid={strategy_id} log={log_uid} tf={tf} bin={adx_bin}")
                    else:
                        log.debug(f"[ADXBIN] skip: bad adx for sid={strategy_id} log={log_uid} tf={tf} val={adx_str}")
                else:
                    log.debug(f"[ADXBIN] skip: no {adx_key} for sid={strategy_id} log={log_uid} tf={tf}")

            # =========================
            # DMI-GAP bin: per-TF + композит
            # =========================
            if tf in ("m5", "m15", "h1"):
                tf_len = 14 if tf in ("m5", "m15") else 28
                plus_key  = f"adx_dmi{tf_len}_plus_di"
                minus_key = f"adx_dmi{tf_len}_minus_di"

                plus_str  = payload.get(plus_key)
                minus_str = payload.get(minus_key)

                if plus_str is not None and minus_str is not None:
                    try:
                        gap = float(plus_str) - float(minus_str)
                    except Exception:
                        gap = None

                    if gap is not None:
                        gap_bin = _bucket_dmigap(gap)
                        if gap_bin is not None:
                            # per-TF KV с TTL
                            try:
                                await redis.set(_kv_key_tf_dmigap(strategy_id, log_uid, tf), str(gap_bin), ex=REDIS_KV_TTL_SEC)
                            except Exception as e:
                                log.warning(f"[KV_TF_SET_DMIGAP] err: {e}")

                            # композит (ждём m5+m15+h1)
                            key_gap = (strategy_id, log_uid)
                            lock_gap = _locks_gap.setdefault(key_gap, asyncio.Lock())
                            async with lock_gap:
                                cur = _state_gap.get(key_gap) or {}
                                cur[tf] = gap_bin
                                _state_gap[key_gap] = cur
                                _state_gap_ts[key_gap] = time.time()

                                mask = 0
                                for tfn, bit in _TF_BIT.items():
                                    if tfn in cur:
                                        mask |= bit

                                if mask == _ALL_MASK:
                                    combo_gap = f"{cur['m5']}-{cur['m15']}-{cur['h1']}"
                                    try:
                                        await redis.set(_kv_key_combo_dmigap(strategy_id, log_uid), combo_gap, ex=REDIS_KV_TTL_SEC)
                                        log.info(f"[DMIGAP_COMBO] sid={strategy_id} log={log_uid} combo={combo_gap}")
                                    except Exception as e:
                                        log.warning(f"[KV_COMBO_SET_DMIGAP] err: {e}")

                                    _state_gap.pop(key_gap, None)
                                    _state_gap_ts.pop(key_gap, None)
                                    _locks_gap.pop(key_gap, None)

                            log.debug(f"[DMIGAP] sid={strategy_id} log={log_uid} tf={tf} bin={gap_bin}")
                        else:
                            log.debug(f"[DMIGAP] skip: bad gap bin for sid={strategy_id} log={log_uid} tf={tf} gap={gap}")
                else:
                    log.debug(f"[DMIGAP] skip: missing {plus_key}/{minus_key} for sid={strategy_id} log={log_uid} tf={tf}")

        except Exception:
            log.exception("[POSTPROC] unexpected error")

# 🔸 GC зависших агрегатов (если один из TF не пришёл)
async def _garbage_collector():
    while True:
        now = time.time()

        # RSI14
        for key, ts in list(_state_ts.items()):
            if now - ts > STATE_GC_SEC:
                _state.pop(key, None)
                _state_ts.pop(key, None)
                _locks.pop(key, None)

        # ADXbin
        for key, ts in list(_state_adx_ts.items()):
            if now - ts > STATE_GC_SEC:
                _state_adx.pop(key, None)
                _state_adx_ts.pop(key, None)
                _locks_adx.pop(key, None)

        # DMI-GAP
        for key, ts in list(_state_gap_ts.items()):
            if now - ts > STATE_GC_SEC:
                _state_gap.pop(key, None)
                _state_gap_ts.pop(key, None)
                _locks_gap.pop(key, None)
                
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