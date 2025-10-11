# laboratory_pack_live.py — воркер laboratory_v4: PACK (rsi/mfi/ema/atr/lr/adx_dmi/macd/bb) с использованием live-кеша IND; публикация в lab_live:pack:...

# 🔸 Импорты
import asyncio
import logging
import time
import json
from typing import Tuple, Dict, List, Set

from lab_utils import floor_to_bar
from compute_only import compute_snapshot_values_async  # fallback-расчёт live при промахе кеша
from laboratory_config import get_live_values           # чтение live из in-memory кеша IND

# 🔸 Пакетные билдеры базовых индикаторов (работают на текущем баре)
from packs.rsi_pack import build_rsi_pack
from packs.mfi_pack import build_mfi_pack
from packs.ema_pack import build_ema_pack
from packs.atr_pack import build_atr_pack
from packs.lr_pack import build_lr_pack
from packs.adx_dmi_pack import build_adx_dmi_pack
from packs.macd_pack import build_macd_pack
from packs.bb_pack import build_bb_pack

# 🔸 Логгер
log = logging.getLogger("LAB_PACK_LIVE")

# 🔸 Константы воркера
TF_SET = ("m5", "m15", "h1")     # поддерживаемые TF
LAB_PREFIX = "lab_live"          # префикс пространства лаборатории
LAB_TTL_SEC = 60                 # TTL KV-записей
TICK_INTERVAL_SEC = 15           # период тика публикации
MAX_CONCURRENCY = 64             # одновременно обрабатываемые пары (symbol, tf)
BASE_CONCURRENCY = 16            # параллелизм по базам внутри пары (rsi14, ema21, macd12, bb20_2_0, ...)

# 🔸 Сопоставление имени индикатора → builder и тип параметров
BUILDERS = {
    "rsi": ("length", build_rsi_pack),
    "mfi": ("length", build_mfi_pack),
    "ema": ("length", build_ema_pack),
    "atr": ("length", build_atr_pack),
    "lr":  ("length", build_lr_pack),
    "adx_dmi": ("length", build_adx_dmi_pack),
    "macd": ("fast", build_macd_pack),
    "bb":  ("bb", build_bb_pack),  # пара (length, std)
}

# 🔸 Сформировать KV-ключ для PACK
def _pack_key(indicator: str, symbol: str, tf: str, base: str) -> str:
    return f"{LAB_PREFIX}:pack:{indicator}:{symbol}:{tf}:{base}"

# 🔸 Собрать набор баз (base) по кешу инстансов для указанного TF
def _collect_bases(instances: List[Dict]) -> Dict[str, Set]:
    """
    Возвращает:
      {
        "rsi": {14, 21, ...},
        "bb": {(20, 2.0), ...},
        "macd": {12, 5, ...},
        ...
      }
    """
    out: Dict[str, Set] = {
        "rsi": set(), "mfi": set(), "ema": set(), "atr": set(), "lr": set(), "adx_dmi": set(),
        "macd": set(), "bb": set()
    }
    for inst in instances:
        kind = inst.get("indicator")
        params = inst.get("params") or {}
        if kind not in out:
            continue
        try:
            if kind in ("rsi", "mfi", "ema", "atr", "lr", "adx_dmi"):
                L = int(params.get("length"))
                out[kind].add(L)
            elif kind == "macd":
                F = int(params.get("fast"))
                out[kind].add(F)
            elif kind == "bb":
                L = int(params.get("length"))
                S = round(float(params.get("std")), 2)
                out[kind].add((L, S))
        except Exception:
            # если параметры кривые — пропускаем этот инстанс
            continue
    return out

# 🔸 Публикация одного PACK под ключ lab_live:pack:... с TTL
async def _persist_pack(redis, indicator: str, symbol: str, tf: str, pack_obj: Dict) -> bool:
    try:
        base = pack_obj.get("base")
        if not base:
            return False
        key = _pack_key(indicator, symbol, tf, base)
        await redis.set(key, json.dumps(pack_obj), ex=LAB_TTL_SEC)
        return True
    except Exception as e:
        log.warning("persist error %s/%s %s base=? : %s", symbol, tf, indicator, e)
        return False

# 🔸 Обработка одной пары (symbol, tf): строим все требуемые PACK по активным инстансам
async def _process_pair(
    redis,
    symbol: str,
    tf: str,
    open_ms: int,
    precision: int,
    instances: List[Dict],
) -> Tuple[int, int]:
    """
    Возвращает (published_packs, skipped_packs)
    """
    bases = _collect_bases(instances)
    published = 0
    skipped = 0

    # обёртка compute_fn: сначала пытаемся взять live из кеша IND (по (symbol, tf, open_ms)), при промахе — считаем
    async def compute_fn_cached(inst: dict, symbol_: str, df, precision_: int) -> Dict[str, str]:
        try:
            tf_ = inst.get("timeframe")
            if tf_ is None:
                return await compute_snapshot_values_async(inst, symbol_, df, precision_)
            cached = get_live_values(symbol_, tf_, open_ms)
            if not cached:
                return await compute_snapshot_values_async(inst, symbol_, df, precision_)

            indicator = inst.get("indicator")
            params = inst.get("params") or {}
            if indicator == "macd":
                base = f"macd{params['fast']}"
            elif "length" in params:
                base = f"{indicator}{params['length']}"
            else:
                base = str(indicator)

            out: Dict[str, str] = {}
            for k, v in cached.items():
                s = str(k)
                if s == base or s.startswith(f"{base}_"):
                    out[s] = str(v)

            if not out:
                return await compute_snapshot_values_async(inst, symbol_, df, precision_)
            return out
        except Exception:
            return await compute_snapshot_values_async(inst, symbol_, df, precision_)

    # ограничим параллелизм по базам локальным семафором
    base_sem = asyncio.Semaphore(BASE_CONCURRENCY)

    async def _build_and_publish(ind: str, base_item) -> Tuple[int, int]:
        async with base_sem:
            try:
                which, builder = BUILDERS[ind]
                if which == "length":
                    pack_obj = await builder(symbol, tf, int(base_item), open_ms, precision, redis, compute_fn_cached)
                elif which == "fast":
                    pack_obj = await builder(symbol, tf, int(base_item), open_ms, precision, redis, compute_fn_cached)
                elif which == "bb":
                    L, S = base_item
                    pack_obj = await builder(symbol, tf, int(L), float(S), open_ms, precision, redis, compute_fn_cached)
                else:
                    return (0, 1)

                if not pack_obj:
                    return (0, 1)

                ok = await _persist_pack(redis, ind, symbol, tf, pack_obj)
                return (1 if ok else 0, 0 if ok else 1)
            except Exception as e:
                log.warning("pack build error %s/%s %s %r: %s", symbol, tf, ind, base_item, e)
                return (0, 1)

    # собираем задачи по всем индикаторам и их базам
    tasks = []
    for ind, items in bases.items():
        for it in items:
            tasks.append(asyncio.create_task(_build_and_publish(ind, it)))

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for pub, sk in results:
            published += pub
            skipped += sk

    return (published, skipped)

# 🔸 Одиночный тик: выполнить один проход по указанным TF (без задержки/цикла)
async def tick_pack(
    pg,
    redis,
    get_active_symbols,      # callable() -> list[str]
    get_precision,           # callable(symbol) -> int|None
    get_instances_by_tf,     # callable(tf) -> list[instance]
    get_last_bar,            # callable(symbol, tf) -> int|None
    tf_set: Tuple[str, ...] = TF_SET,
) -> Tuple[int, int, int, int]:
    """
    Выполнить один PACK-проход по всем символам и tf_set.
    Возвращает агрегаты: (pairs, published_packs, skipped, elapsed_ms).
    """
    t0 = time.monotonic()
    now_ms = int(time.time() * 1000)

    symbols = get_active_symbols()
    if not symbols:
        return (0, 0, 0, int((time.monotonic() - t0) * 1000))

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    # кэш инстансов по TF на тик (они общие для всех символов)
    inst_by_tf: Dict[str, List[Dict]] = {tf: get_instances_by_tf(tf) for tf in tf_set}

    total_pairs = 0
    total_published = 0
    total_skipped = 0

    async def run_one(sym: str, tf: str):
        nonlocal total_pairs, total_published, total_skipped
        async with sem:
            last = get_last_bar(sym, tf)
            open_ms = last if last is not None else floor_to_bar(now_ms, tf)
            prec = get_precision(sym) or 8
            try:
                pub, sk = await _process_pair(redis, sym, tf, open_ms, prec, inst_by_tf.get(tf, []))
            except Exception as e:
                log.warning("pair error %s/%s: %s", sym, tf, e)
                pub, sk = 0, 0
            total_pairs += 1
            total_published += pub
            total_skipped += sk

    tasks = [
        asyncio.create_task(run_one(sym, tf))
        for sym in symbols
        for tf in tf_set
    ]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=False)

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    return (total_pairs, total_published, total_skipped, elapsed_ms)

# 🔸 Основной воркер (не используется в координаторе, оставлен для совместимости)
async def run_lab_pack_live(
    pg,
    redis,
    get_active_symbols,      # callable() -> list[str]
    get_precision,           # callable(symbol) -> int|None
    get_instances_by_tf,     # callable(tf) -> list[instance]
    get_last_bar,            # callable(symbol, tf) -> int|None
    tf_set: Tuple[str, ...] = TF_SET,
    tick_interval_sec: int = TICK_INTERVAL_SEC,
):
    while True:
        pairs, published, skipped, elapsed_ms = await tick_pack(
            pg=pg,
            redis=redis,
            get_active_symbols=get_active_symbols,
            get_precision=get_precision,
            get_instances_by_tf=get_instances_by_tf,
            get_last_bar=get_last_bar,
            tf_set=tf_set,
        )

        log.info(
            "LAB PACK: tick done tf=%s pairs=%d packs=%d skipped=%d elapsed_ms=%d",
            ",".join(tf_set), total_pairs, total_published, total_skipped, elapsed_ms
        )

        await asyncio.sleep(tick_interval_sec)