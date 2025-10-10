# laboratory_mw_live.py — воркер laboratory_v4: периодическая публикация MW-состояний (trend/volatility/momentum/extremes) в Redis lab_live:mw:...

# 🔸 Импорты
import asyncio
import logging
import time
import json
from datetime import datetime
from typing import Tuple, Dict, Optional

from lab_utils import floor_to_bar
from compute_only import compute_snapshot_values_async  # прокидываем как compute_fn в пакеты

# 🔸 Пакетные билдеры MW (работают на текущем баре, учитывают hysteresis+dwell через laboratory_mw_shared)
from packs.trend_pack import build_trend_pack
from packs.volatility_pack import build_volatility_pack
from packs.momentum_pack import build_momentum_pack
from packs.extremes_pack import build_extremes_pack

# 🔸 Логгер
log = logging.getLogger("LAB_MW_LIVE")

# 🔸 Константы воркера
TF_SET = ("m5", "m15", "h1")        # поддерживаемые TF
LAB_PREFIX = "lab_live"             # префикс пространства лаборатории
LAB_TTL_SEC = 60                    # TTL KV-записей
TICK_INTERVAL_SEC = 15              # период тика публикации
MAX_CONCURRENCY = 64                # параллельно обрабатываемые пары (symbol, tf)
KIND_CONCURRENCY = 4                # параллелизм по видам MW внутри одной пары (у нас их 4)

# 🔸 Сопоставление kind → builder
BUILDERS = {
    "trend": build_trend_pack,
    "volatility": build_volatility_pack,
    "momentum": build_momentum_pack,
    "extremes": build_extremes_pack,
}

# 🔸 Сформировать KV-ключ MW
def _mw_key(symbol: str, tf: str, kind: str) -> str:
    return f"{LAB_PREFIX}:mw:{symbol}:{tf}:{kind}"

# 🔸 Нормализовать детали для хранения (добавить streak, если есть только streak_preview)
def _normalize_details(details: Dict) -> Dict:
    if not isinstance(details, dict):
        return {}
    # переносим streak_preview → streak (для корректной памяти prev_streak)
    sp = details.get("streak_preview")
    if sp is not None and "streak" not in details:
        try:
            details["streak"] = int(sp)
        except Exception:
            pass
    return details

# 🔸 Публикация одного MW-пакета в KV lab_live:mw:... с TTL
async def _persist_mw(redis, symbol: str, tf: str, kind: str, pack_obj: Dict) -> bool:
    try:
        pack = pack_obj.get("pack", {})
        state = pack.get("state")
        if state is None:
            return False

        open_time = pack.get("open_time")
        if open_time is None:
            # если пакет не заполнил, fallback: сейчас
            open_time = datetime.utcnow().isoformat()

        details = _normalize_details(dict(pack))  # копия, чтобы не трогать исходник
        payload = {
            "state": state,
            "version": 1,
            "open_time": open_time,
            "computed_at": datetime.utcnow().isoformat(),
            "details": details,
        }
        key = _mw_key(symbol, tf, kind)
        await redis.set(key, json.dumps(payload), ex=LAB_TTL_SEC)
        return True
    except Exception as e:
        log.warning("persist error %s/%s kind=%s: %s", symbol, tf, kind, e)
        return False

# 🔸 Обработка одной пары (symbol, tf): расчёт 4 MW-пакетов и публикация
async def _process_pair(
    redis,
    symbol: str,
    tf: str,
    open_ms: int,
    precision: int,
) -> Tuple[int, int]:
    """
    Возвращает (published_states, skipped_states)
    """
    published = 0
    skipped = 0

    # внутри пары ограничим параллелизм по видам MW
    kind_sem = asyncio.Semaphore(KIND_CONCURRENCY)

    async def _build_and_publish(kind: str) -> Tuple[int, int]:
        async with kind_sem:
            try:
                builder = BUILDERS.get(kind)
                if builder is None:
                    return (0, 1)
                # пакеты ожидают now_ms и сами нормализуют к open_time (floor_to_bar)
                pack_obj = await builder(symbol, tf, open_ms, precision, redis, compute_snapshot_values_async)
                if not pack_obj:
                    return (0, 1)
                ok = await _persist_mw(redis, symbol, tf, kind, pack_obj)
                return (1 if ok else 0, 0 if ok else 1)
            except Exception as e:
                log.warning("mw build error %s/%s kind=%s: %s", symbol, tf, kind, e)
                return (0, 1)

    results = await asyncio.gather(
        *[asyncio.create_task(_build_and_publish(kind)) for kind in BUILDERS.keys()],
        return_exceptions=False,
    )

    for pub, sk in results:
        published += pub
        skipped += sk

    return (published, skipped)

# 🔸 Основной воркер: каждые N секунд публикует MW-состояния по всем активным тикерам и TF
async def run_lab_mw_live(
    pg,
    redis,
    get_active_symbols,      # callable() -> list[str]
    get_precision,           # callable(symbol) -> int|None
    get_last_bar,            # callable(symbol, tf) -> int|None
    tf_set: Tuple[str, ...] = TF_SET,
    tick_interval_sec: int = TICK_INTERVAL_SEC,
):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        t0 = time.monotonic()
        now_ms = int(time.time() * 1000)

        symbols = get_active_symbols()
        if not symbols:
            await asyncio.sleep(tick_interval_sec)
            continue

        total_pairs = 0
        total_published = 0   # количество успешных состояний (из 4 на пару)
        total_skipped = 0

        async def run_one(sym: str, tf: str):
            nonlocal total_pairs, total_published, total_skipped
            async with sem:
                last = get_last_bar(sym, tf)
                # для идентичности методологии работаем на закрытом баре, если есть метка; иначе — на текущем floored
                open_ms = last if last is not None else floor_to_bar(now_ms, tf)
                prec = get_precision(sym) or 8
                try:
                    pub, sk = await _process_pair(redis, sym, tf, open_ms, prec)
                except Exception as e:
                    log.warning("pair error %s/%s: %s", sym, tf, e)
                    pub, sk = 0, 4
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

        t1 = time.monotonic()
        elapsed_ms = int((t1 - t0) * 1000)

        # итоговый лог тика
        log.info(
            "LAB MW: tick done pairs=%d states=%d skipped=%d elapsed_ms=%d",
            total_pairs, total_published, total_skipped, elapsed_ms
        )

        await asyncio.sleep(tick_interval_sec)