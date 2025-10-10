# laboratory_ind_live.py — воркер laboratory_v4: периодическая публикация IND-снапшотов в Redis (lab_live:ind:...) без БД/стримов

# 🔸 Импорты
import asyncio
import logging
import time
from typing import Dict, Tuple, Optional

from lab_utils import floor_to_bar, load_ohlcv_df
from compute_only import compute_snapshot_values_async

# 🔸 Логгер
log = logging.getLogger("LAB_IND_LIVE")

# 🔸 Константы воркера
TF_SET = ("m5", "m15", "h1")          # работаем только с этими TF
LAB_PREFIX = "lab_live"               # префикс пространства лаборатории
LAB_TTL_SEC = 45                      # TTL для всех lab_live ключей
TICK_INTERVAL_SEC = 30                # период тика публикации
MAX_CONCURRENCY = 64                  # параллельные (symbol, tf)

# 🔸 Путь KV-ключа для IND
def _ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"{LAB_PREFIX}:ind:{symbol}:{tf}:{param_name}"

# 🔸 Обработка одной пары (symbol, tf) на вибранном open_ms
async def _process_pair(
    redis,
    symbol: str,
    tf: str,
    open_ms: int,
    precision: int,
    get_instances_by_tf,
) -> Tuple[int, int]:
    """
    Возвращает (published_params, skipped_params).
    skipped включает случаи: нет DF / нет инстансов / пустой расчёт.
    """
    # загрузка OHLCV (до 800 баров к open_ms)
    df = await load_ohlcv_df(redis, symbol, tf, open_ms, bars=800)
    if df is None or df.empty:
        return (0, 1)

    # список инстансов по TF (enabled=true уже в кеше)
    instances = get_instances_by_tf(tf)
    if not instances:
        return (0, 1)

    published = 0
    skipped = 0

    # последовательно по инстансам (DF один и тот же)
    for inst in instances:
        try:
            values = await compute_snapshot_values_async(inst, symbol, df, precision)
        except Exception as e:
            log.warning("calc error %s/%s id=%s: %s", symbol, tf, inst.get("id"), e)
            skipped += 1
            continue

        if not values:
            skipped += 1
            continue

        # публикация KV с TTL (45 сек)
        tasks = []
        for param_name, str_value in values.items():
            key = _ind_key(symbol, tf, param_name)
            # Redis хранит строки — формат уже сделан в compute_only
            tasks.append(redis.set(key, str_value, ex=LAB_TTL_SEC))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            published += len(tasks)

    return (published, skipped)

# 🔸 Основной воркер: каждые N секунд публикует IND-снапшоты по всем активным тикерам и TF
async def run_lab_ind_live(
    pg,
    redis,
    get_instances_by_tf,     # callable(tf) -> list[instance]
    get_precision,           # callable(symbol) -> int|None
    get_active_symbols,      # callable() -> list[str]
    get_last_bar,            # callable(symbol, tf) -> int|None
    tf_set: Tuple[str, ...] = TF_SET,
    tick_interval_sec: int = TICK_INTERVAL_SEC,
):
    # семафор на (symbol, tf)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    while True:
        t0 = time.monotonic()
        now_ms = int(time.time() * 1000)

        symbols = get_active_symbols()
        if not symbols:
            await asyncio.sleep(tick_interval_sec)
            continue

        # агрегаторы
        total_pairs = 0
        total_published = 0
        total_skipped = 0

        async def run_one(sym: str, tf: str):
            nonlocal total_published, total_skipped, total_pairs
            async with sem:
                # выбор open_ms: закрытый бар из кеша (предпочтительно) или текущий floored
                last = get_last_bar(sym, tf)
                open_ms = last if last is not None else floor_to_bar(now_ms, tf)
                # точность по символу (дефолт 8)
                prec = get_precision(sym) or 8
                try:
                    pub, sk = await _process_pair(redis, sym, tf, open_ms, prec, get_instances_by_tf)
                except Exception as e:
                    log.warning("pair error %s/%s: %s", sym, tf, e)
                    pub, sk = 0, 1
                total_pairs += 1
                total_published += pub
                total_skipped += sk

        # запускаем задачи по всем (symbol, tf)
        tasks = [
            asyncio.create_task(run_one(sym, tf))
            for sym in symbols
            for tf in tf_set
        ]
        # дожидаемся завершения всех
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=False)

        t1 = time.monotonic()
        elapsed_ms = int((t1 - t0) * 1000)

        # итоговый лог тика
        log.info(
            "LAB IND: tick done pairs=%d params=%d skipped=%d elapsed_ms=%d",
            total_pairs, total_published, total_skipped, elapsed_ms
        )

        # пауза до следующего тика
        # (на случай долгой итерации — простой sleep без учёта перерасхода)
        await asyncio.sleep(tick_interval_sec)