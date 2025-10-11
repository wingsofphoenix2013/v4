# laboratory_ind_live.py — воркер laboratory_v4: IND-снапшоты (публикация в Redis и обновление in-memory кеша)

# 🔸 Импорты
import asyncio
import logging
import time
from typing import Dict, Tuple

from lab_utils import floor_to_bar, load_ohlcv_df
from compute_only import compute_snapshot_values_async
from laboratory_config import put_live_values  # in-memory кеш live-значений

# 🔸 Логгер
log = logging.getLogger("LAB_IND_LIVE")

# 🔸 Константы воркера
TF_SET = ("m5", "m15", "h1")          # работаем только с этими TF
LAB_PREFIX = "lab_live"               # префикс пространства лаборатории
LAB_TTL_SEC = 60                      # TTL для всех lab_live ключей
TICK_INTERVAL_SEC = 15                # период тика публикации
MAX_CONCURRENCY = 64                  # параллельные (symbol, tf)
INST_CONCURRENCY = 8                  # лёгкий параллелизм по инстансам внутри одной пары

# 🔸 Путь KV-ключа для IND
def _ind_key(symbol: str, tf: str, param_name: str) -> str:
    return f"{LAB_PREFIX}:ind:{symbol}:{tf}:{param_name}"

# 🔸 Обработка одной пары (symbol, tf) на выбранном open_ms
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
    skipped включает случаи: нет DF / нет инстансов / пустой расчёт / ошибки.
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

    # параллелизм по инстансам ограничиваем локальным семафором
    inst_sem = asyncio.Semaphore(INST_CONCURRENCY)

    async def _compute_and_publish(inst: Dict) -> Tuple[int, int]:
        # параллельная обработка одного инстанса с ограничением
        async with inst_sem:
            try:
                values = await compute_snapshot_values_async(inst, symbol, df, precision)
            except Exception as e:
                log.warning("calc error %s/%s id=%s: %s", symbol, tf, inst.get("id"), e)
                return (0, 1)

            if not values:
                return (0, 1)

            # обновляем in-memory live-кеш для текущего бара
            put_live_values(symbol, tf, open_ms, values)

            # публикация KV с TTL
            tasks = []
            for param_name, str_value in values.items():
                key = _ind_key(symbol, tf, param_name)
                tasks.append(redis.set(key, str_value, ex=LAB_TTL_SEC))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                return (len(tasks), 0)
            return (0, 0)

    # запускаем параллельно все инстансы данной пары
    results = await asyncio.gather(
        *[asyncio.create_task(_compute_and_publish(inst)) for inst in instances],
        return_exceptions=False,
    )

    # агрегируем результат
    for pub, sk in results:
        published += pub
        skipped += sk

    return (published, skipped)

# 🔸 Одиночный тик: выполнить один проход по указанным TF (без задержки/цикла)
async def tick_ind(
    pg,
    redis,
    get_instances_by_tf,     # callable(tf) -> list[instance]
    get_precision,           # callable(symbol) -> int|None
    get_active_symbols,      # callable() -> list[str]
    get_last_bar,            # callable(symbol, tf) -> int|None
    tf_set: Tuple[str, ...] = TF_SET,
) -> Tuple[int, int, int, int]:
    """
    Выполнить один IND-проход по всем символам и tf_set.
    Возвращает агрегаты: (pairs, published_params, skipped, elapsed_ms).
    """
    t0 = time.monotonic()
    now_ms = int(time.time() * 1000)

    symbols = get_active_symbols()
    if not symbols:
        return (0, 0, 0, int((time.monotonic() - t0) * 1000))

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

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
                pub, sk = await _process_pair(redis, sym, tf, open_ms, prec, get_instances_by_tf)
            except Exception as e:
                log.warning("pair error %s/%s: %s", sym, tf, e)
                pub, sk = 0, 1
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
    while True:
        pairs, published, skipped, elapsed_ms = await tick_ind(
            pg=pg,
            redis=redis,
            get_instances_by_tf=get_instances_by_tf,
            get_precision=get_precision,
            get_active_symbols=get_active_symbols,
            get_last_bar=get_last_bar,
            tf_set=tf_set,
        )

        log.info(
            "LAB IND: tick done tf=%s pairs=%d params=%d skipped=%d elapsed_ms=%d",
            ",".join(tf_set), pairs, published, skipped, elapsed_ms
        )

        await asyncio.sleep(tick_interval_sec)