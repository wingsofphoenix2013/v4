# position_emapattern_worker.py — генерация всех total preorders для EMA/PRICE (4683 вариантов)

import asyncio
import logging
import json
from typing import List, Set, Tuple
from itertools import permutations

log = logging.getLogger("IND_EMA_PATTERN_DICT")

EMA_LENGTHS = [9, 21, 50, 100, 200]
TOKENS = ["PRICE"] + [f"EMA{l}" for l in EMA_LENGTHS]
IDLE_SLEEP_SEC = 3600
BATCH = 2000


# 🔸 Канонизация порядка внутри группы равенства
def _canonicalize_group(group: Set[str]) -> List[str]:
    ordered: List[str] = []
    if "PRICE" in group:
        ordered.append("PRICE")
    emas = [t for t in group if t.startswith("EMA")]
    emas_sorted = sorted(emas, key=lambda t: int(t[3:]))
    if "PRICE" not in group:
        ordered = emas_sorted
    else:
        ordered.extend(emas_sorted)
    return ordered


# 🔸 Сборка текстовой формы паттерна
def _pattern_text_from_blocks(blocks: List[Set[str]]) -> str:
    parts = []
    for blk in blocks:
        inner = " = ".join(_canonicalize_group(blk))
        parts.append(inner)
    return " > ".join(parts)


# 🔸 Сборка json-формы паттерна
def _pattern_json_from_blocks(blocks: List[Set[str]]) -> List[List[str]]:
    return [_canonicalize_group(blk) for blk in blocks]


# 🔸 Генерация всех разбиений множества (Stirling S(n,k)) с помощью RGS
def _generate_set_partitions(items: List[str]) -> List[List[Set[str]]]:
    n = len(items)
    if n == 0:
        return [[]]
    results: List[List[Set[str]]] = []
    rgs = [0] * n

    def backtrack(i: int, max_label: int):
        if i == n:
            k = max_label + 1
            blocks = [set() for _ in range(k)]
            for idx, lbl in enumerate(rgs):
                blocks[lbl].add(items[idx])
            results.append(blocks)
            return
        for lbl in range(max_label + 2):
            rgs[i] = lbl
            if lbl == max_label + 1:
                backtrack(i + 1, max_label + 1)
            else:
                backtrack(i + 1, max_label)

    backtrack(0, 0)
    return results


# 🔸 Генерация всех упорядоченных разбиений (total preorders): перестановки блоков для каждого разбиения
def _generate_ordered_partitions(items: List[str]) -> List[List[Set[str]]]:
    ordered: List[List[Set[str]]] = []
    partitions = _generate_set_partitions(items)
    for blocks in partitions:
        k = len(blocks)
        if k == 0:
            continue
        for perm in permutations(range(k)):
            ordered.append([blocks[i] for i in perm])
    return ordered


# 🔸 Вставка паттернов пачкой в БД
async def _insert_patterns(pg, rows: List[Tuple[str, str]]):
    if not rows:
        return
    async with pg.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO indicator_emapattern_dict (pattern_text, pattern_json)
                VALUES ($1, $2::jsonb)
                ON CONFLICT (pattern_text) DO NOTHING
                """,
                rows,
            )


# 🔸 Полная генерация каталога и запись в БД
async def _generate_and_store_catalog(pg):
    log.info("Старт генерации полного каталога EMA/PRICE (total preorders)")

    # быстрый выход, если таблица уже содержит данные
    async with pg.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM indicator_emapattern_dict")
    if count and int(count) > 0:
        log.info(f"Словарь уже содержит {count} записей — генерация пропущена")
        return

    # генерация всех упорядоченных разбиений
    opartitions = _generate_ordered_partitions(TOKENS)

    # построение строковой и json-формы, защита от дубликатов
    seen = set()
    rows: List[Tuple[str, str]] = []
    for blocks in opartitions:
        ptext = _pattern_text_from_blocks(blocks)
        if ptext in seen:
            continue
        seen.add(ptext)
        pjson = json.dumps(_pattern_json_from_blocks(blocks), ensure_ascii=False)
        rows.append((ptext, pjson))

    # ожидание 4683 уникальных паттернов
    log.debug(f"Сгенерировано уникальных паттернов: {len(rows)}")

    # вставка пачками
    total = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        await _insert_patterns(pg, chunk)
        total += len(chunk)
        log.debug(f"Вставлено {total}/{len(rows)} записей")

    # финальная проверка
    async with pg.acquire() as conn:
        final_count = await conn.fetchval("SELECT COUNT(*) FROM indicator_emapattern_dict")
    log.info(f"Генерация завершена: {final_count} записей в словаре")


# 🔸 Точка входа воркера для run_safe_loop
async def run_position_emapattern_worker(pg, redis):
    try:
        await _generate_and_store_catalog(pg)
    except Exception:
        log.exception("Ошибка генерации словаря паттернов")
    while True:
        await asyncio.sleep(IDLE_SLEEP_SEC)