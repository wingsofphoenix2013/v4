# position_emapattern_worker.py — разовая генерация каталога паттернов EMA/PRICE

import asyncio
import logging
import json
from typing import List, Set

log = logging.getLogger("IND_EMA_PATTERN_DICT")

EMA_LENGTHS = [9, 21, 50, 100, 200]
TOKENS = ["PRICE"] + [f"EMA{l}" for l in EMA_LENGTHS]
IDLE_SLEEP_SEC = 10800


# 🔸 Канонизация группы равенства (PRICE слева, EMA по возрастанию длины)
def _canonicalize_group(group: Set[str]) -> List[str]:
    ordered = []
    if "PRICE" in group:
        ordered.append("PRICE")
    emas = [t for t in group if t.startswith("EMA")]
    emas_sorted = sorted(emas, key=lambda t: int(t[3:]))
    ordered.extend(emas_sorted)
    return ordered


# 🔸 Сборка канонической текстовой строки паттерна
def _pattern_text_from_blocks(blocks: List[Set[str]]) -> str:
    parts = []
    for blk in blocks:
        inner = " = ".join(_canonicalize_group(blk))  # равные значения через '='
        parts.append(inner)
    return " > ".join(parts)  # группы по убыванию через ' > '


# 🔸 Сборка машинной формы паттерна (json-ready)
def _pattern_json_from_blocks(blocks: List[Set[str]]) -> List[List[str]]:
    return [_canonicalize_group(blk) for blk in blocks]


# 🔸 Генерация всех упорядоченных разбиений токенов (weak orderings)
def _generate_ordered_partitions(items: List[str]) -> List[List[Set[str]]]:
    results: List[List[Set[str]]] = []

    def rec(idx: int, blocks: List[Set[str]]):
        if idx == len(items):
            results.append([set(b) for b in blocks])  # копия текущего решения
            return

        token = items[idx]

        # вариант: открыть новый блок справа
        rec(idx + 1, blocks + [set([token])])

        # вариант: присоединить к одному из существующих блоков (сделать равным)
        for i in range(len(blocks)):
            blocks[i].add(token)
            rec(idx + 1, blocks)
            blocks[i].remove(token)

    rec(0, [])
    return results


# 🔸 Вставка паттернов пачкой в БД
async def _insert_patterns(pg, rows: List[tuple]):
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


# 🔸 Полная генерация каталога и запись в БД (идемпотентно)
async def _generate_and_store_catalog(pg):
    log.info("Старт генерации каталога паттернов EMA/PRICE")

    # быстрый выход, если таблица уже наполнена
    async with pg.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM indicator_emapattern_dict")
    if count and int(count) > 0:
        log.info(f"Словарь уже содержит {count} записей — генерация пропущена")
        return

    # генерация всех упорядоченных разбиений 6 токенов
    blocks_list = _generate_ordered_partitions(TOKENS)

    # построение строковой и json-формы; защита от дубликатов
    seen = set()
    rows: List[tuple] = []
    for blocks in blocks_list:
        ptext = _pattern_text_from_blocks(blocks)
        if ptext in seen:
            continue
        seen.add(ptext)
        pjson = json.dumps(_pattern_json_from_blocks(blocks), ensure_ascii=False)
        rows.append((ptext, pjson))

    # вставка пачками
    BATCH = 2000
    total = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        await _insert_patterns(pg, chunk)
        total += len(chunk)
        log.debug(f"Вставлено {total}/{len(rows)} записей")

    # финальная проверка числа строк
    async with pg.acquire() as conn:
        final_count = await conn.fetchval("SELECT COUNT(*) FROM indicator_emapattern_dict")
    log.info(f"Генерация завершена: {final_count} записей в словаре")


# 🔸 Точка входа воркера для run_safe_loop
async def run_position_emapattern_worker(pg, redis):
    try:
        await _generate_and_store_catalog(pg)
    except Exception:
        log.exception("Ошибка генерации словаря паттернов")
    # удерживаем воркер «живым», чтобы не было бесконечных перезапусков
    while True:
        await asyncio.sleep(IDLE_SLEEP_SEC)