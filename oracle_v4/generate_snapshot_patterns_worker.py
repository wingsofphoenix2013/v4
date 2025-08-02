# generate_snapshot_patterns_worker.py

import asyncio
import logging
import itertools

import infra

log = logging.getLogger("PATTERN_GENERATOR")
logging.basicConfig(level=logging.INFO)

ELEMENTS = ["PRICE", "EMA9", "EMA21", "EMA50", "EMA100", "EMA200"]
EPSILON = 0.0005  # условный порог — не нужен здесь, т.к. симуляция всех вариантов


# 🔸 Сортировка внутри группы (EMA по числу, PRICE — в конец)
def sort_key(x):
    if x == "PRICE":
        return 999
    return int(x.replace("EMA", ""))


# 🔸 Все упорядоченные разбиения множества (с генерацией слипания)
def generate_structural_patterns(trio):
    from itertools import permutations

    results = set()

    # Все возможные разбиения на 1, 2, 3 групп
    # Группы: [[A,B,C]], [[A,B], [C]], [[A], [B,C]], [[A],[B],[C]]
    partitions = []

    # Одной группой
    partitions.append([trio])

    # Две группы (A+B)+C
    for i in range(3):
        group1 = [trio[i], trio[(i+1)%3]]
        group2 = [trio[(i+2)%3]]
        partitions.append([group1, group2])
        partitions.append([group2, group1])

    # Три отдельных группы
    for perm in permutations(trio):
        partitions.append([[perm[0]], [perm[1]], [perm[2]]])

    # Построим строки с сортировкой внутри групп
    for part in partitions:
        groups = ["=".join(sorted(group, key=sort_key)) for group in part]
        results.add(" > ".join(groups))

    return results


# 🔸 Основная функция
async def run_generate_snapshot_patterns():
    await infra.setup_pg()
    log.info("🚀 Генерация всех уникальных EMA паттернов (3 из 6 с слипанием)")

    insert_query = """
        INSERT INTO oracle_emasnapshot_pattern (pattern)
        VALUES ($1)
        ON CONFLICT (pattern) DO NOTHING
    """

    all_patterns = set()
    combos = list(itertools.combinations(ELEMENTS, 3))

    for trio in combos:
        patterns = generate_structural_patterns(trio)
        all_patterns.update(patterns)

    log.info(f"🔢 Всего уникальных паттернов: {len(all_patterns)}")

    async with infra.pg_pool.acquire() as conn:
        for pattern in sorted(all_patterns):
            await conn.execute(insert_query, pattern)
            log.debug(f"➕ {pattern}")

    log.info(f"✅ Готово: {len(all_patterns)} строк добавлено в oracle_emasnapshot_pattern")