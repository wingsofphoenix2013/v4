# generate_all_snapshot_variants.py

import asyncio
import logging
import itertools

import infra

log = logging.getLogger("SNAPSHOT_DICT_GENERATOR")
logging.basicConfig(level=logging.INFO)

ELEMENTS = ["EMA9", "EMA21", "EMA50", "EMA100", "EMA200", "PRICE"]


# 🔸 Генерация всех разбиений множества (рекурсивно)
def all_partitions(collection):
    if len(collection) == 1:
        yield [collection]
        return

    first = collection[0]
    for smaller in all_partitions(collection[1:]):
        for n, subset in enumerate(smaller):
            yield smaller[:n] + [[first] + subset] + smaller[n+1:]
        yield [[first]] + smaller


# 🔸 Построение всех вариантов ordering с сортировкой внутри групп
def generate_all_orderings():
    unique_orderings = set()

    for partition in all_partitions(ELEMENTS):
        for permuted_groups in itertools.permutations(partition):
            ordered = []
            for group in permuted_groups:
                group_sorted = sorted(group, key=lambda x: (999 if x == "PRICE" else int(x.replace("EMA", ""))))
                ordered.append("=".join(group_sorted))
            ordering_str = " > ".join(ordered)
            unique_orderings.add(ordering_str)

    return unique_orderings


# 🔸 Основной запуск
async def run_generate_all_snapshots():
    await infra.setup_pg()
    log.info("🚀 Генерация всех возможных snapshot-комбинаций")

    insert_query = """
        INSERT INTO oracle_emasnapshot_dict (ordering)
        VALUES ($1)
        ON CONFLICT (ordering) DO NOTHING
    """

    all_orderings = generate_all_orderings()
    log.info(f"🔢 Сгенерировано уникальных комбинаций: {len(all_orderings)}")

    async with infra.pg_pool.acquire() as conn:
        for ordering in sorted(all_orderings):
            await conn.execute(insert_query, ordering)
            log.debug(f"➕ Добавлено: {ordering}")

    log.info(f"✅ Готово: {len(all_orderings)} строк добавлено в oracle_emasnapshot_dict")